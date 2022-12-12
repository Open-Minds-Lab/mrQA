import math
import os
import subprocess
import warnings
from pathlib import Path
from typing import Iterable

from MRdataset.base import save_mr_dataset
from MRdataset.config import MRDS_EXT
from MRdataset.utils import random_name, valid_dirs

from mrQA.run_subset import read_subset
from mrQA.utils import execute_local, list2txt, txt2list, split_index


def parallel_dataset(data_root=None,
                     style='dicom',
                     name=None,
                     reindex=False,
                     include_phantom=False,
                     verbose=False,
                     output_dir=None,
                     debug=False,
                     subjects_per_job=None,
                     submit_job=False,
                     hpc=False,
                     conda_dist=None,
                     conda_env=None):
    # It is not possible to submit jobs while debugging, why would you submit
    # a job, if code is still being debugged
    if debug and submit_job:
        raise AttributeError('Cannot debug when submitting jobs')
    if style != 'dicom':
        raise NotImplementedError(f'Expects dicom, Got {style}')

    # Check if dataroot is a valid directory, or list of valid directories
    data_root = valid_dirs(data_root)

    # Check if output_dir was provided.
    # RULE : If not, it will be saved in 'mrqa_files'
    # created in the parent folder of data_root
    if not output_dir:
        if isinstance(data_root, Iterable):
            # If dataroot is a bunch of directories, the above RULE cannot
            # be followed, just pass a directory to save the file.
            raise RuntimeError("Need an output directory to store files")

        # Didn't find a good alternative in pathlib, please raise a issue if
        # you know one, happy to incorporate
        output_dir = data_root.parent / 'mrqa_files'
        if not os.access(output_dir, os.F_OK):
            if os.access(output_dir, os.W_OK):
                warnings.warn('Expected a directory to save job scripts. Using '
                              'parent folder of --data_root instead.')
                output_dir.mkdir(exist_ok=True)
            else:
                raise PermissionError(f'You do not have write permission to'
                                      f'create a folder in {data_root.parent}'
                                      f'Please provide output_dir')
        else:
            warnings.warn('Expected a directory to save job scripts. Using '
                          'parent folder of --data_root instead.')

    if not Path(output_dir).is_dir():
        # If the output_dir argument doesn't exist, or it is not a directory
        # Need not check permissions, because this path is supplied by the user
        # The user should check himself.
        output_dir.mkdir(exist_ok=True, parents=True)
    output_dir = Path(output_dir).resolve()

    # Information about conda env is required for creating slurm scripts
    # The snippet below sets some defaults, may not be true for everyone.
    # The user can use the arguments to specify
    if not conda_env:
        conda_env = 'mrqa' if hpc else 'mrcheck'
    if not conda_dist:
        conda_dist = 'miniconda3' if hpc else 'anaconda3'

    # # TODO: Add the name flag to parser arguments
    # if name is None:
    #     warnings.warn(
    #         'Expected a unique identifier for caching data. Got NoneType. '
    #         'Using a random name. Use --name flag for persistent metadata',
    #         stacklevel=2)
    #     name = random_name()

    # Create a folder id_lists for storing list of subject ids for each job
    # in a separate txt file. The files are saved as batch0000.txt,
    # batch0001.txt etc. And store the original complete list (contains all
    # subject ids) in "id_complete_list.txt"
    id_folder = output_dir / 'id_lists'
    id_folder.mkdir(parents=True, exist_ok=True)
    all_batches_ids_filepath = output_dir / 'id_complete_list.txt'
    ids_path_list = create_index(
        data_root=data_root,
        output_path=all_batches_ids_filepath,
        output_dir=id_folder,
        reindex=reindex,
        subjects_per_job=subjects_per_job)

    # Create folder to save slurm scripts
    scripts_folder = output_dir / 'bash_scripts'
    scripts_folder.mkdir(parents=True, exist_ok=True)
    # Create a text file to point to all the scripts that were generated
    all_batches_scripts_filepath = output_dir / 'scripts_complete_list.txt'
    # Create a folder to save partial mrds pickle files
    partial_mrds_folder = output_dir / 'partial_mrds'
    partial_mrds_folder.mkdir(parents=True, exist_ok=True)
    # Create a text file to point to all the partial mrds pickle files
    # which were created
    all_batches_mrds_filepath = output_dir / 'partial_mrds_paths.txt'

    scripts_path_list = []
    mrds_path_list = []
    processes = []
    # create a slurm job script for each sub_group of subject ids
    for ids_filepath in ids_path_list:
        # Filename of the bash script should be same as text file.
        # Say batch0000.txt points to set of 10 subjects. Then create a
        # slurm script file, batch0000.sh which will run for these 10 subjects,
        # and the final partial mrds pickle file will have the name
        # batch0000.mrds.pkl
        script_filepath = scripts_folder / (ids_filepath.stem + '.sh')
        partial_mrds_filepath = partial_mrds_folder / (
                ids_filepath.stem + MRDS_EXT)

        # Keep storing the filenames. The entire list would be saved at the end
        scripts_path_list.append(script_filepath)
        mrds_path_list.append(partial_mrds_filepath)

        # Finally create the slurm script and save to disk
        create_slurm_script(filename=script_filepath,
                            ids_filepath=ids_filepath,
                            env=conda_env,
                            conda_dist=conda_dist,
                            num_subj_per_job=subjects_per_job,
                            reindex=reindex,
                            verbose=verbose,
                            include_phantom=include_phantom,
                            partial_mrds_filename=partial_mrds_filepath)
        # Run the script file
        output = run_single(debug=debug,
                            txt_filepath=ids_filepath,
                            reindex=reindex,
                            verbose=verbose,
                            include_phantom=include_phantom,
                            s_filename=script_filepath,
                            submit_job=submit_job,
                            hpc=hpc,
                            partial_mrds_filename=partial_mrds_filepath)
        # Keep track of processes started, if running locally. Useful to
        # keep python script active until all scripts have completed
        # execution. Not useful if running in debug mode or using hpc
        processes.append(output)
    # Finally, save the all the paths to create mrds pickle files and all the
    # paths to generated scripts in a text file for reference.
    list2txt(path=all_batches_mrds_filepath, list_=mrds_path_list)
    list2txt(path=all_batches_scripts_filepath, list_=scripts_path_list)

    # Wait only if executing locally
    if not (submit_job or debug or hpc):
        exit_codes = [p.wait() for p in processes]
    return


def run_single(debug, txt_filepath, reindex, verbose,
               include_phantom, s_filename, submit_job, hpc=False,
               partial_mrds_filename=None):
    # submit job or run with bash or execute with python
    if debug:
        partial_dataset = read_subset(partial_mrds_filename,
                                      txt_filepath, 'dicom',
                                      reindex, verbose,
                                      include_phantom)
        # partial_dataset.set_cache_path()
        partial_dataset.is_complete = False
        # save_filename = txt_filepath.with_suffix(MRDS_EXT)
        save_mr_dataset(partial_mrds_filename, partial_dataset)
        return None
    elif not partial_mrds_filename.exists() or reindex:
        if not hpc:
            # print('Started job')
            return execute_local(s_filename)
        if hpc and submit_job:
            subprocess.call(['sbatch', s_filename])
            return


def create_slurm_script(filename, ids_filepath, env='mrqa',
                        conda_dist='anaconda3',
                        num_subj_per_job=50, reindex=False, verbose=False,
                        include_phantom=False, partial_mrds_filename=None):
    # Memory and CPU time :  typical usage observed locally

    # For subjects_per_job = 50
    # Max RSS Size (Memory) ~150 MB,
    # Sys Time (CPU Time) : 10 minutes

    # For subjects_per_job = 100
    # Max RSS Size (Memory) ~160 MB,
    # Sys Time (CPU Time) : 20 minutes

    mem_reqd = 2000  # MB;
    num_mins_per_subject = 1  # minutes
    num_hours = int(math.ceil(num_subj_per_job * num_mins_per_subject / 60))
    time_limit = 3 if num_hours < 3 else num_hours
    python_cmd = f'mrpc_subset -o {partial_mrds_filename} -b {ids_filepath}'

    if reindex:
        python_cmd += ' --reindex'
    if verbose:
        python_cmd += ' --verbose'
    if include_phantom:
        python_cmd += ' --include_phantom'

    with open(filename, 'w') as fp:
        fp.writelines("\n".join([
            '#!/bin/bash',
            '#SBATCH -A med220005p',
            '#SBATCH -N 1',
            '#SBATCH -p RM-shared',
            f'#SBATCH --mem-per-cpu={mem_reqd}M #memory per cpu-core',
            f'#SBATCH --time={time_limit}:00:00',
            '#SBATCH --ntasks-per-node=1',
            f'#SBATCH --error={ids_filepath.stem}.%J.err',
            f'#SBATCH --output={ids_filepath.stem}.%J.out',
            '#SBATCH --mail-type=end          # send email when job ends',
            '#SBATCH --mail-type=fail         # send email if job fails',
            '#SBATCH --mail-user=mail.sinha.harsh@gmail.com',
            '#Clear the environment from any previously loaded modules',
            'module purge > /dev/null 2>&1',
            f'source  ${{HOME}}/{conda_dist}/etc/profile.d/conda.sh',
            f'conda activate {env}',
            python_cmd,
            'date\n',
        ])
        )


def create_index(data_root, output_path, output_dir, reindex=False,
                 subjects_per_job=50):
    batch_ids_path_list = []
    if output_path.exists() and not reindex:
        warnings.warn(f"Found a  pre-existing list of subjects on disk."
                      f"Reusing existing {output_path}, Use reindex?",
                      stacklevel=2)
        dir_index = txt2list(output_path)
    else:
        dir_index = []
        for root, dirs, files in os.walk(data_root):
            if 'sub-' in Path(root).name:
                dir_index.append(root)
        list2txt(output_path, list(set(dir_index)))

    if subjects_per_job < 1:
        raise RuntimeError("subjects_per_job cannot be less than 1.")
    elif subjects_per_job > len(dir_index):
        raise RuntimeError("Trying to create more jobs than total number of "
                           "subjects in the directory. Why?")
    workers = len(dir_index) // subjects_per_job
    if workers == 1:
        raise RuntimeError("Decrease number of subjects per job. Expected"
                           "workers > 1 for parallel processing. Got 1")
    index_subsets = split_index(dir_index, num_chunks=workers)
    output_dir.mkdir(exist_ok=True, parents=True)

    for i, subset in enumerate(index_subsets):
        batch_filename = output_dir/f'batch{i:04}.txt'
        list2txt(batch_filename, subset)
        batch_ids_path_list.append(batch_filename)
    return batch_ids_path_list
