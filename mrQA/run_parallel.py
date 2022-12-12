import math
import os
import subprocess
import warnings
from pathlib import Path
from typing import Iterable, Union, Optional

from MRdataset.base import save_mr_dataset
from MRdataset.config import MRDS_EXT
from MRdataset.utils import random_name, valid_dirs

from mrQA.run_subset import read_subset
from mrQA.utils import execute_local, list2txt, txt2list, split_index, \
    is_integer_number


def parallel_dataset(data_root: Union[str, Iterable] = None,
                     style: str = 'dicom',
                     reindex: bool = False,
                     include_phantom: bool = False,
                     verbose: bool = False,
                     output_dir: str = None,
                     debug: bool = False,
                     subjects_per_job: int = None,
                     submit_job: bool = False,
                     hpc: bool = False,
                     conda_dist: str = None,
                     conda_env: str = None) -> None:
    """
    Given a folder(or List[folder]) it will divide the work into smaller
    jobs. Each job will contain a fixed number of subjects. These jobs can be
    executed in parallel to save time.

    Parameters
    ----------
    data_root: str or List[str]
        /path/to/my/dataset containing files
    style: str
        Specify dataset type. Use one of [dicom]
    reindex: bool
        Similar to --no-cache. rejects all cached files and rebuilds the dataset
    include_phantom: bool
        Include phantom scans in the dataset
    verbose: bool
        Print progress
    output_dir: str
        Path to save the output dataset
    debug: bool
        If True, the dataset will be created locally. This is useful for testing
    subjects_per_job: int
        Number of subjects per job. Recommended value is 50 or 100
    submit_job: bool
        If True, the scripts will be executed
    hpc: bool
        If True, the scripts will be generated for HPC, not for local execution
    conda_dist: str
        Name of conda distribution
    conda_env: str
        Name of conda environment

    Returns
    -------
    None
    """

    # It is not possible to submit jobs while debugging, why would you submit
    # a job, if code is still being debugged
    if debug and submit_job:
        raise AttributeError('Cannot debug when submitting jobs')
    if style != 'dicom':
        raise NotImplementedError(f'Expects dicom, Got {style}')
    if not is_integer_number(subjects_per_job):
        raise RuntimeError('Expects an integer value for subjects per job.'
                           f'Got {subjects_per_job}')
    if subjects_per_job < 1:
        raise RuntimeError('subjects_per_job cannot be less than 1')

    # Check if data_root is a valid directory, or list of valid directories
    data_root = valid_dirs(data_root)

    # Check if output_dir was provided.
    # RULE : If not, it will be saved in 'mrqa_files'
    # created in the parent folder of data_root
    if not output_dir:
        if isinstance(data_root, Iterable):
            # If data_root is a bunch of directories, the above RULE cannot
            # be followed, just pass a directory to save the file.
            raise RuntimeError("Need an output directory to store files")

        # Didn't find a good alternative to os.access
        # in pathlib, please raise a issue if
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


def run_single(debug: bool,
               txt_filepath: str,
               reindex: bool,
               verbose: bool,
               include_phantom: bool,
               s_filename: str,
               submit_job: bool,
               hpc: bool,
               partial_mrds_filename: Union[str, Path]) -> Optional[subprocess.Popen]:
    """
    Runs a single script file either locally or on hpc.

    Parameters
    ----------
    debug : bool
        If True, runs the script in debug mode
    txt_filepath : str
        Path to text file containing list of subject ids
    reindex: bool
        If True, reject cache and reindex
    verbose: bool
        If True, prints the output of the script
    include_phantom: bool
        If True, includes phantom, localizer and calibration studies
    s_filename: str
        Path to slurm script file
    submit_job: bool
        If True, executes the script either locally or on hpc
    hpc: bool
        If True, runs the slurm script on a hpc
    partial_mrds_filename: str
        Path to the partial mrds pickle file

    Returns
    -------

    """
    # If debug mode, run the script in debug mode
    # The debug mode is used for local testing
    # and not for running on a hpc
    if isinstance(partial_mrds_filename, str):
        partial_mrds_filename = Path(partial_mrds_filename)
    if debug:
        # Run the script in debug mode
        partial_dataset = read_subset(partial_mrds_filename,
                                      txt_filepath, 'dicom',
                                      reindex, verbose,
                                      include_phantom)
        # Save the partial mrds pickle file
        partial_dataset.is_complete = False
        save_mr_dataset(partial_mrds_filename, partial_dataset)
        return None
    elif not partial_mrds_filename.exists() or reindex:
        if not hpc:
            # If running locally, and the user does not want to
            # submit the job, run the script using the bash command
            return execute_local(s_filename)
        if hpc and submit_job:
            # If running on a hpc, use the sbatch command
            # to submit the script
            subprocess.call(['sbatch', s_filename])
            return


def create_slurm_script(filename: Union[str, Path],
                        ids_filepath: Union[str, Path],
                        env: str = 'mrqa',
                        conda_dist: str = 'anaconda3',
                        num_subj_per_job: int = 50,
                        reindex: bool = False,
                        verbose: bool = False,
                        include_phantom: bool = False,
                        partial_mrds_filename: bool = None):
    """
    Creates a slurm script file which can be submitted to a hpc.

    Parameters
    ----------
    filename : str
        Path to slurm script file
    ids_filepath : str
        Path to text file containing list of subject ids
    env : str
        Conda environment name
    conda_dist : str
        Conda distribution
    num_subj_per_job : int
        Number of subjects to process in each slurm job
    reindex : bool
        If True, reject cache and reindex
    verbose : bool
        If True, prints the output of the script
    include_phantom : bool
        If True, includes phantom, localizer and calibration studies
    partial_mrds_filename : str
        Path to the partial mrds pickle file

    Returns
    -------
    None
    """

    # Memory and CPU time :  typical usage observed locally

    # For subjects_per_job = 50
    # Max RSS Size (Memory) ~150 MB,
    # Sys Time (CPU Time) : 10 minutes

    # For subjects_per_job = 100
    # Max RSS Size (Memory) ~160 MB,
    # Sys Time (CPU Time) : 20 minutes

    # Set the memory and cpu time limits
    mem_reqd = 2000  # MB;
    num_mins_per_subject = 1  # minutes
    num_hours = int(math.ceil(num_subj_per_job * num_mins_per_subject / 60))
    # Set the number of hours to 3 if less than 3
    time_limit = 3 if num_hours < 3 else num_hours
    # Setup python command to run
    python_cmd = f'mrpc_subset -o {partial_mrds_filename} -b {ids_filepath}'

    # Add flags to python command
    if reindex:
        python_cmd += ' --reindex'
    if verbose:
        python_cmd += ' --verbose'
    if include_phantom:
        python_cmd += ' --include_phantom'

    # Create the slurm script file
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
    """
    Splits a given set of subjects into multiple jobs and creates separate
    text files containing the list of subjects. Each text file
    contains the list of subjects to be processed in a single job.

    Parameters
    ----------
    data_root : str
        Path to the root directory of the data
    output_path : str
        Path to the output directory
    output_dir : str
        Name of the output directory
    reindex : bool
        If True, reject cache and reindex
    subjects_per_job : int
        Number of subjects to process in each job

    Returns
    -------
    batch_ids_path_list : list
        List of paths to the text files containing the list of subjects
    """

    # Create the output path
    output_path = Path(output_path)
    # List of paths to the text files containing the list of subjects for
    # each job
    batch_ids_path_list = []

    subject_list = []
    # Get the list of subject ids
    for root, dirs, files in os.walk(data_root):
        if 'sub-' in Path(root).name:
            # Get the subject id
            subject_list.append(root)
    # Store the list of unique subject ids
    list2txt(output_path, list(set(subject_list)))

    if subjects_per_job < 1:
        # If subjects_per_job is less than 1, process all subjects in a single
        # job
        raise RuntimeError("subjects_per_job cannot be less than 1.")
    elif subjects_per_job > len(subject_list):
        # If subjects_per_job is greater than the number of subjects,
        # process all subjects in a single job
        raise RuntimeError("Trying to create more jobs than total number of "
                           "subjects in the directory. Why?")
    # Get the number of jobs
    workers = len(subject_list) // subjects_per_job
    if workers == 1:
        # If there is only one job, process all subjects in a single job
        raise RuntimeError("Decrease number of subjects per job. Expected"
                           "workers > 1 for parallel processing. Got 1")
    index_subsets = split_index(dir_index, num_chunks=workers)
    output_dir.mkdir(exist_ok=True, parents=True)

    for i, subset in enumerate(index_subsets):
        batch_filename = output_dir/f'batch{i:04}.txt'
        list2txt(batch_filename, subset)
        batch_ids_path_list.append(batch_filename)
    return batch_ids_path_list
