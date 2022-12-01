import warnings
from pathlib import Path
from MRdataset.utils import random_name
from MRdataset.config import CACHE_DIR, MRDS_EXT
from mrQA.run_subset import read_subset
from mrQA.run_merge import check_and_merge
from MRdataset.base import save_mr_dataset
from mrQA.utils import create_index, execute_local, list2txt, txt2list
import subprocess
import math


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
    if debug and submit_job:
        raise AttributeError('Cannot debug when submitting jobs')
    if style != 'dicom':
        raise NotImplementedError(f'Expects dicom, Got {style}')

    if not Path(data_root).is_dir():
        raise OSError('Expected valid directory for --data_root argument,'
                      ' Got {0}'.format(data_root))
    data_root = Path(data_root).resolve()

    if not output_dir:
        warnings.warn('Expected a directory to save job scripts. Using '
                      '--data_root instead.')
        output_dir = data_root.parent / 'mrqa_files'
        output_dir.mkdir(exist_ok=True)

    if not Path(output_dir).is_dir():
        raise OSError('Expected valid directory for --output_dir argument,'
                      ' Got {0}'.format(output_dir))
    output_dir = Path(output_dir).resolve()

    if name is None:
        warnings.warn(
            'Expected a unique identifier for caching data. Got NoneType. '
            'Using a random name. Use --name flag for persistent metadata',
            stacklevel=2)
        name = random_name()

    txt_path_list = create_index(data_root, output_dir, name,
                                 reindex, subjects_per_job)

    all_batches_txt_filepath = output_dir / (name+'_txt_files.txt')
    list2txt(path=all_batches_txt_filepath, list_=txt_path_list)

    processes = []
    for txt_filepath in txt_path_list:
        # create slurm script to call run_subset.py
        s_folderpath = output_dir / 'scripts'
        s_folderpath.mkdir(parents=True, exist_ok=True)
        s_filename = s_folderpath / (txt_filepath.stem + '.sh')
        if not conda_env:
            conda_env = 'mrqa' if hpc else 'mrcheck'
        if not conda_dist:
            conda_dist = 'miniconda3' if hpc else 'anaconda3'

        partial_mrds_folder = output_dir / 'saved_files'
        partial_mrds_folder.mkdir(parents=True, exist_ok=True)
        partial_mrds_filename = partial_mrds_folder / (txt_filepath.stem + MRDS_EXT)
        create_slurm_script(s_filename, name, output_dir, txt_filepath,
                            conda_env, conda_dist, subjects_per_job, reindex,
                            verbose, include_phantom, partial_mrds_filename)
        output = run_single(debug, output_dir, txt_filepath, reindex,
                            verbose, include_phantom, s_filename, submit_job,
                            hpc, partial_mrds_filename)
        processes.append(output)
    if not (submit_job or debug or hpc):
        exit_codes = [p.wait() for p in processes]
    return


def run_single(debug, output_dir, txt_filepath, reindex, verbose,
               include_phantom, s_filename, submit_job, hpc=False,
               partial_mrds_filename=None):
    # submit job or run with bash or execute with python
    if debug:
        partial_dataset = read_subset(output_dir,
                                      txt_filepath, 'dicom',
                                      reindex, verbose,
                                      include_phantom)
        partial_dataset.set_cache_path()
        partial_dataset.is_complete = False
        save_filename = txt_filepath.with_suffix(MRDS_EXT)
        save_mr_dataset(save_filename, output_dir, partial_dataset)
        return None
    elif not partial_mrds_filename.exists() or reindex:
        if not hpc:
            return execute_local(s_filename)
        if hpc and submit_job:
            subprocess.call(['sbatch', s_filename])
            return


def create_slurm_script(filename, dataset_name, output_dir,
                        txt_batch_filepath, env='mrqa', conda_dist='anaconda3',
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
    python_cmd = f'mrpc_subset -o {partial_mrds_filename} -b {txt_batch_filepath}'

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
            f'#SBATCH --error={txt_batch_filepath.stem}.%J.err',
            f'#SBATCH --output={txt_batch_filepath.stem}.%J.out',
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

