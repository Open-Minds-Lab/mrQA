import os
import subprocess
from pathlib import Path
from time import sleep
from typing import Union, Iterable

from MRdataset import valid_dirs

from mrQA import logger
from mrQA.utils import is_integer_number, execute_local, list2txt, \
    folders_with_min_files


def _check_args(data_source: Union[str, Path, Iterable] = None,
                ds_format: str = 'dicom',
                output_dir: Union[str, Path] = None,
                debug: bool = False,
                subjects_per_job: int = None,
                hpc: bool = False,
                conda_dist: str = None,
                conda_env: str = None,
                config_path: Union[str, Path] = None):
    # It is not possible to submit jobs while debugging, why would you submit
    # a job, if code is still being debugged
    if debug and hpc:
        raise AttributeError('Dont debug on hpc!')
    if ds_format != 'dicom':
        raise NotImplementedError(f'Expects dicom, Got {ds_format}')
    if not is_integer_number(subjects_per_job):
        raise RuntimeError('Expects an integer value for subjects per job.'
                           f'Got {subjects_per_job}')
    if subjects_per_job < 1:
        raise RuntimeError('subjects_per_job cannot be less than 1')

    # Check if data_source is a valid directory, or list of valid directories
    data_source = valid_dirs(data_source)

    # RULE : If output_dir not provided, output will be saved in 'mrqa_files'
    # created in the parent folder of data_source
    if not output_dir:
        if isinstance(data_source, Iterable):
            # If data_source is a bunch of directories, the above RULE cannot
            # be followed, just pass a directory to save the file.
            raise RuntimeError('Need an output directory to store files')

        # Didn't find a good alternative to os.access
        # in pathlib, please raise an issue if you know one,
        # happy to incorporate
        parent_dir = Path(data_source[0]).parent
        output_dir = parent_dir / (data_source[0].name + '_mrqa_files')

        # Check if permission to create a folder in data_source.parent
        if os.access(parent_dir, os.W_OK):
            logger.warning('Expected a directory to save job scripts. Using '
                           'parent folder of --data_source instead.')
        else:
            raise PermissionError('You do not have write permission to'
                                  'create a folder in '
                                  f'{parent_dir}'
                                  'Please provide output_dir')
    else:
        output_dir = Path(output_dir)
    # Information about conda env is required for creating slurm scripts
    # The snippet below sets some defaults, may not be true for everyone.
    # The user can use the arguments to specify
    if not conda_env:
        conda_env = 'mrqa' if hpc else 'mrcheck'
    if not conda_dist:
        conda_dist = 'miniconda3' if hpc else 'anaconda3'
    if not Path(config_path).exists():
        raise FileNotFoundError(f'Config file not found at {config_path}')
    return data_source, output_dir, conda_env, conda_dist


def _make_file_folders(output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    # Create a folder id_lists for saving list of subject ids for each job
    # in a separate txt file.
    # Create a folder bash_scripts for saving bash_script for each job
    # Create a folder 'partial_mrds' for saving partial mrds pkl file
    # created by the corresponding bash script

    folder_paths = {
        'fnames': output_dir / 'fname_lists',
        'scripts': output_dir / 'bash_scripts',
        'mrds': output_dir / 'partial_mrds'
    }
    files_per_batch = {
        'fnames': output_dir / 'per_batch_folders_list.txt',
        'scripts': output_dir / 'per_batch_script_list.txt',
        'mrds': output_dir / 'per_batch_partial_mrds_list.txt'
    }

    for path in folder_paths.values():
        path.mkdir(parents=True, exist_ok=True)

    # And store the original complete list (contains all
    # subject ids) in "complete_id_list.txt"
    all_ids_path = output_dir / 'complete_fname_list.txt'
    return folder_paths, files_per_batch, all_ids_path


def _run_single_batch(script_path: Union[str, Path],
                      hpc: bool,
                      output_mrds_path: Union[str, Path]):
    """
    Runs a single script file either locally or on hpc.

    Parameters
    ----------
    script_path: str
        Path to slurm script file
    hpc: bool
        If True, runs the slurm script on a hpc
    output_mrds_path: str
        Path to save partial mrds pickle file
    """
    if isinstance(output_mrds_path, str):
        output_mrds_path = Path(output_mrds_path)

    if not output_mrds_path.exists():
        if hpc:
            # If running on a hpc, use the sbatch command
            # to submit the script
            # TODO: Add try/except block here
            subprocess.run(['sbatch', script_path], check=True, shell=True)
            # Without any delay, you may receive NODE_FAIL error
            sleep(2)
            # print(out.stdout)
            # some way to check was submitted/accepted

        else:
            # If running locally, and the user does not want to
            # submit the job, run the script using the bash command
            execute_local(script_path)
            # check successful completion, or log error

    else:
        logger.warning('%s already exists, skipping.  Use sbatch %s to'
                       ' overwrite', output_mrds_path, script_path)


def _create_slurm_script(output_script_path: Union[str, Path],
                         fnames_filepath: Union[str, Path],
                         env: str = 'mrqa',
                         conda_dist: str = 'anaconda3',
                         folders_per_job: int = 50,
                         verbose: bool = False,
                         config_path: Union[str, Path] = None,
                         output_mrds_path: bool = None,
                         email='mail.sinha.harsh@gmail.com') -> None:
    """
    Creates a slurm script file which can be submitted to a hpc.

    Parameters
    ----------
    output_script_path : str
        Path to slurm script file
    fnames_filepath : str
        Path to text file containing list of subject ids
    env : str
        Conda environment name
    conda_dist : str
        Conda distribution
    folders_per_job : int
        Number of subjects to process in each slurm job
    verbose : bool
        If True, prints the output of the script
    output_mrds_path : str
        Path to the partial mrds pickle file
    """

    # Memory and CPU time :  typical usage observed locally

    # For subjects_per_job = 50             100
    # Max RSS Size (Memory) ~150 MB,        ~160 MB
    # Sys Time (CPU Time) : 10 minutes      20 minutes

    # Set the memory and cpu time limits
    mem_required = 2000  # MB;
    # num_mins_per_subject = 1  # minutes
    # Set the number of hours to 3 if less than 3
    time_limit = 24
    # Setup python command to run
    python_cmd = (f'mrqa_subset -o {output_mrds_path} -b {fnames_filepath} '
                  f'--config {config_path}')

    # Add flags to python command
    if verbose:
        python_cmd += ' --verbose'
    python_cmd += ' --is-partial'

    # Create the slurm script file
    with open(output_script_path, 'w', encoding='utf-8') as fp:
        fp.writelines('\n'.join([
            '#!/bin/bash',
            '#SBATCH -A med220005p',
            '#SBATCH -N 1',
            '#SBATCH -p RM-shared',
            f'#SBATCH --mem-per-cpu={mem_required}M #memory per cpu-core',
            f'#SBATCH --time={time_limit}:00:00',
            '#SBATCH --ntasks-per-node=1',
            f'#SBATCH --error={fnames_filepath.stem}.%J.err',
            f'#SBATCH --output={fnames_filepath.stem}.%J.out',
            '#SBATCH --mail-type=end          # send email when job ends',
            '#SBATCH --mail-type=fail         # send email if job fails',
            f'#SBATCH --mail-user={email}',
            '#Clear the environment from any previously loaded modules',
            'module purge > /dev/null 2>&1',
            f'source  ${{HOME}}/{conda_dist}/etc/profile.d/conda.sh',
            f'conda activate {env}',
            python_cmd,
            'date\n',
        ])
        )


def _get_num_workers(folders_per_job, folder_list):
    if folders_per_job > len(folder_list):
        # If subjects_per_job is greater than the number of subjects,
        # process all subjects in a single job. Stop execution.
        raise RuntimeError('Trying to create more jobs than total number of '
                           'folders in the directory. Why?')

    # Get the number of jobs
    workers = len(folder_list) // folders_per_job
    if workers == 1:
        # If there is only one job, process all subjects in a single job
        raise RuntimeError('Decrease number of folders per job. Expected'
                           'workers > 1 for parallel processing. Got 1')

    return workers


def _get_terminal_folders(data_source: Union[str, Path],
                          all_ids_path: Union[str, Path],
                          pattern='*',
                          min_count=1) -> Iterable:
    """
    Get the list of subject ids from the data source folder

    Parameters
    ----------
    data_source : Union[str, Path]
        Path to the root directory of the data
    all_ids_path : Union[str, Path]
        Path to the output directory

    Returns
    -------
    subject_list : Iterable
        List of subject ids
    """
    terminal_folder_list = []
    # Get the list of subject ids
    for directory in valid_dirs(data_source):
        sub_folders = folders_with_min_files(directory, pattern,
                                             min_count)
        terminal_folder_list.extend(sub_folders)
    # Store the list of unique subject ids to a text file given by
    # output_path
    list2txt(all_ids_path, list(set(terminal_folder_list)))
    return terminal_folder_list
