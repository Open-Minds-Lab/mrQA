import math
import os
import subprocess
from pathlib import Path
from typing import Union, Iterable

from MRdataset.log import logger
from MRdataset.utils import valid_dirs

from mrQA.utils import is_integer_number, execute_local, list2txt


def _check_args(data_source_folders: Union[str, Path, Iterable] = None,
                style: str = 'dicom',
                output_dir: Union[str, Path] = None,
                debug: bool = False,
                subjects_per_job: int = None,
                hpc: bool = False,
                conda_dist: str = None,
                conda_env: str = None):
    # It is not possible to submit jobs while debugging, why would you submit
    # a job, if code is still being debugged
    if debug and hpc:
        raise AttributeError('Dont debug on hpc!')
    if style != 'dicom':
        raise NotImplementedError(f'Expects dicom, Got {style}')
    if not is_integer_number(subjects_per_job):
        raise RuntimeError('Expects an integer value for subjects per job.'
                           f'Got {subjects_per_job}')
    if subjects_per_job < 1:
        raise RuntimeError('subjects_per_job cannot be less than 1')

    # Check if data_root is a valid directory, or list of valid directories
    data_source_folders = valid_dirs(data_source_folders)

    # RULE : If output_dir not provided, output wil be saved in 'mrqa_files'
    # created in the parent folder of data_root
    if not output_dir:
        if isinstance(data_source_folders, Iterable):
            # If data_root is a bunch of directories, the above RULE cannot
            # be followed, just pass a directory to save the file.
            raise RuntimeError("Need an output directory to store files")

        # Didn't find a good alternative to os.access
        # in pathlib, please raise a issue if you know one, happy to incorporate
        output_dir = data_source_folders.parent / (
            data_source_folders.name + '_mrqa_files')

        # Check if permission to create a folder in data_root.parent
        if os.access(data_source_folders.parent, os.W_OK):
            logger.warning('Expected a directory to save job scripts. Using '
                           'parent folder of --data_root instead.')
        else:
            raise PermissionError(f'You do not have write permission to'
                                  f'create a folder in '
                                  f'{data_source_folders.parent}'
                                  f'Please provide output_dir')

    # Information about conda env is required for creating slurm scripts
    # The snippet below sets some defaults, may not be true for everyone.
    # The user can use the arguments to specify
    if not conda_env:
        conda_env = 'mrqa' if hpc else 'mrcheck'
    if not conda_dist:
        conda_dist = 'miniconda3' if hpc else 'anaconda3'
    return data_source_folders, output_dir, conda_env, conda_dist


def _make_file_folders(output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    # Create a folder id_lists for saving list of subject ids for each job
    # in a separate txt file.
    # Create a folder bash_scripts for saving bash_script for each job
    # Create a folder 'partial_mrds' for saving partial mrds pkl file
    # created by the corresponding bash script

    folder_paths = {
        'ids': output_dir / 'id_lists',
        'scripts': output_dir / 'bash_scripts',
        'mrds': output_dir / 'partial_mrds'
    }
    files_per_batch = {
        'ids': output_dir / 'per_batch_id_list.txt',
        'scripts': output_dir / 'per_batch_script_list.txt',
        'mrds': output_dir / 'per_batch_partial_mrds_list.txt'
    }

    for path in folder_paths.values():
        path.mkdir(parents=True, exist_ok=True)

    # And store the original complete list (contains all
    # subject ids) in "complete_id_list.txt"
    all_ids_path = output_dir / 'complete_id_list.txt'
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
            out = subprocess.Popen(['sbatch', script_path])
            # print(out.stdout)
            # some way to check was submitted/accepted

        else:
            # If running locally, and the user does not want to
            # submit the job, run the script using the bash command
            execute_local(script_path)
            # check successful completion, or log error

    else:
        logger.warning(f"{output_mrds_path} already exists, skipping. "
                       f" Use 'sbatch {script_path} to overwrite")


def _create_slurm_script(output_script_path: Union[str, Path],
                         ids_filepath: Union[str, Path],
                         env: str = 'mrqa',
                         conda_dist: str = 'anaconda3',
                         num_subj_per_job: int = 50,
                         verbose: bool = False,
                         include_phantom: bool = False,
                         output_mrds_path: bool = None) -> None:
    """
    Creates a slurm script file which can be submitted to a hpc.

    Parameters
    ----------
    output_script_path : str
        Path to slurm script file
    ids_filepath : str
        Path to text file containing list of subject ids
    env : str
        Conda environment name
    conda_dist : str
        Conda distribution
    num_subj_per_job : int
        Number of subjects to process in each slurm job
    verbose : bool
        If True, prints the output of the script
    include_phantom : bool
        If True, includes phantom, localizer and calibration studies
    output_mrds_path : str
        Path to the partial mrds pickle file

    Returns
    -------
    None
    """

    # Memory and CPU time :  typical usage observed locally

    # For subjects_per_job = 50             100
    # Max RSS Size (Memory) ~150 MB,        ~160 MB
    # Sys Time (CPU Time) : 10 minutes      20 minutes

    # Set the memory and cpu time limits
    mem_reqd = 2000  # MB;
    num_mins_per_subject = 1  # minutes
    num_hours = int(math.ceil(num_subj_per_job * num_mins_per_subject / 60))
    # Set the number of hours to 3 if less than 3
    time_limit = 3 if num_hours < 3 else num_hours
    # Setup python command to run
    python_cmd = f'mrpc_subset -o {output_mrds_path} -b {ids_filepath}'

    # Add flags to python command
    if verbose:
        python_cmd += ' --verbose'
    if include_phantom:
        python_cmd += ' --include_phantom'
    python_cmd += ' --is_partial'

    # Create the slurm script file
    with open(output_script_path, 'w') as fp:
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


def _get_num_workers(subjects_per_job, subject_list):
    if subjects_per_job > len(subject_list):
        # If subjects_per_job is greater than the number of subjects,
        # process all subjects in a single job. Stop execution.
        raise RuntimeError("Trying to create more jobs than total number of "
                           "subjects in the directory. Why?")

    # Get the number of jobs
    workers = len(subject_list) // subjects_per_job
    if workers == 1:
        # If there is only one job, process all subjects in a single job
        raise RuntimeError("Decrease number of subjects per job. Expected"
                           "workers > 1 for parallel processing. Got 1")

    return workers


def _get_subject_ids(data_source_folders: Union[str, Path],
                     all_ids_path: Union[str, Path]) -> list:
    """
    Get the list of subject ids from the data source folder

    Parameters
    ----------
    data_source_folders : Union[str, Path]
        Path to the root directory of the data
    all_ids_path : Union[str, Path]
        Path to the output directory

    Returns
    -------
    subject_list : list
        List of subject ids
    """
    subject_list = []
    # Get the list of subject ids
    for root, dirs, files in os.walk(data_source_folders):
        if 'sub-' in Path(root).name:
            # Get the subject id
            num_files_in_root =  len(list(Path(root).rglob('*/*')))
            if num_files_in_root > 0:
                subject_list.append(root)
    # Store the list of unique subject ids to a text file given by
    # output_path
    list2txt(all_ids_path, list(set(subject_list)))
    return subject_list
