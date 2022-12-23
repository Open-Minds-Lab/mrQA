import time
from pathlib import Path
from typing import Iterable, Union, List

from MRdataset.config import MRDS_EXT
from MRdataset.utils import valid_paths

from mrQA.parallel_utils import _check_args, _make_file_folders, \
    _run_single_batch, _create_slurm_script, _get_num_workers, _get_subject_ids
from mrQA.run_merge import check_and_merge
from mrQA.utils import list2txt, split_list, \
    txt2list


def process_parallel(data_root,
                     output_dir,
                     output_path,
                     name=None):
    # One function to process them all!
    # note that it will generate scripts only
    script_list_filepath, mrds_list_filepath = create_script(
                                                data_source_folders=data_root,
                                                subjects_per_job=5,
                                                conda_env='mrcheck',
                                                conda_dist='anaconda3',
                                                output_dir=output_dir,
                                                hpc=False,
                                                )
    # Generate slurm scripts and submit jobs, for local parallel processing
    submit_job(scripts_list_filepath=script_list_filepath,
               mrds_list_filepath=mrds_list_filepath,
               hpc=False)

    # wait until processing completes
    mrds_files = valid_paths(txt2list(mrds_list_filepath))
    for file in mrds_files:
        while not file.exists():
            time.sleep(100)

    check_and_merge(
        mrds_list_filepath=mrds_list_filepath,
        output_path=output_path,
        name=name
    )


def submit_job(scripts_list_filepath: Union[str, Path],
               mrds_list_filepath: Union[str, Path],
               debug: bool = False,
               hpc: bool = False) -> None:
    """
    Given a folder(or List[folder]) it will divide the work into smaller
    jobs. Each job will contain a fixed number of subjects. These jobs can be
    executed in parallel to save time.

    Parameters
    ----------
    scripts_list_filepath: str
        Path to the file containing list of bash scripts to be executed
    mrds_list_filepath: str
        Path to the file containing list of partial mrds files to be created
    debug: bool
        If True, the dataset will be created locally. This is useful for testing
    hpc: bool
        If True, the scripts will be generated for HPC, not for local execution
    Returns
    -------
    None
    """

    processes = []
    bash_scripts = valid_paths(txt2list(scripts_list_filepath))
    mrds_files = txt2list(mrds_list_filepath)
    for script_path, output_mrds_path in zip(bash_scripts, mrds_files):
        # Run the script file
        _run_single_batch(script_path=script_path,
                                   hpc=hpc,
                                   output_mrds_path=output_mrds_path)


def create_script(data_source_folders: Union[str, Path, Iterable] = None,
                  style: str = 'dicom',
                  include_phantom: bool = False,
                  verbose: bool = False,
                  output_dir: Union[str, Path] = None,
                  debug: bool = False,
                  subjects_per_job: int = None,
                  hpc: bool = False,
                  conda_dist: str = None,
                  conda_env: str = None):
    """
    Given a folder(or List[folder]) it will divide the work into smaller
    jobs. Each job will contain a fixed number of subjects. These jobs can be
    executed in parallel to save time.

    Parameters
    ----------
    data_source_folders: str or List[str]
        /path/to/my/dataset containing files
    style: str
        Specify dataset type. Use one of [dicom]
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
    hpc: bool
        If True, the scripts will be generated for HPC, not for local execution
    conda_dist: str
        Name of conda distribution
    conda_env: str
        Name of conda environment
    """

    data_src, output_dir, env, dist = _check_args(data_source_folders, style,
                                                  output_dir, debug,
                                                  subjects_per_job, hpc,
                                                  conda_dist, conda_env)
    folder_paths, files_per_batch, all_ids_path = _make_file_folders(output_dir)
    ids_path_list = split_ids_list(
        data_src,
        all_ids_path=all_ids_path,
        per_batch_ids=files_per_batch['ids'],
        output_dir=folder_paths['ids'],
        subjects_per_job=subjects_per_job
    )

    scripts_path_list = []
    mrds_path_list = []
    # create a slurm job script for each sub_group of subject ids
    for ids_filepath in ids_path_list:
        # Filename of the bash script should be same as text file.
        # Say batch0000.txt points to set of 10 subjects. Then create a
        # slurm script file batch0000.sh which will run for these 10 subjects,
        # and the final partial mrds pickle file will have the name
        # batch0000.mrds.pkl
        script_filename = ids_filepath.stem + '.sh'
        partial_mrds_filename = ids_filepath.stem + MRDS_EXT
        script_filepath = folder_paths['scripts'] / script_filename
        partial_mrds_filepath = folder_paths['mrds'] / partial_mrds_filename

        # Keep storing the filenames. The entire list would be saved at the end
        scripts_path_list.append(script_filepath)
        mrds_path_list.append(partial_mrds_filepath)

        # Finally create the slurm script and save to disk
        _create_slurm_script(output_script_path=script_filepath,
                             ids_filepath=ids_filepath,
                             env=conda_env,
                             conda_dist=conda_dist,
                             num_subj_per_job=subjects_per_job,
                             verbose=verbose,
                             include_phantom=include_phantom,
                             output_mrds_path=partial_mrds_filepath)

    # Finally, save the all the paths to create mrds pickle files and all the
    # paths to generated scripts in a text file for reference.
    list2txt(path=files_per_batch['mrds'], list_=mrds_path_list)
    list2txt(path=files_per_batch['scripts'], list_=scripts_path_list)
    return files_per_batch['scripts'], files_per_batch['mrds']


def split_ids_list(data_source_folders: Union[str, Path],
                   all_ids_path: Union[str, Path],
                   per_batch_ids,
                   output_dir: Union[str, Path],
                   subjects_per_job: int = 50):
    """
    Splits a given set of subjects into multiple jobs and creates separate
    text files containing the list of subjects. Each text file
    contains the list of subjects to be processed in a single job.

    Parameters
    ----------
    data_source_folders : Union[str, Path]
        Path to the root directory of the data
    all_ids_path : Union[str, Path]
        Path to the output directory
    output_dir : Union[str, Path]
        Name of the output directory
    subjects_per_job : int
        Number of subjects to process in each job

    Returns
    -------
    batch_ids_path_list : list
        Paths to the text files, each containing a list of subjects
    """

    all_ids_path = Path(all_ids_path)
    # List of paths to the txt files,
    # each containing the list of subjects per job
    batch_ids_path_list = []

    subject_list = _get_subject_ids(data_source_folders, all_ids_path)
    # Get the list of subjects for each job
    workers = _get_num_workers(subjects_per_job, subject_list)
    subject_subsets = split_list(subject_list, num_chunks=workers)

    # Create a text file for each job
    for i, subset in enumerate(subject_subsets):
        # Create a text file containing the list of subjects for each job
        batch_filepath = output_dir / f'batch{i:04}.txt'
        # Store to the path given to the text file
        list2txt(batch_filepath, subset)
        # Add the path to the text file ( containing the
        # list of subjects for each job) to a list, return the list
        batch_ids_path_list.append(batch_filepath)
    list2txt(path=per_batch_ids, list_=batch_ids_path_list)
    return batch_ids_path_list


