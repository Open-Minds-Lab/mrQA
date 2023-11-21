""" This module contains functions to run the compliance checks in parallel"""
import argparse
import sys
import time
from pathlib import Path
from typing import Iterable, Union

from MRdataset import load_mr_dataset, MRDS_EXT, DatasetEmptyException

from mrQA import check_compliance
from mrQA import logger
from mrQA.config import PATH_CONFIG, THIS_DIR
from mrQA.parallel_utils import _check_args, _make_file_folders, \
    _run_single_batch, _create_slurm_script, _get_num_workers, \
    _get_terminal_folders
from mrQA.run_merge import check_and_merge
from mrQA.utils import list2txt, split_list, \
    txt2list
from mrQA.utils import valid_paths, is_writable


def get_parser():
    """Parser for the CLI"""
    parser = argparse.ArgumentParser(
        description='Parallelize the mrQA compliance checks',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    # Add help
    optional.add_argument('-h', '--help', action='help',
                          help='show this help message and exit')
    required.add_argument('-d', '--data-source', type=str, required=True,
                          help='directory containing downloaded dataset with '
                               'dicom files, supports nested hierarchies')
    required.add_argument('--config', type=str,
                          help='path to config file',
                          default=THIS_DIR / 'resources/mri-config.json')
    optional.add_argument('-o', '--output-dir', type=str,
                          help='specify the directory where the report'
                               ' would be saved. By default, the --data_source '
                               'directory will be used to save reports')
    optional.add_argument('-p', '--out-mrds-path', type=str,
                          help='specify the path to the output mrds file. ')
    optional.add_argument('-n', '--name', type=str,
                          help='provide a identifier/name for the dataset')
    optional.add_argument('-j', '--job-size', type=int, default=5,
                          help='number of folders to process per job')
    optional.add_argument('-e', '--conda-env', type=str, default='mrcheck',
                          help='name of conda environment to use')
    optional.add_argument('-c', '--conda-dist', type=str, default='anaconda3',
                          help='name of conda distribution to use')
    optional.add_argument('-H', '--hpc', action='store_true',
                          help='flag to run on HPC')
    optional.add_argument('-v', '--verbose', action='store_true',
                          help='allow verbose output on console')
    optional.add_argument('-ref', '--ref-protocol-path', type=str,
                          help='XML file containing desired protocol. If not '
                               'provided, the protocol will be inferred from '
                               'the dataset.')
    optional.add_argument('--decimals', type=int, default=3,
                          help='number of decimal places to round to '
                               '(default:0). If decimals are negative it '
                               'specifies the number of positions to the left'
                               'of the decimal point.')
    optional.add_argument('-t', '--tolerance', type=float, default=0,
                          help='tolerance for checking against reference '
                               'protocol. Default is 0')

    if len(sys.argv) < 2:
        logger.critical('Too few arguments!')
        parser.print_help()
        parser.exit(1)

    return parser


def cli():
    """Console script for mrQA."""
    args = parse_args()
    process_parallel(data_source=args.data_source,
                     output_dir=args.output_dir,
                     out_mrds_path=args.out_mrds_path,
                     name=args.name,
                     job_size=args.job_size,
                     conda_env=args.conda_env,
                     conda_dist=args.conda_dist,
                     config_path=args.config,
                     hpc=args.hpc)
    dataset = load_mr_dataset(args.out_mrds_path)
    try:
        check_compliance(dataset=dataset,
                         output_dir=args.output_dir,
                         decimals=args.decimals,
                         verbose=args.verbose,
                         tolerance=args.tolerance,
                         config_path=args.config,
                         reference_path=args.ref_protocol_path, )
    except DatasetEmptyException:
        logger.error("Cannot check compliance if the dataset doesn't have "
                     "any scans. Please check the dataset.")
    except NotADirectoryError:
        logger.error('Provided output directory for saving reports is invalid.'
                     'Either it is not a directory or it does not exist. ')


def parse_args():
    """Argument parser for the CLI"""
    parser = get_parser()
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel('INFO')
    else:
        logger.setLevel('WARNING')

    if args.output_dir is None:
        logger.info('Use --output-dir to specify dir for final directory. '
                    'Using default')
        args.output_dir = PATH_CONFIG['output_dir'] / args.name.lower()
    else:
        if not Path(args.output_dir).is_dir():
            try:
                Path(args.output_dir).mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise exc

    if args.ref_protocol_path is not None:
        if not Path(args.ref_protocol_path).is_file():
            raise OSError(
                'Expected valid file for --ref-protocol-path argument, '
                'Got {0}'.format(args.ref_protocol_path))

    if not is_writable(args.output_dir):
        raise OSError(f'Output Folder {args.output_dir} is not writable')

    if not Path(args.config).is_file():
        raise FileNotFoundError(f'Expected valid config file, '
                                f'Got {args.config}')
    return args


def process_parallel(data_source: Union[str, Path],
                     output_dir: Union[str, Path],
                     out_mrds_path: Union[str, Path],
                     name: str = None,
                     job_size: int = 5,
                     conda_env: str = 'mrcheck',
                     conda_dist: str = 'anaconda3',
                     config_path: Union[str, Path] = None,
                     hpc: bool = False):
    """
    Given a folder(or List[folder]) it will divide the work into smaller
    jobs. Each job will contain a fixed number of folders. These jobs can be
    executed in parallel to save time.

    Parameters
    ----------
    data_source: str | Path
        Valid path to the folder containing the multiple folders
    output_dir: str | Path
        Valid path to the folder where the output will be saved
    out_mrds_path: str | Path
        Valid path to the final output .mrds.pkl file
    name: str
        Name of the final output file
    job_size: int
        Number of folders to be processed in each job
    conda_env: str
        Name of the conda environment to be used
    conda_dist: str
        Name of the conda distribution to be used
    hpc: bool
        Whether to use HPC or not
    config_path: str
        Path to the config file
    """
    # One function to process them all!
    # note that it will generate scripts only
    script_list_filepath, mrds_list_filepath = create_script(
        ds_format='dicom',
        verbose=False,
        debug=False,
        config_path=config_path,
        data_source=data_source,
        folders_per_job=job_size,
        conda_env=conda_env,
        conda_dist=conda_dist,
        output_dir=output_dir,
        hpc=hpc,
    )
    # Generate slurm scripts and submit jobs, for local parallel processing
    submit_job(scripts_list_filepath=script_list_filepath,
               mrds_list_filepath=mrds_list_filepath,
               hpc=hpc)

    # wait until processing completes
    mrds_files = txt2list(mrds_list_filepath)
    for file in mrds_files:
        while not Path(file).exists():
            time.sleep(100)

    check_and_merge(
        mrds_list_filepath=mrds_list_filepath,
        output_path=out_mrds_path,
        name=name
    )


def submit_job(scripts_list_filepath: Union[str, Path],
               mrds_list_filepath: Union[str, Path],
               hpc: bool = False) -> None:
    """
    Given a folder(or List[folder]) it will divide the work into smaller
    jobs. Each job will contain a fixed number of folders. These jobs can be
    executed in parallel to save time.

    Parameters
    ----------
    scripts_list_filepath: str
        Path to the file containing list of bash scripts to be executed
    mrds_list_filepath: str
        Path to the file containing list of partial mrds files to be created
    hpc: bool
        If True, the scripts will be generated for HPC, not for local execution
    Returns
    -------
    None
    """

    bash_scripts = valid_paths(txt2list(scripts_list_filepath))
    mrds_files = txt2list(mrds_list_filepath)
    for script_path, output_mrds_path in zip(bash_scripts, mrds_files):
        # Run the script file
        _run_single_batch(script_path=script_path,
                          hpc=hpc,
                          output_mrds_path=output_mrds_path)


def create_script(data_source: Union[str, Path, Iterable] = None,
                  ds_format: str = 'dicom',
                  verbose: bool = False,
                  output_dir: Union[str, Path] = None,
                  debug: bool = False,
                  folders_per_job: int = None,
                  hpc: bool = False,
                  conda_dist: str = None,
                  conda_env: str = None,
                  config_path: Union[str, Path] = None):
    """
    Given a folder(or List[folder]) it will divide the work into smaller
    jobs. Each job will contain a fixed number of folders. These jobs can be
    executed in parallel to save time.

    Parameters
    ----------
    data_source: str or List[str]
        /path/to/my/dataset containing files
    ds_format: str
        Specify dataset type. Use one of [dicom]
    verbose: bool
        Print progress
    output_dir: str
        Path to save the output dataset
    debug: bool
        If True, the dataset will be created locally. This is useful for testing
    folders_per_job: int
        Number of folders per job. Recommended value is 50 or 100
    hpc: bool
        If True, the scripts will be generated for HPC, not for local execution
    conda_dist: str
        Name of conda distribution
    conda_env: str
        Name of conda environment
    config_path: str
        Path to the config file
    """

    data_src, output_dir, env, dist = _check_args(data_source, ds_format,
                                                  output_dir, debug,
                                                  folders_per_job, hpc,
                                                  conda_dist, conda_env,
                                                  config_path)
    folder_paths, files_per_batch, all_fnames_path = _make_file_folders(
        output_dir)
    fnames_path_list = split_folders_list(
        data_src,
        all_fnames_path=all_fnames_path,
        per_batch_ids=files_per_batch['fnames'],
        output_dir=folder_paths['fnames'],
        folders_per_job=folders_per_job
    )

    scripts_path_list = []
    mrds_path_list = []
    # create a slurm job script for each sub_group of folders
    for fnames_filepath in fnames_path_list:
        # Filename of the bash script should be same as text file.
        # Say batch0000.txt points to set of 10 folders. Then create a
        # slurm script file batch0000.sh which will run for these 10 folders,
        # and the final partial mrds pickle file will have the name
        # batch0000.mrds.pkl
        script_filename = fnames_filepath.stem + '.sh'
        partial_mrds_filename = fnames_filepath.stem + MRDS_EXT
        script_filepath = folder_paths['scripts'] / script_filename
        partial_mrds_filepath = folder_paths['mrds'] / partial_mrds_filename

        # Keep storing the filenames. The entire list would be saved at the end
        scripts_path_list.append(script_filepath)
        mrds_path_list.append(partial_mrds_filepath)

        # Finally create the slurm script and save to disk
        _create_slurm_script(output_script_path=script_filepath,
                             fnames_filepath=fnames_filepath,
                             env=conda_env,
                             conda_dist=conda_dist,
                             folders_per_job=folders_per_job,
                             verbose=verbose,
                             config_path=config_path,
                             output_mrds_path=partial_mrds_filepath)

    # Finally, save the all the paths to create mrds pickle files and all the
    # paths to generated scripts in a text file for reference.
    list2txt(fpath=files_per_batch['mrds'], list_=mrds_path_list)
    list2txt(fpath=files_per_batch['scripts'], list_=scripts_path_list)
    return files_per_batch['scripts'], files_per_batch['mrds']


def split_folders_list(data_source: Union[str, Path],
                       all_fnames_path: Union[str, Path],
                       per_batch_ids: Union[str, Path],
                       output_dir: Union[str, Path],
                       folders_per_job: int = 50):
    """
    Splits a given set of folders into multiple jobs and creates separate
    text files containing the list of folders. Each text file
    contains the list of folders to be processed in a single job.

    Parameters
    ----------
    data_source : Union[str, Path]
        Path to the root directory of the data
    all_fnames_path : Union[str, Path]
        Path to the output directory
    per_batch_ids : Union[str, Path]
        filepath to a file which has paths to all txt files for all jobs.
        Each of these txt files contains a list of folder ids for
        corresponding job.
    output_dir : Union[str, Path]
        Name of the output directory
    folders_per_job : int
        Number of folders to process in each job

    Returns
    -------
    batch_ids_path_list : Sized
        Paths to the text files, each containing a list of folders
    """

    all_fnames_path = Path(all_fnames_path)
    # List of paths to the txt files,
    # each containing the list of folders per job
    batch_fnames_path_list = []

    folder_list = _get_terminal_folders(data_source, all_fnames_path)
    # Get the list of folders for each job
    workers = _get_num_workers(folders_per_job, folder_list)
    folder_subsets = split_list(folder_list, num_chunks=workers)

    # Create a text file for each job
    for i, subset in enumerate(folder_subsets):
        # Create a text file containing the list of folders for each job
        batch_filepath = output_dir / f'batch{i:04}.txt'
        # Store to the path given to the text file
        list2txt(batch_filepath, subset)
        # Add the path to the text file ( containing the
        # list of folders for each job) to a list, return the list
        batch_fnames_path_list.append(batch_filepath)
    list2txt(fpath=per_batch_ids, list_=batch_fnames_path_list)
    return batch_fnames_path_list
