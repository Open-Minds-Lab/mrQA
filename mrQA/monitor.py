"""Console script for mrQA."""
import argparse
import sys
from pathlib import Path
from typing import Union, List

from MRdataset import import_dataset, load_mr_dataset
from MRdataset.log import logger
from MRdataset.utils import is_writable

from mrQA import check_compliance
from mrQA.config import PATH_CONFIG
from mrQA.utils import files_modified_since, get_last_valid_record


def get_parser():
    """Console script for mrQA."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    # Add help
    optional.add_argument('-n', '--name', type=str,
                          help='provide a identifier/name for the dataset')
    required.add_argument('-d', '--data-source', type=str, required=True,
                          help='directory containing downloaded dataset with '
                               'dicom files, supports nested hierarchies')
    optional.add_argument('-o', '--output-dir', type=str,
                          help='specify the directory where the report'
                               ' would be saved. By default, the --data_source '
                               'directory will be used to save reports')
    optional.add_argument('-f', '--format', type=str, default='dicom',
                          help='type of dataset, one of [dicom]')
    optional.add_argument('-h', '--help', action='help',
                          default=argparse.SUPPRESS,
                          help='show this help message and exit')
    optional.add_argument('--decimals', type=int, default=3,
                          help='number of decimal places to round to '
                               '(default:0). If decimals are negative it '
                               'specifies the number of positions to the left'
                               'of the decimal point.')
    optional.add_argument('-v', '--verbose', action='store_true',
                          help='allow verbose output on console')
    optional.add_argument('-ref', '--reference-path', type=str,
                          help='.yaml file containing protocol specification')
    optional.add_argument('--strategy', type=str, default='majority',
                          help='how to examine parameters [majority|reference].'
                               '--reference-path required if using reference')
    optional.add_argument('--include-phantom', action='store_true',
                          help='whether to include phantom, localizer, '
                               'aahead_scout')

    if len(sys.argv) < 2:
        logger.critical('Too few arguments!')
        parser.print_help()
        parser.exit(1)

    return parser


def parse_args():
    """
    Parse command line arguments

    Returns
    -------
    args : argparse.Namespace
        parsed arguments

    Raises
    ------
    FileNotFoundError
        if data_source is not a valid directory
    ValueError
        if name is not a valid dataset name which has been processed before
    OSError
        if output_dir cannot be created
    """
    parser = get_parser()
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel('INFO')
    else:
        logger.setLevel('WARNING')

    if not Path(args.data_source).is_dir():
        raise FileNotFoundError('Invalid data_source specified '
                                'for reading files.')

    # valid_names = _datasets_processed(PATH_CONFIG['output_dir'],
    #                                   ignore_case=True)
    # if args.name not in valid_names:
    #     raise ValueError('Need valid project name to monitor! '
    #                      f'Expected one of {valid_names}. Got {args.name}')

    if args.output_dir is None:
        logger.info('Use --output_dir to specify dir for final directory. '
                    'Using default')
        args.output_dir = PATH_CONFIG['output_dir'] / args.name
    else:
        if not Path(args.output_dir).is_dir():
            try:
                Path(args.output_dir).mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise exc
    # TODO: Add this check to mrqa and MRdataset
    if not is_writable(args.output_dir):
        raise OSError(f'Output Folder {args.output_dir} is not writable')
    return args


def main():
    args = parse_args()
    monitor(name=args.name,
            data_source=args.data_source,
            output_dir=args.output_dir,
            verbose=args.verbose,
            include_phantom=args.include_phantom,
            decimals=args.decimals,
            ds_format=args.format,
            strategy=args.strategy)


def monitor(name: str,
            data_source: Union[str, List, Path],
            output_dir: Union[str, Path],
            verbose: bool = False,
            include_phantom: bool = False,
            decimals: int = 3,
            ds_format: str = 'dicom',
            strategy: str = 'majority') -> Path:
    """
    Monitor a dataset folder for changes. Read new files and append to
    existing dataset. Run compliance check on the updated dataset.
    Generate a report and save it to the output directory.

    Parameters
    ----------
    name :  str
        Identifier for the dataset, like ABCD. The name used to save results
    data_source: str or list
        Path to the folder containing the dataset or list of files/folders.
    output_dir: str
        Path to the folder where the report, and dataset would be saved.
    verbose: bool
        Whether to print verbose output on console.
    include_phantom: bool
        Whether to include phantom, localizer, aahead_scout
    decimals: int
        Number of decimal places to round to (default:3).
    ds_format: str
        Type of dataset, one of [dicom]
    strategy: str
        How to examine parameters [majority|reference]

    Returns
    -------
    report_path: Path
        Posix path to the new generated report.
    """
    output_dir = Path(output_dir)
    last_record = get_last_valid_record(output_dir)
    if last_record:
        last_reported_on, last_report_path, last_mrds_path = last_record
        # TODO: delete old logs, only keep latest 3-4 reports in the folder
        dataset = load_mr_dataset(last_mrds_path)
        modified_files = files_modified_since(input_dir=data_source,
                                              last_reported_on=last_reported_on,
                                              output_dir=output_dir)
        if modified_files:
            new_dataset = import_dataset(data_source=modified_files,
                                         ds_format='dicom',
                                         name=name,
                                         verbose=verbose,
                                         include_phantom=include_phantom)
            dataset.merge(new_dataset)
        else:
            logger.warning('No new files found since last report. '
                           'Regenerating report')
    else:
        logger.warning('Dataset %s not found in records. Running '
                       'compliance check on entire dataset', name)
        dataset = import_dataset(data_source=data_source,
                                 ds_format=ds_format,
                                 name=name,
                                 verbose=verbose,
                                 include_phantom=include_phantom)

    report_path = check_compliance(dataset=dataset,
                                   strategy=strategy,
                                   output_dir=output_dir,
                                   decimals=decimals)
    return report_path


if __name__ == '__main__':
    sys.exit(main())  # pragma: no cover
