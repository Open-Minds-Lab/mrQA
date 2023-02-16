"""Console script for mrQA."""
import argparse
import sys
from pathlib import Path
from typing import Union, List

from MRdataset import import_dataset, load_mr_dataset
from MRdataset.log import logger

from mrQA import check_compliance
from mrQA.config import PATH_CONFIG, mrds_fpath
from mrQA.utils import files_modified_since, _datasets_processed, \
    get_last_valid_record


def get_parser():
    """Console script for mrQA."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    # Add help
    optional.add_argument('-n', '--name', type=str.lower,
                          help='provide a identifier/name for the dataset')
    required.add_argument('-d', '--data-source', nargs='+', required=True,
                          help='directory containing downloaded dataset with '
                               'dicom files, supports nested hierarchies')
    optional.add_argument('-o', '--output-dir', type=str,
                          help='specify the directory where the report'
                               ' would be saved. By default, the --data_source '
                               'directory will be used to save reports')
    optional.add_argument('-s', '--style', type=str, default='dicom',
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

    valid_names = _datasets_processed(PATH_CONFIG['output_dir'],
                                      ignore_case=True)
    if args.name not in valid_names:
        raise ValueError('Need valid project name to monitor! '
                         f'Expected one of {valid_names}. Got {args.name}')

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
    return args


def main():
    args = parse_args()
    last_record = get_last_valid_record(args.output_dir)
    if last_record:
        last_reported_on, last_fname, _ = last_record
        mrqa_monitor(name=args.name,
                     data_source=args.data_source,
                     output_dir=args.output_dir,
                     last_reported_on=last_reported_on,
                     last_fname=last_fname,
                     verbose=args.verbose,
                     include_phantom=args.include_phantom,
                     decimals=args.decimals)
    else:
        logger.warning('Dataset %s not found in records. Running '
                       'compliance check on entire dataset', args.name)
        dataset = import_dataset(data_source=args.data_source,
                                 style=args.style,
                                 name=args.name,
                                 verbose=args.verbose,
                                 include_phantom=args.include_phantom)

        check_compliance(dataset=dataset,
                         strategy=args.strategy,
                         output_dir=args.output_dir,
                         decimals=args.decimals)


def mrqa_monitor(name: str,
                 data_source: Union[str, List],
                 output_dir: str,
                 last_reported_on: str,
                 last_fname: str,
                 verbose: bool = False,
                 include_phantom: bool = False,
                 decimals: int = 3):
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
    last_reported_on: str
        Time of last report. Used to find files modified since then.
    last_fname: str
        Name of the last report. Used to find the dataset.
    verbose: bool
        Whether to print verbose output on console.
    include_phantom: bool
        Whether to include phantom, localizer, aahead_scout
    decimals: int
        Number of decimal places to round to (default:3).

    Returns
    -------
    report_path: str
        Path to the new generated report.
    """

    # TODO: delete old logs, only keep latest 3-4 reports in the folder
    modified_files = files_modified_since(data_source,
                                          last_reported_on,
                                          output_dir)

    last_mrds_fpath = mrds_fpath(output_dir, last_fname)
    last_mrds = load_mr_dataset(last_mrds_fpath)
    new_dataset = import_dataset(data_source=modified_files,
                                 style='dicom',
                                 name=name,
                                 verbose=verbose,
                                 include_phantom=include_phantom)
    last_mrds.merge(new_dataset)
    updated_mrds = last_mrds
    report_path = check_compliance(dataset=updated_mrds,
                                   output_dir=output_dir,
                                   decimals=decimals)
    return report_path


if __name__ == '__main__':
    sys.exit(main())  # pragma: no cover
