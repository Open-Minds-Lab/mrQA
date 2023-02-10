"""Console script for mrQA."""
import argparse
import sys
from pathlib import Path

from MRdataset import MRDS_EXT, import_dataset, load_mr_dataset
from MRdataset.log import logger

from mrQA import check_compliance
from mrQA.config import PATH_CONFIG
from mrQA.utils import get_files_by_mtime, _projects_processed


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
    required.add_argument('-d', '--data_root', type=str, required=True,
                          help='directory containing downloaded dataset with '
                               'dicom files, supports nested hierarchies')
    optional.add_argument('-o', '--output_dir', type=str,
                          help='specify the directory where the report'
                               ' would be saved. By default, the --data_root '
                               'directory will be used to save reports')
    optional.add_argument('-s', '--style', type=str, default='dicom',
                          help='type of dataset, one of [dicom|bids|other]')
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
    optional.add_argument('-ref', '--reference_path', type=str,
                          help='.yaml file containing protocol specification')
    optional.add_argument('--strategy', type=str, default='majority',
                          help='how to examine parameters [majority|reference].'
                               '--reference_path required if using reference')
    optional.add_argument('--include_phantom', action='store_true',
                          help='whether to include phantom, localizer, '
                               'aahead_scout')
    optional.add_argument('--include_nifti_header', action='store_true',
                          help='whether to check nifti headers for compliance,'
                               'only used when --style==bids')

    if len(sys.argv) < 2:
        logger.critical('Too few arguments!')
        parser.print_help()
        parser.exit(1)

    return parser


def parse_args():
    parser = get_parser()
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel('INFO')
    else:
        logger.setLevel('WARNING')

    if not Path(args.data_root).is_dir():
        raise FileNotFoundError('Invalid data_root specified '
                                'for reading files.')

    valid_names = _projects_processed(PATH_CONFIG["output_dir"], ignore_case=True)
    if args.name not in valid_names:
        raise ValueError("Need valid project name to monitor! "
                         f"Expected one of {valid_names}. Got {args.name}")

    if args.output_dir is None:
        logger.info('Use --output_dir to specify dir for final directory. '
                    'Using default')
        args.output_dir = PATH_CONFIG["output_dir"] / args.name
    else:
        if not Path(args.output_dir).is_dir():
            try:
                Path(args.output_dir).mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise exc
    return args


def get_last_filenames(args):
    folder_path = args.output_dir
    log_filepath = folder_path / 'log.txt'
    with open(log_filepath, 'r') as fp:
        for line in fp.readlines():
            values = line.split(',')
            if check_valid_files(values, folder_path):
                return values
        else:
            raise ValueError(f"Project {args.name} not processed. "
                             f"Consider running mrqa, before mrqa_monitor")


def check_valid_files(values, folder_path):
    mtime, fname, _ = values
    report_path = folder_path / f'{fname}.html'
    mrds_path = folder_path / f'{fname}{MRDS_EXT}'
    if report_path.is_file() and mrds_path.is_file():
        return True
    raise FileNotFoundError(f'Could not find expected files'
                            f' {report_path} and {mrds_path}.')


def mrqa_monitor():
    args = parse_args()
    mtime, fname, _ = get_last_filenames(args)
    modified_files = get_files_by_mtime(args.data_root, mtime)

    last_mrds_fpath = Path(args.output_dir) / f"{fname}{MRDS_EXT}"
    last_mrds = load_mr_dataset(last_mrds_fpath)
    partial_dataset = import_dataset(data_source_folders=modified_files,
                                     style='dicom',
                                     name=args.name,
                                     verbose=args.verbose,
                                     include_phantom=args.include_phantom)
    last_mrds.merge(partial_dataset)
    check_compliance(dataset=last_mrds,
                     output_dir=args.output_dir)
    return


if __name__ == "__main__":
    sys.exit(mrqa_monitor())  # pragma: no cover
