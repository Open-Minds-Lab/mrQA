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

    valid_names = _projects_processed(PATH_CONFIG["output_dir"], ignore_case=True)
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


def get_last_filenames(name, output_dir):
    folder_path = output_dir
    log_filepath = folder_path / 'log.txt'
    with open(log_filepath, 'r') as fp:
        # we only need the last line, and reading all the lines is inefficient
        # in future, implement an approach to directly access the EOF.
        # See https://stackoverflow.com/a/54278929/3140172
        last_line = fp.readlines()[-1]
        # for line in fp.readlines():
        values = last_line.split(',')
        if check_valid_files(values, folder_path):
            return values


def check_valid_files(values, folder_path):
    mtime, fname, _ = values
    report_path = folder_path / f'{fname}.html'
    mrds_path = folder_path / f'{fname}{MRDS_EXT}'
    if report_path.is_file() and mrds_path.is_file():
        return True
    raise FileNotFoundError(f'Could not find expected files'
                            f' {report_path} and {mrds_path}.')


def main():
    args = parse_args()
    mrqa_monitor(args.name, args.data_source, args.output_dir)


def mrqa_monitor(name, data_source, output_dir):
    values = get_last_filenames(name, output_dir)
    if not values:
        raise ValueError(f'Dataset {name} not found in log. Consider using mrqa'
                         f'before mrqa_monitor')
    mtime, fname, _ = values
    modified_files = get_files_by_mtime(data_source, mtime)

    last_mrds_fpath = Path(output_dir) / f"{fname}{MRDS_EXT}"
    last_mrds = load_mr_dataset(last_mrds_fpath)
    # TODO : Add other arguments of import_dataset here?
    partial_dataset = import_dataset(data_source=modified_files,
                                     style='dicom',
                                     name=name)
    last_mrds.merge(partial_dataset)
    report_path = check_compliance(dataset=last_mrds,
                     output_dir=output_dir)
    return report_path


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
