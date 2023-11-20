"""Console script for mrQA."""
import argparse
import sys
from pathlib import Path

from MRdataset import import_dataset, load_mr_dataset, valid_dirs, \
    DatasetEmptyException

from mrQA import check_compliance
from mrQA import logger
from mrQA.config import PATH_CONFIG, THIS_DIR
from mrQA.utils import is_writable


def get_parser():
    """Parser for command line interface."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    # Add help
    required.add_argument('-d', '--data-source', nargs='+', required=True,
                          help='directory containing downloaded dataset with '
                               'dicom files, supports nested hierarchies')
    required.add_argument('--config', type=str,
                          help='path to config file',
                          default=THIS_DIR / 'resources/mri-config.json')
    optional.add_argument('-o', '--output-dir', type=str,
                          help='specify the directory where the report'
                               ' would be saved. By default, the --data_source '
                               'directory will be used to save reports')
    optional.add_argument('-f', '--format', type=str, default='dicom',
                          help='type of dataset, one of [dicom|bids]')
    optional.add_argument('-n', '--name', type=str,
                          help='provide a identifier/name for the dataset')
    optional.add_argument('-h', '--help', action='help',
                          default=argparse.SUPPRESS,
                          help='show this help message and exit')
    optional.add_argument('--decimals', type=int, default=3,
                          help='number of decimal places to round to '
                               '(default:0). If decimals are negative it '
                               'specifies the number of positions to the left'
                               'of the decimal point.')
    optional.add_argument('-t', '--tolerance', type=float, default=0,
                          help='tolerance for checking against reference '
                               'protocol. Default is 0')
    # TODO: use this flag to store cache
    optional.add_argument('-v', '--verbose', action='store_true',
                          help='allow verbose output on console')
    optional.add_argument('-ref', '--ref-protocol-path', type=str,
                          help='XML file containing desired protocol. If not '
                               'provided, the protocol will be inferred from '
                               'the dataset.')
    optional.add_argument('-pkl', '--mrds-pkl-path', type=str,
                          help='.mrds.pkl file can be provided to facilitate '
                               'faster re-runs.')
    if len(sys.argv) < 2:
        logger.critical('Too few arguments!')
        parser.print_help()
        parser.exit(1)

    return parser


def cli():
    """
    Console script for mrQA.
    """
    args = parse_args()
    if args.mrds_pkl_path:
        dataset = load_mr_dataset(args.mrds_pkl_path)
    else:
        dataset = import_dataset(data_source=args.data_source,
                                 ds_format=args.format,
                                 name=args.name,
                                 verbose=args.verbose,
                                 config_path=args.config,
                                 output_dir=args.output_dir)

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
    return 0


def parse_args():
    """Validates command line arguments and returns parsed arguments"""
    parser = get_parser()
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel('WARNING')
    else:
        logger.setLevel('ERROR')

    if not valid_dirs(args.data_source):
        raise OSError('Expected valid directory for --data_source argument, '
                      'Got {0}'.format(args.data_source))

    if args.output_dir is None:
        logger.info('Use --output-dir to specify dir for final directory. '
                    'Using default')
        args.output_dir = PATH_CONFIG['output_dir'] / args.name.lower()
        args.output_dir.mkdir(exist_ok=True, parents=True)
    else:
        if not Path(args.output_dir).is_dir():
            try:
                Path(args.output_dir).mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                logger.error(f'Unable to create folder {args.output_dir} for '
                             f'saving reports')
                raise exc

    if not is_writable(args.output_dir):
        raise OSError(f'Output Folder {args.output_dir} is not writable')

    check_path(args.config, '--config')
    check_path(args.ref_protocol_path, '--ref-protocol-path')
    check_path(args.mrds_pkl_path, '--mrds-pkl-path')
    return args


def check_path(path, arg_name):
    """Validates if the path is a valid file"""
    if path is not None:
        if not Path(path).is_file():
            raise OSError(
                f'Expected valid file for {arg_name} argument, Got {path}')


if __name__ == "__main__":
    cli()
