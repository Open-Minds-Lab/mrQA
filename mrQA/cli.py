"""Console script for mrQA."""
import argparse
import sys
from pathlib import Path

from MRdataset import import_dataset, load_mr_dataset
from MRdataset.utils import is_writable, valid_dirs

from mrQA import check_compliance
from mrQA.config import PATH_CONFIG
from mrQA import logger
from mrQA.logger import init_log_files


def get_parser():
    """Console script for mrQA."""
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
                          help='path to config file')
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
                               'protocol. Default is 0.1')
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
    # Experimental features, not implemented yet.
    optional.add_argument('-l', '--logging', type=int, default=40,
                          help='set logging to appropriate level')
    optional.add_argument('--skip', nargs='+',
                          help='skip these parameters')

    if len(sys.argv) < 2:
        logger.critical('Too few arguments!')
        parser.print_help()
        parser.exit(1)

    return parser


def main():
    args = parse_args()
    if args.mrds_pkl_path:
        dataset = load_mr_dataset(args.mrds_pkl_path)
    else:
        dataset = import_dataset(data_source=args.data_source,
                                 ds_format=args.format,
                                 name=args.name,
                                 verbose=args.verbose,
                                 config_path=args.config)

    check_compliance(dataset=dataset,
                     output_dir=args.output_dir,
                     decimals=args.decimals,
                     verbose=args.verbose,
                     tolerance=args.tolerance,
                     config_path=args.config,
                     reference_path=args.ref_protocol_path, )
    return 0


def parse_args():
    parser = get_parser()
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel('INFO')
    else:
        logger.setLevel('WARNING')

    if not valid_dirs(args.data_source):
        raise OSError('Expected valid directory for --data_source argument, '
                      'Got {0}'.format(args.data_source))
    if not args.config:
        raise ValueError('Expected --config argument, '
                         'Got {0}'.format(args.config))

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
                raise exc

    if not is_writable(args.output_dir):
        raise OSError(f'Output Folder {args.output_dir} is not writable')

    if args.ref_protocol_path is not None:
        if not Path(args.ref_protocol_path).is_file():
            raise OSError(f'Expected valid file for --ref-protocol-path argument, '
                          'Got {0}'.format(args.ref_protocol_path))
    if args.mrds_pkl_path is not None:
        if not Path(args.mrds_pkl_path).is_file():
            raise OSError(f'Expected valid file for --mrds-pkl-path argument, '
                          'Got {0}'.format(args.mrds_pkl_path))

    return args


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
