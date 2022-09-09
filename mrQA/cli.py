"""Console script for mrQA."""
import argparse
import sys
from pathlib import Path

from MRdataset import import_dataset

from mrQA import check_compliance
from mrQA.common import set_logging

import logging


def main():
    """Console script for mrQA."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    # Add help
    required.add_argument('-d', '--data_root', type=str, required=True,
                          help='directory containing downloaded dataset with '
                               'dicom files, supports nested hierarchies')
    optional.add_argument('-o', '--output_dir', type=str,
                          help='specify the directory where the report'
                               ' would be saved. By default, the --data_root '
                               'directory will be used to save reports')
    optional.add_argument('-s', '--style', type=str, default='xnat',
                          help='type of dataset, one of [xnat|bids|other]')
    optional.add_argument('-n', '--name', type=str,
                          help='provide a identifier/name for the dataset')
    optional.add_argument('-h', '--help', action='help',
                          default=argparse.SUPPRESS,
                          help='show this help message and exit')
    # TODO: use this flag to store cache
    optional.add_argument('-r', '--reindex', action='store_true',
                          help='reindex dataset & regenerate mrQA report')
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
    optional.add_argument('--metadata_root', type=str,
                          help='directory containing cache')
    # Experimental features, not implemented yet.
    optional.add_argument('-l', '--logging', type=int, default=40,
                          help='set logging to appropriate level')
    optional.add_argument('--skip', nargs='+',
                          help='skip these parameters')

    logger = set_logging('root')

    if len(sys.argv) < 2:
        logger.critical('Too few arguments!')
        parser.print_help()
        parser.exit(1)

    args = parser.parse_args()
    if not Path(args.data_root).is_dir():
        raise OSError('Expected valid directory for --data_root argument, '
                      'Got {0}'.format(args.data_root))

    if args.output_dir is None:
        logger.info('Use --output_dir to specify dir for final directory. '
                    'By default, report will be saved at present working '
                    'directory')
        args.output_dir = Path().cwd()
    else:
        if not Path(args.output_dir).is_dir():
            try:
                Path(args.output_dir).mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise exc

    dataset = import_dataset(data_root=args.data_root,
                             style=args.style,
                             name=args.name,
                             reindex=args.reindex,
                             verbose=args.verbose,
                             include_phantom=args.include_phantom,
                             metadata_root=args.metadata_root)
    check_compliance(dataset=dataset,
                     strategy=args.strategy,
                     output_dir=args.output_dir,
                     reference_path=args.reference_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
