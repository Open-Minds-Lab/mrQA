"""Console script for compliance."""
import argparse
import sys
from pathlib import Path
from MRdataset import create_dataset
from compliance import create_report
import logging

def main():
    """Console script for compliance."""
    parser = argparse.ArgumentParser(description='ProtocolCompliance, check consistency of dicom files',
                                     add_help=False)
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    # Add help
    optional.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS,
                          help='show this help message and exit')
    required.add_argument('-i', '--dataroot', type=str,
                          help='directory containing downloaded dataset with dicom files, supports nested hierarchies')
    optional.add_argument('-m', '--metadataroot', type=str,
                          help='directory to store metadata files')
    required.add_argument('-n', '--name', type=str,
                         help='provide a identifier/name for the dataset')
    optional.add_argument('-r', '--reindex', action='store_true',
                          help='recreate compliance report')
    optional.add_argument('-v', '--verbose', action='store_true',
                          help='allow verbose output on console')
    optional.add_argument('-c', '--create', action='store_true',
                          help='create directories if required')
    optional.add_argument('-s', '--style', type=str, default='xnat',
                          help='choose type of dataset, one of [xnat|bids|other]')
    optional.add_argument('-p', '--protocol', type=str,
                          help='.yaml file containing protocol specification')
    optional.add_argument('-l', '--logging', type=int, default=40,
                          help='set logging to appropriate level [0|10|20|30|40|50]')
    optional.add_argument('--probe', type=str, default='first',
                          help='how to examine parameters [reference|majority|first].'
                               'Option --protocol is required if using reference ')

    args = parser.parse_args()
    logging.basicConfig(filename=Path(args.metadataroot)/'execution.log',
                        format='%(asctime)s | %(levelname)s: %(message)s',
                        level=args.logging)

    if not Path(args.dataroot).is_dir():
        raise OSError('Expected valid directory for --dataroot argument, Got {0}'.format(args.dataroot))
    metadata_dir = Path(args.metadataroot)
    if not metadata_dir.is_dir():
        if args.create:
            metadata_dir.mkdir(parents=True, exist_ok=True)
        else:
            raise OSError(
                'Expected valid directory for --metadata argument. Use -c flag to create new directories automatically')
    dataset = create_dataset(args)
    report = create_report(dataset, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
