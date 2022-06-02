"""Console script for compliance."""
import argparse
import sys
from pathlib import Path
from MRdataset import create_dataset
from compliance.elements import project
from compliance.delta import diff

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
    required.add_argument('-m', '--metadataroot', type=str,
                          help='directory to store metadata files')
    required.add_argument('-n', '--name', type=str,
                         help='provide a identifier/name for the dataset')
    optional.add_argument('-r', '--reindex', action='store_true',
                          help='recreate compliance report')
    optional.add_argument('-v', '--verbose', action='store_true',
                          help='allow verbose output on console')
    optional.add_argument('-c', '--create', action='store_true',
                          help='create directories if required')
    required.add_argument('-s', '--style', type=str,
                          help='choose type of dataset, one of [xnat|bids|other]')
    required.add_argument('-p', '--protocol', type=str,
                          help='.yaml file containing protocol specification')

    args = parser.parse_args()
    if not Path(args.dataroot).is_dir():
        raise OSError('Expected valid directory for --dataroot argument, Got {0}'.format(args.dataroot))
    metadata_dir = Path(args.metadataroot)
    # print(metadata_dir)
    if not metadata_dir.is_dir():
        if args.create:
            metadata_dir.mkdir(parents=True, exist_ok=True)
        else:
            raise OSError(
                'Expected valid directory for --metadata argument. Use -c flag to create new directories automatically')
    dataset = create_dataset(args)
    proj = project.Project(dataset, args.protocol)
    # monitor = diff.Monitor(proj)
    proj.check_compliance()

    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
