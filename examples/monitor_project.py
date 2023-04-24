import argparse
import os.path
import sys
from pathlib import Path

from mrQA import monitor
from mrQA.utils import txt2list
from MRdataset.log import logger
from MRdataset.utils import valid_dirs
from MRdataset.config import DatasetEmptyException


def main():
    """Console script for mrQA."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    # Add help
    required.add_argument('-d', '--datasets-txt', type=str, required=True,
                          help='A txt file which contains a list of projects'
                               'to process')
    optional.add_argument('-o', '--output-dir', type=str,
                          default='/home/mrqa/mrqa_reports/',
                          help='specify the directory where the report'
                               ' would be saved. By default, the --data_source '
                               'directory will be used to save reports')

    args = parser.parse_args()
    if Path(args.datasets_txt).exists():
        datasets_path = txt2list(args.datasets_txt)
    else:
        raise ValueError("Need a valid path to a txt file, which consists of "
                         f"names of projects to process. "
                         f"Got {args.datasets_txt}")
    dirs = valid_dirs(datasets_path)

    for folder_path in dirs:
        name = Path(folder_path).stem
        print(f"\nProcessing {name}\n")
        output_folder = Path(args.output_dir) / name
        try:
            monitor(name=name,
                    data_source=folder_path,
                    output_dir=output_folder,
                    decimals=2,
                    )
        except DatasetEmptyException as e:
            logger.warning(f'{e}: Folder {name} has no DICOM files.')


if __name__ == "__main__":
    main()
