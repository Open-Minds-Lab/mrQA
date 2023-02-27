import argparse
import os.path
import sys
from pathlib import Path

from mrQA import monitor


def main():
    """Console script for mrQA."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    # Add help
    required.add_argument('-d', '--data-source', type=str, required=True,
                          help='directory containing downloaded dataset with '
                               'dicom files, supports nested hierarchies')
    optional.add_argument('-o', '--output-dir', type=str,
                          default='/home/mrqa/mrqa_reports/',
                          help='specify the directory where the report'
                               ' would be saved. By default, the --data_source '
                               'directory will be used to save reports')
    args = parser.parse_args()
    dir_name = Path(args.data_source)
    if not dir_name.is_dir():
        raise NotADirectoryError(f'{dir_name} is not a directory')
    for sub_dir in sorted(dir_name.iterdir(), key=os.path.getmtime):
        if sub_dir.is_dir():
            monitor(name=Path(dir_name).stem,
                    data_source=sub_dir,
                    output_dir=args.output_dir)


if __name__ == "__main__":
    main()

