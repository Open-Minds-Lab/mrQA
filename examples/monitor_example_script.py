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
    optional.add_argument('-N', '--num-projects', type=int,
                          default=10,
                          help='Number of sub-folders to process.')

    args = parser.parse_args()
    dir_name = Path(args.data_source)
    if not dir_name.is_dir():
        raise NotADirectoryError(f'{dir_name} is not a directory')
    sub_dirs = sorted(dir_name.iterdir(),
                      key=os.path.getmtime,
                      reverse=True)[:args.N]
    for folder in sub_dirs:
        if folder.is_dir():
            name = Path(folder).stem
            output_folder = Path(args.output_dir)/name
            monitor(name=name,
                    data_source=folder,
                    output_dir=output_folder)


if __name__ == "__main__":
    main()
