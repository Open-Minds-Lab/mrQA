import argparse
import multiprocessing as mp
from pathlib import Path

from MRdataset.config import DatasetEmptyException
from MRdataset.log import logger
from MRdataset.utils import valid_dirs

from mrQA import monitor
from mrQA.utils import txt2list


def main():
    """Console script for mrQA."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    # Add help
    required.add_argument('-d', '--data-root', type=str, required=True,
                          help='A folder which contains projects'
                               'to process')
    optional.add_argument('-o', '--output-dir', type=str,
                          default='/home/mrqa/mrqa_reports/',
                          help='specify the directory where the report'
                               ' would be saved')
    optional.add_argument('-x', '--exclude-fpath', type=str,
                          help='A txt file containing a'
                               'list of folders to be skipped while'
                               'monitoring')
    args = parser.parse_args()
    if Path(args.data_root).exists():
        data_root = Path(args.data_root)
        non_empty_folders = []
        for folder in data_root.iterdir():
            if folder.is_dir() and any(folder.iterdir()):
                non_empty_folders.append(folder)
    else:
        raise ValueError("Need a valid path to a folder, which consists of "
                         f"projects to process. "
                         f"Got {args.data_root}")

    dirs = valid_dirs(non_empty_folders)

    if len(non_empty_folders) < 2:
        dirs = [dirs]
    if args.exclude_fpath is not None:
        if not Path(args.exclude_fpath).exists():
            raise FileNotFoundError("Need a valid filepath to the exclude list")
        exclude_filepath = Path(args.exclude_fpath).resolve()
        skip_list = [Path(i).resolve() for i in txt2list(exclude_filepath)]

        for fpath in dirs:
            if Path(fpath).resolve() in skip_list:
                dirs.remove(fpath)

    pool = mp.Pool(processes=10)
    arguments = [(f, args.output_dir) for f in dirs]
    pool.starmap(run, arguments)


def run(folder_path, output_dir):
    name = Path(folder_path).stem
    print(f"\nProcessing {name}\n")
    output_folder = Path(output_dir) / name
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
