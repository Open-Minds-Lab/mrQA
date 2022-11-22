import pickle

from MRdataset import import_dataset
from MRdataset.config import CACHE_DIR

"""Console script for mrQA."""
import argparse
import sys
from pathlib import Path

from MRdataset import import_dataset

from mrQA import check_compliance
from mrQA.common import set_logging
from mrQA.utils import default_thread_count, txt2list, save2pickle
import logging


def main():
    """Console script for mrQA."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    required.add_argument('-m', '--metadata_root', type=str, required=True,
                          help='directory containing pickle files for each '
                               'part')
    required.add_argument('-n', '--name', type=str,
                          help='provide a identifier/name for the dataset')
    required.add_argument('-b', '--batch_txt_file', type=str, required=True,
                          help='text file path specifying the directories')
    optional.add_argument('-h', '--help', action='help',
                          default=argparse.SUPPRESS,
                          help='show this help message and exit')
    # TODO: use this flag to store cache
    optional.add_argument('-r', '--reindex', action='store_true',
                          help='reindex dataset & regenerate mrQA report')
    optional.add_argument('-v', '--verbose', action='store_true',
                          help='allow verbose output on console')
    optional.add_argument('--include_phantom', action='store_true',
                          help='whether to include phantom, localizer, '
                               'aahead_scout')
    optional.add_argument('--metadata_root', type=str,
                          help='directory containing cache')
    optional.add_argument('-w', '--max_workers', type=int, default=-1,
                          help='max workers threads for processing'
                               ' in parallel')
    logger = set_logging('root')

    if len(sys.argv) < 2:
        logger.critical('Too few arguments!')
        parser.print_help()
        parser.exit(1)

    args = parser.parse_args()
    # if not Path(args.data_root).is_dir():
    #     raise OSError('Expected valid directory for --data_root argument, '
    #                   'Got {0}'.format(args.data_root))

    partial_dataset = read_subset(args.metadata_root,
                                  args.batch_txt_file, 'dicom',
                                  args.reindex, args.verbose,
                                  args.include_phantom)
    partial_dataset.set_cache_path()
    partial_dataset.is_complete = False
    save2pickle(partial_dataset)


def read_subset(name, seq_num, style, reindex, verbose, include_phantom,
                metadata_root):
    if not metadata_root:
        metadata_root = Path.home() / CACHE_DIR
        metadata_root.mkdir(exist_ok=True)

    if not Path(metadata_root).is_dir():
        raise OSError('Expected valid directory for --metadata_root argument,'
                      ' Got {0}'.format(metadata_root))

    metadata_root = Path(metadata_root).resolve()

    subset = txt2list(batch_txt_file)
    # for j, folder in enumerate(subset):
    identifier = batch_txt_file.stem
    partial_dataset = import_dataset(data_root=subset,
                                     style=style,
                                     name=identifier,
                                     reindex=reindex,
                                     verbose=verbose,
                                     include_phantom=include_phantom,
                                     metadata_root=metadata_root,
                                     save=False)
    return partial_dataset


def merge_subset(parent, final_name):
    master = None
    if len(parent) < 1:
        raise EOFError('Cannot merge an empty list!')
    master = parent[0]
    for child in parent[1:]:
        master.merge(child)
    master.name = final_name
    return master


def merge_from_disk(metadata_root, name, txt_path_list):
    chunks = []
    pkl_file_list = [filepath.with_suffix('.pkl') for filepath in txt_path_list]
    for file in pkl_file_list:
        if file.is_file():
            try:
                with open(file, 'rb') as f:
                    temp_dict = pickle.load(f)
                    chunks.append(temp_dict)
            except OSError:
                print(f"Unable to read file: {file}")
    return merge_subset(chunks, name)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
