import pickle

from MRdataset.config import CACHE_DIR

"""Console script for mrQA."""
import argparse
import sys
from pathlib import Path

from MRdataset import import_dataset, load_mr_dataset, save_mr_dataset
from MRdataset.config import MRDS_EXT
from mrQA.common import set_logging
from mrQA.utils import txt2list, save2pickle


def main():
    """Console script for mrQA."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    required.add_argument('-o', '--output_dir', type=str, required=True,
                          help='directory containing pickle files for each '
                               'part')
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
    logger = set_logging('root')

    if len(sys.argv) < 2:
        logger.critical('Too few arguments!')
        parser.print_help()
        parser.exit(1)

    args = parser.parse_args()
    txt_file_path = Path(args.batch_txt_file).resolve()
    save_filename = txt_file_path.with_suffix(MRDS_EXT)

    if not save_filename.exists() or args.reindex:
        partial_dataset = read_subset(args.output_dir,
                                      args.batch_txt_file, 'dicom',
                                      args.reindex, args.verbose,
                                      args.include_phantom)
        partial_dataset.set_cache_path()
        partial_dataset.is_complete = False
        save_mr_dataset(save_filename, args.output_dir, partial_dataset)


def read_subset(output_dir, batch_txt_file, style, reindex, verbose,
                include_phantom):
    if not output_dir:
        output_dir = Path.home() / CACHE_DIR
        output_dir.mkdir(exist_ok=True)

    if not Path(output_dir).is_dir():
        raise OSError('Expected valid directory for --output_dir argument,'
                      ' Got {0}'.format(output_dir))

    output_dir = Path(output_dir).resolve()

    if Path(batch_txt_file).exists():
        batch_txt_file = Path(batch_txt_file)
    else:
        raise FileNotFoundError(f'Invalid path : {batch_txt_file}')
    subset = txt2list(batch_txt_file)
    # for j, folder in enumerate(subset):
    identifier = batch_txt_file.stem
    partial_dataset = import_dataset(data_root=subset,
                                     style=style,
                                     name=identifier,
                                     reindex=reindex,
                                     verbose=verbose,
                                     include_phantom=include_phantom,
                                     metadata_root=output_dir,
                                     save=False)
    return partial_dataset


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
