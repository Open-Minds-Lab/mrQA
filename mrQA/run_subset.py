import MRdataset.base
from MRdataset.config import CACHE_DIR

"""Console script for mrQA."""
import argparse
import sys
from pathlib import Path

from MRdataset import import_dataset, save_mr_dataset
from mrQA.common import set_logging
from mrQA.utils import txt2list

from typing import Union

def main():
    """Console script for mrQA."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    required.add_argument('-o', '--output_path', type=str, required=True,
                          help='complete path to pickle file for storing '
                               'partial dataset')
    required.add_argument('-b', '--batch_txt_file', type=str, required=True,
                          help='text file path specifying the folders to read')
    optional.add_argument('-h', '--help', action='help',
                          default=argparse.SUPPRESS,
                          help='show this help message and exit')
    # TODO: use this flag to store cache
    optional.add_argument('-r', '--reindex', action='store_true',
                          help='rebuild cache, ignore existing')
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
    output_path = Path(args.output_path).resolve()
    if not output_path.exists() or args.reindex:
        partial_dataset = read_subset(output_path=args.output_path,
                                      batch_ids_file=args.batch_txt_file,
                                      style='dicom',
                                      reindex=args.reindex,
                                      verbose=args.verbose,
                                      include_phantom=args.include_phantom)
        partial_dataset.is_complete = False
        save_mr_dataset(args.output_path, partial_dataset)


def read_subset(output_path: Union[str, Path],
                batch_ids_file: str,
                style: str,
                reindex: bool,
                verbose: bool,
                include_phantom: bool) -> MRdataset.base.Project:
    """
    Given a list of folder paths, reads all dicom files in those folders
    and returns a MRdataset object. In context, when this function was created,
    each folder corresponds to a different subject.

    Parameters
    ----------
    output_path : str
        where should the MRdataset object should be saved finally
    batch_ids_file : str
        path to a text file containing a list of paths (to several folders)
    style : str
        what kind of MRdataset to create, dicom, bids etc.
    reindex : bool
        re-create the files, even if they exist on disk
    verbose : bool
        print more while doing the job
    include_phantom : bool
        whether to include phantom files in processing

    Returns
    -------
    MRdataset.base.Project

    """
    if not output_path:
        output_dir = Path.home() / CACHE_DIR
        output_dir.mkdir(exist_ok=True)
    else:
        output_dir = Path(output_path).parent

    if style != 'dicom':
        raise NotImplementedError(f"Expected style as dicom, Got {style}")

    if Path(batch_ids_file).exists():
        batch_ids_file = Path(batch_ids_file)
    else:
        raise FileNotFoundError(f'Invalid path : {batch_ids_file}')
    subset = txt2list(batch_ids_file)
    identifier = batch_ids_file.stem
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
