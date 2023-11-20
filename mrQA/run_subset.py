""" Console script for running subset of dataset, a part of parallel
processing"""
import argparse
import sys
from pathlib import Path
from typing import Union

from MRdataset import import_dataset, save_mr_dataset, BaseDataset

from mrQA import logger
from mrQA.config import THIS_DIR
from mrQA.utils import txt2list


def parse_args():
    parser = get_parser()
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel('WARNING')
    else:
        logger.setLevel('ERROR')

    return args


def get_parser():
    """Console script for mrQA."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    required.add_argument('-o', '--output-path', type=str,
                          required=True,
                          help='complete path to pickle file for storing '
                               'partial dataset')
    required.add_argument('-b', '--batch-ids-file', type=str,
                          required=True,
                          help='text file path specifying the folders to read')
    optional.add_argument('-h', '--help', action='help',
                          default=argparse.SUPPRESS,
                          help='show this help message and exit')
    optional.add_argument('--is-partial', action='store_true',
                          help='flag dataset as a partial dataset')
    # TODO: use this flag to store cache
    optional.add_argument('-v', '--verbose', action='store_true',
                          help='allow verbose output on console')
    required.add_argument('--config', type=str,
                          help='path to config file',
                          default=THIS_DIR / 'resources/mri-config.json')

    if len(sys.argv) < 2:
        logger.critical('Too few arguments!')
        parser.print_help()
        parser.exit(1)

    return parser


def cli():
    """Console script for mrQA subset."""
    args = parse_args()
    output_path = Path(args.output_path).resolve()

    if not output_path.exists():
        partial_dataset = read_subset(output_dir=Path(args.output_path).parent,
                                      batch_ids_file=args.batch_ids_file,
                                      ds_format='dicom',
                                      verbose=args.verbose,
                                      config_path=args.config,
                                      is_complete=not args.is_partial)

        partial_dataset.is_complete = False
        save_mr_dataset(args.output_path, partial_dataset)


def read_subset(output_dir: Union[str, Path],
                batch_ids_file: str,
                ds_format: str,
                verbose: bool,
                config_path: str = None,
                is_complete: bool = True,
                **kwargs) -> BaseDataset:
    """
    Given a list of folder paths, reads all dicom files in those folders
    and returns a MRdataset object. In context, when this function was created,
    each folder corresponds to a different subject.

    Parameters
    ----------
    output_dir : Path | str
        path to a folder where the partial dataset will be saved
    batch_ids_file : str
        path to a text file containing a list of paths (to several folders)
    ds_format : str
        what kind of MRdataset to create, dicom, bids etc.
    verbose : bool
        print more while doing the job
    config_path : str
        path to config file
    is_complete : bool
        whether the dataset is subset of a larger dataset. It is useful for
        parallel processing of large datasets.
    **kwargs: dict
        additional arguments to pass to import_dataset


    Returns
    -------
    BaseDataset

    Raises
    ------
    NotImplementedError
        if ds_format is not dicom
    """
    # Supports only dicom for now
    if ds_format != 'dicom':
        raise NotImplementedError(
            f'Expected ds_format as dicom, Got {ds_format}')

    subset = txt2list(batch_ids_file)
    identifier = Path(batch_ids_file).stem
    partial_dataset = import_dataset(data_source=subset,
                                     ds_format=ds_format,
                                     name=identifier,
                                     verbose=verbose,
                                     config_path=config_path,
                                     output_dir=output_dir,
                                     is_complete=is_complete,
                                     **kwargs)
    # partial_dataset.load(), import_dataset already does this
    return partial_dataset


if __name__ == '__main__':
    sys.exit(cli())  # pragma: no cover
