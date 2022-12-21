from pathlib import Path
from typing import List, Union

from MRdataset import load_mr_dataset
from MRdataset import save_mr_dataset
from MRdataset.base import BaseDataset
from MRdataset.config import MRDS_EXT

from mrQA.utils import txt2list


def _check_partial_datasets(mrds_list_filepath: str) -> List[Path]:
    """
    Given a list of paths, check if all of them are valid and all of them
    have a non-zero size. Return list of valid paths

    Parameters
    ----------
    mrds_list_filepath : str
    Path to a text file, which contains the paths to all partial mrds files

    Returns
    -------
    mrds_paths : List[Path]
    List of paths pointing to valid partial mrds files
    """

    # Read the text file to a list of paths
    mrds_path_list = txt2list(mrds_list_filepath)

    # Check all the paths and cast them to pathlib.Path
    valid_mrds_paths = []
    invalid_mrds_paths = []
    for file in mrds_path_list:
        filepath = Path(file)
        # Check for existence of regular file
        if filepath.is_file() and filepath.stat().st_size > 0:
            valid_mrds_paths.append(filepath)
        else:
            invalid_mrds_paths.append(filepath)

    if len(invalid_mrds_paths) > 0:
        raise FileNotFoundError(f"Invalid mrds files: {invalid_mrds_paths}")

    return valid_mrds_paths


def _merge_and_save(mrds_path_list: List[Path],
                    output_path: str,
                    name: str = None) -> None:
    """
    Given a list of paths, each pointing to a partial mrds pickle file.
    Merges all mrds pickle file into a single mrds pickle, and saves to
    disk

    Parameters
    ----------
    name: str
        Provide a name for final pickle file on disk
    mrds_path_list: List[Path]
        List of mrds pickle files to be merged
    output_path: str
        Folder to save the file
    """
    if isinstance(output_path, str):
        output_path = Path(output_path)
    complete_dataset = _merge_from_disk(mrds_path_list)
    complete_dataset.is_complete = True
    if name:
        complete_dataset.name = name
    save_mr_dataset(output_path, complete_dataset)


def _merge_from_disk(mrds_path_list: Union[List[Path], List[str]]) \
                    -> BaseDataset:
    """
    Given a list of paths to partial mrds datasets, read and merge
    Keep aggregating along with the loop.

    Parameters
    ----------
    mrds_path_list : List[Path]
        List of paths to mrds datasets to merge.

    Returns
    -------
    complete_mrds: MRdataset.base.Project
        Complete merged mrds file
    """
    complete_mrds = None
    for file in mrds_path_list:
        # Load the partial mrds file
        partial_mrds = load_mr_dataset(file)
        if complete_mrds is None:
            # Add the first partial dataset
            complete_mrds = partial_mrds
        else:
            # otherwise, keep aggregating, and return in the end
            complete_mrds.merge(partial_mrds)
    return complete_mrds


def check_and_merge(mrds_list_filepath: Union[str, Path],
                    output_path: Union[str, Path] = None,
                    name: str = None) -> None:
    """
    Entry point function to merge partial datasets.
     Use this function, and other function will be called internally.

    Parameters
    ----------
    name : str
        filename to use while saving the dataset. Typically, it can be
        name of the study project, like ABCD, Oasis etc.
    mrds_list_filepath : str
        Filepath to a text file which contains complete list of paths
        each pointing to a partial mrds file
    output_path : Union[str, Path]
        Specify folder to save the final file

    Returns
    -------

    """
    valid_mrds_list = _check_partial_datasets(mrds_list_filepath)
    if output_path is None:
        raise AttributeError("Pass a file path to save the file!")
    _merge_and_save(valid_mrds_list, output_path, name)
