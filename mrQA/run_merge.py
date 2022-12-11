from MRdataset import load_mr_dataset
from mrQA.utils import txt2list, get_outliers
from MRdataset.config import MRDS_EXT, CACHE_DIR
import warnings
from MRdataset import save_mr_dataset
from pathlib import Path
from typing import List
import MRdataset


def check_partial_datasets(all_batches_mrds: str,
                           force: bool = False) -> List[Path]:
    # Read the text file to a list of paths
    mrds_path_list = txt2list(all_batches_mrds)

    # Check all the paths and cast them to pathlib.Path
    valid_mrds_paths = []
    for file in mrds_path_list:
        filepath = Path(file)
        # Check for existence of regular fike
        # and checks if not directory
        if filepath.is_file():
            valid_mrds_paths.append(filepath)

    # Check if all the files in mrds_path_list are valid!
    # Otherwise, raise error.
    # User may use the argument --force to continue
    if len(valid_mrds_paths) < len(mrds_path_list):
        if force:
            warnings.warn(f"Files for {len(mrds_path_list)-len(valid_mrds_paths)}"
                          f" batches were not found. Skipping! "
                          f"Using The Force to continue!")
        else:
            raise FileNotFoundError("Some txt files were not found!")

    mrds_paths = []
    # mrds_sizes = []
    for file in valid_mrds_paths:
        size = file.stat().st_size
        if size > 0:
            # mrds_sizes.append(size)
            mrds_paths.append(file)

    # outlier_idxs = get_outliers(mrds_sizes)
    # if not outlier_idxs:
    #     return mrds_paths
    # bad_files = "\n".join([str(mrds_paths[i]) for i in outlier_idxs])
    # if force:
    #     warnings.warn(f"File size for these files seem too off, assuming that "
    #                   f"subjects were equally divided among all the jobs\n"
    #                   f"{bad_files}"
    #                   f"Using The Force to merge anyway")
    # else:
    #     raise RuntimeError(f"File size for these files seem too off, "
    #                        f"assuming that subjects were equally divided"
    #                        f" among all the jobs\n"
    #                        f"{bad_files}\n"
    #                        f"Use The Force to merge anyway. Or re-submit "
    #                        f"jobs for these files.")
    return mrds_paths


def merge_and_save(name: str,
                   mrds_path_list: List[str],
                   save_folder: str):
    """
    Given a list of paths, each pointing to a partial mrds pickle file.
    Merges all mrds pickle file into a single mrds pickle, and saves to
    disk

    Parameters
    ----------
    name: str
        Provide a name for final pickle file on disk
    mrds_path_list: List[str]
        List of mrds pickle files to be merged
    save_folder: str
        Folder to save the file
    """
    if isinstance(save_folder, str):
        save_folder = Path(save_folder)
    if not isinstance(save_folder, Path):
        raise RuntimeError(f"Expect save_folder to be one of "
                           f"[str|pathlib.Path]. Got {type(save_folder)}")
    complete_dataset = merge_from_disk(mrds_path_list)
    complete_dataset.is_complete = True
    filename = save_folder / (name + MRDS_EXT)
    save_mr_dataset(filename, complete_dataset)


def merge_from_disk(mrds_path_list: List[str]) -> MRdataset.base.Project:
    """
    Given a list of paths to partial mrds datasets, read and merge
    Keep aggregating along with the loop.

    Parameters
    ----------
    mrds_path_list : List[str]
        List of paths to mrds datasets to merge.

    Returns
    -------
    complete_mrds: MRdataset.base.Project
        Complete merged mrds file
    """
    complete_mrds = None
    for file in mrds_path_list:
        # Check if it is valid path to file, i.e. it exists, and
        # it is not a directory
        filepath = Path(file)
        if filepath.is_file():
            try:
                partial_mrds = load_mr_dataset(filepath)
                if complete_mrds is None:
                    # Add the first partial dataset
                    complete_mrds = partial_mrds
                else:
                    # otherwise, keep aggregating, and return in the end
                    complete_mrds.merge(partial_mrds)
            except FileNotFoundError:
                print(f"Unable to read file: {filepath}")
    return complete_mrds


def check_and_merge(name: str, mrds_paths: List[str], save_dir: str = None):
    mrds_paths = check_partial_datasets(mrds_paths)
    if save_dir is None:
        raise AttributeError("Pass a directory to save the file!")
    merge_and_save(name, mrds_paths, save_dir)
