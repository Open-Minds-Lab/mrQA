from MRdataset import load_mr_dataset
from mrQA.utils import txt2list, get_outliers
from MRdataset.config import MRDS_EXT, CACHE_DIR
import warnings
from MRdataset import save_mr_dataset
from pathlib import Path
from typing import List


def check_partial_datasets(all_batches_mrds: str,
                           force: bool = False) -> List[Path]:
    mrds_path_list = txt2list(all_batches_mrds)
    valid_mrds_paths = []
    for file in mrds_path_list:
        if Path(file).exists():
            valid_mrds_paths.append(Path(file))

    if len(valid_mrds_paths) < len(mrds_path_list):
        if force:
            warnings.warn(f"Files for {len(mrds_path_list)-len(valid_mrds_paths)}"
                          f" batches were not found. Skipping! "
                          f"Using The Force to continue!")
        else:
            raise FileNotFoundError("Some txt files were not found!")

    mrds_paths = []
    mrds_sizes = []
    for file in valid_mrds_paths:
        if file.exists():
            size = file.stat().st_size
            mrds_sizes.append(size)
            mrds_paths.append(file)

    outlier_idxs = get_outliers(mrds_sizes)
    if not outlier_idxs:
        return mrds_paths
    bad_files = "\n".join([str(mrds_paths[i]) for i in outlier_idxs])
    if force:
        warnings.warn(f"File size for these files seem too off, assuming that "
                      f"subjects were equally divided among all the jobs\n"
                      f"{bad_files}"
                      f"Using The Force to merge anyway")
    else:
        raise RuntimeError(f"File size for these files seem too off, "
                           f"assuming that subjects were equally divided"
                           f" among all the jobs\n"
                           f"{bad_files}\n"
                           f"Use The Force to merge anyway. Or re-submit "
                           f"jobs for these files.")
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

    complete_dataset = merge_from_disk(name, mrds_path_list)
    complete_dataset.is_complete = True
    filename = save_folder / (name + MRDS_EXT)
    save_mr_dataset(filename, complete_dataset)


def merge_from_disk(name, mrds_path_list):
    chunks = []
    for file in mrds_path_list:
        # Check if it is valid path to file, i.e. it exists and
        # it is not a directory
        if file.is_file():
            try:
                temp_dict = load_mr_dataset(file)
                chunks.append(temp_dict)
            except OSError:
                print(f"Unable to read file: {file}")
    return merge_subset(chunks, name)



def check_and_merge(name, mrds_paths, save_dir=None):
    mrds_paths = check_partial_datasets(mrds_paths)
    if save_dir is None:
        raise AttributeError("Pass a directory to save the file!")
    merge_partial_datasets(name, mrds_paths, save_dir)
