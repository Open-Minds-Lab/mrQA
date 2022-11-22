from MRdataset import load_mr_dataset
from mrQA.utils import txt2list
from MRdataset.config import MRDS_EXT, CACHE_DIR
import warnings
from MRdataset import save_mr_dataset
from pathlib import Path


def check_partial_datasets(all_batches_txt, force=False):
    txt_path_list = txt2list(all_batches_txt)
    mrds_paths = []
    for txt_file in txt_path_list:
        saved_mrds_path = txt_file.with_suffix(MRDS_EXT)
        if saved_mrds_path.exists():
            size = saved_mrds_path.stat().st_size
            if not (size > 0):
                warnings.warn(f"Skipping file {saved_mrds_path}"
                              "which was not saved properly")
            else:
                mrds_paths.append(saved_mrds_path)
    return mrds_paths


def merge_partial_datasets(name, mrds_path_list, metadata_root):
    complete_dataset = merge_from_disk(name, mrds_path_list)
    complete_dataset.is_complete = True
    save_mr_dataset(name, metadata_root, complete_dataset)


def merge_from_disk(name, mrds_path_list):
    chunks = []
    for file in mrds_path_list:
        if file.is_file():
            try:
                temp_dict = load_mr_dataset(file)
                chunks.append(temp_dict)
            except OSError:
                print(f"Unable to read file: {file}")
    return merge_subset(chunks, name)


def merge_subset(list_, final_name):
    if len(list_) < 1:
        raise EOFError('Cannot merge an empty list!')
    head = list_[0]
    for next in list_[1:]:
        head.merge(next)
    head.name = final_name
    return head


def check_and_merge(name, all_batches_txtpaths, save_dir=None):
    mrds_paths = check_partial_datasets(all_batches_txtpaths)
    if save_dir is None:
        save_dir = Path.home() / CACHE_DIR
        save_dir.mkdir(exist_ok=True, parents=True)
    merge_partial_datasets(name, mrds_paths, save_dir)
