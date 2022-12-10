import pathlib
import pickle
import time, os
import warnings
from collections import Counter, defaultdict
from pathlib import Path
from typing import Union, List, Any, Generator

from MRdataset.utils import is_hashable


def timestamp():
    """Generate a timestamp as a string"""
    time_string = time.strftime("%m_%d_%Y_%H_%M")
    return time_string


def majority_attribute_values(iterable, missing=None):
    """
    Given a list of dictionaries, it generates the most common
    values for each key

    Parameters
    ----------
    iterable : list
        a list of dictionaries
    missing : python object, default None
        a default value if the key is missing in any dictionary

    Returns
    -------
    dict
        Key-value pairs specifying the most common values for each key
    """
    counts = {}
    categories = set(counts)
    for length, element in enumerate(iterable):
        categories.update(element)
        for cat in categories:
            try:
                counter = counts[cat]
            except KeyError:
                counts[cat] = counter = Counter({missing: 0})
            value = element.get(cat, missing)
            if not is_hashable(value):
                value = str(value)
            counter[value] += 1
    params = {}
    for k in counts.keys():
        params[k] = counts[k].most_common(1)[0][0]
    return params


def extract_reasons(data):
    """
    Given a list of tuples, extract all the elements at index 1, and return
    as a list

    Parameters
    ----------
    data : List of tuples

    Returns
    -------
    list
        List of values at index 1
    """
    return list(zip(*data))[1]


def default_thread_count():
    workers = min(32, os.cpu_count() + 4)
    return workers


def split_index(dir_index: list, num_chunks: int) -> Generator[List[str]]:
    """
    Adapted from https://stackoverflow.com/questions/2130016/splitting-a-list-into-n-parts-of-approximately-equal-length

    Given a list of n elements, split it into k parts, where k = num_chunks.
    Each part has atleast n/k elements. And the remaining elements
    n % k are distributed uniformly among the sub-parts such that
    each part has almost same number of elements. The first n % k will have
    floor(n/k) + 1 elements.

    Parameters
    ----------
    dir_index : list
        list to split
    num_chunks : int
        number of parts

    Returns
    -------
    tuple of all subsets
    """
    if num_chunks == 0:
        raise RuntimeError("Cannot divide list into chunks of size 0")
    if len(dir_index) == 0:
        raise RuntimeError("List of directories is empty!")
    if len(dir_index) < num_chunks:
        warnings.warn(f"Got num_chunks={num_chunks}, list_size={len(dir_index)}"
                             f"Expected num_chunks < list_size")
        num_chunks = len(dir_index)
    k, m = divmod(len(dir_index), num_chunks)
    #  k, m = (len(dir_index)//num_chunks, len(dir_index)%num_chunks)
    return (dir_index[i * k + min(i, m):(i + 1) * k + min(i + 1, m)]
            for i in range(num_chunks))


def txt2list(path):
    if not isinstance(path, Path):
        path = Path(path)
    if not path.exists():
        raise FileNotFoundError
    # with open(path, 'r') as fp:
    data = path.read_text().splitlines()
    return data


def list2txt(path, data):
    if Path(path).exists():
        warnings.warn("Overwriting pre-existing index on disk.")
    with open(path, 'w') as fp:
        for line in data:
            fp.write("%s\n" % line)


def create_index(data_root, metadata_root: pathlib.Path, name, reindex=False,
                 subjects_per_job=50):
    output_path = metadata_root / (name+'_index.txt')
    if output_path.exists() or not reindex:
        dir_index = txt2list(output_path)
    else:
        dir_index = []
        for root, dirs, files in os.walk(data_root):
            if 'sub-' in Path(root).name:
                dir_index.append(root)
        list2txt(output_path, list(set(dir_index)))

    workers = len(dir_index) // subjects_per_job
    index_subsets = split_index(dir_index, num_chunks=workers)
    for i, subset in enumerate(index_subsets):
        list2txt(metadata_root/(name+f'_master{i}.txt'), subset)
    return i+1


def save2pickle(dataset):
    if not dataset.modalities:
        raise EOFError('Dataset is empty!')
    with open(dataset.cache_path, "wb") as f:
        pickle.dump(dataset, f)
