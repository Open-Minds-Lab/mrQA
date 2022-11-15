import pathlib
import time, os
import warnings
from collections import Counter, defaultdict
from pathlib import Path

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


def split_index(dir_index, num_chunks):
    """
    https://stackoverflow.com/questions/2130016/splitting-a-list-into-n-parts-of-approximately-equal-length
    Parameters
    ----------
    dir_index
    num_chunks

    Returns
    -------

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


def create_index(data_root, metadata_root: pathlib.Path, name, reindex=False):
    output_path = metadata_root / (name+'_index.txt')
    if output_path.exists():
        if not reindex:
            dir_index = txt2list(output_path)
            return dir_index

    dir_index = []
    for root, dirs, files in os.walk(data_root):
        # if not dirs:
        if 'sub-' in Path(root).name:
            dir_index.append(root)
    list2txt(output_path, dir_index)
    return dir_index
