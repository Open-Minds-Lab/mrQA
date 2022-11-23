import pathlib
import pickle
import time, os
import warnings
from collections import Counter, defaultdict
from pathlib import Path
import subprocess
import numpy as np

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


def txt2list(txt_filepath):
    if not isinstance(txt_filepath, Path):
        txt_filepath = Path(txt_filepath).resolve()
    if not txt_filepath.exists():
        raise FileNotFoundError(f'Invalid path {txt_filepath}')
    # Generate a list of folder paths stored in given txt_file
    line_list = txt_filepath.read_text().splitlines()
    return line_list


def list2txt(path, list_):
    # if Path(path).exists():
    #     warnings.warn("Overwriting pre-existing index on disk.")
    with open(path, 'w') as fp:
        for line in list_:
            fp.write("%s\n" % line)


def create_index(data_root, metadata_root: pathlib.Path, name, reindex=False,
                 subjects_per_job=50):
    output_path = metadata_root / (name+'_index.txt')
    batch_txt_path_list = []
    if output_path.exists() or not reindex:
        warnings.warn(f"Found a pre-existing list of subjects on disk."
                      f"Reusing existing {output_path}, Use reindex?",
                      stacklevel=2)
        dir_index = txt2list(output_path)
    else:
        dir_index = []
        for root, dirs, files in os.walk(data_root):
            if 'sub-' in Path(root).name:
                dir_index.append(root)
        list2txt(output_path, list(set(dir_index)))

    if subjects_per_job < 1:
        raise RuntimeError("subjects_per_job cannot be less than 1.")
    elif subjects_per_job > len(dir_index):
        raise RuntimeError("Trying to create more jobs than total number of "
                           "subjects in the directory. Why?")
    workers = len(dir_index) // subjects_per_job
    if workers == 1:
        raise RuntimeError("Decrease number of subjects per job. Expected"
                           "workers > 1 for parallel processing. Got 1")
    index_subsets = split_index(dir_index, num_chunks=workers)
    for i, subset in enumerate(index_subsets):
        batch_filename = metadata_root/(name+f'_batch{i}.txt')
        list2txt(batch_filename, subset)
        batch_txt_path_list.append(batch_filename)
    return batch_txt_path_list


def save2pickle(dataset):
    if not dataset.modalities:
        raise EOFError('Dataset is empty!')
    with open(dataset.cache_path, "wb") as f:
        pickle.dump(dataset, f)


def execute_local(filename):
    format_params = "\n".join(['"File system outputs: %O',
                               'Maximum RSS size: %M',
                               'CPU percentage used: %P',
                               'Real Time: %E',
                               'User Time: %U',
                               'Sys Time: %S"'])
    return subprocess.Popen([
            '/usr/bin/time',
            '-f',
            format_params,
            'bash',
            filename
        ])


def get_outliers(data, m=25.0):
    """
    Check for outliers. Adapted from
    https://stackoverflow.com/a/16562028/3140172
    Parameters
    ----------
    data
    m

    Returns
    -------

    """
    d = np.abs(data - np.median(data))
    mdev = np.median(d)
    s = d/mdev if mdev else 0.
    if np.any(s > m):
        indices = np.argwhere(s > m).flatten()
        return indices
    return None
