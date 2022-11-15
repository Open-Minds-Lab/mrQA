import pickle
import warnings
from pathlib import Path
from itertools import repeat
from MRdataset.utils import random_name
from MRdataset.config import CACHE_DIR
from MRdataset import import_dataset

from mrQA.utils import split_index, create_index


def parallel_dataset(data_root=None,
                     style='dicom',
                     name=None,
                     reindex=False,
                     include_phantom=False,
                     verbose=False,
                     metadata_root=None,
                     include_nifti_header=False,
                     workers=None,
                     subjects_per_job=50):
    if style != 'dicom':
        raise NotImplementedError(f'Expects dicom, Got {style}')

    if not Path(data_root).is_dir():
        raise OSError('Expected valid directory for --data_root argument,'
                      ' Got {0}'.format(data_root))
    data_root = Path(data_root).resolve()

    if not metadata_root:
        metadata_root = Path.home() / CACHE_DIR
        metadata_root.mkdir(exist_ok=True)

    if not Path(metadata_root).is_dir():
        raise OSError('Expected valid directory for --metadata_root argument,'
                      ' Got {0}'.format(metadata_root))
    metadata_root = Path(metadata_root).resolve()

    if name is None:
        warnings.warn(
            'Expected a unique identifier for caching data. Got NoneType. '
            'Using a random name. Use --name flag for persistent metadata',
            stacklevel=2)
        name = random_name()

    dir_index = create_index(data_root, metadata_root, name, reindex)

    if workers is None:
        workers = len(dir_index) // subjects_per_job

    index_subsets = split_index(dir_index, num_chunks=workers)
    for i, subset in enumerate(index_subsets):
        sub_dataset = read_subset(subset, name, i, style, reindex, verbose,
                    include_phantom, metadata_root)
        master = merge_subset(sub_dataset, name+f'_master{i}')
        master.set_cache_path()
        save2pickle(master)
        # TODO: Debugging code, remove later
        if i > 1:
            break
    complete_data = merge_from_disk(metadata_root, name)
    return


def read_subset(subset, name, seq_num, style, reindex, verbose, include_phantom,
                metadata_root):
    parent_set = []
    for j, folder in enumerate(subset):
        identifier = name + f'_part{seq_num + j}'
        child_set = import_dataset(data_root=folder,
                                       style=style,
                                       name=identifier,
                                       reindex=reindex,
                                       verbose=verbose,
                                       include_phantom=include_phantom,
                                       metadata_root=metadata_root,
                                       save=False)
        parent_set.append(child_set)
    return parent_set


def save2pickle(dataset):
    if not dataset.modalities:
        raise EOFError('Dataset is empty!')
    with open(dataset.cache_path, "wb") as f:
        pickle.dump(dataset, f)


def merge_subset(parent, name):
    master = None
    if len(parent) < 1:
        raise EOFError('Cannot merge an empty list!')
    master = parent[0]
    for child in parent[1:]:
        master.merge(child)
    master.name = name
    return master


def merge_from_disk(metadata_root, name):
    chunks = []
    for file in metadata_root.rglob(name+'_master*'):
        if file.is_file():
            # chunks.append(file)
            with open(file, 'rb') as f:
                temp_dict = pickle.load(f)
                chunks.append(temp_dict)
    return merge_subset(chunks, name)


if __name__ == '__main__':
    parallel_dataset(data_root='/media/sinhah/extremessd/ABCD-375/dicom-baseline',
                 name='abcd-375',
                     reindex=True)
    # metadata_root = Path.home() / CACHE_DIR
    # create_index(data_root='/home/sinhah/datasets/TCIA_REMBRANDT_06-22-2015',
    #              metadata_root=metadata_root,
    #              name='rembrandt_debug')
