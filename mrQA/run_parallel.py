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


def create_slurm_script(filename, dataset_name, seq_no, name):
    with open(filename, 'w') as fp:
        fp.write('# !/bin/bash')
        fp.write('# SBATCH -A med220005p')
        fp.write('# SBATCH -N 1')
        fp.write('# SBATCH -p RM-shared')
        fp.write('# SBATCH --time=01:05:00')
        fp.write('# SBATCH --ntasks-per-node=1')
        fp.write(f'# SBATCH --error={dataset_name}.master{seq_no}.%J.err')
        fp.write(f'# SBATCH --output={dataset_name}.master{seq_no}.%J.out')

        fp.write('# Clear the environment from any previously loaded modules')
        fp.write('# module purge > /dev/null 2>&1')

        # fp.write(' Use '&' to start the first job in the background')
        fp.write('source ${HOME}/.bashrc')
        fp.write('conda activate mrqa')
        fp.write(f'mrpc_subset  -i {seq_no} --style dicom --name {name} &)')
        fp.write('wait')
        fp.write('date')
        fp.write('echo - e "\n\n\n\n--------------------------------\nDownload completed"')


if __name__ == '__main__':
    parallel_dataset(data_root='/media/sinhah/extremessd/ABCD-375/dicom-baseline',
                 name='abcd-375',
                     reindex=True)
    # metadata_root = Path.home() / CACHE_DIR
    # create_index(data_root='/home/sinhah/datasets/TCIA_REMBRANDT_06-22-2015',
    #              metadata_root=metadata_root,
    #              name='rembrandt_debug')
