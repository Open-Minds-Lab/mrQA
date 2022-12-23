from MRdataset import import_dataset, save_mr_dataset, load_mr_dataset
from mrQA import check_compliance
from pathlib import Path
from mrQA.run_parallel import process_parallel, split_ids_list
from MRdataset.config import MRDS_EXT
from MRdataset.log import logger
from mrQA.run_parallel import create_script, submit_job
from MRdataset.utils import valid_paths
from mrQA.parallel_utils import _make_file_folders
from mrQA.utils import txt2list, list2txt
import time
import itertools
from mrQA.run_merge import check_and_merge

dummy_DS = []
logger.setLevel('WARNING')


def test_equivalence_seq_vs_parallel(data_source):
    output_dir = Path(data_source).parent / 'test_mrqa_files'
    output_path = {
        'sequential': output_dir / ('sequential' + MRDS_EXT),
        'parallel': output_dir / ('parallel' + MRDS_EXT)
    }
    if not output_path['sequential'].exists():
        sequential_ds = import_dataset(data_source_folders=data_source,
                                       style='dicom',
                                       name='sequential')
        save_mr_dataset(output_path['sequential'], sequential_ds)
    else:
        sequential_ds = load_mr_dataset(output_path['sequential'])

    process_parallel(data_source,
                     output_dir,
                     output_path['parallel'],
                     'parallel')

    # Generate a report for the merged dataset
    parallel_ds = load_mr_dataset(output_path['parallel'], style='dicom')

    report_path = {
        'sequential': check_compliance(dataset=sequential_ds,
                                       output_dir=output_dir),
        'parallel': check_compliance(dataset=parallel_ds,
                                     output_dir=output_dir)
    }

    if is_same(report_path['sequential'], report_path['parallel']):
        print("Reports are same")
    else:
        print('Reports are different')


def test_merging(data_source):
    # Sequential complete processing of the dataset
    output_dir = Path(data_source).parent / 'test_merge_mrqa_files'
    output_path_seq = output_dir / ('sequential' + MRDS_EXT)

    if not output_path_seq.exists():
        sequential_ds = import_dataset(data_source_folders=data_source,
                                       style='dicom',
                                       name='sequential')
        save_mr_dataset(output_path_seq, sequential_ds)
    else:
        sequential_ds = load_mr_dataset(output_path_seq)

    # Start processing in batches
    folder_paths, files_per_batch, all_ids_path = _make_file_folders(output_dir)
    ids_path_list = split_ids_list(
        data_source,
        all_ids_path=all_ids_path,
        per_batch_ids=files_per_batch['ids'],
        output_dir=folder_paths['ids'],
        subjects_per_job=5
    )
    output_path = {i: f'seq{i}{MRDS_EXT}' for i in range(len(ids_path_list))}
    ds_list = []
    for i, filepath in enumerate(ids_path_list):
        folders = txt2list(filepath)
        if not output_path[i].exists():
            ds = import_dataset(data_source_folders=folders,
                                style='dicom',
                                name=f'seq{i}')
            save_mr_dataset(output_path[i], ds)
        else:
            ds = load_mr_dataset(output_path['sequential'])
        ds_list.append(ds)

    # Merge batches
    combined_mrds = None
    for ds in ds_list:
        if combined_mrds is None:
            # Add the first partial dataset
            combined_mrds = ds
        else:
            # otherwise, keep aggregating, and return in the end
            combined_mrds.merge(ds)






def is_same(file1, file2):
    file1, file2 = valid_paths([file1, file2])
    with open(file1) as f1, open(file2) as f2:
        for line1, line2 in zip(f1, f2):
            if line1 != line2:
                return False
    return True


if __name__ == '__main__':
    test_equivalence_seq_vs_parallel(
        '/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/')
    # test_equivalence_in_combinations()
