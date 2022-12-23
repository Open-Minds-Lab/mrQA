from pathlib import Path

from MRdataset import import_dataset, save_mr_dataset, load_mr_dataset
from MRdataset.config import MRDS_EXT
from MRdataset.log import logger
from MRdataset.utils import valid_paths

from mrQA import check_compliance
from mrQA.parallel_utils import _make_file_folders
from mrQA.run_parallel import process_parallel, split_ids_list
from mrQA.utils import txt2list

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

    # For each batch create the list of ids to be processed
    ids_path_list = split_ids_list(
        data_source,
        all_ids_path=all_ids_path,
        per_batch_ids=files_per_batch['ids'],
        output_dir=folder_paths['ids'],
        subjects_per_job=5
    )

    # The paths to the output files
    output_path = {i: output_dir/f'seq{i}{MRDS_EXT}'
                   for i in range(len(ids_path_list))}
    ds_list = []
    for i, filepath in enumerate(ids_path_list):
        # Read the list of subject ids to be processed
        subject_folders_list = txt2list(filepath)
        if not output_path[i].exists():
            # Process the batch of subjects
            ds = import_dataset(data_source_folders=subject_folders_list,
                                style='dicom',
                                name=f'seq{i}')
            save_mr_dataset(output_path[i], ds)
        else:
            ds = load_mr_dataset(output_path[i])
        ds_list.append(ds)

    # Merge batches
    combined_mrds = None
    for ds in ds_list:
        if combined_mrds is None:
            # Add the first partial dataset
            combined_mrds = ds
        else:
            # otherwise, keep aggregating
            combined_mrds.merge(ds)

    # Check if both datasets are the same
    assert is_same_dataset(combined_mrds, sequential_ds)


def is_same_dataset(dataset1, dataset2):
    modalities_list1 = sorted(dataset1.modalities)
    modalities_list2 = sorted(dataset2.modalities)
    for modality1, modality2 in zip(modalities_list1, modalities_list2):
        assert modality1.name == modality2.name
        assert modality1.compliant == modality2.compliant
        assert modality1._reference == modality2._reference
        assert modality1.non_compliant_data.equals(modality2.non_compliant_data)
        subjects_list1 = sorted(modality1.subjects)
        subjects_list2 = sorted(modality2.subjects)
        for subject1, subject2 in zip(subjects_list1, subjects_list2):
            assert subject1.name == subject2.name
            assert subject1.__dict__ == subject2.__dict__
            sessions_list1 = sorted(subject1.sessions)
            sessions_list2 = sorted(subject2.sessions)
            for session1, session2 in zip(sessions_list1, sessions_list2):
                assert session1.name == session2.name
                assert session1.__dict__ == session2.__dict__
                runs_list1 = sorted(session1.runs)
                runs_list2 = sorted(session2.runs)
                for run1, run2 in zip(runs_list1, runs_list2):
                    assert run1.__dict__ == run2.__dict__
                    assert run1.name == run2.name
                    assert run1.params == run2.params
    return True


def is_same(file1, file2):
    file1, file2 = valid_paths([file1, file2])
    with open(file1) as f1, open(file2) as f2:
        for line1, line2 in zip(f1, f2):
            if line1 != line2:
                return False
    return True


if __name__ == '__main__':
    # test_equivalence_seq_vs_parallel(
    #     '/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/')
    test_merging('/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/')
