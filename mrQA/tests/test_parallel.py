import shutil
import tempfile
from pathlib import Path

from MRdataset import import_dataset, save_mr_dataset, load_mr_dataset
from MRdataset.config import MRDS_EXT
from mrQA import check_compliance
from mrQA.parallel_utils import _make_file_folders
from mrQA.run_parallel import process_parallel, split_folders_list
from mrQA.tests.conftest import THIS_DIR
from mrQA.tests.simulate import sample_dicom_dataset
from mrQA.utils import txt2list

dummy_DS = []


def test_equivalence_seq_vs_parallel():
    with tempfile.TemporaryDirectory() as tempdir:
        shutil.copytree(sample_dicom_dataset(), tempdir, dirs_exist_ok=True)
        data_source = tempdir
        config_path = THIS_DIR / 'resources/mri-config.json'
        output_dir = Path(data_source) / 'test_mrqa_files'
        output_path = {
            'sequential': output_dir / ('sequential' + MRDS_EXT),
            'parallel': output_dir / ('parallel' + MRDS_EXT)
        }
        if not output_path['sequential'].exists():
            sequential_ds = import_dataset(data_source=data_source,
                                           ds_format='dicom',
                                           name='sequential',
                                           config_path=config_path,
                                           output_dir=output_dir)
            save_mr_dataset(output_path['sequential'], sequential_ds)
        else:
            sequential_ds = load_mr_dataset(output_path['sequential'])

        process_parallel(data_source=data_source,
                         output_dir=output_dir,
                         out_mrds_path=output_path['parallel'],
                         name='parallel',
                         job_size=5,
                         config_path=config_path,
                         hpc=False, )

        # Generate a report for the merged dataset
        parallel_ds = load_mr_dataset(output_path['parallel'])

        results = {
            'sequential': check_compliance(dataset=sequential_ds,
                                           output_dir=output_dir,
                                           config_path=config_path),
            'parallel': check_compliance(dataset=parallel_ds,
                                         output_dir=output_dir,
                                         config_path=config_path)
        }
    # check hz_audit_results
    sequential_hz_results, sequential_vt_results, report_path = results['sequential']
    parallel_hz_results, parallel_vt_results, report_path = results['parallel']

    assert sequential_hz_results['complete_ds'] == parallel_hz_results[
        'complete_ds']
    assert sequential_hz_results['reference'] == parallel_hz_results[
        'reference']
    assert sequential_hz_results['compliant'] == parallel_hz_results[
        'compliant']
    assert sequential_hz_results['non_compliant'] == parallel_hz_results[
        'non_compliant']
    assert sequential_hz_results['undetermined'] == parallel_hz_results[
        'undetermined']

    assert sequential_vt_results['complete_ds'] == parallel_vt_results[
        'complete_ds']
    assert sequential_vt_results['sequence_pairs'] == parallel_vt_results[
        'sequence_pairs']
    assert sequential_vt_results['compliant'] == parallel_vt_results[
        'compliant']
    assert sequential_vt_results['non_compliant'] == parallel_vt_results[
        'non_compliant']
    assert sequential_vt_results['parameters'] == parallel_vt_results[
        'parameters']


def test_merging():
    # Sequential complete processing of the dataset
    data_source = sample_dicom_dataset()
    with tempfile.TemporaryDirectory() as tempdir:
        output_dir = Path(tempdir)
        output_path_seq = output_dir / ('sequential' + MRDS_EXT)
        config_path = THIS_DIR / 'resources/mri-config.json'

        if not output_path_seq.exists():
            sequential_ds = import_dataset(data_source=data_source,
                                           ds_format='dicom',
                                           name='sequential',
                                           config_path=config_path,
                                           output_dir=output_dir)
            save_mr_dataset(output_path_seq, sequential_ds)
        else:
            sequential_ds = load_mr_dataset(output_path_seq)

        # Start processing in batches
        folder_paths, files_per_batch, all_ids_path = _make_file_folders(output_dir)

        # For each batch create the list of ids to be processed
        ids_path_list = split_folders_list(
            data_source,
            all_fnames_path=all_ids_path,
            per_batch_ids=files_per_batch['fnames'],
            output_dir=folder_paths['fnames'],
            folders_per_job=5
        )

        # The paths to the output files
        output_path = {i: folder_paths['mrds']/f'seq{i}{MRDS_EXT}'
                       for i in range(len(ids_path_list))}
        ds_list = []
        for i, filepath in enumerate(ids_path_list):
            # Read the list of subject ids to be processed
            subject_folders_list = txt2list(filepath)
            if not output_path[i].exists():
                # Process the batch of subjects
                ds = import_dataset(data_source=subject_folders_list,
                                    ds_format='dicom',
                                    name=f'seq{i}',
                                    config_path=config_path,
                                    output_dir=folder_paths['mrds'])
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
        save_mr_dataset(output_dir / ('parallel' + MRDS_EXT), combined_mrds)
        # Check if both datasets are the same
        assert combined_mrds == sequential_ds
