from MRdataset import import_dataset, save_mr_dataset, load_mr_dataset
from mrQA import check_compliance
from pathlib import Path
from mrQA.run_parallel import process_parallel
from MRdataset.config import MRDS_EXT
from MRdataset.log import logger

dummy_DS = []
logger.setLevel('WARNING')


def test_equivalence_seq_vs_parallel(data_source):
    output_dir = Path(data_source).parent / 'test_mrqa_files'
    output_path = {
        'sequential': output_dir / ('sequential' + MRDS_EXT),
        'parallel': output_dir / ('parallel' + MRDS_EXT)
    }

    sequential_ds = import_dataset(data_source_folders=data_source,
                                   style='dicom',
                                   name='sequential')
    save_mr_dataset(output_path['parallel'], sequential_ds)

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

    assert not diff(report_path['sequential'], report_path['parallel'])


def diff(file1, file2):
    with open(file1, 'r') as f:
        lines_file1 = set(f.readlines())

    with open(file2, 'r') as f:
        lines_file2 = set(f.readlines())

    if list(lines_file1 - lines_file2):
        return True
    return False

if __name__ == '__main__':
    test_equivalence_seq_vs_parallel('/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/')