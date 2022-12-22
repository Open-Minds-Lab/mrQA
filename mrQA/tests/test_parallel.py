from MRdataset import import_dataset, save_mr_dataset, load_mr_dataset
from mrQA import check_compliance
from pathlib import Path
from mrQA.run_parallel import process_parallel
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


# def test_equivalence_in_combinations(data_source):
#     subsets = itertools.combinations(mrds_files, 2)
#     subset_dir = output_dir/'subsets'
#     for i, item in enumerate(subsets):
#         subset_filepath = subset_dir / f'subset{i}.txt'
#         list2txt(subset_filepath, item)
#         check_and_merge(
#             mrds_list_filepath=subset_filepath,
#             output_path=subset_dir / f'parallel{i}{MRDS_EXT}',
#             name=f'subset{i}'
#         )

    with open(file2, 'r') as f:
        lines_file2 = set(f.readlines())

    if list(lines_file1 - lines_file2):
        return True
    return False

if __name__ == '__main__':
    test_equivalence_seq_vs_parallel('/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/')
