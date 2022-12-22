from MRdataset import import_dataset, save_mr_dataset, load_mr_dataset
from mrQA import check_compliance
from pathlib import Path
from mrQA.run_parallel import process_parallel
from MRdataset.config import MRDS_EXT
from MRdataset.log import logger

dummy_DS = []
logger.setLevel('WARNING')


def test_equivalence_seq_vs_parallel(data_source):
    output_dir = Path(data_source) / 'test_mrqa_files'
    output_path = {
        'sequential': output_dir / ('sequential' + MRDS_EXT),
        'parallel': output_dir / ('parallel' + MRDS_EXT)
    }

    sequential_ds = import_dataset(data_root=data_source,
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

    compare(report_path['sequential'], report_path['parallel'])


def compare(file1, file2):
    with open(file1, 'r') as f:
        d = set(f.readlines())

    with open(file2, 'r') as f:
        e = set(f.readlines())

    open('file3.txt', 'w').close()  # Create the file

    with open('file3.txt', 'a') as f:
        for line in list(d - e):
            f.write(line)
