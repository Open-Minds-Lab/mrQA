from pathlib import Path

from MRdataset import load_mr_dataset
from MRdataset.config import MRDS_EXT
from mrQA import check_compliance
from mrQA.run_merge import check_and_merge
from mrQA.run_parallel import parallel_dataset
import argparse
import sys


def main():
    """
    Console script only for ABCD HPC processing. Not for general usage
    by anyone using the mrQA library. You may imitate the flow
    to process datasets, but do not use the script as is.
    Guaranteed not to work!
    """
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of ABCD',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    required.add_argument('-t', '--task', type=str, required=False,
                          help='[submit_job|merge|report]',
                          default='submit_job')


    args = parser.parse_args()
    HOME = Path('/jet/home/sinhah/').resolve()
    ARCHIVE = Path('/ocean/projects/med220005p/sinhah/')
    METADATA_ROOT = HOME / '.mrdataset'
    DATA_ROOT = ARCHIVE / 'ABCD/t1w/dicom'
    name = 'abcd-T1w-baseline'

    if args.task == 'submit_job':
        parallel_dataset(data_root=DATA_ROOT,
                         name=name,
                         reindex=True,
                         subjects_per_job=100,
                         debug=False,
                         submit_job=False,
                         conda_env='mrqa')
    elif args.task == 'merge':
        check_and_merge(
            name=name,
            all_batches_txtpaths=METADATA_ROOT / (name + '_txt_files.txt')
        )
    elif args.task == 'report':
        dataset = load_mr_dataset(METADATA_ROOT / (name + MRDS_EXT))
        check_compliance(dataset=dataset,
                         output_dir=HOME/'mr_reports')
    else:
        raise NotImplementedError(f"Expected one of [submit_job|merge|report], "
                                  f"Got {args.task}")


if __name__ == '__main__':
    sys.exit(main())

