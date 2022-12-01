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
    Console script only for ABCD local processing. Not for general usage
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
    DATA_ROOT = Path('/media/sinhah/extremessd/ABCD-375/dicom-baseline')

    OUTPUT_DIR = DATA_ROOT.parent / (DATA_ROOT.stem+'_mrqa_files')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    name = 'abcd-375'
    if args.task == 'debug':
        parallel_dataset(data_root=DATA_ROOT,
                         name=name,
                         reindex=True,
                         subjects_per_job=50,
                         debug=True,
                         submit_job=False,
                         conda_env='mrcheck',
                         conda_dist='anaconda3',
                         hpc=False,
                         output_dir=OUTPUT_DIR)
    elif args.task == 'submit_job':
        parallel_dataset(data_root=DATA_ROOT,
                         name=name,
                         reindex=True,
                         subjects_per_job=100,
                         debug=False,
                         submit_job=False,
                         conda_env='mrcheck',
                         conda_dist='anaconda3',
                         hpc=False,
                         output_dir=OUTPUT_DIR)
    elif args.task == 'merge':
        mrds_paths = OUTPUT_DIR / (name + '_mrds_files.txt')
        check_and_merge(
            name=name,
            mrds_paths=mrds_paths,
            save_dir=OUTPUT_DIR
        )
    elif args.task == 'report':
        dataset = load_mr_dataset(OUTPUT_DIR / (name + MRDS_EXT))
        check_compliance(dataset=dataset,
                         output_dir=OUTPUT_DIR/'reports')
    else:
        raise NotImplementedError(f"Expected one of [submit_job|merge|report], "
                                  f"Got {args.task}")


if __name__ == '__main__':
    sys.exit(main())
