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
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of ABCD',
        add_help=False
    )
    # Required arguments group
    required = parser.add_argument_group('required arguments')
    # Optional arguments group
    optional = parser.add_argument_group('optional arguments')

    # Required arguments
    required.add_argument('-t', '--task', type=str, required=False,
                          help='[submit_job|merge|report]',
                          default='submit_job')

    # Parse arguments
    args = parser.parse_args()
    # Set constants
    DATA_ROOT = Path('/media/sinhah/extremessd/ABCD-375/dicom-baseline')
    OUTPUT_DIR = DATA_ROOT.parent / (DATA_ROOT.stem+'_mrqa_files')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    name = 'abcd-375'

    # Choose a task, one of [debug|submit_job|merge|report]
    if args.task == 'debug':
        # Debugging code, note that it will generate scripts and also run them
        # sequentially, so it is not recommended to use this for large datasets
        parallel_dataset(data_root=DATA_ROOT,
                         reindex=True,
                         subjects_per_job=50,
                         debug=True,
                         submit_job=False,
                         conda_env='mrcheck',
                         conda_dist='anaconda3',
                         hpc=False,
                         output_dir=OUTPUT_DIR)
    elif args.task == 'submit_job':
        # Generate slurm scripts and submit jobs, for local parallel processing
        parallel_dataset(data_root=DATA_ROOT,
                         reindex=True,
                         subjects_per_job=100,
                         debug=False,
                         submit_job=False,
                         conda_env='mrcheck',
                         conda_dist='anaconda3',
                         hpc=False,
                         output_dir=OUTPUT_DIR)
    elif args.task == 'merge':
        # Merge partial datasets into a single dataset
        mrds_paths = OUTPUT_DIR / 'partial_mrds_paths.txt'
        check_and_merge(
            name=name,
            mrds_list_filepath=mrds_paths,
            save_dir=OUTPUT_DIR
        )
    elif args.task == 'report':
        # Generate a report for the merged dataset
        dataset = load_mr_dataset(OUTPUT_DIR / (name + MRDS_EXT), style='dicom')
        check_compliance(dataset=dataset,
                         output_dir=OUTPUT_DIR/'reports')
    else:
        # Invalid task
        raise NotImplementedError(f"Expected one of [submit_job|merge|report], "
                                  f"Got {args.task}")


if __name__ == '__main__':
    # Run the main function
    sys.exit(main())
