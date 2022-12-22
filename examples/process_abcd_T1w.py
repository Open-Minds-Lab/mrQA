import argparse
import sys
from pathlib import Path

from MRdataset import load_mr_dataset
from MRdataset.config import MRDS_EXT

from mrQA import check_compliance
from mrQA.run_merge import check_and_merge
from mrQA.run_parallel import parallel_dataset


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
                          help='[create_script|submit_job|merge|report]',
                          default='create_script')

    args = parser.parse_args()
    ARCHIVE = Path('/ocean/projects/med220005p/sinhah')
    DATA_ROOT = ARCHIVE / 'ABCD/t1w/dicom'
    name = 'abcd-T1w-baseline'
    OUTPUT_DIR = DATA_ROOT.parent / (DATA_ROOT.stem + '_mrqa_files')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.task == 'create_script':
        # Create scripts but do not submit jobs
        create_script(data_source_folders=DATA_ROOT,
                      subjects_per_job=50,
                      conda_env='mrcheck',
                      conda_dist='anaconda3',
                      hpc=True,
                      )
    elif args.task == 'submit_job':
        SCRIPTS_LIST_PATH = OUTPUT_DIR / 'per_batch_script_list.txt'
        MRDS_LIST_PATH = OUTPUT_DIR / 'per_batch_partial_mrds_list.txt'
        submit_job(scripts_list_filepath=SCRIPTS_LIST_PATH,
                   mrds_list_filepath=MRDS_LIST_PATH,
                   hpc=False)
    elif args.task == 'merge':
        # Merge partial datasets into a single dataset
        MRDS_LIST_PATH = OUTPUT_DIR / 'per_batch_partial_mrds_list.txt'
        check_and_merge(
            name=name,
            mrds_list_filepath=MRDS_LIST_PATH,
            output_path=OUTPUT_DIR / (name + MRDS_EXT)
        )
    elif args.task == 'report':
        # Generate the final report
        dataset = load_mr_dataset(OUTPUT_DIR / (name + MRDS_EXT), style='dicom')
        check_compliance(dataset=dataset,
                         output_dir=OUTPUT_DIR / 'reports')
    else:
        raise NotImplementedError(f"Expected one of "
                                  f"[create_script|submit_job|merge|report], "
                                  f"Got {args.task}")


if __name__ == '__main__':
    sys.exit(main())
