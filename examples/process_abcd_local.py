import argparse
import sys
from pathlib import Path

from MRdataset import load_mr_dataset
from MRdataset.config import MRDS_EXT
from mrQA import check_compliance
from mrQA.run_merge import check_and_merge
from mrQA.run_parallel import create_script, submit_job


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
                          help='[create_script|submit_job|merge|report]',
                          default='report')
    optional.add_argument('-ref', '--ref-protocol-path', type=str,
                          help='XML file containing desired protocol. If not '
                               'provided, the protocol will be inferred from '
                               'the dataset.')
    required.add_argument('--config', type=str,
                          help='path to config file',
                          default='/home/sinhah/github/mrQA/examples/mri-config-abcd.json')
    # Parse arguments
    args = parser.parse_args()
    # Set constants
    DATA_ROOT = Path('/home/sinhah/scan_data/vertical_abcd')
    OUTPUT_DIR = DATA_ROOT.parent / (DATA_ROOT.stem + '_mrqa_files')
    # OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    name = 'abcd-vertical'

    # Choose a task, one of [debug|submit_job|merge|report]
    if args.task == 'create_script':
        # note that it will generate scripts only
        create_script(data_source=DATA_ROOT,
                      folders_per_job=5,
                      conda_env='mrcheck',
                      conda_dist='anaconda3',
                      hpc=False,
                      config_path=args.config,
                      output_dir=OUTPUT_DIR,
                      )
    elif args.task == 'submit_job':
        # Generate slurm scripts and submit jobs, for local parallel processing
        SCRIPTS_LIST_PATH = OUTPUT_DIR / 'per_batch_script_list.txt'
        MRDS_LIST_PATH = OUTPUT_DIR / 'per_batch_partial_mrds_list.txt'
        submit_job(scripts_list_filepath=SCRIPTS_LIST_PATH,
                   mrds_list_filepath=MRDS_LIST_PATH,
                   hpc=False)

    elif args.task == 'merge':
        # Merge partial datasets into a single dataset
        MRDS_LIST_PATH = OUTPUT_DIR / 'per_batch_partial_mrds_list.txt'
        # TODO : directly pass the output path to final file, skip name
        check_and_merge(
            name=name,
            mrds_list_filepath=MRDS_LIST_PATH,
            output_path=OUTPUT_DIR / (name + MRDS_EXT)
        )
    elif args.task == 'report':
        # Generate a report for the merged dataset
        dataset = load_mr_dataset(OUTPUT_DIR / (name + MRDS_EXT))
        check_compliance(dataset=dataset,
                         output_dir=OUTPUT_DIR,
                         decimals=2,
                         tolerance=0,
                         config_path=args.config,
                         reference_path=args.ref_protocol_path,)
    else:
        # Invalid task
        raise NotImplementedError(f"Expected one of [submit_job|merge|report], "
                                  f"Got {args.task}")


if __name__ == '__main__':
    # Run the main function
    sys.exit(main())
