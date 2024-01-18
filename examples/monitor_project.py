import argparse
import multiprocessing as mp
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from MRdataset import DatasetEmptyException, valid_dirs, load_mr_dataset

from mrQA import monitor, logger, check_compliance
from mrQA.config import PATH_CONFIG, status_fpath, DATETIME_FORMAT
from mrQA.utils import txt2list, log_latest_non_compliance, is_writable, \
    send_email


def get_parser():
    """Console script for mrQA."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    # Add help
    optional.add_argument('-d', '--data-root', type=str,
                          help='A folder which contains projects'
                               'to process. Required if task is monitor')
    optional.add_argument('-t', '--task', type=str,
                          help='specify the task to be performed, one of'
                               ' [monitor, compile]', default='monitor')
    optional.add_argument('-a', '--audit', type=str,
                          help='specify the audit type if compiling reports. '
                               'Choose one of [hz, vt]. Required if task is '
                               'compile',
                          default='hz')
    optional.add_argument('-i', '--input-dir', type=str,
                          help='specify the directory where the reports'
                               ' are saved. Required if task is compile')
    optional.add_argument('--date', type=str,
                          help='compile all non-compliant subjects scanned '
                               'after this date. Format: MM_DD_YYYY. Required'
                               ' if task is compile')
    optional.add_argument('-o', '--output-dir', type=str,
                          default='/home/mrqa/mrqa_reports/',
                          help='specify the directory where the report'
                               ' would be saved')
    optional.add_argument('-x', '--exclude-fpath', type=str,
                          help='A txt file containing a'
                               'list of folders to be skipped while'
                               'monitoring')
    required.add_argument('--config', type=str,
                          help='path to config file')
    optional.add_argument('-e', '--email-config-path', type=str,
                          help='filepath to email config file')

    if len(sys.argv) < 2:
        logger.critical('Too few arguments!')
        parser.print_help()
        parser.exit(1)

    return parser


def parse_args():
    """Parse command line arguments."""
    parser = get_parser()
    args = parser.parse_args()
    dirs = []

    if args.exclude_fpath is not None:
        if not Path(args.exclude_fpath).is_file():
            raise FileNotFoundError("Need a valid filepath to the exclude list")
        exclude_filepath = Path(args.exclude_fpath).resolve()
        skip_list = [Path(i).resolve() for i in txt2list(exclude_filepath)]
    else:
        skip_list = []

    if args.task == 'monitor':
        if Path(args.data_root).is_dir():
            data_root = Path(args.data_root)
            non_empty_folders = []
            all_folders = sorted(Path(data_root).iterdir(), key=os.path.getmtime)
            for folder in all_folders:
                if folder.is_dir() and any(folder.iterdir()):
                    non_empty_folders.append(folder)
        else:
            raise ValueError("Need a valid path to a folder, which consists of "
                             f"projects to process. "
                             f"Got {args.data_root}")

        dirs = valid_dirs(non_empty_folders)

        if len(non_empty_folders) < 2:
            # If there is only one project, then the cast it into a list
            dirs = [dirs]

    elif args.task == 'compile':
        if Path(args.input_dir).is_dir():
            dirs = valid_dirs(Path(args.input_dir))

        if args.date is None:
            two_weeks_ago = datetime.now() - timedelta(days=14)
            args.date = two_weeks_ago.strftime(DATETIME_FORMAT)
        else:
            try:
                datetime.strptime(args.date, DATETIME_FORMAT)
            except ValueError:
                raise ValueError(f"Incorrect date format, "
                                 f"should be {DATETIME_FORMAT}")

    else:
        raise NotImplementedError(f"Task {args.task} not implemented. Choose "
                                  "one of [monitor, compile]")

    if args.audit not in ['hz', 'vt']:
        raise ValueError(f"Invalid audit type {args.audit}. Choose one of "
                         f"[hz, vt]")

    if args.output_dir is None:
        logger.info('Use --output-dir to specify dir for final directory. '
                    'Using default')
        args.output_dir = PATH_CONFIG['output_dir'] / args.name.lower()
        args.output_dir.mkdir(exist_ok=True, parents=True)
    else:
        if not Path(args.output_dir).is_dir():
            try:
                Path(args.output_dir).mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                logger.error(
                    f'Unable to create folder {args.output_dir} for '
                    f'saving reports')
                raise exc
    if not is_writable(args.output_dir):
        raise OSError(f'Output Folder {args.output_dir} is not writable')

    for fpath in dirs:
        if Path(fpath).resolve() in skip_list:
            dirs.remove(fpath)

    if not Path(args.config).is_file():
        raise FileNotFoundError(
            f'Expected valid file for config,  Got {args.config}'
            f'the file does not exist')
    if args.email_config_path:
        if not Path(args.email_config_path).is_file():
            raise FileNotFoundError(
                f'Expected valid file for config_path,  Got {args.config_path}'
                f'the file does not exist')
    else:
        logger.info('Use --email-config-path to specify filepath to email. '
                    'Skipping')
    return args, dirs


def main():
    """Console script for mrQA monitor project."""
    args, dirs = parse_args()

    if args.task == 'monitor':
        pool = mp.Pool(processes=10)
        arguments = [(f, args.output_dir, args.config,
                      args.email_config_path) for f in dirs]
        pool.starmap(run_monitor, arguments)
    elif args.task == 'compile':
        compile_reports(folder_paths=dirs, output_dir=args.output_dir,
                        config_path=args.config,
                        date=args.date, audit=args.audit)
    else:
        raise NotImplementedError(f"Task {args.task} not implemented. Choose "
                                  "one of [monitor, compile]")


def run_monitor(folder_path, output_dir, config_path, email_config_path=None):
    """Run monitor for a single project"""
    name = Path(folder_path).stem
    print(f"\nProcessing {name}\n")
    output_folder = Path(output_dir) / name
    try:
        hz_flag, vt_flag, report_path = monitor(name=name,
                                                data_source=folder_path,
                                                output_dir=output_folder,
                                                decimals=2,
                                                verbose=False,
                                                ds_format='dicom',
                                                tolerance=0,
                                                config_path=config_path,
                                                )
        if hz_flag:
            log_fpath = status_fpath(output_folder, audit='hz')
            logger.info(f"Non-compliant scans found for {name}")
            logger.info(f"Check {log_fpath} for horizontal audit")
            send_email(log_fpath, project_code=name, report_path=report_path,
                       email_config=email_config_path)
    except DatasetEmptyException as e:
        logger.warning(f'{e}: Folder {name} has no DICOM files.')


def compile_reports(folder_paths, output_dir, config_path, audit='hz',
                    date=None):
    """Compile reports for all projects in the folder_paths"""
    output_dir = Path(output_dir)
    complete_log = []
    # Look for all mrds.pkl file in the output_dir. For ex, mrqa_reports
    # Collect mrds.pkl files for all projects
    for sub_folder in folder_paths:
        mrds_files = list(Path(sub_folder).rglob('*.mrds.pkl'))
        if not mrds_files:
            continue
        latest_mrds = max(mrds_files, key=os.path.getctime)
        # for mrds in mrds_files:
        ds = load_mr_dataset(latest_mrds)
        # TODO : check compliance, but maybe its better is to save
        #  compliance results which can be re-used here
        hz_audit_results, vt_audit_results, _ = check_compliance(
            ds,
            output_dir=output_dir / 'compiled_reports',
            config_path=config_path,
        )
        log_latest_non_compliance(dataset=hz_audit_results['non_compliant'],
                                  config_path=config_path,
                                  output_dir=output_dir / sub_folder.stem,
                                  audit=audit,
                                  date=date)


if __name__ == "__main__":
    main()
