import argparse
import multiprocessing as mp
from pathlib import Path

from MRdataset import DatasetEmptyException, valid_dirs, load_mr_dataset
from mrQA import monitor, logger, check_compliance
from mrQA.utils import txt2list, log_latest_non_compliance


def main():
    """Console script for mrQA."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    # Add help
    required.add_argument('-d', '--data-root', type=str, required=True,
                          help='A folder which contains projects'
                               'to process')
    optional.add_argument('-t', '--task', type=str,
                          help='specify the task to be performed, one of'
                               ' [monitor, compile]', default='monitor')
    optional.add_argument('-a', '--audit', type=str,
                          help='specify the audit type if compiling reports. '
                               'Choose one of [hz, vt]. Required if task is '
                               'compile',
                          default='vt')
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

    args = parser.parse_args()
    if Path(args.data_root).exists():
        data_root = Path(args.data_root)
        non_empty_folders = []
        for folder in data_root.iterdir():
            if folder.is_dir() and any(folder.iterdir()):
                non_empty_folders.append(folder)
    else:
        raise ValueError("Need a valid path to a folder, which consists of "
                         f"projects to process. "
                         f"Got {args.data_root}")

    dirs = valid_dirs(non_empty_folders)

    if len(non_empty_folders) < 2:
        dirs = [dirs]
    if args.exclude_fpath is not None:
        if not Path(args.exclude_fpath).exists():
            raise FileNotFoundError("Need a valid filepath to the exclude list")
        exclude_filepath = Path(args.exclude_fpath).resolve()
        skip_list = [Path(i).resolve() for i in txt2list(exclude_filepath)]

        for fpath in dirs:
            if Path(fpath).resolve() in skip_list:
                dirs.remove(fpath)
    if args.task == 'monitor':
        pool = mp.Pool(processes=10)
        arguments = [(f, args.output_dir, args.config) for f in dirs]
        pool.starmap(run, arguments)
    elif args.task == 'compile':
        compile_reports(args.data_root, args.output_dir, args.config,
                        args.audit)
    else:
        raise NotImplementedError(f"Task {args.task} not implemented. Choose "
                                  "one of [monitor, compile]")


def run(folder_path, output_dir, config_path):
    name = Path(folder_path).stem
    print(f"\nProcessing {name}\n")
    output_folder = Path(output_dir) / name
    try:
        monitor(name=name,
                data_source=folder_path,
                output_dir=output_folder,
                decimals=2,
                verbose=False,
                ds_format='dicom',
                tolerance=0,
                config_path=config_path,
                )
    except DatasetEmptyException as e:
        logger.warning(f'{e}: Folder {name} has no DICOM files.')


def compile_reports(folder_path, output_dir, config_path, audit='vt', date=None):
    output_dir = Path(output_dir)
    complete_log = []
    # Look for all mrds.pkl file in the output_dir. For ex, mrqa_reports
    # Collect mrds.pkl files for all projects
    mrds_files = list(Path(folder_path).rglob('*.mrds.pkl'))
    if not mrds_files:
        raise FileNotFoundError(f"No .mrds.pkl files found in {folder_path}")

    for mrds in mrds_files:
        ds = load_mr_dataset(mrds)
        # TODO : check compliance, but maybe its better is to save
        #  compliance results which can be re-used here
        hz_audit_results, vt_audit_results = check_compliance(
            ds,
            output_dir=output_dir / 'compiled_reports',
            config_path=config_path,
        )
        log_latest_non_compliance(dataset=hz_audit_results['non_compliant'],
                                  config_path=config_path,
                                  output_dir=output_dir / 'compiled_reports',
                                  audit='hz',
                                  date=date)
        # if audit == 'hz':
        #     non_compliant_ds = hz['non_compliant']
        #     filter_fn = None
        #     nc_params = config.get("include_parameters", None)
        #     # nc_params = ['ReceiveCoilActiveElements']
        #     supplementary_params = ['BodyPartExamined']
        # elif audit == 'vt':
        #     non_compliant_ds = vt['non_compliant']
        #     nc_params = ['ShimSetting', 'PixelSpacing']
        #     supplementary_params = []
        #     # TODO: discuss what parameters can be compared between anatomical
        #     #   and functional scans
        #     # after checking compliance just look for epi-fmap pairs for now
        #     filter_fn = filter_epi_fmap_pairs
        # else:
        #     raise ValueError(f"Invalid audit type {audit}. Choose one of "
        #                      f"[hz, vt]")
        #
        # nc_log = non_compliant_ds.generate_nc_log(
        #     parameters=nc_params,
        #     suppl_params=supplementary_params,
        #     filter_fn=filter_fn,
        #     output_dir=output_dir,
        #     audit=audit,
        #     verbosity=4)



if __name__ == "__main__":
    main()
