import argparse
import multiprocessing as mp
from pathlib import Path

from MRdataset import DatasetEmptyException, valid_dirs, load_mr_dataset

from mrQA import monitor, logger, check_compliance
from mrQA.utils import txt2list, filter_epi_fmap_pairs


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
        compile_reports(args.data_root, args.output_dir, args.config)
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


def compile_reports(folder_path, output_dir, config_path):
    output_dir = Path(output_dir)
    complete_log = []
    mrds_files = list(Path(folder_path).rglob('*.mrds.pkl'))
    if not mrds_files:
        raise FileNotFoundError(f"No .mrds.pkl files found in {output_dir}")

    for mrds in mrds_files:
        ds = load_mr_dataset(mrds)
        # TODO : check compliance, but better is to save
        #  compliance results which can be re-used here
        hz, vt = check_compliance(
            ds,
            output_dir=output_dir / 'compiled_reports',
            config_path=config_path,
        )
        non_compliant_ds = vt['non_compliant']
        parameters = ['ShimSetting', 'PixelSpacing']
        # after checking compliance just look for epi-fmap pairs for now
        nc_log = non_compliant_ds.generate_nc_log(
                                                parameters=parameters,
                                                filter_fn=filter_epi_fmap_pairs,
                                                output_dir=output_dir,
                                                audit='vt')
        # print(nc_log)
        # complete_log.append(nc_log)


if __name__ == "__main__":
    main()
