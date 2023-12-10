"""Console script for mrQA."""
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union, List

from MRdataset import import_dataset, load_mr_dataset
from mrQA import logger
from mrQA.config import PATH_CONFIG, THIS_DIR, DATETIME_FORMAT
from mrQA.project import check_compliance
from mrQA.utils import is_writable, folders_modified_since, \
    get_last_valid_record, log_latest_non_compliance


def get_parser():
    """Console script for mrQA."""
    parser = argparse.ArgumentParser(
        description='Protocol Compliance of MRI scans',
        add_help=False
    )

    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    # Add help
    optional.add_argument('-n', '--name', type=str,
                          help='provide a identifier/name for the dataset')
    required.add_argument('-d', '--data-source', type=str, required=True,
                          help='directory containing downloaded dataset with '
                               'dicom files, supports nested hierarchies')
    required.add_argument('--config', type=str,
                          help='path to config file',
                          default=THIS_DIR / 'resources/mri-config.json')
    optional.add_argument('-o', '--output-dir', type=str,
                          help='specify the directory where the report'
                               ' would be saved. By default, the --data_source '
                               'directory will be used to save reports')
    optional.add_argument('-f', '--format', type=str, default='dicom',
                          help='type of dataset, one of [dicom]')
    optional.add_argument('-h', '--help', action='help',
                          default=argparse.SUPPRESS,
                          help='show this help message and exit')
    optional.add_argument('--decimals', type=int, default=3,
                          help='number of decimal places to round to '
                               '(default:0). If decimals are negative it '
                               'specifies the number of positions to the left'
                               'of the decimal point.')
    optional.add_argument('-t', '--tolerance', type=float, default=0,
                          help='tolerance for checking against reference '
                               'protocol. Default is 0')
    optional.add_argument('-v', '--verbose', action='store_true',
                          help='allow verbose output on console')
    optional.add_argument('-ref', '--ref-protocol-path', type=str,
                          help='XML file containing desired protocol. If not '
                               'provided, the protocol will be inferred from '
                               'the dataset.')
    if len(sys.argv) < 2:
        logger.critical('Too few arguments!')
        parser.print_help()
        parser.exit(1)

    return parser


def parse_args():
    """
    Parse command line arguments

    Returns
    -------
    args : argparse.Namespace
        parsed arguments

    Raises
    ------
    FileNotFoundError
        if data_source is not a valid directory
    ValueError
        if name is not a valid dataset name which has been processed before
    OSError
        if output_dir cannot be created
    """
    parser = get_parser()
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel('INFO')
    else:
        logger.setLevel('WARNING')

    if not Path(args.data_source).is_dir():
        raise FileNotFoundError('Invalid data_source specified '
                                'for reading files.')

    # valid_names = _datasets_processed(PATH_CONFIG['output_dir'],
    #                                   ignore_case=True)
    # if args.name not in valid_names:
    #     raise ValueError('Need valid project name to monitor! '
    #                      f'Expected one of {valid_names}. Got {args.name}')

    if args.output_dir is None:
        logger.info('Use --output_dir to specify dir for final directory. '
                    'Using default')
        args.output_dir = PATH_CONFIG['output_dir'] / args.name
    else:
        if not Path(args.output_dir).is_dir():
            try:
                Path(args.output_dir).mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise exc
    # TODO: Add this check to mrqa and MRdataset
    if not is_writable(args.output_dir):
        raise OSError(f'Output Folder {args.output_dir} is not writable')

    if not Path(args.config).is_file():
        raise FileNotFoundError(f'Expected valid config file, '
                                f'Got {args.config}')
    else:
        args.config = Path(args.config).resolve()
    return args


def cli():
    """Console script for mrQA monitor."""
    args = parse_args()
    monitor(name=args.name,
            data_source=args.data_source,
            output_dir=args.output_dir,
            verbose=args.verbose,
            decimals=args.decimals,
            ds_format=args.format,
            config_path=args.config,
            tolerance=args.tolerance,
            reference_path=args.ref_protocol_path, )


def monitor(name: str,
            data_source: Union[str, List, Path],
            output_dir: Union[str, Path],
            verbose: bool = False,
            decimals: int = 3,
            ds_format: str = 'dicom',
            config_path: Union[Path, str] = None,
            tolerance=0,
            reference_path=None):
    """
    Monitor a dataset folder for changes. Read new files and append to
    existing dataset. Run compliance check on the updated dataset.
    Generate a report and save it to the output directory.

    Parameters
    ----------
    name :  str
        Identifier for the dataset, like ABCD. The name used to save results
    data_source: str or list
        Path to the folder containing the dataset or list of files/folders.
    output_dir: str
        Path to the folder where the report, and dataset would be saved.
    verbose: bool
        Whether to print verbose output on console.
    decimals: int
        Number of decimal places to round to (default:3).
    ds_format: str
        Type of dataset, one of [dicom]
    config_path: str
        Path to the config file
    tolerance: float
        Tolerance for checking against reference protocol. Default is 0
    reference_path: str
        Path to the reference protocol file.
    """
    output_dir = Path(output_dir)
    last_record = get_last_valid_record(output_dir)
    last_reported_on = None

    if last_record:
        last_reported_on, last_report_path, last_mrds_path = last_record
        # TODO: delete old logs, only keep latest 3-4 reports in the folder
        dataset = load_mr_dataset(last_mrds_path)
        modified_folders = folders_modified_since(
            input_dir=data_source,
            last_reported_on=last_reported_on,
            output_dir=output_dir
        )
        if modified_folders:
            new_dataset = import_dataset(data_source=modified_folders,
                                         ds_format='dicom',
                                         name=name,
                                         verbose=verbose,
                                         config_path=config_path,
                                         output_dir=output_dir)
            # prev_status = get_status(dataset)
            dataset.merge(new_dataset)
        else:
            logger.warning('No new files found since last report. '
                           'Returning last report')
            return None, None, last_report_path
    else:
        logger.warning('Dataset %s not found in records. Running '
                       'compliance check on entire dataset', name)
        dataset = import_dataset(data_source=data_source,
                                 ds_format=ds_format,
                                 name=name,
                                 verbose=verbose,
                                 config_path=config_path,
                                 output_dir=output_dir)
        new_dataset = None

    if last_reported_on is None:
        # if this is the first time, set last_reported_on to 1 year ago
        last_reported_on = datetime.now() - timedelta(days=365)
        last_reported_on = last_reported_on.strftime(DATETIME_FORMAT)

    hz_audit_results, vt_audit_results, report_path = check_compliance(
        dataset=dataset,
        output_dir=output_dir,
        decimals=decimals,
        verbose=verbose,
        tolerance=tolerance,
        reference_path=reference_path,
        config_path=config_path)

    flag_hz = log_latest_non_compliance(
        dataset=hz_audit_results['non_compliant'],
        config_path=config_path,
        output_dir=output_dir, audit='hz',
        date=last_reported_on)

    flag_vt = log_latest_non_compliance(
        dataset=vt_audit_results['non_compliant'],
        config_path=config_path,
        output_dir=output_dir, audit='vt',
        date=last_reported_on)

    return flag_hz, flag_vt, report_path


if __name__ == '__main__':
    sys.exit(cli())  # pragma: no cover
