import logging
import tempfile
from pathlib import Path

from MRdataset import MRDS_EXT
from MRdataset.config import MRException
from protocol import UnspecifiedType

THIS_DIR = Path(__file__).parent.resolve()


def configure_logger(log, output_dir, mode='w', level='WARNING'):
    """
    Initiate log files.

    Parameters
    ----------
    log : logging.Logger
        The logger object.
    mode : str, (``'w'``, ``'a'``)
        The writing mode to the log files.
        Defaults to ``'w'``, overwrites previous files.
    output_dir : str or Path
        The path to the output directory.
    level : str,
        The level of logging to the console. One of ['WARNING', 'ERROR']
    """

    console_handler = logging.StreamHandler()  # creates the handler
    warn_formatter = ('%(filename)s:%(name)s:%(funcName)s:%(lineno)d:'
                      ' %(message)s')
    error_formatter = '%(asctime)s - %(levelname)s - %(message)s'
    if output_dir is None:
        output_dir = tempfile.gettempdir()
    output_dir = Path(output_dir) / '.mrdataset'
    output_dir.mkdir(parents=True, exist_ok=True)

    options = {
        "warn" : {
            'level'    : logging.WARN,
            'file'     : output_dir / 'warn.log',
            'formatter': warn_formatter
        },
        "error": {
            'level'    : logging.ERROR,
            'file'     : output_dir / 'error.log',
            'formatter': error_formatter
        }
    }

    if level == 'ERROR':
        config = options['error']
    else:
        config = options['warn']

    file_handler = logging.FileHandler(config['file'], mode=mode)
    file_handler.setLevel(config['level'])
    file_handler.setFormatter(logging.Formatter(config['formatter']))
    log.addHandler(file_handler)

    console_handler.setLevel(config['level'])  # sets the handler info
    console_handler.setFormatter(logging.Formatter(config['formatter']))
    log.addHandler(console_handler)
    return log


PATH_CONFIG = {
    'data_source': Path.home() / 'scan_data',
    'output_dir' : Path.home() / 'mrqa_reports',
}

DATE_SEPARATOR = '_DATE_'
ATTRIBUTE_SEPARATOR = '_ATTR_'
DATETIME_FORMAT = '%m_%d_%Y_%H_%M_%S'
DATE_FORMAT = '%m_%d_%Y'
Unspecified = UnspecifiedType()


def past_records_fpath(folder):
    """Constructs the path to the past record file"""
    return Path(folder / 'past_record.txt')


def status_fpath(folder, audit):
    """Constructs the path to the status file"""
    return Path(folder) / f'{audit}_non_compliance_log.txt'


def report_fpath(folder_path, fname):
    """Constructs the path to the report file"""
    return Path(folder_path) / f'{fname}.html'


def mrds_fpath(folder_path, fname):
    """Constructs the path to the MRDS file"""
    return Path(folder_path) / f'{fname}{MRDS_EXT}'


def subject_list_dir(folder_path, fname):
    """Constructs the path to the folder containing subject list files"""
    return Path(folder_path) / f'{fname}_files'


def daily_log_fpath(folder_path, ds_name, audit):
    """Constructs the path to the daily log file"""
    return Path(folder_path) / f'{ds_name}_{audit}_log.json'


class CannotComputeMajority(MRException):
    """Custom error that is raised when majority cannot be computed."""

    def __init__(self, name):
        super().__init__(
            f"Could not compute majority for {name}")


#
# class ReferenceNotSetForModality(MRException):
#     """Custom error that is raised when majority cannot be computed."""
#
#     def __init__(self, name):
#         super().__init__(
#             f"Cannot compute delta for runs in modality {name}"
#             f"as not reference protocol doesn't exist.")
#
#
# class ReferenceNotSetForEchoTime(MRException):
#     """Custom error that is raised when majority cannot be computed."""
#
#     def __init__(self, name, echo_time):
#         super().__init__(
#             f"Cannot compute delta for runs in modality {name} "
#             f"with TE {echo_time}"
#             f" as not reference protocol is not set.")
#
#
# class ComplianceException(Exception):
#     """
#     Custom error that is raised when some critical properties are not
#     found in dicom file
#     """
#
#     def __init__(self, message, **kwargs):
#         super().__init__(message)
#
#
# class EmptySubject(ComplianceException):
#     """"""
#     pass
#
#
# class NonCompliantSubject(ComplianceException):
#     """"""
#     pass
#
#
# class ChangingParamsinSeries(ComplianceException):
#     """
#     Custom error that is raised when parameter values are different for
#     different slices even though the SeriesInstanceUID is same.
#     """
#
#     def __init__(self, filepath):
#         super().__init__("Expected all dicom slices to have same parameters. "
#                          "Got changing parameters : {}".format(filepath))
#
#
# class ComplianceWarning(Warning):
#     """Library specific exception"""
#
#     pass


class EqualCountType(UnspecifiedType):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'EqualCount'

    def __repr__(self):
        return 'EqualCount'


EqualCount = EqualCountType()
