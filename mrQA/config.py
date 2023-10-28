import logging
import tempfile
from pathlib import Path

from MRdataset import MRDS_EXT
from MRdataset.config import MRException
from protocol import UnspecifiedType


def configure_logger(log, output_dir, mode='w', level='ERROR'):
    """
    Initiate log files.

    Parameters
    ----------
    log : logging.Logger
        The logger object.
    mode : str, (``'w'``, ``'a'``)
        The writing mode to the log files.
        Defaults to ``'w'``, overwrites previous files.    """
    console_handler = logging.StreamHandler()  # creates the handler
    warn_formatter = ('%(filename)s:%(name)s:%(funcName)s:%(lineno)d:'
                      ' %(message)s')
    error_formatter = '%(asctime)s - %(levelname)s - %(message)s'
    if output_dir is None:
        output_dir = tempfile.gettempdir()
    output_dir = Path(output_dir) / '.mrdataset'
    output_dir.mkdir(parents=True, exist_ok=True)

    warn_file = output_dir / 'warn.log'
    if level == 'WARNING':
        warn = logging.FileHandler(warn_file, mode=mode)
        warn.setLevel(logging.WARN)
        warn.setFormatter(logging.Formatter(warn_formatter))
        log.addHandler(warn)

    # keep only errors on console
    console_handler.setLevel(logging.ERROR)  # sets the handler info
    console_handler.setFormatter(logging.Formatter(error_formatter))
    log.addHandler(console_handler)

    error_file = output_dir / 'error.log'
    error = logging.FileHandler(error_file, mode=mode)
    error.setLevel(logging.ERROR)
    error.setFormatter(logging.Formatter(error_formatter))
    log.addHandler(error)
    return log


PATH_CONFIG = {
    'data_source': Path.home() / 'scan_data',
    'output_dir': Path.home() / 'mrqa_reports',
}

DATE_SEPARATOR = '_DATE_'
ATTRIBUTE_SEPARATOR = '_ATTR_'

Unspecified = UnspecifiedType()


def past_records_fpath(folder):
    """Constructs the path to the past record file"""
    return Path(folder / 'past_record.txt')


def status_fpath(folder):
    """Constructs the path to the status file"""
    return Path(folder / 'non_compliance_log.txt')


def report_fpath(folder_path, fname):
    """Constructs the path to the report file"""
    return folder_path / f'{fname}.html'


def mrds_fpath(folder_path, fname):
    """Constructs the path to the MRDS file"""
    return folder_path / f'{fname}{MRDS_EXT}'


def subject_list_dir(folder_path, fname):
    """Constructs the path to the folder containing subject list files"""
    return folder_path / f'{fname}_files'


class CannotComputeMajority(MRException):
    """Custom error that is raised when majority cannot be computed."""

    def __init__(self, name):
        super().__init__(
            f"Could not compute majority for {name}")


class ReferenceNotSetForModality(MRException):
    """Custom error that is raised when majority cannot be computed."""

    def __init__(self, name):
        super().__init__(
            f"Cannot compute delta for runs in modality {name}"
            f"as not reference protocol doesn't exist.")


class ReferenceNotSetForEchoTime(MRException):
    """Custom error that is raised when majority cannot be computed."""

    def __init__(self, name, echo_time):
        super().__init__(
            f"Cannot compute delta for runs in modality {name} "
            f"with TE {echo_time}"
            f" as not reference protocol is not set.")


class ComplianceException(Exception):
    """
    Custom error that is raised when some critical properties are not
    found in dicom file
    """

    def __init__(self, message, **kwargs):
        super().__init__(message)


class EmptySubject(ComplianceException):
    """"""
    pass


class NonCompliantSubject(ComplianceException):
    """"""
    pass


class ChangingParamsinSeries(ComplianceException):
    """
    Custom error that is raised when parameter values are different for
    different slices even though the SeriesInstanceUID is same.
    """
    def __init__(self, filepath):
        super().__init__("Expected all dicom slices to have same parameters. "
                         "Got changing parameters : {}".format(filepath))


class ComplianceWarning(Warning):
    """Library specific exception"""

    pass


class EqualCountType(UnspecifiedType):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'EqualCount'

    def __repr__(self):
        return 'EqualCount'


EqualCount = EqualCountType()
