from pathlib import Path
from MRdataset import MRDS_EXT
from MRdataset.config import MRException


STRATEGIES_ALLOWED = ['majority', ]

PARAMETER_NAMES = [
    'Manufacturer',
    'BodyPartExamined',
    'RepetitionTime',
    'MagneticFieldStrength',
    'FlipAngle',
    'EchoTrainLength',
    'PixelBandwidth',
    'NumberOfPhaseEncodingSteps',
    ]


PATH_CONFIG = {
    'data_source': Path.home() / 'scan_data',
    'output_dir': Path.home() / 'mrqa_reports',
}

DATE_SEPARATOR = '_DATE_'


def past_records_fpath(folder):
    return Path(folder/'past_record.txt')


def report_fpath(folder_path, fname):
    return folder_path / f'{fname}.html'


def mrds_fpath(folder_path, fname):
    return folder_path / f'{fname}{MRDS_EXT}'


def subject_list_dir(folder_path, fname):
    return folder_path / f'{fname}_files'


class CannotComputeMajority(MRException):
    """Custom error that is raised when majority cannot be computed."""

    def __init__(self, name, te):
        super().__init__(
            f"Could not compute majority for {name} with echo time {te}")


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
