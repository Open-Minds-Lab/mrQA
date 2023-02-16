from pathlib import Path
from MRdataset import MRDS_EXT


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


def past_records_fpath(folder):
    return folder/'past_record.txt'


def report_fpath(folder_path, fname):
    return folder_path / f'{fname}.html'


def mrds_fpath(folder_path, fname):
    return folder_path / f'{fname}{MRDS_EXT}'


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
