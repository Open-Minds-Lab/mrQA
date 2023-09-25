"""Top-level package for mrQA."""

__author__ = """Harsh Sinha"""
__email__ = 'harsh.sinha@pitt.edu'
__version__ = '0.1.0'

import logging

from mrQA.logger import INFO_FORMATTER, init_log_files
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)
# defines the stream handler
_ch = logging.StreamHandler()  # creates the handler
_ch.setLevel(logging.WARNING)  # sets the handler info
# sets the handler formatting
_ch.setFormatter(logging.Formatter(INFO_FORMATTER))
# adds the handler to the global variable: log
logger.addHandler(_ch)
init_log_files(logger, mode='w')

from mrQA.monitor import monitor # noqa
from mrQA.project import check_compliance # noqa
from . import _version # noqa

__version__ = _version.get_versions()['version'] # noqa
