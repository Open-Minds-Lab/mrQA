"""Top-level package for mrQA."""

__author__ = """Harsh Sinha"""
__email__ = 'harsh.sinha@pitt.edu'
# __version__ = '0.1.0'

import logging

from MRdataset.config import configure_logger

logger = logging.getLogger(__name__)
logger = configure_logger(logger, output_dir=None, mode='w')

from mrQA.monitor import monitor # noqa
from mrQA.project import check_compliance # noqa
from . import _version # noqa

__version__ = _version.get_versions()['version'] # noqa
