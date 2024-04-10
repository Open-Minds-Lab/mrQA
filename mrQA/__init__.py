"""Top-level package for mrQA."""

__author__ = """Harsh Sinha"""
__email__ = 'harsh.sinha@pitt.edu'
# __version__ = '0.1.0'

import logging
import sys

from mrQA.config import configure_logger

logger = logging.getLogger(__name__)
logger = configure_logger(logger, output_dir=None, mode='w')

from mrQA.monitor import monitor # noqa
from mrQA.project import check_compliance # noqa
# from . import _version # noqa
#
# __version__ = _version.get_versions()['version'] # noqa

try:
    from mrQA._version import __version__
except ImportError:
    if sys.version_info < (3, 8):
        from importlib_metadata import version
    else:
        from importlib.metadata import version

    try:
        __version__ = version('mrQA')
    except Exception:
        __version__ = "unknown"
