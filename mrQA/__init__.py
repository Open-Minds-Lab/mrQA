"""Top-level package for mrQA."""

__author__ = """Harsh Sinha"""
__email__ = 'harsh.sinha@pitt.edu'
__version__ = '0.1.0'

from mrQA.project import check_compliance

from . import _version
__version__ = _version.get_versions()['version']
