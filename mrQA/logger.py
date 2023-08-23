import logging
from pathlib import Path


DEBUG_FORMATTER = '%(filename)s:%(name)s:%(funcName)s:%(lineno)d: %(message)s'
"""Debug file formatter."""

INFO_FORMATTER = '%(asctime)s - %(levelname)s - %(message)s'
"""Log file and stream output formatter."""

ROOT_FOLDER = Path.home()/'.protocol'
"""Root folder for mrQA."""
if not ROOT_FOLDER.exists():
    ROOT_FOLDER.mkdir(parents=True, exist_ok=True)

DEBUG_FILE = ROOT_FOLDER/'debug.log'
"""Debug file name."""

INFO_FILE = ROOT_FOLDER/'info.log'
"""Log file name."""


def init_log_files(log, mode='w'):
    """
    Initiate log files.

    Parameters
    ----------
    log : logging.Logger
        The logger object.
    mode : str, (``'w'``, ``'a'``)
        The writing mode to the log files.
        Defaults to ``'w'``, overwrites previous files.    """

    db = logging.FileHandler(DEBUG_FILE, mode=mode)
    db.setLevel(logging.DEBUG)
    db.setFormatter(logging.Formatter(DEBUG_FORMATTER))

    info = logging.FileHandler(INFO_FILE, mode=mode)
    info.setLevel(logging.INFO)
    info.setFormatter(logging.Formatter(INFO_FORMATTER))

    log.addHandler(db)
    log.addHandler(info)
