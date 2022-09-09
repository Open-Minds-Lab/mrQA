import logging

from pathlib import Path


def set_logging(name):
    format_string = '%(asctime)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt=format_string)
    handler = logging.StreamHandler()
    # dup_filter = DuplicateFilter()
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    # handler.addFilter(dup_filter)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
    # logging.basicConfig(filename=Path(metadata_root) / 'execution.log',
    #                     format='%(asctime)s | %(levelname)s: %(message)s',
    #                     level=level)
