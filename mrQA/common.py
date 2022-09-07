import logging

from pathlib import Path


def set_logging(metadata_root, level):
    logging.basicConfig(filename=Path(metadata_root) / 'execution.log',
                        format='%(asctime)s | %(levelname)s: %(message)s',
                        level=level)
