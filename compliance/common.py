import logging
from collections import defaultdict

from pathlib import Path


def set_logging(metadata_root, level):
    logging.basicConfig(filename=Path(metadata_root) / 'execution.log',
                        format='%(asctime)s | %(levelname)s: %(message)s',
                        level=level)


class ModalityNode:
    """
    Class specifying and managing a modality.
    Encapsulates all the details necessary for a single modality.
    Maintains which subjects are compliant or not.
    """
    def __init__(self, path):
        self.path = path
        self.fully_compliant = False
        self.name = None
        self.error = False
        self.subjects = list()
        self.params = defaultdict()

        self.delta = None

        # various lists
        self.compliant = list()
        self.non_compliant = list()
        self.error_children = list()

    def add_subject(self, other):
        self.subjects.append(other)


class SubjectNode(object):
    """Container to manage properties and issues at the subject level"""

    def __init__(self, sub_path):
        self.path = sub_path
        self.issues = list()
        self.params = dict()
