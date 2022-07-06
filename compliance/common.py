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
    def __init__(self, path=None, name=None):
        self.path = path

        self.name = name
        self.error = False
        self.subjects = list()
        self.params = defaultdict()

        self.delta = None

        # various lists
        self.compliant = list()
        self.non_compliant = list()
        self.error_children = list()

        self.fully_compliant = False

    def add_subject(self, subj_node):
        self.subjects.append(subj_node)

    def __str__(self):
        return "Modality {} with {} subjects".format(self.name, len(self.subjects))

    def __repr__(self):
        return self.__str__()


class SubjectNode(object):
    """Container to manage properties and issues at the subject level"""

    def __init__(self, sub_path):

        self.path = Path(sub_path).resolve()

        self.params = dict()

        self.issues = list()

        self.delta = list()
