from MRdataset.base import BaseDataset
from mrQA import logger

class CompliantDataset(BaseDataset):
    def __init__(self, name=None):
        super().__init__(name=name, data_source=None)

    def load(self):
        pass


class UndeterminedDataset(BaseDataset):
    """
    Container to manage properties of sequences whose reference protocol could
     not be determined. Reasons could be:
    1. No reference protocol was found
    2. Multiple reference protocols were found
    3. Reference protocol was not valid
    """

    def __init__(self, name=None):
        super().__init__(name=name, data_source=None)

    def load(self):
        pass


class NonCompliantDataset(BaseDataset):
    def __init__(self, name=None):
        super().__init__(name=name, data_source=None)
        self._nc_flat_map = {}
        self._nc_tree_map = {}
        self._nc_params_map = {}

    def get_non_compliant_param_ids(self, seq_id):
        return list(self._nc_params_map[seq_id])

    def get_non_compliant_param_values(self, seq_id, param_name=None):
        for subj in self._subj_ids:
            for sess in self._nc_tree_map[subj]:
                if seq_id in self._nc_tree_map[subj][sess]:
                    for run in self._nc_tree_map[subj][sess][seq_id]:
                        nc_params = self._nc_tree_map[subj][sess][seq_id][run]
                        if param_name in nc_params.keys():
                            params = nc_params[param_name]
                            path = self.get_path(subj, sess, run, seq_id)
                            yield params, (subj, path)

    def get_path(self, subject_id, session_id, run_id, seq_id):
        return str(self._tree_map[subject_id][session_id][seq_id][run_id].path)

    def add_non_compliant_params(self, subject_id, session_id, run_id, seq_id,
                                 non_compliant_params):
        """adds a given subject, session or run to the dataset"""
        for param in non_compliant_params:
            if seq_id not in self._nc_params_map:
                self._nc_params_map[seq_id] = set()
            self._nc_params_map[seq_id].add(param.name)

            # if (subject_id, session_id, run_id, seq_id, param.name) not in self._nc_flat_map:
            self._nc_flat_map[(subject_id, session_id, run_id, seq_id, param.name)] = param
            self._nc_tree_add_node(subject_id, session_id, run_id,
                                   seq_id, param)

    def _nc_tree_add_node(self, subject_id, session_id, run_id, seq_name,
                          param):
        """helper to add nodes deep in the tree

        hierarchy: Subject > Session > Sequence > Run

        """

        if subject_id not in self._nc_tree_map:
            self._nc_tree_map[subject_id] = dict()

        if session_id not in self._nc_tree_map[subject_id]:
            self._nc_tree_map[subject_id][session_id] = dict()

        if seq_name not in self._nc_tree_map[subject_id][session_id]:
            self._nc_tree_map[subject_id][session_id][seq_name] = dict()

        if run_id not in self._nc_tree_map[subject_id][session_id][seq_name]:
            self._nc_tree_map[subject_id][session_id][seq_name][run_id] = dict()

        # if param.name not in self._nc_tree_map[subject_id][session_id][seq_name][run_id]:
        self._nc_tree_map[subject_id][session_id][seq_name][run_id][param.name] = param

    def load(self):
        pass
