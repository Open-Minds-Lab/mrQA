from MRdataset.experiment import BaseDataset


class CompliantDataset(BaseDataset):
    def __init__(self, name=None):
        super().__init__(name=name, data_source=None)

    def load(self):
        pass


class NonCompliantDataset(BaseDataset):
    def __init__(self, name=None):
        super().__init__(name=name, data_source=None)
        self._nc_flat_map = {}
        self._nc_tree_map = {}

    def get_non_compliant_params(self, seq_id):
        for subj in self._subj_ids:
            for sess in self._nc_tree_map[subj]:
                if seq_id in self._nc_tree_map[subj][sess]:
                    for run in self._nc_tree_map[subj][sess][seq_id]:
                        yield (subj, sess, run,
                               self._nc_tree_map[subj][sess][seq_id][run])

    def add_non_compliant_params(self, subject_id, session_id, run_id, seq_id,
            non_compliant_params):
        """adds a given subject, session or run to the dataset"""

        if (subject_id, session_id, run_id, seq_id) not in self._nc_flat_map:
            self._nc_flat_map[(subject_id, session_id, run_id, seq_id)] = []
        self._nc_tree_add_node(subject_id, session_id, run_id, seq_id,
                            non_compliant_params)
        self._nc_flat_map[(subject_id, session_id, run_id, seq_id)].extend(
            non_compliant_params
        )

    def _nc_tree_add_node(self, subject_id, session_id, run_id, seq_name,
                       non_compliant_params):
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
            self._nc_tree_map[subject_id][session_id][seq_name][run_id] = []

        self._nc_tree_map[subject_id][session_id][seq_name][run_id].extend(
            non_compliant_params
        )


    def load(self):
        pass
