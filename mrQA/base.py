from MRdataset.base import BaseDataset


class CompliantDataset(BaseDataset):
    def __init__(self, name=None, data_source=None, ds_format=None):
        super().__init__(name=name, data_source=data_source,
                         ds_format=ds_format)

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

    def __init__(self, name=None, data_source=None, ds_format=None):
        super().__init__(name=name,
                         data_source=data_source,
                         ds_format=ds_format)

    def load(self):
        pass


class NonCompliantDataset(BaseDataset):
    def __init__(self, name=None, data_source=None, ds_format=None):
        super().__init__(name=name,
                         data_source=data_source,
                         ds_format=ds_format)
        self._nc_flat_map = {}
        self._nc_tree_map = {}
        self._nc_params_map = {}

    def get_non_compliant_param_ids(self, seq_id):
        if seq_id not in self._nc_params_map:
            return []
        else:
            return list(self._nc_params_map[seq_id])

    def get_non_compliant_param_values(self, seq_id, param_name, ref_seq=None):
        if ref_seq is None:
            ref_seq = '__NOT_SPECIFIED__'
        if param_name in self._nc_params_map[seq_id]:
            if seq_id in self._nc_tree_map[param_name]:
                for subject_id in self._nc_tree_map[param_name][seq_id]:
                    for session_id in self._nc_tree_map[param_name][seq_id][
                        subject_id]:
                        if ref_seq in \
                            self._nc_tree_map[param_name][seq_id][subject_id][
                                session_id]:
                            for run_id in \
                            self._nc_tree_map[param_name][seq_id][subject_id][
                                session_id][ref_seq]:
                                param = self._nc_tree_map[param_name][seq_id][
                                    subject_id][session_id][ref_seq][run_id]
                                path = self.get_path(subject_id, session_id,
                                                     seq_id, run_id)
                                yield param, (subject_id, path)

    def get_non_compliant_params(self, subject_id, session_id, seq_id, run_id):
        for param_name in self._nc_tree_map:
            yield self._nc_tree_map[param_name][subject_id][session_id][seq_id][
                run_id]

    def get_path(self, subject_id, session_id, seq_id, run_id):
        return str(self._tree_map[subject_id][session_id][seq_id][run_id].path)

    def add_non_compliant_params(self, subject_id, session_id, seq_id, run_id,
                                 seq, non_compliant_params, ref_seq=None):
        """adds a given subject, session or run to the dataset"""
        if ref_seq and (seq_id == ref_seq):
            raise ValueError('Both seq and ref_seq cannot be the same for given'
                             f'subject {subject_id}, session {session_id}, '
                             'and still be non-compliant!')
        if ref_seq is None:
            ref_seq = '__NOT_SPECIFIED__'

        for param in non_compliant_params:
            self._nc_flat_map[
                (param.name, subject_id, session_id, seq_id, ref_seq,
                 run_id)] = param
            self._nc_tree_add_node(subject_id=subject_id, session_id=session_id,
                                   seq_id=seq_id, run_id=run_id, param=param,
                                   ref_seq=ref_seq)
            if seq_id not in self._nc_params_map:
                self._nc_params_map[seq_id] = set()
            self._nc_params_map[seq_id].add(param.name)

    def _nc_tree_add_node(self, subject_id, session_id, seq_id, run_id,
                          param, ref_seq=None):
        """helper to add nodes deep in the tree

        hierarchy: Subject > Session > Sequence > Run

        """
        # TODO: improve it later
        if ref_seq is None:
            ref_seq = '__NOT_SPECIFIED__'

        param_name = param.name
        if param_name not in self._nc_tree_map:
            self._nc_tree_map[param_name] = dict()

        if seq_id not in self._nc_tree_map[param_name]:
            self._nc_tree_map[param_name][seq_id] = dict()

        if subject_id not in self._nc_tree_map[param_name][seq_id]:
            self._nc_tree_map[param_name][seq_id][subject_id] = dict()

        if session_id not in self._nc_tree_map[param_name][seq_id][subject_id]:
            self._nc_tree_map[param_name][seq_id][subject_id][
                session_id] = dict()

        if ref_seq not in self._nc_tree_map[param_name][seq_id][subject_id][
            session_id]:
            self._nc_tree_map[param_name][seq_id][subject_id][session_id][
                ref_seq] = dict()

        if run_id not in \
            self._nc_tree_map[param_name][seq_id][subject_id][session_id][
                ref_seq]:
            self._nc_tree_map[param_name][seq_id][subject_id][session_id][
                ref_seq][run_id] = dict()

        self._nc_tree_map[param_name][seq_id][subject_id][session_id][ref_seq][
            run_id] = param

    def load(self):
        pass
