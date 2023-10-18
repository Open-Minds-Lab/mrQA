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
        return [i['parameter'].name for i in self._nc_params_map[seq_id]]
        # return list(self._nc_params_map[seq_id])

    def get_non_compliant_param_values(self, seq_id, param_name, ref_seq=None):
        for entry in self._nc_params_map[seq_id]:
            if entry['parameter'].name == param_name:
                if ref_seq is None:
                    yield (entry['parameter'],
                           (entry['subject_id'], entry['path']))
                elif ref_seq == entry['ref_sequence']:
                    yield (entry['parameter'],
                           (entry['subject_id'], entry['path']))


    def get_non_compliant_params(self, subject_id, session_id, seq_id, run_id):
        return self._nc_tree_map[subject_id][session_id][seq_id][run_id]

    def get_path(self, subject_id, session_id, seq_id, run_id):
        return str(self._tree_map[subject_id][session_id][seq_id][run_id].path)

    def add_non_compliant_params(self, subject_id, session_id, seq_id, run_id,
                                 seq, non_compliant_params, ref_seq=None):
        """adds a given subject, session or run to the dataset"""
        if ref_seq and (seq_id == ref_seq):
            raise ValueError('Both seq and ref_seq cannot be the same, '
                             'and still be non-compliant')

        for param in non_compliant_params:
            if seq_id not in self._nc_params_map:
                self._nc_params_map[seq_id] = []

            self._nc_params_map[seq_id].append({
                'ref_sequence': ref_seq,
                'parameter'   : param,
                'subject_id'  : subject_id,
                'session_id'  : session_id,
                'run_id'      : run_id,
                'seq'         : seq,
                'path'        : seq.path}
            )

            self._nc_flat_map[
                (subject_id, session_id, seq_id, run_id, param.name)] = param
            self._nc_tree_add_node(subject_id=subject_id, session_id=session_id,
                                   seq_id=seq_id, run_id=run_id, param=param)

    def _nc_tree_add_node(self, subject_id, session_id, seq_id, run_id,
                          param):
        """helper to add nodes deep in the tree

        hierarchy: Subject > Session > Sequence > Run

        """

        if subject_id not in self._nc_tree_map:
            self._nc_tree_map[subject_id] = dict()

        if session_id not in self._nc_tree_map[subject_id]:
            self._nc_tree_map[subject_id][session_id] = dict()

        if seq_id not in self._nc_tree_map[subject_id][session_id]:
            self._nc_tree_map[subject_id][session_id][seq_id] = dict()

        if run_id not in self._nc_tree_map[subject_id][session_id][seq_id]:
            self._nc_tree_map[subject_id][session_id][seq_id][run_id] = dict()

        # if param.name not in self._nc_tree_map[subject_id][session_id][seq_id][run_id]:
        self._nc_tree_map[subject_id][session_id][seq_id][run_id][
            param.name] = param

    def load(self):
        pass
