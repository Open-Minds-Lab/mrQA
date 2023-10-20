from MRdataset.base import BaseDataset
from protocol import BaseParameter, BaseSequence


class CompliantDataset(BaseDataset):
    """
    Container to manage properties of sequences that are compliant with the
    reference protocol. It is a subclass of BaseDataset, and inherits all
    its properties and methods.

    Parameters
    ----------
    name: str
        Name of the dataset
    data_source: Path | List | str
        Path to the dataset
    ds_format: str
        Format of the dataset, one of ['dicom']
    """

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

    Parameters
    ----------
    name: str
        Name of the dataset
    data_source: Path | List | str
        Path to the dataset
    ds_format: str
        Format of the dataset, one of ['dicom']
    """

    def __init__(self, name=None, data_source=None, ds_format=None):
        super().__init__(name=name,
                         data_source=data_source,
                         ds_format=ds_format)

    def load(self):
        pass


class NonCompliantDataset(BaseDataset):
    """
    Container to manage properties of sequences that are non-compliant with the
    reference protocol. It is a subclass of BaseDataset, and inherits all
    its properties and methods.

    Parameters
    ----------
    name: str
        Name of the dataset
    data_source: Path | List | str
        Path to the dataset
    ds_format: str
        Format of the dataset, one of ['dicom']
    """

    def __init__(self, name=None, data_source=None, ds_format=None):
        super().__init__(name=name,
                         data_source=data_source,
                         ds_format=ds_format)
        self._nc_flat_map = {}
        self._nc_tree_map = {}
        self._nc_params_map = {}

    def get_non_compliant_param_ids(self, seq_id):
        """
        Returns a list of all non-compliant parameter names for a given
        sequence id.

        Parameters
        ----------
        seq_id: str
            Name of the sequence, e.g. T1w, T2w etc.
        """
        if seq_id not in self._nc_params_map:
            return []
        else:
            return list(self._nc_params_map[seq_id])

    def get_non_compliant_param_values(self, seq_id, param_name, ref_seq=None):
        """
        Returns a list of all non-compliant parameter values for a given
        sequence id and parameter name.

        Parameters
        ----------
        seq_id: str
            Name of the sequence, e.g. rs-fMRI etc.
        param_name: str
            Name of the parameter, e.g. RepetitionTime, EchoTime etc.
        ref_seq: str
            Name of the reference sequence, e.g. field-map

        Returns
        -------
        Iterator
            All non-compliant parameter values

        .. note:: It is recommended to also use the name of sequence used as
            the reference protocol. For horizontal audit,
            this is not essential, as each sequence is compared against its own
            reference protocol. However, in case of vertical audit, it is
            essential to provide the name of the sequence used as the
            reference protocol.
            For example, if field map and the rs-fMRI sequence are compared,
            then the seq_id can be rs-fMRI, and ref_seq can be field map.
            This will return only those values that are non-compliant with
            the field map sequence. If ref_seq is provided, returns only
            those values that are non-compliant with the reference protocol.

        """
        if ref_seq is None:
            ref_seq = '__NOT_SPECIFIED__'
        if param_name in self._nc_params_map[seq_id]:
            if seq_id in self._nc_tree_map[param_name]:
                for subject_id in self._nc_tree_map[param_name][seq_id]:
                    for session_id in (
                            self._nc_tree_map[param_name][seq_id][subject_id]):
                        if ref_seq in \
                            self._nc_tree_map[param_name][seq_id][subject_id][
                                session_id]:
                            for run_id in \
                                self._nc_tree_map[param_name][seq_id][
                                    subject_id][
                                    session_id][ref_seq]:
                                param = self._nc_tree_map[param_name][seq_id][
                                    subject_id][session_id][ref_seq][
                                    run_id]  # noqa
                                path = self.get_path(subject_id, session_id,
                                                     seq_id, run_id)
                                yield param, (subject_id, path)

    def get_non_compliant_params(self, subject_id, session_id, seq_id, run_id):
        """
        A generator that returns all non-compliant parameters for a given
        subject, session, sequence and run.

        Parameters
        ----------
        subject_id: str
            Subject ID e.g. sub-01
        session_id: str
            Session ID e.g. ses-01
        seq_id: str
            Sequence ID e.g. T1w, T2w etc.
        run_id: str
            Run ID e.g. run-01

        Returns
        -------
        Iterator
            All non-compliant parameters
        """
        for param_name in self._nc_tree_map:
            yield self._nc_tree_map[param_name][subject_id][session_id][seq_id][
                run_id]

    def get_path(self, subject_id, session_id, seq_id, run_id):
        """
        Returns the path to the folder where DICOM files for a
        given subject, session, sequence and run are stored.

        Parameters
        ----------
        subject_id: str
            Subject ID e.g. sub-01
        session_id: str
            Session ID e.g. ses-01
        seq_id: str
            Sequence ID e.g. T1w, T2w etc.
        run_id: str
            Run ID e.g. run-01

        Returns
        -------
        str
            Path to the folder where DICOM files are stored
        """
        # Get image sequence for the given subject, session, sequence and run
        img_sequence = self._tree_map[subject_id][session_id][seq_id][run_id]
        return img_sequence.path

    def add_non_compliant_params(self, subject_id, session_id, seq_id, run_id,
                                 non_compliant_params, ref_seq=None):
        """
        Add non-compliant parameters to the dataset. This is a helper function
        that is used by the (horizontal/vertical) audit to add non-compliant
        parameters to the dataset.

        Parameters
        ----------
        subject_id: str
            Subject ID e.g. sub-01
        session_id: str
            Session ID e.g. ses-01
        seq_id: str
            Sequence ID e.g. T1w, T2w etc.
        run_id: str
            Run ID e.g. run-01
        non_compliant_params: List[BaseParameter]
            List of non-compliant parameters
        ref_seq: str
            Name of the reference sequence, e.g. field-map
        """
        if ref_seq is None:
            ref_seq = '__NOT_SPECIFIED__'

        if not isinstance(non_compliant_params, list):
            raise TypeError(
                'Expected list of BaseParameter, got {}'.format(
                    type(non_compliant_params)))

        if isinstance(seq_id, BaseSequence):
            raise TypeError("Expected str, got BaseSequence. Use "
                            ".name attribute to get the name of the sequence")

        if not isinstance(seq_id, str):
            raise TypeError(
                'Expected str, got {}'.format(type(seq_id)))

        if not isinstance(ref_seq, str):
            raise TypeError(
                'Expected str, got {}'.format(type(ref_seq)))

        for param in non_compliant_params:
            if not isinstance(param, BaseParameter):
                raise TypeError(
                    'Expected BaseParameter, got {}'.format(type(param)))

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
        """
        Add a node to the tree map. This is a private function that is used by
        the (horizontal/vertical) audit to add non-compliant parameters to the
        dataset.

        Parameters
        ----------
        subject_id: str
            Subject ID e.g. sub-01
        session_id: str
            Session ID e.g. ses-01
        seq_id: str
            Sequence ID e.g. T1w, T2w etc.
        run_id: str
            Run ID e.g. run-01
        ref_seq: Optional[str]
            Name of the reference sequence, e.g. field-map
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

        if ref_seq not in (
                self._nc_tree_map[param_name][seq_id][subject_id][session_id]):
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
