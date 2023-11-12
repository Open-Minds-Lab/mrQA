import json
import tempfile
from abc import ABC, abstractmethod
from typing import List

from MRdataset import valid_dirs
from MRdataset.base import BaseDataset
from bokeh.palettes import turbo, d3
from protocol import BaseParameter, BaseSequence

from mrQA.config import ATTRIBUTE_SEPARATOR


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
        # BaseDataset checks if data_source is valid, and if not,
        # it raises an error.
        # It is very likely that data is processed by MRdataset on a
        # different machine, and the processed data is then transferred to
        # another machine for audit. In such cases, the data_source of
        # original dataset will be invalid on the machine where audit is
        # performed. Hence, we set data_source in super() to None.
        try:
            data_source = valid_dirs(data_source)
        except (OSError, ValueError):
            data_source = tempfile.gettempdir()

        super().__init__(name=name, data_source=data_source,
                         ds_format=ds_format)
        self._org2mod_seq_names = {}
        self._mod2org_seq_names = {}

    def get_modified_seq_name(self, seq_name):
        return self._org2mod_seq_names[seq_name]

    def _get_original_seq_name(self, seq_name):
        return self._mod2org_seq_names[seq_name]

    def set_modified_seq_name(self, original, modified):
        self._org2mod_seq_names[original] = modified
        self._mod2org_seq_names[modified] = original

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
        # BaseDataset checks if data_source is valid, and if not,
        # it raises an error.
        # It is very likely that data is processed by MRdataset on a
        # different machine, and the processed data is then transferred to
        # another machine for audit. In such cases, the data_source of
        # original dataset will be invalid on the machine where audit is
        # performed. Hence, we set data_source in super() to None.
        try:
            data_source = valid_dirs(data_source)
        except (OSError, ValueError):
            data_source = tempfile.gettempdir()

        super().__init__(name=name, data_source=data_source,
                         ds_format=ds_format)
        self._org2mod_seq_names = {}
        self._mod2org_seq_names = {}

    def get_modified_seq_name(self, seq_name):
        return self._org2mod_seq_names[seq_name]

    def get_original_seq_name(self, seq_name):
        return self._mod2org_seq_names[seq_name]

    def set_modified_seq_name(self, original, modified):
        self._org2mod_seq_names[original] = modified
        self._mod2org_seq_names[modified] = original

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
        # BaseDataset checks if data_source is valid, and if not,
        # it raises an error.
        # It is very likely that data is processed by MRdataset on a
        # different machine, and the processed data is then transferred to
        # another machine for audit. In such cases, the data_source of
        # original dataset will be invalid on the machine where audit is
        # performed. Hence, we set data_source in super() to None.
        try:
            data_source = valid_dirs(data_source)
        except (OSError, ValueError):
            data_source = tempfile.gettempdir()

        super().__init__(name=name, data_source=data_source,
                         ds_format=ds_format)
        self._nc_flat_map = {}
        self._nc_tree_map = {}
        self._nc_params_map = {}
        self._vt_sequences = set()
        self._org2mod_seq_names = {}
        self._mod2org_seq_names = {}

    def get_modified_seq_name(self, seq_name):
        return self._org2mod_seq_names[seq_name]

    def get_original_seq_name(self, seq_name):
        return self._mod2org_seq_names[seq_name]

    def set_modified_seq_name(self, original, modified):
        self._org2mod_seq_names[original] = modified
        self._mod2org_seq_names[modified] = original

    def get_vt_sequences(self)->List:
        """
        Returns a list of all sequences that were checked for vertical
        audit.
        """
        return list(self._vt_sequences)

    def add_sequence_pair_names(self, list_seqs):
        """
        Add a sequence to the list of sequences that were checked for
        vertical audit.
        """
        self._vt_sequences.add(list_seqs)

    def get_nc_log(self, parameters, filter_fn=None, output_dir=None,
                   audit='vt'):
        """Generate a log of all non-compliant parameters in the dataset"""
        nc_log = {}
        if audit == 'hz':
            # TODO: implement it later
            raise NotImplementedError('Creating log files for horizontal audit'
                                      ' is not supported yet. Use the html '
                                      'report instead.')
        if audit not in ['vt', 'hz']:
            raise ValueError('Expected one of [vt, hz], got {}'.format(audit))

        # Implementation for vertical audit only
        if filter_fn is None:
            filter_fn = lambda x : True

        sequence_pairs = self.get_vt_sequences()

        for pair in filter(filter_fn, sequence_pairs):
            for param_name in parameters:
                nc_values = list(self.get_vt_param_values(pair, param_name))
                for tupl1, tupl2 in nc_values:
                    param1, (sub1, path1) = tupl1
                    param2, (sub2, path2) = tupl2

                    # vertical audit works within session. For sanity check,
                    # we assert that the subject ids are same. If not, something
                    # is wrong with the implementation.
                    assert sub1 == sub2, (f'Expected same subject ids, '
                                          f'got {sub1} and {sub2}')

                    if param_name not in nc_log:  # empty
                        nc_log[param_name] = []

                    nc_log[param_name].append({
                        'subject': sub1,
                        'sequence_names': pair,
                        'values' : (param1.get_value(), param2.get_value()),
                        'paths' : (str(path1), str(path2))
                    })
        # if output_dir is provided, dump it as a json file
        if nc_log and output_dir is not None:
            filename = self.name + '_vt_log.json'
            with open(output_dir / filename, 'w') as f:
                json.dump(nc_log, f, indent=4)
        return nc_log

    def get_nc_param_ids(self, seq_id):
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

    def get_nc_param_values(self, seq_id, param_name,
                            ref_seq=None):
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
                            yield from self._get_all_nc_param_values(
                                seq_id=seq_id, param_name=param_name,
                                subject_id=subject_id, session_id=session_id,
                                ref_seq=ref_seq)

    def _get_all_nc_param_values(self, seq_id, param_name,
                                 subject_id,
                                 session_id, ref_seq=None):
        """
        Returns a list of all non-compliant parameter values for a given
        sequence id and parameter name.
        """
        for run_id in self._nc_tree_map[param_name][seq_id][
                subject_id][session_id][ref_seq]:
            param = self._nc_tree_map[param_name][seq_id][
                subject_id][session_id][ref_seq][run_id]  # noqa
            path = self.get_path(subject_id, session_id,
                                 seq_id, run_id)
            yield param, (subject_id, path)

    def get_vt_param_values(self, seq_pair, param_name):
        seq1, seq2 = seq_pair
        list1 = list(self.get_nc_param_values(seq1, param_name, seq2))
        list2 = list(self.get_nc_param_values(seq2, param_name, seq1))
        yield from zip(list1, list2)

    def get_nc_subject_ids(self, seq_id, param_name, ref_seq=None):
        """
        Returns a list of all non-compliant subject ids for a given
        sequence id and parameter name. Created for vertical audit report
        as we are not listing any parameter values or paths in the report, just
        the subject ids.
        """
        if ref_seq is None:
            ref_seq = '__NOT_SPECIFIED__'
        if seq_id not in self._nc_params_map:
            # return empty generator
            return
        if param_name in self._nc_params_map[seq_id]:
            if seq_id in self._nc_tree_map[param_name]:
                for subject_id in self._nc_tree_map[param_name][seq_id]:
                    for session_id in (
                            self._nc_tree_map[param_name][seq_id][subject_id]):
                        if ref_seq in \
                            self._nc_tree_map[param_name][seq_id][subject_id][
                                session_id]:
                            yield subject_id

    def total_nc_subjects_by_sequence(self, seq_id, ref_seq=None):
        """
        Returns the total number of non-compliant subjects for a given
        sequence id and parameter name.
        """
        subject_ids = set()
        for parameter in self.get_nc_param_ids(seq_id=seq_id):
            for subject_id in self.get_nc_subject_ids(
                    seq_id=seq_id,
                    param_name=parameter,
                    ref_seq=ref_seq):
                subject_ids.add(subject_id)
        return len(subject_ids)

    def total_nc_subjects_by_parameter(self, param_name):
        """
        Returns the total number of non-compliant subjects for a given
        sequence id and parameter name.
        """
        total_subjects = set()
        for seq_id, ref_seq in self.get_vt_sequences():
            if seq_id in self._nc_params_map:
                subjects = list(self.get_nc_subject_ids(seq_id=seq_id,
                                                        param_name=param_name,
                                                        ref_seq=ref_seq))
                total_subjects.update(subjects)
        return len(total_subjects)

    def get_nc_params(self, subject_id, session_id, seq_id, run_id):
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

    def add_nc_params(self, subject_id, session_id, seq_id, run_id,
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


class BasePlot(ABC):
    _name = None

    def __init__(self, name=None):
        if name is not None:
            self._name = name
            self.div = None
            self.script = None
            self.plot_height = None
            self.plot_width = None
            self.title = None
            self.x_axis_label = None
            self.y_axis_label = None
            self.x_range = None
            self.label = None
            self.legend_label = None
            self.colors = None

    @abstractmethod
    def plot(self, non_compliant_ds, complete_ds, parameters):
        """Creates a plot for the given data"""

    @abstractmethod
    def compute_counts(self, non_compliant_ds, complete_ds, parameters):
        """Computes the counts for the given dataset and parameters."""

    @abstractmethod
    def get_plot_components(self, data):
        """getter method for plotting components"""

    @abstractmethod
    def get_counter(self, dataset, parameters):
        """getter method for counter"""

    def set_cmap(self, length):
        """Sets the color map for the plot"""
        if length > 10:
            colors = turbo(length)
        else:
            palette = d3['Category10']
            if length > 3:
                colors = palette[length]
            else:
                colors = palette[10][:length]
        self.colors = colors
