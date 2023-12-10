import json
import tempfile
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import List

from MRdataset import valid_dirs
from MRdataset.base import BaseDataset
from bokeh.palettes import turbo, d3
from mrQA.config import status_fpath
from protocol import BaseSequence


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
        # performed. Hence, we set data_source in super() to temp dir.
        try:
            data_source = valid_dirs(data_source)
        except (OSError, ValueError):
            data_source = tempfile.gettempdir()

        super().__init__(name=name, data_source=data_source,
                         ds_format=ds_format)

        # If the sequence name was modified, then we need to keep track of
        # the original sequence name as well. For example, if the sequence
        # name was modified from T1w to T1w_modified, then we need to keep
        # track of the original sequence name T1w as well. Why is modification
        # of sequence name required? For example, if the sequence name is
        # same, but the sequence is acquired twice, then we need to modify
        # the sequence name to distinguish between the two sequences.
        self._org2mod_seq_names = {}
        self._mod2org_seq_names = {}

    def get_modified_seq_name(self, seq_name):
        """Get the modified sequence name"""
        return self._org2mod_seq_names[seq_name]

    def _get_original_seq_name(self, seq_name):
        """Get the original sequence name"""
        return self._mod2org_seq_names[seq_name]

    def set_modified_seq_name(self, original, modified):
        """Set the modified sequence name"""
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

        # If the sequence name was modified, then we need to keep track of
        # the original sequence name as well. For example, if the sequence
        # name was modified from T1w to T1w_modified, then we need to keep
        # track of the original sequence name T1w as well. Why is modification
        # of sequence name required? For example, if the sequence name is
        # same, but the sequence is acquired twice, then we need to modify
        # the sequence name to distinguish between the two sequences.

        self._org2mod_seq_names = {}
        self._mod2org_seq_names = {}

    def get_modified_seq_name(self, seq_name):
        """Get the modified sequence name"""
        return self._org2mod_seq_names[seq_name]

    def get_original_seq_name(self, seq_name):
        """Get the original sequence name"""
        return self._mod2org_seq_names[seq_name]

    def set_modified_seq_name(self, original, modified):
        """Set the modified sequence name"""
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

        # Dictionary to store all non-compliant parameters
        self._nc_flat_map = {}
        self._nc_tree_map = {}
        self._nc_params_map = {}

        # Set to store all sequence pairs that were checked for vertical audit
        self._vt_sequences = set()

        # If the sequence name was modified, then we need to keep track of
        # the original sequence name as well. For example, if the sequence
        # name was modified from T1w to T1w_modified, then we need to keep
        # track of the original sequence name T1w as well. Why is modification
        # of sequence name required? For example, if the sequence name is
        # same, but the sequence is acquired twice, then we need to modify
        # the sequence name to distinguish between the two sequences.
        self._org2mod_seq_names = {}
        self._mod2org_seq_names = {}

    def get_modified_seq_name(self, seq_name):
        """Get the modified sequence name"""
        try:
            return self._org2mod_seq_names[seq_name]
        except KeyError:
            return seq_name

    def get_original_seq_name(self, seq_name):
        """Get the original sequence name"""
        try:
            return self._mod2org_seq_names[seq_name]
        except KeyError:
            return seq_name

    def set_modified_seq_name(self, original, modified):
        """Set the modified sequence name"""
        self._org2mod_seq_names[original] = modified
        self._mod2org_seq_names[modified] = original

    def get_vt_sequences(self) -> List:
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

    def _is_scanned_before(self, date, seq):
        # Provide an option to include those subjects that were
        # scanned after the given date
        content_date = seq['ContentDate'].get_value()
        # Suppose date for report generation is 2023-11-21 01:00:00 am
        # However content date doesn't have time information, so it is
        # 2023-11-21 00:00:00 am. Now, if we compare the two dates, date for
        # report generation will always be greater than content date,
        # even though the scan could have been performed on the same day.
        # Hence, we add 1 day to content date, so that the two dates
        # can be compared.

        # A better option is to use content time, but not all scanners
        # provide content time. Hence, we use content date + 1 day. This means
        # that the scan will be skipped only if it was performed at least
        # 1 day before the date of report generation.
        if date >= content_date + timedelta(days=1):
            return True
        return False

    def generate_hz_log(self, parameters, suppl_params, filter_fn=None,
                        verbosity=1, date=None):
        sequences = self.get_sequence_ids()
        nc_log = {}
        for seq_id in sequences:
            for param_name in parameters:
                for param_tupl, sub, path, seq in self.get_nc_param_values(
                        seq_id, param_name):
                    if self._is_scanned_before(date, seq):
                        continue
                    if param_name not in nc_log:  # empty
                        nc_log[param_name] = []

                    nc_dict = self._populate_nc_dict(param_tuple=param_tupl,
                                                     sub=sub, path=path,
                                                     seq=seq, seq_ids=seq_id,
                                                     suppl_params=suppl_params,
                                                     verbosity=verbosity)
                    nc_log[param_name].append(nc_dict)
        return nc_log

    def _populate_nc_dict(self, param_tuple, seq_ids, sub, path, seq,
                          suppl_params, verbosity):

        nc_dict = {}
        nc_dict['date'] = str(seq['ContentDate'].get_value().date())
        nc_dict['subject'] = sub
        nc_dict['sequence_name'] = seq_ids

        # if additional parameters have to be included in the log
        if suppl_params:
            for i in suppl_params:
                nc_dict[i] = seq[i].get_value()

        if verbosity > 1:
            nc_dict['values'] = [p.get_value() for p in param_tuple]
        if verbosity > 2:
            nc_dict['path'] = str(path)
        return nc_dict

    def generate_nc_log(self, parameters, filter_fn=None, output_dir=None,
                        suppl_params=None, audit='vt', verbosity=1, date=None):
        """
        Generate a log of all non-compliant parameters in the dataset.
        Apart from returning the log, it also dumps the log as a json file
        """
        nc_log = {}
        if audit == 'hz':
            nc_log = self.generate_hz_log(parameters, suppl_params,
                                          filter_fn, verbosity, date=date)
        elif audit == 'vt':
            nc_log = self.generate_vt_log(parameters, suppl_params,
                                          filter_fn, verbosity, date=date)
        if audit not in ['vt', 'hz']:
            raise ValueError('Expected one of [vt, hz], got {}'.format(audit))

        # if output_dir is provided, dump it as a json file
        if nc_log and output_dir is not None:
            filepath = status_fpath(output_dir, audit=audit)
            with open(filepath, 'w') as f:
                json.dump(nc_log, f, indent=4)
        return nc_log

    def generate_vt_log(self, parameters, suppl_params, filter_fn=None,
                        verbosity=1, date=None):

        nc_log = {}
        sequence_pairs = self.get_vt_sequences()

        # Don't create the log for all sequence pairs. For example, we only
        # want to highlight the issues in field-map and epi sequences.
        for pair in filter(filter_fn, sequence_pairs):
            for param_name in parameters:
                for param_tuple, sub, path, seq in self.get_vt_param_values(
                        pair, param_name):
                    # Provide a date to include those subjects that were
                    # scanned after the given date
                    if self._is_scanned_before(date, seq):
                        continue

                    if param_name not in nc_log:  # empty
                        nc_log[param_name] = []

                    nc_dict = self._populate_nc_dict(param_tuple=param_tuple,
                                                     sub=sub, path=path,
                                                     seq=seq, seq_ids=pair,
                                                     suppl_params=suppl_params,
                                                     verbosity=verbosity)
                    nc_log[param_name].append(nc_dict)
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

    def get_nc_param_values(self, seq_id, param_name, ref_seq=None):
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
            the field map sequence. If ref_seq is provided, it returns only
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

    def _get_all_nc_param_values(self, seq_id, param_name, subject_id,
                                 session_id, ref_seq=None):
        """
        Returns a list of all non-compliant parameter values for a given
        sequence id, subject_id, session_id and parameter name.
        """
        for run_id in self._nc_tree_map[param_name][seq_id][
                subject_id][session_id][ref_seq]:
            param_tupl = self._nc_tree_map[param_name][seq_id][
                subject_id][session_id][ref_seq][run_id]  # noqa
            path = self.get_path(subject_id, session_id,
                                 seq_id, run_id)
            seq = self.get(subject_id, session_id, seq_id, run_id)
            yield param_tupl, subject_id, path, seq

    def get_vt_param_values(self, seq_pair, param_name):
        """Wrapper around get_nc_param_values() for vertical audit"""
        seq1, seq2 = seq_pair
        if seq1 not in self._nc_params_map:
            return
        yield from self.get_nc_param_values(seq1, param_name, seq2)

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
        non_compliant_params: List[Tuple]
            List of non-compliant parameters. Each tuple contains
            non-compliant parameter and the reference parameter.
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

        for param_tupl in non_compliant_params:
            # if not isinstance(param_tupl, BaseParameter):
            #     raise TypeError(
            #         'Expected BaseParameter, got {}'.format(type(param_tupl)))

            param_name = param_tupl[0].name
            self._nc_flat_map[
                (param_name, subject_id, session_id, seq_id, ref_seq,
                 run_id)] = param_tupl
            self._nc_tree_add_node(subject_id=subject_id, session_id=session_id,
                                   seq_id=seq_id, run_id=run_id,
                                   param=param_tupl, param_name=param_name,
                                   ref_seq=ref_seq)
            if seq_id not in self._nc_params_map:
                self._nc_params_map[seq_id] = set()
            self._nc_params_map[seq_id].add(param_name)

    def _nc_tree_add_node(self, subject_id, session_id, seq_id, run_id,
                          param, param_name, ref_seq=None):
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
