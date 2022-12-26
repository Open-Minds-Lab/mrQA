import os
import pathlib
import pickle
import subprocess
import time
import typing
import logging
import warnings
from collections import Counter
from itertools import groupby
from pathlib import Path
from typing import Union, List, Iterable

import numpy as np
from MRdataset.utils import is_hashable, param_difference, make_hashable
from MRdataset.utils import valid_dirs, timestamp
from MRdataset.log import logger
from itertools import takewhile


def get_items_upto_count(dict_, rank=1):
    values_desc_order = dict_.most_common()
    value_at_rank = values_desc_order[rank-1][1]
    return list(takewhile(lambda x: x[1] >= value_at_rank, values_desc_order))


def timestamp():
    """Generate a timestamp as a string"""
    time_string = time.strftime("%m_%d_%Y_%H_%M")
    return time_string


def majority_attribute_values(list_of_dicts, default=None):
    """
    Given a list of dictionaries, it generates the most common
    values for each key

    Parameters
    ----------
    list_of_dicts : list
        a list of dictionaries
    default : python object, default None
        a default value if the key is missing in any dictionary

    Returns
    -------
    dict
        Key-value pairs specifying the most common values for each key
    """
    maj_attr_vals = _check_args_validity(list_of_dicts)
    if not (maj_attr_vals is None):
        return maj_attr_vals

    counters_dict = dict()
    categories = set()
    for dict_ in list_of_dicts:
        categories.update(dict_.keys())
        for key, value in dict_.items():
            counter = counters_dict.get(key, Counter({default: 0}))
            value = make_hashable(value)
            counter[value] += 1
            counters_dict[key] = counter

    majority_attr_dict = {}
    for k in counters_dict.keys():
        majority_attr_dict[k] = pick_majority(counters_dict[k])
    return majority_attr_dict


def extract_reasons(data):
    """
    Given a list of tuples, extract all the elements at index 1, and return
    as a list

    Parameters
    ----------
    data : List of tuples

    Returns
    -------
    list
        List of values at index 1
    """
    return list(zip(*data))[1]


def pick_majority(counter_, default=None):
    if len(counter_) == 0:
        raise ValueError("Expected atleast one entry in counter. Got 0")
    if len(counter_) == 1:
        return list(counter_.keys())[0]
    # there are more than 1 value, remove default, and computer majority
    _ = counter_.pop(default, None)
    items_rank1 = get_items_upto_count(counter_, rank=1)
    # If there are many values for rank 1 with equal count, cannot say which is majority
    if len(items_rank1) > 1:
        return None
    return items_rank1[0][0]


def default_thread_count():
    workers = min(32, os.cpu_count() + 4)
    return workers


def _check_args_validity(list_of_dicts):
    if list_of_dicts is None:
        raise ValueError('Expected a list of dicts, Got NoneType')
    if len(list_of_dicts) == 0:
        raise ValueError('List is empty.')
    for dict_ in list_of_dicts:
        if len(dict_) == 0:
            raise ValueError("Atleast one of dictionaries is empty.")
    if len(list_of_dicts) < 3:
        logger.error("Cannot compute majority attribute values. Got less than 3 values for each "
                     "parameter. Returns majority values as None.")
        maj_attr_values = dict()
        for key in list_of_dicts[0].keys():
            maj_attr_values[key] = None
        return maj_attr_values


def split_list(dir_index: list, num_chunks: int) -> typing.Iterable[List[str]]:
    """
    Adapted from https://stackoverflow.com/questions/2130016/splitting-a-list-into-n-parts-of-approximately-equal-length # noqa

    Given a list of n elements, split it into k parts, where k = num_chunks.
    Each part has atleast n/k elements. And the remaining elements
    n % k are distributed uniformly among the sub-parts such that
    each part has almost same number of elements. The first n % k will have
    floor(n/k) + 1 elements.

    Parameters
    ----------
    dir_index : list
        list to split
    num_chunks : int
        number of parts

    Returns
    -------
    tuple of all subsets
    """
    if not is_integer_number(num_chunks):
        raise RuntimeError(f"Number of chunks must be an integer. "
                           f"Got {num_chunks}")
    if num_chunks == 0:
        raise RuntimeError("Cannot divide list into chunks of size 0")
    if len(dir_index) == 0:
        raise RuntimeError("List of directories is empty!")
    if len(dir_index) < num_chunks:
        warnings.warn(f"Got num_chunks={num_chunks}, list_size={len(dir_index)}"
                      f"Expected num_chunks < list_size",
                      stacklevel=2)
        num_chunks = len(dir_index)
    k, m = divmod(len(dir_index), num_chunks)
    #  k, m = (len(dir_index)//num_chunks, len(dir_index)%num_chunks)
    return (dir_index[i * k + min(i, m):(i + 1) * k + min(i + 1, m)]
            for i in range(num_chunks))


def txt2list(txt_filepath: Union[str, Path]) -> list:
    """
    Given a filepath to a text file, read all the lines and return as a list
    of lines.

    Parameters
    ----------
    txt_filepath: str or pathlib.Path
        valid filepath to a text file

    Returns
    -------
    list of lines in the text file

    """
    if not isinstance(txt_filepath, Path):
        txt_filepath = Path(txt_filepath).resolve()
    if not txt_filepath.exists():
        raise FileNotFoundError(f'Invalid path {txt_filepath}')
    # Generate a list of folder paths stored in given txt_file
    line_list = txt_filepath.read_text().splitlines()
    return line_list


def list2txt(path, list_):
    """
    Given a list of values, dump all the lines to a text file. Each element of
    the list is on a separate line.

    Parameters
    ----------
    path : pathlib.Path
        output path of the final text file
    list_ : list
        values to be exported in a text file
    """

    # if Path(path).exists():
    #     warnings.warn("Overwriting pre-existing index on disk.")
    with open(path, 'w') as fp:
        for line in list_:
            fp.write("%s\n" % line)


def save2pickle(dataset):
    if not dataset.modalities:
        raise EOFError('Dataset is empty!')
    with open(dataset.cache_path, "wb") as f:
        pickle.dump(dataset, f)


def execute_local(filename):
    format_params = "\n".join(['File system outputs: %O',
                               'Maximum RSS size: %M',
                               'CPU percentage used: %P',
                               'Real Time: %E',
                               'User Time: %U',
                               'Sys Time: %S'])
    ret_code = subprocess.Popen(['/usr/bin/time', '-f', format_params,
                                'bash', filename])
    ret_code.wait()
    # TODO : check if file was created
    return


def get_outliers(data, m=25.0):
    """
    Check for outliers. Adapted from
    https://stackoverflow.com/a/16562028/3140172
    Parameters
    ----------
    data
    m

    Returns
    -------

    """
    d = np.abs(data - np.median(data))
    mdev = np.median(d)
    s = d / mdev if mdev else 0.
    if np.any(s > m):
        indices = np.argwhere(s > m).flatten()
        return indices
    return None


def apply_round(dict_, decimals):
    new_dict = dict_.copy()
    for key, value in new_dict.items():
        new_dict[key] = round_if_number(value, decimals)
    return new_dict


def is_integer_number(n: Union[int, float]) -> bool:
    if isinstance(n, int):
        return True
    if isinstance(n, float):
        return n.is_integer()
    return False


def _get_runs_by_echo(modality, decimals=3):
    runs_in_modality = []
    for subject in modality.subjects:
        for session in subject.sessions:
            runs_in_modality.extend(session.runs)

    def _sort_key(run):
        return run.echo_time

    run_params_by_te = dict()
    runs_in_modality = sorted(runs_in_modality, key=_sort_key)
    for te, group in groupby(runs_in_modality, key=_sort_key):
        te_ = round_if_number(te, decimals)
        for run in list(group):
            if te_ not in run_params_by_te:
                run_params_by_te[te_] = []
            run_params_by_te[te_].append(apply_round(run.params, decimals))
    return run_params_by_te


def _validate_reference(dict_, default=None):
    if not dict_:
        return False
    if all(value == default for value in dict_.values()):
        return False
    return True


def round_if_number(value, decimals=3):
    # For historical reasons, bool is a type of int, but we cannot
    # apply np.round on bool
    if isinstance(value, bool):
        return value
    elif isinstance(value, (int, float)):
        return np.around(value, decimals=decimals)
    return value


def _check_against_reference(modality, decimals):
    for subject in modality.subjects:
        for session in subject.sessions:
            for run in session.runs:
                reference = modality.get_reference(run.echo_time)
                run.delta = param_difference(run.params,
                                             reference,
                                             ignore=['modality',
                                                     'phase_encoding_direction'])
                if run.delta:
                    modality.add_non_compliant_subject_name(subject.name)
                    _store(modality, run, subject.name, session.name)
                    # If any of the runs are non-compliant, then the
                    # session is non-compliant.
                    session.compliant = False
                    # If any of the sessions are non-compliant, then the
                    # subject is non-compliant.
                    subject.compliant = False
                    # If any of the subjects are non-compliant, then the
                    # modality is non-compliant.
                    modality.compliant = False
                    # If none of the subjects or modalities are found to
                    # be non-compliant, flag will remain True, after the
                    # loop is finished.
            if session.compliant:
                # If after all the runs, session is compliant, then the
                # session is added to the list of compliant sessions.
                subject.add_compliant_session_name(session.name)
        if subject.compliant:
            # If after all the sessions, subject is compliant, then the
            # subject is added to the list of compliant subjects.
            modality.add_compliant_subject_name(subject.name)
    # If after all the subjects, modality is compliant, then the
    # modality should be added to the list of compliant sessions.
    return modality.compliant


def _cli_report(dataset, report_name):
    """
    CLI report generator.
    Generate a single line report for the dataset

    Parameters
    ----------
    dataset : BaseDataset
        BaseDataset instance for the dataset which is to be checked
    report_name : str
        Filename for the report

    Returns
    -------

    """
    result = {}
    # For all the modalities calculate the percent of non-compliance
    for modality in dataset.modalities:
        percent_non_compliant = len(modality.non_compliant_subject_names) \
                                / len(modality.subjects)
        if percent_non_compliant > 0:
            result[modality.name] = str(100 * percent_non_compliant)
    # Format the result as a string
    if result:
        ret_string = 'In {0} dataset, modalities "{1}" are non-compliant. ' \
                     'See {2} for report'.format(dataset.name,
                                                 ", ".join(result.keys()),
                                                 report_name)
    else:
        ret_string = 'In {0} dataset, all modalities are compliant. ' \
                     'See {1} for report'.format(dataset.name,
                                                 report_name)
    return ret_string


def _store(modality, run, subject_name, session_name):
    """
    Store the sources of non-compliance like flip angle, ped, tr, te

    Parameters
    ----------
    modality : MRdataset.base.Modality
        The modality node, in which these sources of non-compliance were found
        so that these values can be stored
    run : MRdataset.base.Run
        Non-compliant which was found to be non-compliant w.r.t. the reference
    subject_name : str
        Non-compliant subject's name
    session_name : str
        Non-compliant session name
    """
    for entry in run.delta:
        if entry[0] != 'change':
            continue
        _, parameter, [new_value, ref_value] = entry

        if not is_hashable(parameter):
            parameter = str(parameter)

        modality.add_non_compliant_param(
            parameter, run.echo_time, ref_value, new_value,
            '{}_{}'.format(subject_name, session_name)
        )
