import os
import pathlib
import pickle
import subprocess
import time
import typing
import warnings
from collections import Counter
from itertools import groupby
from itertools import takewhile
from pathlib import Path
from typing import Union, List
import unicodedata
import re

import numpy as np
from MRdataset.log import logger
from MRdataset.utils import param_difference, make_hashable


def get_items_upto_count(dict_, rank=1):
    values_desc_order = dict_.most_common()
    value_at_rank = values_desc_order[rank - 1][1]
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
        majority_attr_dict[k] = pick_majority(counters_dict[k], k)
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


def pick_majority(counter_, parameter, default=None):
    if len(counter_) == 0:
        raise ValueError("Expected atleast one entry in counter. Got 0")
    if len(counter_) == 1:
        return list(counter_.keys())[0]
    # there are more than 1 value, remove default, and computer majority
    _ = counter_.pop(default, None)
    items_rank1 = get_items_upto_count(counter_, rank=1)
    # If there are many values for rank 1 with equal count,
    # cannot say which is majority
    values = ", ".join([str(x[0]) for x in items_rank1])
    if len(items_rank1) > 1:
        logger.warning(
            f"Could not compute reference for {parameter}. Got multiple values"
            f" {values} with same count = {items_rank1[0][1]}.")
        return 'Cannot Compute Majority:\nEqual Count'
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
        logger.error("Cannot compute majority attribute values. "
                     "Got less than 3 values for each "
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


def slugify(value, allow_unicode=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')


def is_integer_number(n: Union[int, float]) -> bool:
    if isinstance(n, int):
        return True
    if isinstance(n, float):
        return n.is_integer()
    return False


def subject_list2txt(dataset, output_dir=None):
    output_dir.mkdir(exist_ok=True, parents=True)
    filepaths = {}
    for modality in dataset.modalities:
        if len(modality.non_compliant_subject_names) > 50:
            filepath = output_dir / slugify(modality.name)
            list2txt(filepath, modality.non_compliant_subject_names)
            filepaths[modality.name] = filepath
    return filepaths


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


def _check_single_run(modality, decimals, run_te, run_params):
    te = round_if_number(run_te, decimals)
    params = apply_round(run_params, decimals)
    if te in modality.get_echo_times():
        reference = modality.get_reference(te)
        if _validate_reference(reference):
            te_ref = reference.get('EchoTime', None)
            delta = param_difference(params,
                                     reference,
                                     ignore=['modality', 'BodyPartExamined'])
        else:
            print(reference)
    else:
        # Reference was set, but value for each key is None
        any_te = modality.get_echo_times()
        if any_te:
            te = any_te[0]
            reference = modality.get_reference(te)
            te_ref = reference.get('EchoTime', None)
            delta = param_difference(params,
                                     reference,
                                     ignore=['modality', 'BodyPartExamined'])
        # Reference is an empty dict or None
        else:
            te_ref = None
            delta = params
            logger.warning(f'There is no reference set for the '
                           f'modality : {modality}')
    return delta, te_ref


def _check_against_reference(modality, decimals):
    for subject in modality.subjects:
        for session in subject.sessions:
            for run in session.runs:
                run.delta, te_ref = _check_single_run(modality,
                                                      decimals,
                                                      run.echo_time,
                                                      run.params)
                if run.delta:
                    modality.add_non_compliant_subject_name(subject.name)
                    _store(modality, run.delta, te_ref,
                           subject.name, session.name)
                    # NC = non_compliant
                    # If any run is NC, then session is NC.
                    session.compliant = False
                    # If any session is NC, then subject is NC.
                    subject.compliant = False
                    # If any subject is NC, then modality is NC.
                    modality.compliant = False
            if session.compliant:
                # If after all runs, session is still compliant, then the
                # session is added to the list of compliant sessions.
                subject.add_compliant_session_name(session.name)
        if subject.compliant:
            # If after all sessions, subject is still compliant, then the
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


def _store(modality, delta, echo_time, subject_name, session_name):
    """
    Store the sources of non-compliance like flip angle, ped, tr, te

    Parameters
    ----------
    modality : MRdataset.base.Modality
        The modality node, in which these sources of non-compliance were found
        so that these values can be stored
    subject_name : str
        Non-compliant subject's name
    session_name : str
        Non-compliant session name
    """
    for entry in delta:
        if entry[0] == 'change':
            _, parameter, [new_value, ref_value] = entry
            # if new_value is None:
            #     new_value = 'NoneX'
            # if ref_value is None:
            #     ref_value = 'NoneX'
            if echo_time is None:
                echo_time = 1.0
            # parameter = make_hashable(parameter)
            ref_value = make_hashable(ref_value)
            new_value = make_hashable(new_value)

            modality.add_non_compliant_param(
                parameter, echo_time, ref_value, new_value,
                '{}_{}'.format(subject_name, session_name)
            )
        elif entry[0] == 'add':
            for key, value in entry[2]:
                # if value is None:
                    # value = 'NoneX'
                if echo_time is None:
                    echo_time = 1.0
                modality.add_non_compliant_param(
                    key, echo_time, value, None,
                    '{}_{}'.format(subject_name, session_name)
                )
