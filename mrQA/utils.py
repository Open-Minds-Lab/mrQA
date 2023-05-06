import re
import time
import typing
import unicodedata
import warnings
from collections import Counter
from datetime import datetime, timezone
from itertools import groupby
from itertools import takewhile
from pathlib import Path
from typing import Union, List, Optional, Any
from subprocess import run, CalledProcessError, TimeoutExpired

import numpy as np
import tempfile
from MRdataset.base import Modality, BaseDataset
from MRdataset.log import logger
from MRdataset.utils import param_difference, make_hashable, slugify
from dateutil import parser

from mrQA.config import past_records_fpath, report_fpath, mrds_fpath, \
    subject_list_dir, DATE_SEPARATOR, CannotComputeMajority, \
    ReferenceNotSetForModality, \
    ReferenceNotSetForEchoTime


def get_items_upto_count(dict_: Counter, rank: int = 1):
    """
    Given a dictionary, it returns a list of key-value pairs
    upto the rank specified. If rank is 1, it returns the most
    common value. If rank is 2, it returns the second most common
    value and so on.

    Parameters
    ----------
    dict_: Counter
        A Counter object, which is a dictionary with values as counts
    rank : int
        The rank upto which the key-value pairs are to be returned

    Returns
    -------
    list: List
        A list of key-value pairs upto the rank specified
    """
    values_desc_order = dict_.most_common()
    value_at_rank = values_desc_order[rank - 1][1]
    return list(takewhile(lambda x: x[1] >= value_at_rank, values_desc_order))


def timestamp():
    """Generate a timestamp as a string"""
    time_string = time.strftime('%m_%d_%Y_%H_%M_%S')
    return time_string


def record_out_paths(output_dir, dataset_name):
    ts = timestamp()
    utc = datetime.strptime(ts, '%m_%d_%Y_%H_%M_%S').timestamp()
    filename = f'{dataset_name}{DATE_SEPARATOR}{ts}'
    report_path = report_fpath(output_dir, filename)
    mrds_path = mrds_fpath(output_dir, filename)
    sub_lists_dir_path = subject_list_dir(output_dir, filename)

    records_filepath = past_records_fpath(output_dir)
    if not records_filepath.parent.is_dir():
        records_filepath.parent.mkdir(parents=True)
    with open(records_filepath, 'a', encoding='utf-8') as fp:
        fp.write(f'{utc},{report_path},'
                 f'{mrds_path},{ts}\n')
    return report_path, mrds_path, sub_lists_dir_path


def majority_attribute_values(list_of_dicts: list, echo_time: float,
                              default=None):
    """
    Given a list of dictionaries, it generates the most common
    values for each key

    Parameters
    ----------
    list_of_dicts : list
        a list of dictionaries
    echo_time : float
        echo time
    default : Any
        a default value if the key is missing in any dictionary

    Returns
    -------
    dict
        Key-value pairs specifying the most common values for each key
    """
    args_valid = False
    maj_value = default

    try:
        args_valid = _check_args_validity(list_of_dicts, echo_time)
    except CannotComputeMajority as e:
        maj_value = None  # 'Cannot Compute Majority:\n Count < 3'
        logger.info(f'Cannot compute majority: {e}')
    except ValueError as e:
        maj_value = None
        logger.info(f'Cannot compute majority: {e}')

    if not args_valid:
        maj_attr_values = {}
        for key in list_of_dicts[0].keys():
            maj_attr_values[key] = maj_value
        return maj_attr_values
    counters_dict = {}
    categories = set()
    for dict_ in list_of_dicts:
        categories.update(dict_.keys())
        for key, value in dict_.items():
            counter = counters_dict.get(key, Counter({default: 0}))
            value = make_hashable(value)
            counter[value] += 1
            counters_dict[key] = counter

    majority_attr_dict = {}
    for parameter, counter in counters_dict.items():
        majority_attr_dict[parameter] = pick_majority(counter, parameter)
    return majority_attr_dict


def extract_reasons(data: list):
    """
    Given a list of tuples, extract all the elements at index 1, and return
    as a list

    Parameters
    ----------
    data : List
        A list of tuples

    Returns
    -------
    list
        List of values at index 1
    """
    return list(zip(*data))[1]


def pick_majority(counter_: Counter, parameter: str, default: Any = None):
    """
    Given a counter object, it returns the most common value.

    Parameters
    ----------
    counter_ : Counter
        A Counter object, which is a dictionary with values as counts
        keys are various possible values for a parameter, for ex, for PED
        keys will be ROW, COL ... etc
    parameter: str
        The parameter for which the majority is being computed
    default : Any
        a default value if the key is missing in any dictionary

    Returns
    -------
    parameter name : str
        The most common value for the parameter

    Raises
    ------
    ValueError
        If the counter is empty
    """
    if len(counter_) == 0:
        raise ValueError('Expected atleast one entry in counter. Got 0')
    if len(counter_) == 1:
        return list(counter_.keys())[0]
    # there are more than 1 value, remove default, and computer majority
    _ = counter_.pop(default, None)
    items_rank1 = get_items_upto_count(counter_, rank=1)
    # If there are many values for rank 1 with equal count,
    # cannot say which is majority
    values = ', '.join([str(x[0]) for x in items_rank1])
    if len(items_rank1) > 1:
        logger.info(
            'Could not compute reference for %s. Got multiple values'
            ' %s with same count = %s.', parameter, values, items_rank1[0][1])
        return 'Cannot Compute Majority:\nEqual Count'
    return items_rank1[0][0]


def _check_args_validity(list_of_dicts: List[dict], echo_time) -> bool:
    """
    Checks if the arguments are valid for computing majority attribute values

    Parameters
    ----------
    list_of_dicts : list
        a list of dictionaries
    echo_time : float
         Echo time of run

    Returns
    -------
    bool
        True if arguments are valid, False otherwise

    Raises
    ------
    ValueError
        If the list is empty or if any of the dictionaries is empty
    """
    if list_of_dicts is None:
        raise ValueError('Expected a list of dicts, Got NoneType')
    if len(list_of_dicts) == 0:
        raise ValueError('List is empty.')
    for dict_ in list_of_dicts:
        if len(dict_) == 0:
            raise ValueError('Atleast one of dictionaries is empty.')
    if len(list_of_dicts) < 3:
        logger.info('Cannot compute majority attribute values. '
                    'Got less than 3 values for each '
                    'parameter. Returns majority values as None.')
        raise CannotComputeMajority('Count < 3', te=echo_time)
    return True


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

    Raises
    ------
    ValueError
        If the number of chunks is 0
    """
    if not is_integer_number(num_chunks):
        raise ValueError(f'Number of chunks must be an integer. '
                         f'Got {num_chunks}')
    if num_chunks == 0:
        raise ValueError('Cannot divide list into chunks of size 0')
    if len(dir_index) == 0:
        raise ValueError('List of directories is empty!')
    if len(dir_index) < num_chunks:
        warnings.warn(f'Got num_chunks={num_chunks}, list_size={len(dir_index)}'
                      f'Expected num_chunks < list_size',
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

    Raises
    ------
    FileNotFoundError
        If the file does not exist
    """
    if not isinstance(txt_filepath, Path):
        txt_filepath = Path(txt_filepath).resolve()
    if not txt_filepath.exists():
        raise FileNotFoundError(f'Invalid path {txt_filepath}')
    # Generate a list of folder paths stored in given txt_file
    with open(txt_filepath, 'r', encoding='utf-8') as fp:
        line_list = [line.strip() for line in fp.readlines() if line.strip()]
    # Don't return empty string
    return line_list


def list2txt(fpath: Path, list_: list) -> None:
    """
    Given a list of values, dump all the lines to a text file. Each element of
    the list is on a separate line.

    Parameters
    ----------
    fpath : Path
        output path of the final text file
    list_ : list
        values to be exported in a text file
    """
    parent_dir = fpath.parent
    parent_dir.mkdir(parents=True, exist_ok=True)
    list_str = [str(x) for x in list_]
    with open(fpath, 'w', encoding='utf-8') as fp:
        fp.write('\n'.join(list_str))


def execute_local(script_path: str) -> None:
    """
    Execute a bash script locally and time it.

    Parameters
    ----------
    script_path : str
        path to the bash script

    Raises
    ------
    FileNotFoundError
        if the script_path does not exist
    CalledProcessError
        if the script does not return a successful return code
    TimeoutExpired
        if the script takes more time to execute than the timeout
    """
    if not Path(script_path).is_file():
        raise FileNotFoundError(f'Could not find {script_path}')

    format_params = '\n'.join(['File system outputs: %O',
                               'Maximum RSS size: %M',
                               'CPU percentage used: %P',
                               'Real Time: %E',
                               'User Time: %U',
                               'Sys Time: %S'])
    # cmd = ['/usr/bin/time', '-f', format_params, 'bash', str(script_path)]
    cmd = f'bash {str(script_path)}'

    try:
        run(cmd, check=True, shell=True)
    except FileNotFoundError as exc:
        logger.error(
            "Process failed because 'bash' could not be found.\n %s", exc)
    except CalledProcessError as exc:
        logger.error(
            'Process failed because did not return a successful'
            ' return code. Returned %s \n %s', exc.returncode, exc
        )
    except TimeoutExpired as exc:
        logger.error('Process timed out.\n %s', exc)
    # TODO : check if file was created successfully


def get_outliers(data: list, m=25.0) -> Union[list, None]:
    """
    Check for outliers. Adapted from
    https://stackoverflow.com/a/16562028/3140172
    Parameters
    ----------
    data : list
        list of values
    m : float
        number of standard deviations to use as threshold
    """
    d = np.abs(data - np.median(data))
    mdev = np.median(d)
    s = d / mdev if mdev else 0.
    if np.any(s > m):
        indices = np.argwhere(s > m).flatten()
        return indices
    return None


def round_dict_values(dict_: dict, decimals: int) -> dict:
    """
    Round all the values in a dictionary to a given number of decimals.

    Parameters
    ----------
    dict_ : dict
        dictionary of key, value pairs. Values can be numbers or strings.
        The function will only round the values that are numbers.
    decimals : int
        number of decimals to round to

    Returns
    -------
    dict
        dictionary with all the values rounded to the given number of decimals
    """
    new_dict = dict_.copy()
    for key, value in new_dict.items():
        new_dict[key] = round_if_numeric(value, decimals)
    return new_dict


def is_integer_number(n: Union[int, float]) -> bool:
    """
    Check if a number is an integer.
    Parameters
    ----------
    n : int or float
        number to check

    Returns
    -------
    bool
        True if the number is an integer, False otherwise
    """
    if isinstance(n, int):
        return True
    if isinstance(n, float):
        return n.is_integer()
    return False


def subject_list2txt(dataset: BaseDataset,
                     output_dir: Path) -> dict:
    """
    Given a dataset, write a text file for each modality containing the
    subject names that are non-compliant with the reference protocol.
    Return a dictionary with the modality name as key and the filepath to the
    text file as value.

    Parameters
    ----------
    dataset: BaseDataset
        the text files will be generated for each modality in this dataset
    output_dir: Path
        absolute path to the output directory where the text files
         will be stored

    Returns
    -------
    dict
        dictionary with the modality name as key and the filepath to the
        text file as value
    """
    output_dir.mkdir(exist_ok=True, parents=True)
    filepaths = {}
    for modality in dataset.modalities:
        if not modality.compliant:
            filepath = output_dir / slugify(modality.name)
            list2txt(filepath, modality.non_compliant_subject_names)
            filepaths[modality.name] = filepath
    return filepaths


def _get_runs_by_echo(modality: Modality, decimals: int = 3):
    """
    Given a modality, return a dictionary with the echo time as key and a list
    of run parameters as value. The run parameters are rounded to the given
    number of decimals.

    Parameters
    ----------
    modality
    decimals

    Returns
    -------

    """
    runs_in_modality = []
    for subject in modality.subjects:
        for session in subject.sessions:
            runs_in_modality.extend(session.runs)

    def _sort_key(run_):
        return run_.echo_time

    run_params_by_te = {}
    runs_in_modality = sorted(runs_in_modality, key=_sort_key)
    for te, group in groupby(runs_in_modality, key=_sort_key):
        te_ = round_if_numeric(te, decimals)
        for i_run in list(group):
            if te_ not in run_params_by_te:
                run_params_by_te[te_] = []
            run_params_by_te[te_].append(round_dict_values(i_run.params,
                                                           decimals))
    return run_params_by_te


def _validate_reference(dict_, default=None):
    """
    Check if a dictionary is valid. A dictionary is valid if it is not empty
    and if at least one of its values is different from the default value.

    Parameters
    ----------
    dict_: dict
        dictionary to check
    default: any
        default value to compare the values of the dictionary to

    Returns
    -------
    bool
        True if the dictionary is valid, False otherwise
    """
    if not dict_:
        return False
    if all(value == default for value in dict_.values()):
        return False
    # flag = True
    # for value in dict_.values():
    #     if value and ('Cannot Compute Majority' in value):
    #         flag = False
    #         continue
    return True


def round_if_numeric(value: Union[int, float],
                     decimals: int = 3) -> Union[int, float, np.ndarray]:
    """
    Round a number to a given number of decimals.

    Parameters
    ----------
    value: int or float
        number to round
    decimals :  int
        number of decimals to round to

    Returns
    -------
    int or float
        rounded number
    """
    # For historical reasons, bool is a type of int, but we cannot
    # apply np.round on bool
    if isinstance(value, bool):
        return value
    elif isinstance(value, (int, float)):
        # round using numpy and then convert to native python type
        return np.around(value, decimals=decimals).item()
    return value


def _check_single_run(modality: Modality,
                      decimals: int,
                      run_te: float,
                      run_params: dict,
                      tolerance: float = 0.1):
    """
    Check if a single run is compliant with the reference protocol.

    Parameters
    ----------
    modality : Modality
        modality node from BaseDataset
    decimals: int
        number of decimals to round to
    run_te: float
        echo time of the run
    run_params: dict
        parameters of the run
    tolerance: float
        tolerance for the difference between the parameters of the run and the
        reference protocol

    Returns
    -------
    tuple
        tuple containing the echo time of reference protocol,
        and the delta between the parameters of the run and the reference
        protocol
    """
    te = round_if_numeric(run_te, decimals)
    params = round_dict_values(run_params, decimals)
    ignore_keys = ['modality', 'BodyPartExamined']
    echo_times = modality.get_echo_times()
    if not echo_times:
        raise ReferenceNotSetForModality(modality.name)

    if te in echo_times:
        reference = modality.get_reference(te)
        te_ref = te
    else:
        raise ReferenceNotSetForEchoTime(modality.name, run_te)

    delta = param_difference(params, reference,
                             ignore=ignore_keys,
                             tolerance=tolerance)
    return delta, te_ref


def _check_against_reference(modality, decimals, tolerance):
    """
    Given a modality, check if the parameters of each run are compliant with
    the reference protocol. If all the runs of a session are non-compliant,
    the session is added to the list of non-compliant sessions. If all the
    sessions of a subject are non-compliant, the subject is added to the list
    of non-compliant subjects. If all the subjects of a modality are
    non-compliant, the function returns False.

    The delta between the parameters of a run and the reference protocol is
    stored in modality.non_compliant_data

    Parameters
    ----------
    modality : Modality
        modality node of a dataset
    decimals : int
        number of decimals to round the parameters
    tolerance : float
        tolerance to consider a parameter compliant

    Returns
    -------
    Modality
        True if modality is compliant, False otherwise
    """
    # Set default flags as True, if there is some non-compliance
    # flags will be set to false. Default value in modality class is True,
    # but we cannot rely on that default value.
    modality.compliant = True
    for subject in modality.subjects:
        subject.compliant = True
        for session in subject.sessions:
            session.compliant = True
            for i_run in session.runs:
                try:
                    i_run.delta, te_ref = _check_single_run(modality,
                                                            decimals,
                                                            i_run.echo_time,
                                                            i_run.params,
                                                            tolerance=tolerance)
                    if i_run.delta:
                        modality.add_non_compliant_subject_name(subject.name)
                        _store_non_compliance(modality, i_run.delta, te_ref,
                                              subject.name, session.name)
                        # NC = non_compliant
                        # If any run is NC, then session is NC.
                        session.compliant = False
                        # If any session is NC, then subject is NC.
                        subject.compliant = False
                        # If any subject is NC, then modality is NC.
                        modality.compliant = False
                except ReferenceNotSetForEchoTime as e:
                    modality.add_error_subject_names(f'{subject.name}_'
                                                     f'{session.name}')
                    modality.add_non_compliant_subject_name(subject.name)
                    # _store_non_compliance(modality, i_run.delta, 'Various',
                    #                       subject.name, session.name)
                    # If any run is NC, then session is NC.
                    session.compliant = False
                    # If any session is NC, then subject is NC.
                    subject.compliant = False
                    # If any subject is NC, then modality is NC.
                    modality.compliant = False
                    logger.info(e)
                except ReferenceNotSetForModality as e:
                    modality.add_error_subject_names(f'{subject.name}_'
                                                     f'{session.name}')
                    logger.info(e)

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
    return modality


def _cli_report(dataset: BaseDataset, report_name):
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
        modalities = ', '.join(result.keys())
        ret_string = f'In {dataset.name} dataset,' \
                     f' modalities "{modalities}" are non-compliant. ' \
                     f'See {report_name} for report'
    else:
        ret_string = f'In {dataset.name} dataset, all modalities ' \
                     f'are compliant. See {report_name} for report.'

    return ret_string


def _store_non_compliance(modality: Modality,
                          delta: list,
                          echo_time: float,
                          subject_name: str,
                          session_name: str):
    """
    Store the sources of non-compliance like flip angle, ped, tr, te

    Parameters
    ----------
    modality : MRdataset.base.Modality
        The modality node, in which these sources of non-compliance were found
        so that these values can be stored
    delta : list
        A list of differences between the reference and the non-compliant
    echo_time: float
        Echo time of run
    subject_name : str
        Non-compliant subject's name
    session_name : str
        Non-compliant session name
    """
    for entry in delta:
        if entry[0] == 'change':
            _, parameter, [new_value, ref_value] = entry
            if echo_time is None:
                echo_time = 1.0
            ref_value = make_hashable(ref_value)
            new_value = make_hashable(new_value)

            modality.add_non_compliant_param(
                parameter, echo_time, ref_value, new_value,
                f'{subject_name}_{session_name}'
            )
        elif entry[0] == 'add':
            for key, value in entry[2]:
                if echo_time is None:
                    echo_time = 1.0
                modality.add_non_compliant_param(
                    key, echo_time, value, None,
                    f'{subject_name}_{session_name}')


def _datasets_processed(dir_path, ignore_case=True):
    """
    Add function to retrieve the names of projects that have been processed in
    the past

    Parameters
    ----------
    dir_path : str or Path
        Absolute path to the folder where the processed projects are stored
    ignore_case : bool
        If True, the names of the projects will be searched in lower case.
        If False, the names of the projects will be searched in the original
        case.

    Returns
    -------

    """
    if not ignore_case:
        return [x.name for x in dir_path.iterdir() if x.is_dir()]
    else:
        return [x.name.lower() for x in dir_path.iterdir() if x.is_dir()]


def files_modified_since(last_reported_on: str,
                         input_dir: Union[str, Path],
                         output_dir: Union[str, Path],
                         time_format: str = 'timestamp') -> List:
    """
    Find files modified since a given time

    Parameters
    ----------
    input_dir: str
        Absolute path to the directory to search
    last_reported_on: str
        Reference time to compare against.
    output_dir: str or Path
        Absolute path to the directory where the output file will be stored.
    time_format: str
        Format of the time. One of ['timestamp', 'datetime'].

    Returns
    -------
    valid_files: List
        A list of files modified since the given time.

    Raises
    ------
    ValueError
        If the time format is invalid.
    NotImplementedError
        If the time format is not one of ['timestamp', 'datetime'].
    FileNotFoundError
        If the executable `find` is not found.
    CalledProcessError
        If the command `find` fails.
    TimeoutExpired
        If the command `find` times out.
    """
    str_format = '%m/%d/%Y %H:%M:%S'
    if time_format == 'timestamp':
        mod_time = datetime.fromtimestamp(float(last_reported_on)).strftime(
            str_format)
    elif time_format == 'datetime':
        try:
            mod_time = parser.parse(last_reported_on, dayfirst=False)
        except ValueError as exc:
            raise ValueError(f'Invalid time format. Use {str_format}.') from exc
    else:
        raise NotImplementedError("Expected one of ['timestamp', 'datetime']."
                                  f'Got {time_format}')

    out_path = Path(output_dir) / 'modified_files_since.txt'
    if out_path.is_file():
        out_path.unlink()

    cmd = f"find {input_dir} -type f -newermt '{mod_time}' > {out_path}"

    try:
        run(cmd, check=True, shell=True)
        modified_files = txt2list(out_path)
        valid_files = [f for f in modified_files if Path(f).is_file()]
        return valid_files
    except FileNotFoundError as exc:
        logger.error(
            'Process failed because file could not be found.\n %s', exc)
    except CalledProcessError as exc:
        logger.error(
            'Process failed because did not return a successful'
            ' return code. Returned %s \n %s', exc.returncode, exc
        )
    except TimeoutExpired as exc:
        logger.error('Process timed out.\n %s', exc)


def get_last_valid_record(folder_path: Path) -> Optional[tuple]:
    """
    Get the last valid record of generated report and mrds file

    Parameters
    ----------
    folder_path: Path
        Absolute path to the directory where the files are expected to be stored

    Returns
    -------
    last_reported_on, last_fname: tuple
        last valid record
    """
    record_filepath = past_records_fpath(folder_path)
    if not record_filepath.is_file():
        logger.warning('No past records found.')
        return None
    i = -1
    with open(record_filepath, 'r', encoding='utf-8') as fp:
        while True:
            lines = fp.readlines()
            num_records = len(lines)
            if i < -num_records:
                return None
            last_line = lines[i]
            last_reported_on, last_report_path, last_mrds_path, _ = \
                last_line.split(',')
            if Path(last_mrds_path).is_file():
                return last_reported_on, last_report_path, last_mrds_path
            i -= 1


# def check_valid_files(fname: str, folder_path: Path) -> bool:
#     """
#     Check if the expected files are present in the folder
#
#     Parameters
#     ----------
#     fname: str
#         Name of the file
#     folder_path :  Path
#         Absolute path to the folder where the files are expected to be present
#
#     Returns
#     -------
#     bool
#         True if the files are present, False otherwise
#     """
#     # report_path = report_fpath(folder_path, fname)
#     mrds_path = mrds_fpath(folder_path, fname)
#     # actually we don't need to check if the report is present
#     # because we just need the mrds file, to update.
#     return mrds_path.is_file()


def export_record(output_dir, filename, time_dict):
    record_filepath = past_records_fpath(output_dir)
    if not record_filepath.parent.is_dir():
        record_filepath.parent.mkdir(parents=True)

    with open(record_filepath, 'a', encoding='utf-8') as fp:
        fp.write(f"{time_dict['utc']},{filename},"
                 f"{time_dict['date_time']}\n")


def get_timestamps():
    now = datetime.now(timezone.utc)
    now = now.replace(tzinfo=timezone.utc)
    ts = datetime.timestamp(now)
    date_time = now.strftime('%m/%d/%Y %H:%M:%S%z')
    return {
        'utc': ts,
        'date_time': date_time
    }


def export_subject_lists(output_dir: Union[Path, str],
                         dataset: BaseDataset,
                         folder_name: str) -> dict:
    sub_lists_by_modality = subject_list2txt(dataset, output_dir/folder_name)
    return sub_lists_by_modality

