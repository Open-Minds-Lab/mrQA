import json
import re
import tempfile
import time
import typing
import unicodedata
import warnings
from collections import Counter
from datetime import datetime, timezone
from itertools import takewhile
from pathlib import Path
from subprocess import run, CalledProcessError, TimeoutExpired
from typing import Union, List, Optional, Any, Iterable

import numpy as np
from MRdataset import BaseDataset, is_dicom_file
from dateutil import parser
from protocol import BaseSequence

from mrQA import logger
from mrQA.config import past_records_fpath, report_fpath, mrds_fpath, \
    subject_list_dir, DATE_SEPARATOR, CannotComputeMajority, \
    Unspecified, \
    EqualCount, status_fpath, ATTRIBUTE_SEPARATOR


def is_writable(dir_path):
    try:
        with tempfile.TemporaryFile(dir=dir_path, mode='w') as testfile:
            testfile.write("OS write to directory test.")
            logger.info(f"Created temp file in {dir_path}")
    except (OSError, IOError) as e:
        logger.error(e)
        return False
    return True


def files_under_folder(fpath: Union[str, Path],
                       ext: str = None) -> typing.Iterable[Path]:
    """
    Generates all the files inside the folder recursively. If ext is given
    returns file which have that extension.

    Parameters
    ----------
    fpath: str
        filepath of the directory
    ext: str
        filter files with given extension. For ex. return only .nii files

    Returns
    -------
    generates filepaths
    """
    if not Path(fpath).is_dir():
        raise FileNotFoundError(f"Folder doesn't exist : {fpath}")
    folder_path = Path(fpath).resolve()
    if ext:
        pattern = '*' + ext
    else:
        pattern = '*'
    for file in folder_path.rglob(pattern):
        if file.is_file():
            # If it is a regular file and not a directory, return filepath
            yield file


def files_in_path(fp_list: Union[Iterable, str, Path],
                  ext: Optional[str] = None):
    """
    If given a single folder, returns the list of all files in the directory.
    If given a list of folders, returns concatenated list of all the files
    inside each directory.

    Parameters
    ----------
    fp_list : List[Path]
        List of folder paths
    ext : str
        Used to filter files, and select only those which have this extension
    Returns
    -------
    List of paths
    """
    if isinstance(fp_list, Iterable):
        files = []
        for i in fp_list:
            if str(i) == '' or str(i) == '.' or i == Path():
                logger.warning("Found an empty string. Skipping")
                continue
            if Path(i).is_dir():
                files.extend(list(files_under_folder(i, ext)))
            elif Path(i).is_file():
                files.append(i)
        return sorted(list(set(files)))
    elif isinstance(fp_list, str) or isinstance(fp_list, Path):
        return sorted(list(files_under_folder(fp_list, ext)))
    else:
        raise NotImplementedError("Expected either Iterable or str type. Got"
                                  f"{type(fp_list)}")


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


def make_output_paths(output_dir, dataset):
    ts = timestamp()
    utc = datetime.strptime(ts, '%m_%d_%Y_%H_%M_%S').timestamp()
    filename = f'{dataset.name}{DATE_SEPARATOR}{ts}'
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


def majority_values(list_seqs: list,
                    default=None,
                    include_keys: list = None, ):
    """
    Given a list of dictionaries, it generates the most common
    values for each key

    Parameters
    ----------
    list_seqs : list
        a list of dictionaries
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
        args_valid = _check_args_validity(list_seqs)
    except CannotComputeMajority as e:
        maj_value = None  # 'Cannot Compute Majority:\n Count < 3'
        logger.info(f'Cannot compute majority: {e}')
    except ValueError as e:
        maj_value = None
        logger.info(f'Cannot compute majority: {e}')

    if not args_valid:
        return maj_value

    counters_dict = {}
    categories = set()
    if not include_keys:
        raise ValueError('Expected a list of keys to include. Got None')
    for seq in list_seqs:
        categories.update(include_keys)
        for param in include_keys:
            counter = counters_dict.get(param, Counter({default: 0}))
            value = seq.get(param, default)
            counter[value] += 1
            counters_dict[param] = counter

    majority_dict = {}
    for parameter, counter in counters_dict.items():
        majority_dict[parameter] = pick_majority(counter, parameter)
    return majority_dict


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
        logger.error('Expected atleast one entry in counter. Got 0')
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
        return EqualCount
    return items_rank1[0][0]


def _check_args_validity(list_: List) -> bool:
    """
    Checks if the arguments are valid for computing majority attribute values

    Parameters
    ----------
    list_ : list
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
    if list_ is None:
        raise ValueError('Expected a list of sequences, Got NoneType')
    if len(list_) == 0:
        raise ValueError('List is empty.')
    for seq in list_:
        if len(seq) == 0:
            raise ValueError('Atleast one of sequences is empty.')
    if len(list_) < 3:
        logger.info('Cannot compute majority attribute values. '
                    'Got less than 3 values for each '
                    'parameter. Returns majority values as None.')
        raise CannotComputeMajority('Count < 3')
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
    for seq_name in dataset.get_sequence_ids():
        filepath = output_dir / f'{convert2ascii(seq_name)}.txt'
        subj_with_sequence = dataset.get_subject_ids(seq_name)
        list2txt(filepath, subj_with_sequence)
        filepaths[seq_name] = filepath
    return filepaths


def convert2ascii(value, allow_unicode=False):
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
        value = unicodedata.normalize(
            'NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value)
    return re.sub(r'[-\s]+', '-', value).strip('-_')


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


def compute_majority(dataset: BaseDataset, seq_name, config_dict=None):
    # if config_dict is not None:
    # TODO: parse begin and end times
    # TODO: add option to exclude subjects
    hz_audit_config = config_dict.get("horizontal_audit", None)
    seq_dict = {}
    most_freq_vals = {}

    if hz_audit_config is None:
        return most_freq_vals

    include_parameters = hz_audit_config.get('include_parameters', None)
    stratify_by = hz_audit_config.get('stratify_by', None)

    for subj, sess, runs, seq in dataset.traverse_horizontal(seq_name):
        sequence_id = modify_sequence_name(seq, stratify_by)
        if sequence_id not in seq_dict:
            seq_dict[sequence_id] = []
        seq_dict[sequence_id].append(seq)

    for seq_id in seq_dict:
        most_freq_vals[seq_id] = majority_values(seq_dict[seq_id],
                                                 default=Unspecified,
                                                 include_keys=include_parameters)
    return most_freq_vals


def _cli_report(hz_audit: dict, report_name):
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
    non_compliant_ds = hz_audit['non_compliant']
    compliant_ds = hz_audit['compliant']
    undetermined_ds = hz_audit['undetermined']
    if not (compliant_ds.get_sequence_ids() or
            non_compliant_ds.get_sequence_ids() or
            undetermined_ds.get_sequence_ids()):
        logger.error('No report generated for horizontal audit.')
        return

    for seq_id in non_compliant_ds.get_sequence_ids():
        ncomp_sub_ids = len(non_compliant_ds.get_subject_ids(seq_id))
        comp_sub_ids = len(compliant_ds.get_subject_ids(seq_id))
        total_subjects = comp_sub_ids + ncomp_sub_ids

        percent_non_compliant = ncomp_sub_ids / total_subjects
        if percent_non_compliant > 0:
            result[seq_id] = str(100 * percent_non_compliant)
    # Format the result as a string
    if result:
        modalities = ', '.join(result.keys())
        ret_string = f'In {compliant_ds.name} dataset,' \
                     f' modalities "{modalities}" are non-compliant. ' \
                     f'See {report_name} for report'
    else:
        ret_string = f'In {compliant_ds.name} dataset, all modalities ' \
                     f'are compliant. See {report_name} for report.'

    return ret_string


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


def folders_modified_since(last_reported_on: str,
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

    out_path = Path(output_dir) / 'modified_folders_since.txt'
    if out_path.is_file():
        out_path.unlink()

    cmd = f"find {input_dir} -type f -newermt '{mod_time}' > {out_path}"

    try:
        run(cmd, check=True, shell=True)
        modified_files = txt2list(out_path)
        modified_folders = set()
        for f in modified_files:
            if not Path(f).is_file():
                logger.warning(f'File {f} not found.')
            if not is_dicom_file(f):
                continue
            else:
                modified_folders.add(Path(f).parent)
        return list(modified_folders)
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
                         non_compliant_ds: BaseDataset,
                         folder_name: str) -> dict:
    noncompliant_sub_by_seq = subject_list2txt(non_compliant_ds,
                                               output_dir / folder_name)
    return noncompliant_sub_by_seq


def folders_with_min_files(root: Union[Path, str],
                           pattern: Optional[str] = "*.dcm",
                           min_count=3) -> List[Path]:
    """
    Returns all the folders with at least min_count of files matching the pattern
    One at time via generator.

    Parameters
    ----------
    root : List[Path]
        List of folder paths
    pattern : str
        pattern to filter files

    min_count : int
        size representing the number of files in folder matching the input pattern

    Returns
    -------
    List of folders
    """

    if not isinstance(root, (Path, str)):
        raise ValueError('root must be a Path-like object (str or Path)')

    root = Path(root).resolve()
    if not root.exists():
        raise ValueError('Root folder does not exist')

    terminals = find_terminal_folders(root)

    for folder in terminals:
        if len([file_ for file_ in folder.rglob(pattern)]) >= min_count:
            yield folder

    return


def is_folder_with_no_subfolders(fpath):
    """"""
    if isinstance(fpath, str):
        fpath = Path(fpath)
    if not fpath.is_dir():
        raise FileNotFoundError(f'Folder not found: {fpath}')

    sub_dirs = [file_ for file_ in fpath.iterdir() if file_.is_dir()]

    return len(sub_dirs) < 1, sub_dirs


def find_terminal_folders(root):
    try:
        no_more_subdirs, sub_dirs = is_folder_with_no_subfolders(root)
    except FileNotFoundError:
        return []

    if no_more_subdirs:
        return [root, ]

    terminal = list()
    for sd1 in sub_dirs:
        no_more_subdirs2, level2_subdirs = is_folder_with_no_subfolders(sd1)
        if no_more_subdirs2:
            terminal.append(sd1)
        else:
            for sd2 in level2_subdirs:
                terminal.extend(find_terminal_folders(sd2))

    return terminal


def log_latest_non_compliance(ncomp_data, latest_data, output_dir):
    """
    Log the latest non-compliance data from recent sessions to a file

    Parameters
    ----------
    ncomp_data
    latest_data
    output_dir

    Returns
    -------

    """
    if latest_data is None:
        return
    full_status = []
    for seq_id in latest_data.get_sequence_ids():
        for subj_id, sess_id, run_id, seq in latest_data.traverse_horizontal(
            seq_id):
            try:
                nc_param_dict = ncomp_data.get_non_compliant_params(
                    subject_id=subj_id, session_id=sess_id,
                    run_id=run_id, seq_id=seq_id)
                status = {
                    'ts'      : seq.timestamp,
                    'subject' : subj_id,
                    'sequence': seq_id,
                    'ds_name' : latest_data.name,
                    'nc_params': ';'.join(nc_param_dict.keys())
                }
                full_status.append(status)
            except KeyError:
                continue
    status_filepath = status_fpath(output_dir)
    if not status_filepath.parent.is_dir():
        status_filepath.parent.mkdir(parents=True)

    with open(status_filepath, 'a', encoding='utf-8') as fp:
        for i in full_status:
            fp.write(
                f" {i['ts']}, {i['ds_name']}, {i['sequence']}, {i['subject']}, {i['nc_params']} \n")
    return None  # status_filepath


def tuples2dict(mylist):
    result = {}
    for i in mylist:
        key = i[0]
        result.setdefault(key, []).extend(i[1:])
    return result


def valid_paths(files: Union[List, str]) -> Union[List[Path], Path]:
    """
    If given a single path, the function will just check if it's valid.
    If given a list of paths, the function validates if all the paths exist or
    not. The paths can either be an instance of string or POSIX path.

    Parameters
    ----------
    files : str or List[str]
        The path or list of paths that must be validated

    Returns
    -------
    List of POSIX Paths that exist on disk
    """
    if files is None:
        raise ValueError('Expected a valid path or Iterable, Got NoneType')
    if isinstance(files, str) or isinstance(files, Path):
        if not Path(files).is_file():
            raise OSError('Invalid File {0}'.format(files))
        return Path(files).resolve()
    elif isinstance(files, Iterable):
        for file in files:
            if not Path(file).is_file():
                raise OSError('Invalid File {0}'.format(file))
        return [Path(f).resolve() for f in files]
    else:
        raise NotImplementedError('Expected str or Path or Iterable, '
                                  f'Got {type(files)}')


def modify_sequence_name(seq: "BaseSequence", stratify_by: str) -> str:
    """
    Modifies the sequence name to include the stratification value, if it
    exists.

    Parameters
    ----------

    Returns
    -------
    seq_name_with_stratify : str
        Modified sequence name
    """
    if stratify_by:
        stratify_value = getattr(seq, stratify_by)
        seq_name_with_stratify = ATTRIBUTE_SEPARATOR.join(
            [seq.name, stratify_value])
    else:
        seq_name_with_stratify = seq.name
    return seq_name_with_stratify


def get_config_from_file(config_path: Union[Path, str]) -> dict:
    if isinstance(config_path, str):
        config_path = Path(config_path)
    if not isinstance(config_path, Path):
        raise TypeError(f'Expected Path or str, got {type(config_path)}')
    if not config_path.is_file():
        raise FileNotFoundError(f'{config_path} does not exist')

    # read json file
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config
