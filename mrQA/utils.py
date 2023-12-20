import json
import pickle
import re
import tempfile
import time
import unicodedata
from collections import Counter
from datetime import datetime, timedelta, timezone
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from itertools import takewhile
from pathlib import Path
from smtplib import SMTP
from subprocess import run, CalledProcessError, TimeoutExpired, Popen
from typing import Union, List, Optional, Any, Iterable, Sized

from MRdataset import BaseDataset, is_dicom_file
from dateutil import parser
from mrQA import logger
from mrQA.base import CompliantDataset, NonCompliantDataset, UndeterminedDataset
from mrQA.config import past_records_fpath, report_fpath, mrds_fpath, \
    subject_list_dir, DATE_SEPARATOR, CannotComputeMajority, \
    Unspecified, \
    EqualCount, status_fpath, ATTRIBUTE_SEPARATOR, DATETIME_FORMAT, DATE_FORMAT
from protocol import BaseSequence, MRImagingProtocol, SiemensMRImagingProtocol
from tqdm import tqdm


def get_reference_protocol(dataset: BaseDataset,
                           config: dict,
                           reference_path: Union[str, Path] = None):
    """
    Given a dataset, it returns the reference protocol that contains
    reference protocol for each sequence in the dataset.
    """
    # Infer reference protocol if not provided
    if reference_path is None:
        ref_protocol = infer_protocol(dataset, config=config)
    else:
        try:
            ref_protocol = get_protocol_from_file(reference_path)
        except (TypeError, ValueError, FileNotFoundError) as e:
            logger.error(f'Error while reading reference protocol '
                         f'from filepath : {e}. Falling back to inferred '
                         f'reference protocol')
            ref_protocol = infer_protocol(dataset, config=config)

    if not ref_protocol:
        if reference_path:
            logger.error("Reference protocol is invalid. "
                         "It doesn't contain any sequences. "
                         "Cannot generate results for horizontal audit.")
        else:
            logger.error("Inferred reference protocol doesn't have any"
                         "sequences. It seems the dataset is very small. "
                         ", that is less than 3 subjects for each sequence.")
    return ref_protocol


def get_config(config_path: Union[str, Path], report_type='hz') -> dict:
    try:
        config_dict = get_config_from_file(config_path)
    except (ValueError, FileNotFoundError, TypeError) as e:
        logger.error(f'Error while reading config file: {e}. Please provide'
                     f'a valid path to the configuration JSON file.')
        raise e

    if report_type == 'hz':
        key = "horizontal_audit"
    elif report_type == 'vt':
        key = "vertical_audit"
    elif report_type == 'plots':
        key = "plots"
    else:
        raise ValueError(f'Invalid audit type {report_type}. '
                         f'Expected "hz" or "vt"')

    audit_config = config_dict.get(key, None)
    if audit_config is None:
        logger.error(
            f'No {key} config found in config file. Note '
            f'that the config file should have a key named '
            f'"{key}".')
    else:
        include_params = audit_config.get('include_parameters', None)
        if include_params is None:
            logger.warning(
                'Parameters to be included in the compliance check are '
                'not provided. All parameters will be included in the '
                f'{key}')
    return audit_config


def _init_datasets(dataset: BaseDataset):
    """
    Initialize the three dataset objects for compliant, non-compliant
    and undetermined datasets.
    """
    compliant_ds = CompliantDataset(name=dataset.name,
                                    data_source=dataset.data_source,
                                    ds_format=dataset.format)
    non_compliant_ds = NonCompliantDataset(name=dataset.name,
                                           data_source=dataset.data_source,
                                           ds_format=dataset.format)
    undetermined_ds = UndeterminedDataset(name=dataset.name,
                                          data_source=dataset.data_source,
                                          ds_format=dataset.format)
    return compliant_ds, non_compliant_ds, undetermined_ds


def is_writable(dir_path):
    """
    Check if the directory is writable. For ex. if the directory is
    mounted on a read-only file system, it will return False.
    """
    try:
        with tempfile.TemporaryFile(dir=dir_path, mode='w') as testfile:
            testfile.write("OS write to directory test.")
            logger.info(f"Created temp file in {dir_path}")
    except (OSError, IOError) as e:
        logger.error(e)
        return False
    return True


# def files_under_folder(fpath: Union[str, Path],
#                        ext: str = None) -> typing.Iterable[Path]:
#     """
#     Generates all the files inside the folder recursively. If ext is given
#     returns file which have that extension.
#
#     Parameters
#     ----------
#     fpath: str
#         filepath of the directory
#     ext: str
#         filter_fn files with given extension. For ex. return only .nii files
#
#     Returns
#     -------
#     generates filepaths
#     """
#     if not Path(fpath).is_dir():
#         raise FileNotFoundError(f"Folder doesn't exist : {fpath}")
#     folder_path = Path(fpath).resolve()
#     if ext:
#         pattern = '*' + ext
#     else:
#         pattern = '*'
#     for file in folder_path.rglob(pattern):
#         if file.is_file():
#             # If it is a regular file and not a directory, return filepath
#             yield file


# def files_in_path(fp_list: Union[Iterable, str, Path],
#                   ext: Optional[str] = None):
#     """
#     If given a single folder, returns the list of all files in the directory.
#     If given a list of folders, returns concatenated list of all the files
#     inside each directory.
#
#     Parameters
#     ----------
#     fp_list : List[Path]
#         List of folder paths
#     ext : str
#         Used to filter_fn files, and select only those which have this ext
#     Returns
#     -------
#     List of paths
#     """
#     if isinstance(fp_list, Iterable):
#         files = []
#         for i in fp_list:
#             if str(i) == '' or str(i) == '.' or i == Path():
#                 logger.warning("Found an empty string. Skipping")
#                 continue
#             if Path(i).is_dir():
#                 files.extend(list(files_under_folder(i, ext)))
#             elif Path(i).is_file():
#                 files.append(i)
#         return sorted(list(set(files)))
#     elif isinstance(fp_list, str) or isinstance(fp_list, Path):
#         return sorted(list(files_under_folder(fp_list, ext)))
#     else:
#         raise NotImplementedError("Expected either Iterable or str type. Got"
#                                   f"{type(fp_list)}")


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
    time_string = time.strftime(DATETIME_FORMAT)
    return time_string


def make_output_paths(output_dir, dataset):
    """
    Generate output paths for the report, mrdataset pickle
    file, and subject lists

    Parameters
    ----------
    output_dir : Path
        output directory
    dataset : BaseDataset
        dataset object

    Returns
    -------
    report_path : Path
        Full path to the report file
    mrds_path : Path
        Full path to the mrdataset pickle file
    sub_lists_dir_path : Path
        Full path to the directory containing compliant/non-compliant
        subject lists for each modality
    """
    ts = timestamp()
    # utc = datetime.strptime(ts, '%m_%d_%Y_%H_%M_%S').timestamp()
    filename = f'{dataset.name}{DATE_SEPARATOR}{ts}'
    report_path = report_fpath(output_dir, filename)
    mrds_path = mrds_fpath(output_dir, filename)
    sub_lists_dir_path = subject_list_dir(output_dir, filename)
    log_report_history(output_dir, mrds_path, report_path, ts)
    return report_path, mrds_path, sub_lists_dir_path


def log_report_history(output_dir, mrds_path, report_path, ts):
    """
    Log the report generation history to a text file

    Parameters
    ----------
    output_dir : Path
        fullpath to the output directory
    mrds_path : Path
        fullpath to the mrdataset pickle file
    report_path : Path
        fullpath to the report file
    ts : str
        timestamp
    utc : float
        timestamp in UTC zone
    """
    records_filepath = past_records_fpath(output_dir)
    if not records_filepath.parent.is_dir():
        records_filepath.parent.mkdir(parents=True)
    with open(records_filepath, 'a', encoding='utf-8') as fp:
        fp.write(f'{ts},{report_path},'
                 f'{mrds_path}\n')


def majority_values(list_seqs: list,
                    default=None,
                    include_params: list = None, ):
    """
    Given a list of dictionaries, it generates the most frequent
    values for each key

    Parameters
    ----------
    list_seqs : list
        a list of dictionaries
    default : Any
        a default value if the key is missing in any dictionary
    include_params : list
        a list of parameters for which the most frequent values
        is to be computed
    Returns
    -------
    dict
        Key-value pairs specifying the most frequent values for each
        parameter
    """
    args_valid = False
    maj_value = default
    args_valid = _check_args_validity(list_seqs)

    if not args_valid:
        return maj_value

    counters_dict = {}
    categories = set()
    if not include_params:
        raise ValueError('Expected a list of parameters to include. Got None')
    for seq in list_seqs:
        categories.update(include_params)
        for param in include_params:
            counter = counters_dict.get(param, Counter({default: 0}))
            value = seq.get(param, default)
            counter[value] += 1
            counters_dict[param] = counter

    majority_dict = {}
    for parameter, counter in counters_dict.items():
        majority_dict[parameter] = pick_majority(counter, parameter)
    return majority_dict


# def extract_reasons(data: list):
#     """
#     Given a list of tuples, extract all the elements at index 1, and return
#     as a list
#
#     Parameters
#     ----------
#     data : List
#         A list of tuples
#
#     Returns
#     -------
#     list
#         List of values at index 1
#     """
#     return list(zip(*data))[1]


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
        logger.error('Expected at least one entry in counter. Got 0')
        raise ValueError('Expected at least one entry in counter. Got 0')
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
            raise ValueError('At least one of sequences is empty.')
    if len(list_) < 3:
        logger.info('Cannot compute majority attribute values. '
                    'Got less than 3 values for each '
                    'parameter. Returns majority values as None.')
        raise CannotComputeMajority('Count < 3')
    return True


def split_list(dir_index: Sized, num_chunks: int) -> Iterable:
    """
    Adapted from https://stackoverflow.com/questions/2130016/splitting-a-list-into-n-parts-of-approximately-equal-length # noqa

    Given a list of n elements, split it into k parts, where k = num_chunks.
    Each part has at least n/k elements. And the remaining elements
    n % k are distributed uniformly among the sub-parts such that
    each part has almost same number of elements. The first n % k will have
    floor(n/k) + 1 elements.

    Parameters
    ----------
    dir_index : Sized
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
    if num_chunks < 1:
        raise ValueError('Cannot divide list into chunks of size 0')
    if len(dir_index) == 0:
        raise ValueError('List of directories is empty!')
    if len(dir_index) < num_chunks:
        logger.warning(
            f'Got num_chunks={num_chunks}, list_size={len(dir_index)}'
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

    # format_params = '\n'.join(['File system outputs: %O',
    #                            'Maximum RSS size: %M',
    #                            'CPU percentage used: %P',
    #                            'Real Time: %E',
    #                            'User Time: %U',
    #                            'Sys Time: %S'])
    # cmd = ['/usr/bin/time', '-f', format_params, 'bash', str(script_path)]
    cmd = f'bash {str(script_path)}'

    try:
        Popen(cmd, close_fds=True, shell=True)
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


# def get_outliers(data: list, m=25.0) -> Union[list, None]:
#     """
#     Check for outliers. Adapted from
#     https://stackoverflow.com/a/16562028/3140172
#     Parameters
#     ----------
#     data : list
#         list of values
#     m : float
#         number of standard deviations to use as threshold
#     """
#     d = np.abs(data - np.median(data))
#     mdev = np.median(d)
#     s = d / mdev if mdev else 0.
#     if np.any(s > m):
#         indices = np.argwhere(s > m).flatten()
#         return indices
#     return None


# def round_dict_values(dict_: dict, decimals: int) -> dict:
#     """
#     Round all the values in a dictionary to a given number of decimals.
#
#     Parameters
#     ----------
#     dict_ : dict
#         dictionary of key, value pairs. Values can be numbers or strings.
#         The function will only round the values that are numbers.
#     decimals : int
#         number of decimals to round to
#
#     Returns
#     -------
#     dict
#         dictionary with all the values rounded to the given number of decimals
#     """
#     new_dict = dict_.copy()
#     for key, value in new_dict.items():
#         new_dict[key] = round_if_numeric(value, decimals)
#     return new_dict


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


# def round_if_numeric(value: Union[int, float],
#                      decimals: int = 3) -> Union[int, float, np.ndarray]:
#     """
#     Round a number to a given number of decimals.
#
#     Parameters
#     ----------
#     value: int or float
#         number to round
#     decimals :  int
#         number of decimals to round to
#
#     Returns
#     -------
#     int or float
#         rounded number
#     """
#     # For historical reasons, bool is a type of int, but we cannot
#     # apply np.round on bool
#     if isinstance(value, bool):
#         return value
#     elif isinstance(value, (int, float)):
#         # round using numpy and then convert to native python type
#         return np.around(value, decimals=decimals).item()
#     return value


def compute_majority(dataset: BaseDataset, seq_name, config_dict=None):
    """
    Compute the most frequent values for each acquisition parameter

    Parameters
    ----------
    dataset : BaseDataset
        dataset should contain multiple sequences. The most frequent values
        will be computed for each sequence in the dataset

    """
    # if config_dict is not None:
    # TODO: parse begin and end times
    # TODO: add option to exclude subjects
    seq_dict = {}
    most_freq_vals = {}

    if config_dict is None:
        logger.error('No horizontal audit config found. '
                     f'Returning empty reference protocol for {seq_name}')
        return most_freq_vals

    include_params = config_dict.get('include_parameters', None)
    if include_params is None:
        logger.error('No parameters specified for horizontal audit. '
                     f'Returning empty reference protocol for {seq_name}')
        return most_freq_vals

    stratify_by = config_dict.get('stratify_by', None)

    for subj, sess, runs, seq in dataset.traverse_horizontal(seq_name):
        sequence_id = modify_sequence_name(seq, stratify_by, None)
        if sequence_id not in seq_dict:
            seq_dict[sequence_id] = []
        seq_dict[sequence_id].append(seq)

    for seq_id in seq_dict:
        try:
            most_freq_vals[seq_id] = majority_values(
                seq_dict[seq_id],
                default=Unspecified,
                include_params=include_params)
        except (CannotComputeMajority, ValueError) as e:
            logger.warning(f"Could not compute reference "
                           f"protocol for {seq_name} : {e}.")
    return most_freq_vals


def _cli_report(hz_audit: dict, report_name):
    """
    CLI report generator.
    Generate a single line report for the dataset

    Parameters
    ----------
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
    if not (compliant_ds.get_sequence_ids()
            or non_compliant_ds.get_sequence_ids()
            or undetermined_ds.get_sequence_ids()):
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

    print(ret_string)
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


def _get_time(time_format: str, last_reported_on: str):
    str_format = DATETIME_FORMAT
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
    return mod_time


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
    modified_folders = set()

    mod_time = get_datetime(last_reported_on)
    out_path = Path(output_dir) / 'modified_folders_since.txt'
    if out_path.is_file():
        out_path.unlink()

    cmd = f"find {input_dir} -type f -newermt '{mod_time}' > {out_path}"

    try:
        run(cmd, check=True, shell=True)
        modified_files = txt2list(out_path)
        for f in modified_files:
            if not Path(f).is_file():
                logger.warning(f'File {f} not found.')
            if not is_dicom_file(f):
                continue
            else:
                modified_folders.add(Path(f).parent)
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

    return list(modified_folders)


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
            last_line = lines[i].strip('\n').split(',')
            last_reported_on, last_report_path, last_mrds_path = last_line
            if Path(last_mrds_path).is_file():
                return last_reported_on, last_report_path, last_mrds_path
            i -= 1


def get_timestamps():
    """
    Get the current timestamp in UTC and local time
    """
    now = datetime.now(timezone.utc)
    now = now.replace(tzinfo=timezone.utc)
    ts = datetime.timestamp(now)
    date_time = now.strftime('%m/%d/%Y %H:%M:%S%z')
    return {
        'utc'      : ts,
        'date_time': date_time
    }


def export_subject_lists(output_dir: Union[Path, str],
                         non_compliant_ds: BaseDataset,
                         folder_name: str) -> dict:
    """
    Export subject lists for each sequence to a text file
    """
    noncompliant_sub_by_seq = subject_list2txt(non_compliant_ds,
                                               output_dir / folder_name)
    return noncompliant_sub_by_seq


def folders_with_min_files(root: Union[Path, str],
                           pattern: Optional[str] = "*.dcm",
                           min_count=3) -> List[Path]:
    """
    Returns all the folders with at least min_count of files
    matching the pattern, one at time via generator.

    Parameters
    ----------
    root : List[Path]
        List of folder paths
    pattern : str
        pattern to filter_fn files

    min_count : int
        size representing the number of files in folder
        matching the input pattern

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
    """
    Check if a folder has any subfolders
    """
    if isinstance(fpath, str):
        fpath = Path(fpath)
    if not fpath.is_dir():
        raise FileNotFoundError(f'Folder not found: {fpath}')

    sub_dirs = []
    for file_ in fpath.iterdir():
        if file_.is_dir():
            sub_dirs.append(file_)
        elif file_.suffix == '.dcm':
            # you have reached a folder which contains '.dcm' files
            break

    # sub_dirs = [file_ for file_ in fpath.iterdir() if file_.is_dir()]
    return len(sub_dirs) < 1, sub_dirs


def save_audit_results(filepath: Union[str, Path], result_dict) -> None:
    """
    Save a dataset to a file with extension .mrds.pkl

    Parameters
    ----------
    filepath: Union[str, Path]
        path to the dataset file
    result_dict: dict
        dictionary containing the compliant and non-compliant dataset object

    Returns
    -------
    None

    Examples
    --------
    .. code :: python

        from MRdataset import save_mr_dataset
        my_dataset = import_dataset(data_source='/path/to/my/data/',
                      ds_format='dicom', name='abcd_baseline',
                      config_path='mri-config.json')
        dataset = save_mr_dataset(filepath='/path/to/my/dataset.mrds.pkl',
                                  mrds_obj=my_dataset)
    """

    # Extract extension from filename
    EXT = '.adt.pkl'
    ext = "".join(Path(filepath).suffixes)
    assert ext == EXT, f"Expected extension {EXT}, Got {ext}"
    parent_folder = Path(filepath).parent
    try:
        parent_folder.mkdir(exist_ok=True, parents=True)
    except OSError as exc:
        logger.error(f'Unable to create folder {parent_folder}'
                     ' for saving dataset')
        raise exc

    with open(filepath, 'wb') as f:
        # save dict of the object as pickle
        pickle.dump(result_dict, f)


def find_terminal_folders(root, leave=True, position=0):
    """
    Find all the terminal folders in a given directory
    """
    try:
        no_more_subdirs, sub_dirs = is_folder_with_no_subfolders(root)
    except FileNotFoundError:
        return []

    if no_more_subdirs:
        return [root, ]

    terminal = list()
    for sd1 in tqdm(sub_dirs, leave=leave, position=position):
        no_more_subdirs2, level2_subdirs = is_folder_with_no_subfolders(sd1)
        if no_more_subdirs2:
            terminal.append(sd1)
        else:
            for sd2 in level2_subdirs:
                terminal.extend(find_terminal_folders(sd2, leave=False,
                                                      position=1))

    return terminal


def get_datetime(date):
    try:
        date = datetime.strptime(date, DATETIME_FORMAT)
    except ValueError as exc:
        if 'unconverted data remains' in str(exc):
            try:
                date = datetime.strptime(date, DATE_FORMAT)
            except ValueError as exc:
                raise ValueError(f'Invalid date format. '
                                 f'Use one of '
                                 f'[{DATE_FORMAT}, {DATETIME_FORMAT}]') from exc
    return date


def log_latest_non_compliance(dataset, config_path,
                              filter_fn=None,
                              audit='hz', date=None, output_dir=None):
    """
    Log the latest non-compliance data from recent sessions to a file
    """
    nc_log = {}
    ds_name = None
    date = get_datetime(date)

    config = get_config(config_path=config_path, report_type=audit)
    parameters = config.get("include_parameters", None)

    if audit == 'hz':
        ds_name = dataset.name
        nc_log = dataset.generate_nc_log(parameters, filter_fn,
                                         date=date,
                                         audit='hz', verbosity=1,
                                         output_dir=None)
    elif audit == 'vt':
        ds_name = dataset.name
        nc_log = dataset.generate_nc_log(parameters, filter_fn,
                                         date=date,
                                         audit='vt', verbosity=1,
                                         output_dir=None)

    status_filepath = status_fpath(output_dir, audit)
    if not status_filepath.parent.is_dir():
        status_filepath.parent.mkdir(parents=True)

    if not nc_log:
        # there is no new non-compliant data
        return False
    with open(status_filepath, 'w', encoding='utf-8') as fp:
        fp.write("Scan Date, Dataset Name, Sequence Name,"
                 " Subject ID, Parameter\n")
        for parameter in nc_log:
            for i in nc_log[parameter]:
                fp.write(f" {i['date']}, {ds_name}, {i['sequence_name']},"
                         f" {i['subject']}, {parameter} \n")
    return True


def tuples2dict(mylist):
    """
    Utility function used in jinja2 template. Not used in
    the main code. Do not delete.
    """
    result = {}
    # each entry in mylist is a tuple of the form
    # ((param_nc_sequence, param_ref_sequence), subject_id, path, seq)
    for i in mylist:
        param_tuple = i[0]

        # param_tuple[1] is the parameter from reference sequence
        # param_tuple[0] is the parameter from the non-compliant sequence
        param_sequence = param_tuple[0]
        result.setdefault(param_sequence, []).append(i[1:3])
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
            raise FileNotFoundError('Invalid File {0}'.format(files))
        return Path(files).resolve()
    elif isinstance(files, Iterable):
        for file in files:
            if not Path(file).is_file():
                raise FileNotFoundError('Invalid File {0}'.format(file))
        return [Path(f).resolve() for f in files]
    else:
        raise NotImplementedError('Expected str or Path or Iterable, '
                                  f'Got {type(files)}')


def modify_sequence_name(seq: "BaseSequence", stratify_by: str,
                         datasets) -> str:
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
    # TODO: change stratify_by from attributes to acquisition parameters
    stratify_value = ''
    seq_name_with_stratify = seq.name
    if 'gre_field' in seq.name.lower():
        stratify_by = 'NonLinearGradientCorrection'
        nlgc = seq[stratify_by].get_value()
        if 'P' in nlgc:
            stratify_value = 'P'
        elif 'M' in nlgc:
            stratify_value = 'M'
        else:
            stratify_value = ''

        seq_name_with_stratify = ATTRIBUTE_SEPARATOR.join([seq.name,
                                                           stratify_value])
    # elif stratify_by:
    #     try:
    #         stratify_value = seq[stratify_by].get_value()
    #         seq_name_with_stratify = ATTRIBUTE_SEPARATOR.join(
    #             [seq.name, stratify_value])
    #     except KeyError:
    #         logger.warning(f"Attribute {stratify_by} not found in "
    #                        f"sequence {seq.name}")

    if datasets:
        for ds in datasets:
            ds.set_modified_seq_name(seq.name, seq_name_with_stratify)

    return seq_name_with_stratify


def get_config_from_file(config_path: Union[Path, str]) -> dict:
    """
    Read the configuration file and return the contents as a dictionary

    Parameters
    ----------
    config_path : Path or str
        path to the configuration file

    Returns
    -------
    dict
        contents of the configuration file
    """
    try:
        config_path = Path(config_path)
    except TypeError:
        raise TypeError('Invalid path to the configuration file.'
                        f'Expected Path or str, got {type(config_path)}')
    if not config_path.is_file():
        raise FileNotFoundError('Either provided configuration '
                                'file does not exist or it is not a '
                                'file.')

    # read json file
    with open(config_path, 'r') as f:
        try:
            config = json.load(f)
        except ValueError:
            # json.decoder.JSONDecodeError is a subclass of ValueError
            raise ValueError('Invalid JSON file provided in config_path '
                             'Expected a valid JSON file.')

    return config


def get_protocol_from_file(reference_path: Union[Path, str],
                           vendor: str = 'siemens') -> MRImagingProtocol:
    """
    Extracts the reference protocol from the file. Supports only Siemens
    protocols in xml format. Raises error otherwise.

    Parameters
    ----------
    reference_path : Path | str
        Path to the reference protocol file
    vendor: str
        Vendor of the scanner. Default is Siemens

    Returns
    -------
    ref_protocol : MRImagingProtocol
        Reference protocol extracted from the file
    """
    # Extract reference protocol from file
    ref_protocol = None

    if not isinstance(reference_path, Path):
        reference_path = Path(reference_path)

    if not reference_path.is_file():
        raise FileNotFoundError(f'Unable to access {reference_path}. Maybe it'
                                f'does not exist or is not a file')

    # TODO: Add support for other file formats, like json and dcm
    if reference_path.suffix != '.xml':
        raise ValueError(f'Expected xml file, got {reference_path.suffix} file')

    # TODO: Add support for other vendors, like GE and Philips
    if vendor == 'siemens':
        ref_protocol = SiemensMRImagingProtocol(filepath=reference_path)
    else:
        raise NotImplementedError('Only Siemens protocols are supported')

    return ref_protocol


def infer_protocol(dataset: BaseDataset,
                   config: dict) -> MRImagingProtocol:
    """
    Infers the reference protocol from the dataset. The reference protocol
    is inferred by computing the majority for each of the
    parameters for each sequence in the dataset.

    Parameters
    ----------
    dataset: BaseDataset
        Dataset to be checked for compliance
    config: dict
        Configuration

    Returns
    -------
    ref_protocol : MRImagingProtocol
        Reference protocol inferred from the dataset
    """
    # TODO: Check for subset, if incomplete dataset throw error and stop
    ref_protocol = MRImagingProtocol(f'reference_for_{dataset.name}')

    # create reference protocol for each sequence
    for seq_name in dataset.get_sequence_ids():
        num_subjects = dataset.get_subject_ids(seq_name)

        # If subjects are less than 3, then we can't infer a reference protocol
        if len(num_subjects) < 3:
            logger.warning(f'Skipping {seq_name}. Not enough subjects to'
                           f' infer a reference protocol')
            continue

        # If subjects are more than 3, then we can infer a reference protocol
        ref_dict = compute_majority(dataset=dataset,
                                    seq_name=seq_name,
                                    config_dict=config)
        if not ref_dict:
            continue
        # Add the inferred reference to the reference protocol
        for seq_id, param_dict in ref_dict.items():
            ref_protocol.add_sequence_from_dict(seq_id, param_dict)

    return ref_protocol


def filter_epi_fmap_pairs(pair):
    epi_substrings = ['epi', 'bold', 'rest', 'fmri', 'pasl',
                      'asl', 'dsi', 'dti', 'dwi']
    fmap_substrings = ['fmap', 'fieldmap', 'map']
    if (has_substring(pair[0].lower(), epi_substrings)
            and has_substring(pair[1].lower(), fmap_substrings)):
        return True
    if (has_substring(pair[1].lower(), epi_substrings)
            and has_substring(pair[0].lower(), fmap_substrings)):
        return True
    return False


def has_substring(input_string, substrings):
    """Check if a string contains any of the substrings"""
    for substring in substrings:
        if substring in input_string:
            return True
    return False


def previous_month(dt):
    """Return the first day of the previous month."""
    return (dt.replace(day=1) - timedelta(days=1)).replace(day=1)


def next_month(dt):
    """Return the first day of the next month."""
    return (dt.replace(day=28) + timedelta(days=5)).replace(day=1)


def send_email(log_filepath,
               project_code,
               email_config,
               report_path, server='localhost', port=25):
    """
    Send an email alert if there is a change in the status of the audit
    """
    # check if log filepath exists
    if not Path(log_filepath).is_file():
        raise FileNotFoundError(f'Log file not found: {log_filepath}')

    # check if config exists
    try:
        config = get_config_from_file(email_config)
    except (ValueError, FileNotFoundError, TypeError) as e:
        logger.error(f'Error while reading config file: {e}. Please provide'
                     f'a valid path to the email configuration JSON file.')
        raise e
    to_emails = config.get('email_for_project', {})

    # Create email message
    with open(log_filepath) as fp:
        msg = MIMEMultipart()
        msg.attach(MIMEText(fp.read()))

    today = datetime.today()
    msg['Subject'] = (f'mrQA : '
                      f'{project_code} dt. {today.strftime("%m.%d.%Y")}')
    msg['From'] = 'mrqa'
    if project_code in to_emails:
        msg['To'] = ", ".join(to_emails[project_code])
    # else:
    msg['To'] = ", ".join(config['default_email'])

    # Attach report to the email
    with open(report_path, 'rb') as fp:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(fp.read())

    # Encode to base64
    encoders.encode_base64(part)

    # Add header
    part.add_header(
        "Content-Disposition",
        f"attachment; filename={Path(report_path).name}",
    )

    # Add attachment to your message and convert it to string
    msg.attach(part)

    # send your email,
    # the recipient will receive it as spam, very likely
    # assuming postfix server is running on localhost
    # https://medium.com/yavar/send-mail-using-postfix-server-bbb08331d39d # noqa
    try:
        with SMTP(server, port) as s:
            s.send_message(msg)
    except (OSError, ConnectionRefusedError) as e:
        logger.error(f'Unable to send email. {e}')
