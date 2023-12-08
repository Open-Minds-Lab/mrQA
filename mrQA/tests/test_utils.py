import re
import tempfile
from datetime import datetime, timedelta, date
from pathlib import Path

import pytest
from hypothesis import given, settings, assume
from hypothesis.strategies import lists, integers, dates, text, composite, \
    characters, booleans, tuples
from mrQA.tests.conftest import sample_protocol, THIS_DIR, dcm_dataset_strategy
from mrQA.utils import split_list, convert2ascii, next_month, previous_month, \
    has_substring, filter_epi_fmap_pairs, get_protocol_from_file, \
    get_config_from_file, valid_paths, folders_with_min_files, \
    find_terminal_folders, save_audit_results, is_folder_with_no_subfolders, \
    get_reference_protocol, get_config, is_writable, send_email, logger
from protocol import SiemensMRImagingProtocol, MRImagingProtocol


@given(
    dir_index=lists(integers(), min_size=1),
    num_chunks=integers(min_value=1)
)
def test_split_list_hypothesis(dir_index, num_chunks):
    if num_chunks < 0:  # Ensure num_chunks is greater than 0
        with pytest.raises(ValueError):
            split_list(dir_index, num_chunks)
            return

    result = list(split_list(dir_index, num_chunks))

    if len(dir_index) < num_chunks:  # Ensure dir_index has enough elements
        # Assertions for the result based on the expected behavior of split_list
        num_chunks = len(dir_index)

    # Assertions for the result based on the expected behavior of split_list
    assert len(result) == num_chunks
    assert sum(map(len, result)) == len(dir_index)


def test_split_list_value_errors():
    with pytest.raises(ValueError):
        split_list([], 1)
    with pytest.raises(ValueError):
        split_list([1], 0)
    with pytest.raises(ValueError):
        split_list([1], -1)


# Define a strategy for generating strings
@composite
def strings(draw):
    return draw(text())


# Define a strategy for generating ASCII strings
@composite
def ascii_strings(draw):
    return draw(text(
        alphabet=characters(whitelist_categories=('L', 'N', 'P', 'Z', 'S'))))


# Define a strategy for generating booleans
@composite
def booleans(draw):
    return draw(booleans())


# Property-based test: the output should contain only ASCII characters
@given(strings())
def test_contains_only_ascii(value):
    result = convert2ascii(value, allow_unicode=False)
    assert all(ord(char) < 128 for char in result)


# Property-based test: the output should not contain spaces or
# dashes at the beginning or end
@given(strings())
def test_no_spaces_or_dashes_at_ends(value):
    result = convert2ascii(value, False)
    assert not result.startswith((' ', '-'))
    assert not result.endswith((' ', '-'))


# Property-based test: the output should not contain consecutive
# spaces or dashes
@given(ascii_strings())
def test_no_consecutive_spaces_or_dashes(value):
    result = convert2ascii(value, allow_unicode=False)
    assert '  ' not in result
    assert '--' not in result


# Property-based test: the output should not contain any special characters
@given(ascii_strings())
def test_no_special_characters(value):
    result = convert2ascii(value, allow_unicode=False)
    assert re.match(r'^[a-zA-Z0-9_-]*$', result)


# Property-based test: converting twice should be the same as converting once
@given(ascii_strings())
def test_double_conversion_is_same(value):
    result1 = convert2ascii(value, allow_unicode=False)
    result2 = convert2ascii(result1, allow_unicode=False)
    assert result1 == result2


def test_next_month():
    # Test cases with specific dates
    assert next_month(datetime(2023, 1, 15)) == datetime(2023, 2, 1)
    assert next_month(datetime(2022, 12, 5)) == datetime(2023, 1, 1)
    # Add more test cases as needed


@given(dt=dates())
def test_next_month_hypothesis(dt):
    result = next_month(dt)

    # Ensure the result is a datetime object
    assert isinstance(result, date)

    # Ensure the result is the first day of the next month
    expected_result = (dt.replace(day=28) + timedelta(days=5)).replace(day=1)
    assert result == expected_result


def test_previous_month():
    # Test cases with specific dates
    assert previous_month(datetime(2023, 2, 15)) == datetime(2023, 1, 1)
    assert previous_month(datetime(2023, 1, 1)) == datetime(2022, 12, 1)
    # Add more test cases as needed


@given(dt=dates())
def test_previous_month_hypothesis(dt):
    result = previous_month(dt)

    # Ensure the result is a datetime object
    assert isinstance(result, date)

    # Ensure the result is the first day of the previous month
    expected_result = (dt.replace(day=1) - timedelta(days=1)).replace(day=1)
    assert result == expected_result


def test_has_substring():
    # Test cases with specific inputs
    assert has_substring("hello world", ["hello", "world"])
    assert has_substring("python", ["java", "python", "cpp"])
    assert not has_substring("apple", ["orange", "banana"])
    # Add more test cases as needed


@given(
    input_string=text(),
    substrings=lists(text(), min_size=1)
)
def test_has_substring_hypothesis(input_string, substrings):
    result = has_substring(input_string, substrings)

    # Ensure the result is a boolean
    assert isinstance(result, bool)

    # Ensure the result is True if and only if at least one substring is
    # present in the input_string
    expected_result = any(substring in input_string for substring in substrings)
    assert result == expected_result


def test_filter_epi_fmap_pairs():
    # Test cases with specific inputs
    assert filter_epi_fmap_pairs(("epi_bold", "fmap_fieldmap"))
    assert filter_epi_fmap_pairs(("rest_fmri", "map"))
    assert not filter_epi_fmap_pairs(("dti", "asl"))
    # Add more test cases as needed


@given(
    pair=tuples(text(), text())
)
def test_filter_epi_fmap_pairs_hypothesis(pair):
    result = filter_epi_fmap_pairs(pair)
    assert filter_epi_fmap_pairs(('epi', 'fmap'))
    assert filter_epi_fmap_pairs(('fmap', 'epi'))
    # Ensure the result is a boolean
    assert isinstance(result, bool)


def test_get_protocol_from_file():
    ref_protocol = sample_protocol()
    protocol = get_protocol_from_file(str(ref_protocol))

    assert isinstance(protocol, SiemensMRImagingProtocol)

    with pytest.raises(FileNotFoundError):
        get_protocol_from_file("nonexistent_file.txt")

    with pytest.raises(ValueError):
        get_protocol_from_file(THIS_DIR / 'resources/mri-config.json')


def test_get_config_from_file():
    config = get_config_from_file(THIS_DIR / 'resources/mri-config.json')
    with pytest.raises(TypeError):
        get_config_from_file(config)
    with pytest.raises(FileNotFoundError):
        get_config_from_file("nonexistent_file.txt")
    with pytest.raises(ValueError):
        get_config_from_file(THIS_DIR / 'resources/invalid-json.json')


def test_valid_paths():
    with pytest.raises(ValueError):
        valid_paths(None)
    with pytest.raises(FileNotFoundError):
        valid_paths('nonexistent_file.txt')
    with pytest.raises(FileNotFoundError):
        valid_paths(['nonexistent_file.txt'])


# Test find_terminal_folders with terminal folders
def test_find_terminal_folders_with_terminals():
    with tempfile.TemporaryDirectory() as tmpdirname:
        root = Path(tmpdirname)
        folder1 = root / "folder1"
        folder1.mkdir()
        folder2 = folder1 / "folder2"
        folder2.mkdir()

        terminal_folders = find_terminal_folders(root)
        assert terminal_folders == [folder2]

        folder3 = folder2 / "folder3"
        folder3.mkdir()

        terminal_folders = find_terminal_folders(root)
        assert terminal_folders == [folder3]


# Test find_terminal_folders with single folder
def test_find_terminal_folders_single_folder():
    with tempfile.TemporaryDirectory() as tmpdirname:
        root = Path(tmpdirname)
        folder = root / "folder"
        folder.mkdir()

        terminal_folders = find_terminal_folders(root)
        assert terminal_folders == [folder]


# Test find_terminal_folders with non-existent folder
def test_find_terminal_folders_nonexistent_folder():
    with tempfile.TemporaryDirectory() as tmpdirname:
        root = Path(tmpdirname) / "nonexistent_folder"

        terminal_folders = find_terminal_folders(root)
        assert terminal_folders == []


def test_folder_with_min_files_nonexistent_folder():
    with tempfile.TemporaryDirectory() as tmpdirname:
        root = Path(tmpdirname) / "nonexistent_folder"
        with pytest.raises(ValueError):
            a = list(folders_with_min_files(root, pattern="*.dcm", min_count=1))
        with pytest.raises(ValueError):
            a = list(folders_with_min_files([], pattern="*.dcm", min_count=0))


# Test find_terminal_folders with files
def test_find_terminal_folders_with_files():
    with tempfile.TemporaryDirectory() as tmpdirname:
        root = Path(tmpdirname)
        file = root / "file.txt"
        file.touch()

        terminal_folders = find_terminal_folders(root)
        assert terminal_folders == [root]


# Test find_terminal_folders with nested terminal folders
def test_find_terminal_folders_nested_terminals():
    with tempfile.TemporaryDirectory() as tmpdirname:
        root = Path(tmpdirname)
        folder1 = root / "folder1"
        folder1.mkdir()
        folder2 = folder1 / "folder2"
        folder2.mkdir()
        folder3 = folder2 / "folder3"
        folder3.mkdir()

        terminal_folders = find_terminal_folders(folder1)
        assert terminal_folders == [folder3]


# Test find_terminal_folders with multiple terminal folders
def test_find_terminal_folders_multiple_terminals():
    with tempfile.TemporaryDirectory() as tmpdirname:
        root = Path(tmpdirname)
        folder1 = root / "folder1"
        folder1.mkdir()
        folder2 = root / "folder2"
        folder2.mkdir()
        folder3 = root / "folder3"
        folder3.mkdir()

        terminal_folders = find_terminal_folders(root)
        assert set(terminal_folders) == {folder1, folder2, folder3}


def test_find_folders_with_min_files():
    with tempfile.TemporaryDirectory() as tmpdirname:
        root = Path(tmpdirname).resolve()
        thresh = 3
        expected = set()
        for idx, num_files in zip(range(5), [2, 3, 3, 5, 1]):
            folder = root / f"folder{idx}"
            folder.mkdir()
            for count in range(num_files):
                file = folder / f"file{count}.dcm"
                file.touch()

            if num_files >= thresh:
                expected.add(folder.resolve())

        terminal_folders = folders_with_min_files(root, "*.dcm", min_count=thresh)
        assert set(terminal_folders) == expected


def test_save_audit_results():
    with pytest.raises(OSError):
        save_audit_results('/sys/firmware/hz.adt.pkl', {})


# Test when folder has subfolders
def test_has_subfolders():
    with tempfile.TemporaryDirectory() as tmpdirname:
        folder_path = Path(tmpdirname)
        subfolder = folder_path / "subfolder"
        subfolder.mkdir(parents=True, exist_ok=True)

        has_no_subfolders, subfolders = is_folder_with_no_subfolders(
            folder_path)
        assert has_no_subfolders is False
        assert subfolder in subfolders


# Test when folder has no subfolders
def test_no_subfolders():
    with tempfile.TemporaryDirectory() as tmpdirname:
        folder_path = Path(tmpdirname)

        has_no_subfolders, subfolders = is_folder_with_no_subfolders(
            folder_path)
        assert has_no_subfolders is True
        assert subfolders == []


# Test when folder doesn't exist
def test_nonexistent_folder():
    folder_path = Path("nonexistent_folder")

    with pytest.raises(FileNotFoundError):
        is_folder_with_no_subfolders(folder_path)


@settings(max_examples=1, deadline=None)
@given(args=(dcm_dataset_strategy))
def test_get_reference_protocol(args):
    ds1, attributes = args
    assume(len(ds1.name) > 0)
    ds1.load()
    config = get_config_from_file(attributes['config_path'])
    protocol = get_reference_protocol(ds1, config, 'nonexistent_file.txt')
    assert isinstance(protocol, MRImagingProtocol)


def test_get_config():
    with pytest.raises(FileNotFoundError):
        get_config("nonexistent_file.txt")
    with pytest.raises(ValueError):
        get_config(THIS_DIR / 'resources/mri-config.json',
                   report_type='horizontal')
    config_path = THIS_DIR / 'resources/test-config.json'
    config = get_config(config_path, report_type='hz')
    config = get_config(config_path, report_type='vt')
    assert isinstance(config, dict)


def test_is_writable():
    assert not is_writable('/sys/firmware/')

# def test_email():
#     log_fpath = '/home/sinhah/status_check.txt'
#     email_config = '/home/sinhah/github/mrQA/examples/email_config.json'
#     report_fpath =  '/home/sinhah/mrqa_reports/MRRC-reportsv2/mrqa_reports_v2/7T/7T_DATE_11_25_2023_08_04_04.html'
#     try:
#         send_email(log_filepath=log_fpath,
#                    project_code='7T',
#                    email_config=email_config,
#                    report_path=report_fpath)
#     except FileNotFoundError as e:
#         logger.error(f'Could not send email. {e}')
