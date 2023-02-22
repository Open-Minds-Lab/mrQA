import math
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import pytest

from MRdataset import load_mr_dataset, import_dataset
from MRdataset.utils import is_same_dataset

from mrQA import check_compliance
from mrQA.config import report_fpath, mrds_fpath, past_records_fpath
from mrQA.utils import files_modified_since, get_last_valid_record


def test_modified_files(last_reported_on,
                        temp_input_src,
                        temp_output_dest,
                        data_source,
                        file_set):
    modified_files = files_modified_since(
        input_dir=temp_input_src,
        last_reported_on=last_reported_on,
        output_dir=temp_output_dest)
    expected = get_relative_paths(file_set, data_source)
    got = get_relative_paths(modified_files, temp_input_src)
    assert len(expected) == len(got)
    assert sorted(expected) == sorted(got)


def test_output_files_created(fname, folder):
    time_fname = fname[-19:]
    utc = datetime.strptime(time_fname, '%m_%d_%Y_%H_%M_%S').timestamp()
    report_path = report_fpath(folder, fname)
    mrds_path = mrds_fpath(folder, fname)
    records_path = past_records_fpath(folder)
    last_record = get_last_valid_record(folder)
    assert report_path.is_file()
    assert mrds_path.is_file()
    assert records_path.is_file()
    assert last_record[1] == fname
    assert math.isclose(float(last_record[0]), utc)


def test_same_dataset(mrds_path,
                      temp_input_src,
                      temp_output_dest,
                      name):
    monitor_dataset = load_mr_dataset(mrds_path)

    # Read full dataset, acts as ground truth
    ds = import_dataset(data_source=temp_input_src,
                        name=name)
    report = check_compliance(ds, output_dir=temp_output_dest)
    mrds_path = mrds_fpath(report.parent, report.stem)
    complete_dataset = load_mr_dataset(mrds_path)
    assert is_same_dataset(complete_dataset, monitor_dataset)


def get_temp_input_folder(data_source, temp_dir):
    temp_folder_path = temp_dir / Path(data_source).stem
    if temp_folder_path.is_dir():
        shutil.rmtree(temp_folder_path)
    temp_folder_path.mkdir(exist_ok=False, parents=True)
    return temp_folder_path


def get_temp_output_folder(name, temp_dir):
    # Set up output directories
    output_dir = temp_dir / 'output_dir'
    if output_dir.is_dir():
        shutil.rmtree(output_dir)
    output_dir.mkdir(exist_ok=False, parents=True)
    output_folder_path = output_dir / name
    return output_folder_path


def create_random_file_sets(temp_input_src, n, max_files):
    files_in_src = [f for f in temp_input_src.rglob('*') if f.is_file()]
    testing_set = files_in_src[:max_files]

    np.random.shuffle(testing_set)
    try:
        file_sets = np.array_split(testing_set, n)
    except ValueError as e:
        with pytest.raises(ValueError):
            raise ValueError(f"Could not split list of dicom files."
                             f" Got n = {n}") from e
        return None
    return file_sets


def get_relative_paths(file_list, data_root):
    rel_paths = []
    for file in file_list:
        rel_path = Path(file).relative_to(data_root)
        rel_paths.append(str(rel_path))
    return rel_paths


def copy2dest(file_list, data_root, dest):
    for file in file_list:
        rel_path = file.relative_to(data_root)
        new_abs_path = dest / rel_path
        parent = new_abs_path.parent
        parent.mkdir(exist_ok=True, parents=True)
        shutil.copy(file, parent)
