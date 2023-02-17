import shutil
import subprocess
import time
import unittest
import zipfile
from pathlib import Path

import numpy as np
from MRdataset import import_dataset, load_mr_dataset, MRDS_EXT
from MRdataset.utils import is_same_dataset

from mrQA import check_compliance
from mrQA.config import mrds_fpath
from mrQA.monitor import monitor
from mrQA.utils import get_timestamps, files_modified_since


# class TestMonitor(unittest.TestCase):
def test_monitor(data_source=None) -> None:
    data_src = Path(data_source)
    name = data_src.stem
    dest_dir = Path('/tmp/')
    dest_folder_path = dest_dir / Path(data_source).stem
    if dest_folder_path.is_dir():
        shutil.rmtree(dest_folder_path)

    # Set up output directories
    output_dir = dest_dir / 'output_dir'
    if output_dir.is_dir():
        shutil.rmtree(output_dir)
    output_dir.mkdir(exist_ok=False, parents=True)
    output_folder_path = output_dir / Path(data_source).stem
    # output_folder_path = output_folder_path

    # Create n sets
    n = 10
    total_num_files = 10000
    files_in_src = [f for f in data_src.rglob('*') if f.is_file()]
    testing_set = files_in_src

    file_sets = np.array_split(testing_set, n)
    time_dict = None
    for i in range(n):
        copy2dest(file_sets[i], data_source, dest_folder_path)
        time.sleep(10)

        if time_dict:
            last_reported_on = time_dict['utc']
            modified_files = files_modified_since(
                input_dir=dest_folder_path,
                last_reported_on=last_reported_on,
                output_dir=output_folder_path)
            expected = get_relative_paths(file_sets[i], data_src)
            got = get_relative_paths(modified_files, dest_folder_path)
            assert len(expected) == len(got)
            assert sorted(expected) == sorted(got)

        time_dict = get_timestamps()

        report = monitor(name=name,
                         data_source=dest_folder_path,
                         output_dir=output_folder_path)
        mrds_path = mrds_fpath(report.parent, report.stem)
        monitor_dataset = load_mr_dataset(mrds_path)

        # Read full dataset, acts as ground truth
        ds = import_dataset(
            data_source=dest_folder_path,
            name=name)
        report = check_compliance(ds, output_dir=output_folder_path)
        mrds_path = mrds_fpath(report.parent, report.stem)
        complete_dataset = load_mr_dataset(mrds_path)

        assert is_same_dataset(complete_dataset, monitor_dataset)


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


class TestMonitorDummyDataset(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Extract Zip folders
        zip_path = Path(
            '/home/sinhah/github/MRdataset/examples/test_merge_data.zip')
        temp_dir = Path('/tmp/')
        extract_folder = temp_dir / zip_path.stem
        cls.data_source = temp_dir / 'test_merge_data'
        if extract_folder.exists():
            shutil.rmtree(extract_folder)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        # Set up output directories
        output_dir = temp_dir / 'output_dir'
        if output_dir.is_dir():
            shutil.rmtree(output_dir)
        output_dir.mkdir(exist_ok=False, parents=True)
        output_folder_path = output_dir / 'dummy_ds'
        cls.output_folder_path = output_folder_path

        # Read full dataset, acts as ground truth
        complete_dataset = import_dataset(
            data_source=temp_dir / 'test_merge_data/full_data',
            name='dummy_ds')
        report_path = check_compliance(complete_dataset,
                                       output_dir=output_folder_path)
        fname = output_folder_path / f'{report_path.stem}{MRDS_EXT}'
        cls.complete_dataset = load_mr_dataset(fname)

    def test_modalities(self):
        input_folder_path = self.data_source / 'new_modalities'
        self.simulate(input_folder_path, self.output_folder_path)

    def test_subjects(self):
        input_folder_path = self.data_source / 'new_subjects'
        self.simulate(input_folder_path, self.output_folder_path)

    def test_sessions(self):
        input_folder_path = self.data_source / 'new_sessions'
        self.simulate(input_folder_path, self.output_folder_path)

    def test_runs(self):
        input_folder_path = self.data_source / 'new_runs'
        self.simulate(input_folder_path, self.output_folder_path)

    @staticmethod
    def copy_new_scans(src, dest):
        cmd = f"cp -r -n {src}/* {dest}"
        with subprocess.Popen(cmd, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, shell=True) as proc:
            o, e = proc.communicate()
            if proc.returncode:
                raise RuntimeError(e.decode('utf8'))

    def simulate(self, input_fpath, output_fpath):
        ds1 = import_dataset(data_source=input_fpath / 'set1',
                             name='dummy_ds')
        check_compliance(ds1, output_dir=output_fpath)

        self.copy_new_scans(src=input_fpath / 'set2',
                            dest=input_fpath / 'set1')

        report_path = monitor(name='dummy_ds',
                              data_source=input_fpath / 'set1',
                              output_dir=output_fpath)

        mrds_path = mrds_fpath(output_fpath, report_path.stem)
        ds2 = load_mr_dataset(mrds_path)
        assert is_same_dataset(ds2, self.complete_dataset)


if __name__ == '__main__':
    # test_monitor(data_source='/home/sinhah/scan_data/CHA_MJFF')
    test_monitor(data_source='/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/')
