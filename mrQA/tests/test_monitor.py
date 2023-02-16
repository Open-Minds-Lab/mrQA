import unittest
import zipfile
from pathlib import Path
from MRdataset import import_dataset, load_mr_dataset, MRDS_EXT
from mrQA import check_compliance
from mrQA.monitor import mrqa_monitor
import shutil
import subprocess


class TestMergeDatasets(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Extract Zip folders
        zip_path = Path('/home/sinhah/github/MRdataset/examples/test_merge_data.zip')
        temp_dir = Path('/tmp/')
        extract_folder = temp_dir/zip_path.stem
        cls.data_source = temp_dir/'test_merge_data'
        if extract_folder.exists():
            shutil.rmtree(extract_folder)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        # Set up output directories
        output_dir = temp_dir/'output_dir'
        if output_dir.is_dir():
            shutil.rmtree(output_dir)
        output_dir.mkdir(exist_ok=False, parents=True)
        output_folder_path = output_dir / 'dummy_ds'
        cls.output_folder_path = output_folder_path

        # Read full dataset, acts as ground truth
        complete_dataset = import_dataset(
            data_source=temp_dir/'test_merge_data/full_data',
            name='dummy_ds')
        report_path = check_compliance(complete_dataset,
                                       output_dir=output_folder_path)
        fname = output_folder_path / f'{report_path.stem}{MRDS_EXT}'
        cls.complete_dataset = load_mr_dataset(fname)

    def test_modalities(self):
        input_folder_path = self.data_source/'new_modalities'
        self.simulate(input_folder_path, self.output_folder_path)

    def test_subjects(self):
        input_folder_path = self.data_source/'new_subjects'
        self.simulate(input_folder_path, self.output_folder_path)

    def test_sessions(self):
        input_folder_path = self.data_source/'new_sessions'
        self.simulate(input_folder_path, self.output_folder_path)

    def test_runs(self):
        input_folder_path = self.data_source/'new_runs'
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
        ds1 = import_dataset(data_source=input_fpath/'set1',
                             name='dummy_ds')
        check_compliance(ds1, output_dir=output_fpath)

        self.copy_new_scans(src=input_fpath / 'set2',
                            dest=input_fpath / 'set1')

        report_path = mrqa_monitor(name='dummy_ds',
                                   data_source=input_fpath/'set1',
                                   output_dir=output_fpath)
        fname = report_path.stem
        ds2 = load_mr_dataset(output_fpath/f"{fname}{MRDS_EXT}")
        assert self.is_same_dataset(ds2, self.complete_dataset)



