import shlex
import subprocess
import sys
import tempfile
from pathlib import Path
from time import sleep

import pytest
from MRdataset import load_mr_dataset
from hypothesis import given, settings, assume

from mrQA.cli import cli
from mrQA.config import DATE_SEPARATOR
from mrQA.monitor import cli as monitor_cli
from mrQA.run_parallel import cli as parallel_cli
from mrQA.run_subset import cli as subset_cli
from mrQA.tests.conftest import dcm_dataset_strategy
from mrQA.utils import list2txt


@settings(max_examples=5, deadline=None)
@given(args=dcm_dataset_strategy)
def test_binary_mrqa(args):
    ds1, attributes = args
    assume(len(ds1.name) > 0)
    ds1.load()
    with tempfile.TemporaryDirectory() as tempdir:
        # shlex doesn't test work with binaries
        subprocess.run(['mrqa',
                        '--data-source', attributes['fake_ds_dir'],
                        '--config', attributes['config_path'],
                        '--name', ds1.name,
                        '--format', 'dicom',
                        '--decimals', '3',
                        '--tolerance', '0.1',
                        '--verbose',
                        '--output-dir', tempdir])
        report_paths = list(Path(tempdir).glob('*.html'))
        # check if report was generated
        assert_paths_more_than_2_subjects(report_paths, tempdir, attributes,
                                          ds1)
    return


@settings(max_examples=5, deadline=None)
@given(args=dcm_dataset_strategy)
def test_binary_mrqa_with_reference_protocol(args):
    ds1, attributes = args
    assume(len(ds1.name) > 0)
    ds1.load()
    with tempfile.TemporaryDirectory() as tempdir:
        # shlex doesn't test work with binaries
        subprocess.run(['mrqa',
                        '--data-source', attributes['fake_ds_dir'],
                        '--config', attributes['config_path'],
                        '--name', ds1.name,
                        '--format', 'dicom',
                        '--decimals', '3',
                        '--tolerance', '0.1',
                        '--verbose',
                        '--ref-protocol-path', attributes['ref_protocol_path'],
                        '--output-dir', tempdir])
        report_paths = list(Path(tempdir).glob('*.html'))
        assert_report_paths(report_paths, tempdir, attributes, ds1)
    return


@settings(max_examples=10, deadline=None)
@given(args=dcm_dataset_strategy)
def test_cli_with_reference_protocol(args):
    ds1, attributes = args
    assume(len(ds1.name) > 0)
    ds1.load()
    with tempfile.TemporaryDirectory() as tempdir:
        sys.argv = shlex.split(
            f'mrqa --data-source {attributes["fake_ds_dir"]}'
            f' --config {attributes["config_path"]}'
            f' --name {ds1.name}'
            f' --format dicom'
            ' --decimals 3'
            ' --tolerance 0.1'
            ' --verbose'
            f' --ref-protocol-path {attributes["ref_protocol_path"]}'
            f' --output-dir  {tempdir}')
        cli()

        report_paths = list(Path(tempdir).glob('*.html'))
        # check if report was generated
        assert_report_paths(report_paths, tempdir, attributes, ds1)
    return


@settings(max_examples=5, deadline=None)
@given(args=dcm_dataset_strategy)
def test_binary_parallel(args):
    ds1, attributes = args
    assume(len(ds1.name) > 0)
    ds1.load()
    with tempfile.TemporaryDirectory() as tempdir:
        # shlex doesn't test work with binaries
        if attributes['num_subjects'] > 2:
            subprocess.run(['mrqa_parallel',
                            '--data-source', attributes['fake_ds_dir'],
                            '--config', attributes['config_path'],
                            '--name', ds1.name,
                            '--decimals', '3',
                            '--tolerance', '0.1',
                            '--verbose',
                            '--job-size', '1',
                            '--out-mrds-path', Path(tempdir)/'test.mrds.pkl',
                            '--output-dir', tempdir])
            report_paths = list(Path(tempdir).glob('*.html'))
            # check if report was generated
            assert_paths_more_than_2_subjects(report_paths, tempdir, attributes,
                                              ds1)
    return


@settings(max_examples=5, deadline=None)
@given(args=dcm_dataset_strategy)
def test_binary_mrqa_monitor(args):
    ds1, attributes = args
    assume(len(ds1.name) > 0)
    ds1.load()
    with tempfile.TemporaryDirectory() as tempdir:
        # shlex doesn't test work with binaries
        subprocess.run(['mrqa_monitor',
                        '--data-source', attributes['fake_ds_dir'],
                        '--config', attributes['config_path'],
                        '--name', ds1.name,
                        '--format', 'dicom',
                        '--decimals', '3',
                        '--tolerance', '0.1',
                        '--verbose',
                        '--output-dir', tempdir])
        report_paths = list(Path(tempdir).glob('*.html'))
        # check if report was generated
        assert_paths_more_than_2_subjects(report_paths, tempdir, attributes,
                                          ds1)
    return


@settings(max_examples=5, deadline=None)
@given(args=dcm_dataset_strategy)
def test_cli_mrqa_monitor(args):
    ds1, attributes = args
    assume(len(ds1.name) > 0)
    ds1.load()
    with tempfile.TemporaryDirectory() as tempdir:
        # shlex doesn't test work with binaries
        sys.argv = shlex.split(
            f'mrqa_monitor --data-source {attributes["fake_ds_dir"]} '
            f' --config {attributes["config_path"]} '
            f' --name {ds1.name} '
            '--format dicom '
            '--decimals 3 '
            '--tolerance 0.1 '
            '--verbose '
            f'--output-dir {tempdir}')
        monitor_cli()
        report_paths = list(Path(tempdir).glob('*.html'))
        # check if report was generated
        assert_paths_more_than_2_subjects(report_paths, tempdir, attributes,
                                          ds1)
    return


@settings(max_examples=5, deadline=None)
@given(args=dcm_dataset_strategy)
def test_cli_run_subset(args):
    ds1, attributes = args
    assume(len(ds1.name) > 0)
    ds1.load()
    with tempfile.TemporaryDirectory() as tempdir:
        # shlex doesn't test work with binaries
        folders = [f for f in Path(attributes['fake_ds_dir']).iterdir()
                   if f.is_dir()]
        batch_file = Path(tempdir) / 'batch.txt'
        list2txt(batch_file, folders)

        sys.argv = shlex.split(
            f'mrqa_subset  '
            f' --config {attributes["config_path"]} '
            f' -b {batch_file} '
            '--verbose '
            f'--output-path {tempdir}/test.mrds.pkl')
        subset_cli()
        ds2 = load_mr_dataset(f"/{tempdir}/test.mrds.pkl")
        assert ds1 == ds2
    return


@settings(max_examples=5, deadline=None)
@given(args=dcm_dataset_strategy)
def test_cli_parallel(args):
    ds1, attributes = args
    assume(len(ds1.name) > 0)
    ds1.load()
    with tempfile.TemporaryDirectory() as tempdir:
        # shlex doesn't test work with binaries
        sys.argv = shlex.split(
            f'mrqa_parallel --data-source {attributes["fake_ds_dir"]} '
            f' --config {attributes["config_path"]} '
            f' --name {ds1.name} '
            '--job-size 1 '
            '--decimals 3 '
            '--tolerance 0.1 '
            '--verbose '
            f'--out-mrds-path {tempdir}/test.mrds.pkl '
            f'--output-dir {tempdir}')
        if attributes['num_subjects'] < 2:
            with pytest.raises(RuntimeError):
                parallel_cli()
        else:
            parallel_cli()
            report_paths = list(Path(tempdir).glob('*.html'))
            # check if report was generated
            assert_paths_more_than_2_subjects(report_paths, tempdir, attributes,
                                              ds1)
    return


def assert_paths_more_than_2_subjects(report_paths, tempdir, attributes, ds1):
    if attributes['num_subjects'] > 2:
        assert_report_paths(report_paths, tempdir, attributes, ds1)
    else:
        assert not report_paths


def assert_report_paths(report_paths, tempdir, attributes, ds1):
    assert len(report_paths) > 0
    report_path = report_paths[0]
    assert str(report_path.parent) == str(tempdir)
    assert ds1.name in report_path.stem.split(DATE_SEPARATOR)[0]




@settings(max_examples=10, deadline=None)
@given(args=dcm_dataset_strategy)
def test_binary_monitor_with_reference_protocol(args):
    ds1, attributes = args
    assume(len(ds1.name) > 0)
    ds1.load()
    with tempfile.TemporaryDirectory() as tempdir:
        # shlex doesn't test work with binaries
        subprocess.run(['mrqa_monitor',
                        '--data-source', attributes['fake_ds_dir'],
                        '--config', attributes['config_path'],
                        '--name', ds1.name,
                        '--format', 'dicom',
                        '--decimals', '3',
                        '--tolerance', '0.1',
                        '--verbose',
                        '--ref-protocol-path', attributes['ref_protocol_path'],
                        '--output-dir', tempdir])
        report_paths = list(Path(tempdir).glob('*.html'))
        # check if report was generated
        assert len(report_paths) > 0
        report_path = report_paths[0]
        assert str(report_path.parent) == str(tempdir)
        assert ds1.name in report_path.stem.split(DATE_SEPARATOR)[0]
    return


def test_binary_subset():
    pass


@settings(max_examples=10, deadline=None)
@given(args=dcm_dataset_strategy)
def test_report_generated(args):
    ds1, attributes = args
    assume(len(ds1.name) > 0)
    ds1.load()
    with tempfile.TemporaryDirectory() as tempdir:
        sys.argv = shlex.split(f'mrqa --data-source {attributes["fake_ds_dir"]}'
                               f' --config {attributes["config_path"]}'
                               f' --name {ds1.name}'
                               f' --format dicom'
                               ' --decimals 3'
                               ' --tolerance 0.1'
                               ' --verbose'
                               f' --output-dir  {tempdir}')
        cli()
        report_paths = list(Path(tempdir).glob('*.html'))
        # check if report was generated
        assert_paths_more_than_2_subjects(report_paths, tempdir, attributes,
                                          ds1)
        # wait for 2 seconds, otherwise the next test will fail.
        # This happens if report is generated with the same timestamp, then
        # the number of reports will be 1 because the previous report will be
        # overwritten.
        sleep(2)
        # re-run with mrds pkl path
        mrds_paths = list(Path(tempdir).glob('*.mrds.pkl'))
        assert len(mrds_paths) > 0
        sys.argv = shlex.split(
            f'mrqa --data-source {attributes["fake_ds_dir"]} '
            f'--config {attributes["config_path"]} '
            f'--name {ds1.name} '
            f'--format dicom '
            '--decimals 3 '
            '--tolerance 0.1 '
            '--verbose '
            f'--output-dir  {tempdir} '
            f'--mrds-pkl-path  {mrds_paths[0]} ')
        cli()
        report_paths = list(Path(tempdir).glob('*.html'))
        # check if report was generated
        if attributes['num_subjects'] > 2:
            assert len(report_paths) > 1
            report_path = report_paths[0]
            assert str(report_path.parent) == str(tempdir)
            assert ds1.name in report_path.stem.split(DATE_SEPARATOR)[0]
        else:
            assert not report_paths
    return


if __name__ == '__main__':
    test_report_generated()
