import shlex
import subprocess
import sys
import tempfile
from pathlib import Path
from time import sleep

from hypothesis import given, settings, assume

from mrQA.cli import cli
from mrQA.config import DATE_SEPARATOR
from mrQA.tests.conftest import dcm_dataset_strategy


@settings(max_examples=5, deadline=None)
@given(args=(dcm_dataset_strategy))
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
        if attributes['num_subjects'] > 2:
            assert len(report_paths) > 0
            report_path = report_paths[0]
            assert str(report_path.parent) == str(tempdir)
            assert ds1.name in report_path.stem.split(DATE_SEPARATOR)[0]
        else:
            assert not report_paths
    return


def test_binary_parallel():
    pass


def test_binary_monitor():
    pass


def test_binary_subset():
    pass


@settings(max_examples=10, deadline=None)
@given(args=(dcm_dataset_strategy))
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
        if attributes['num_subjects'] > 2:
            assert len(report_paths) > 0
            report_path = report_paths[0]
            assert str(report_path.parent) == str(tempdir)
            assert ds1.name in report_path.stem.split(DATE_SEPARATOR)[0]
        else:
            assert not report_paths
        # wait for 2 seconds, otherwise the next test will fail.
        # This happens if report is generated with the same timestamp, then
        # the number of reports will be 1 because the previous report will be
        # overwritten.
        sleep(2)
        # re-run with mrds pkl path
        mrds_paths = list(Path(tempdir).glob('*.mrds.pkl'))
        assert len(mrds_paths) > 0
        sys.argv = shlex.split(f'mrqa --data-source {attributes["fake_ds_dir"]} '
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
