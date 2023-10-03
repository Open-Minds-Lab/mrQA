import subprocess
import tempfile
from pathlib import Path

from MRdataset.tests.simulate import make_compliant_test_dataset
from hypothesis import given, settings, assume

from MRdataset import load_mr_dataset, import_dataset
from MRdataset.tests.conftest import dcm_dataset_strategy

from mrQA import logger
from mrQA.config import DATE_SEPARATOR


@settings(max_examples=50, deadline=None)
@given(args=(dcm_dataset_strategy))
def test_report_generated(args):
    ds1, attributes = args
    assume(len(ds1.name) > 0)
    ds1.load()
    with tempfile.TemporaryDirectory() as tempdir:
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
            report_path = report_paths[0]
            assert str(report_path.parent) == str(tempdir)
            assert ds1.name in report_path.stem.split(DATE_SEPARATOR)[0]
        else:
            assert not report_paths
    return

