import tempfile
from pathlib import Path

from mrQA import monitor, logger
from mrQA.config import status_fpath
from mrQA.tests.conftest import THIS_DIR
from mrQA.tests.simulate import copy2dest
from mrQA.utils import send_email


# @settings(max_examples=10, deadline=None)
# @given(args=dcm_dataset_strategy)
def run(folder_path):  # args):
    # ds1, attributes = args
    # assume(attributes['num_subjects'] > 4)
    # folder_path = attributes['fake_ds_dir']
    folder_path = Path(folder_path).resolve()
    # config_path = attributes['config_path']
    config_path = THIS_DIR / 'resources/mri-config.json'
    email_config_path = THIS_DIR / 'resources/email_config.json'
    # make a temporary output folder using tempfile
    with tempfile.TemporaryDirectory() as tmpdirname:
        output_dir = Path(tmpdirname) / 'output'
        input_dir = Path(tmpdirname) / 'input'
        output_dir.mkdir(exist_ok=True, parents=True)
        input_dir.mkdir(exist_ok=True, parents=True)
        i = 0
        # copy a folder from folder_path to tmpdirname
        for folder in folder_path.iterdir():
            if folder.is_dir():
                copy2dest(folder, folder_path, input_dir)

                # Run monitor on the temporary folder
                hz_flag, vt_flag, report_path = monitor(name='dummy_dataset',
                                                        data_source=input_dir,
                                                        output_dir=output_dir,
                                                        decimals=2,
                                                        config_path=config_path,
                                                        verbose=False,
                                                        )
                if hz_flag:
                    log_fpath = status_fpath(output_dir, audit='hz')
                    logger.info(f"Non-compliant scans found for dummy_dataset")
                    logger.info(f"Check {log_fpath} for horizontal audit")
                    send_email(log_fpath, project_code='dummy_dataset', report_path=report_path,
                               email_config=email_config_path)
        # copy2dest(output_dir, tmpdirname, '/tmp')
        print('simulation-over')


run('/home/sinhah/scan_data/WPC-6106')
