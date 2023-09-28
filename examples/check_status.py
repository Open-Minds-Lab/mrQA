from pathlib import Path
from mrQA import monitor
import tempfile
import shutil

from mrQA.tests.utils import copy2dest


def run(folder_path):
    folder_path = Path(folder_path).resolve()
    config_path = Path('./mri-config.json').resolve()
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
                copy2dest(folder, folder_path,  input_dir)

                # Run monitor on the temporary folder
                monitor(name='dummy_dataset',
                        data_source=input_dir,
                        output_dir=output_dir,
                        decimals=2,
                        config_path=config_path,
                        verbose=False,
                        reference_path='./wpc-6106.xml'
                        )
        copy2dest(output_dir, tmpdirname, '/tmp')
        print('simulation-over')


run('/home/sinhah/scan_data/WPC-6106')
