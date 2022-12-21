from mrQA.run_parallel import parallel_dataset
from pathlib import Path
from MRdataset.config import MRDS_EXT

DATA_ROOT = Path('/media/sinhah/extremessd/ABCD-375/dicom-baseline')
OUTPUT_DIR = DATA_ROOT.parent  / (DATA_ROOT.stem + '_mrqa_files')
name = 'abcd-375'
output_path = OUTPUT_DIR / f'{name}{MRDS_EXT}'

parallel_dataset(DATA_ROOT, OUTPUT_DIR, output_path, name)
