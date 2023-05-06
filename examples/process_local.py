from mrQA.run_parallel import process_parallel
from pathlib import Path
from MRdataset.config import MRDS_EXT

DATA_ROOT = Path('/media/sinhah/extremessd/ABCD-375/dicom-baseline')
OUTPUT_DIR = DATA_ROOT.parent  / (DATA_ROOT.stem + '_mrqa_files')
name = 'abcd-375'
output_path = OUTPUT_DIR / f'{name}{MRDS_EXT}'

process_parallel(DATA_ROOT, OUTPUT_DIR, output_path, name)
