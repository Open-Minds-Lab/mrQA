from pathlib import Path


DATASET_PATHS = [
    # ['/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/', 0, 200],
    # ['/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/', 1, 10, 1],
    # ['/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/', 2, 100, 1],
    # ['/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/', 3, 200, 1],
    # ['/home/sinhah/scan_data/WPC-7807', 2, 50, 1],
    # ['/home/sinhah/scan_data/WPC-7761', 2, 50, 1],
    # ['/home/sinhah/scan_data/sinhah-20220514_140054', 3, 200, 1],
    # ['/home/sinhah/scan_data/sinhah-20220520_153204', 3, 200, 1],
    # ['/home/sinhah/scan_data/sinhah-20220520_210659', 3, 200, 1],
    ['/media/sinhah/extremessd/ABCD-375/dicom-baseline/', 5, 500],
]

# DATA_ROOT = Path('/media/sinhah/extremessd/ABCD/active_series/non-recommended/')
DATA_ROOT = Path('/ocean/projects/med220005p/sinhah/ABCD/active_series/non-recommended') # noqa
ABCD_DATASET_PATHS = [
    [DATA_ROOT/'t1w', 3, 1],
]


const_bids = {
    'tr': 2.0,
    'b0': 3.0,
    'fa': 80.0
}

const_xnat = {
    'tr': 200,
    'etl': 4000,
    'fa': 80
}
