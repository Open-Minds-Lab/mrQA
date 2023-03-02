
DATASET_PATHS = [
    ('/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/', 0, 1000),
    ('/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/', 1, 1000),
    ('/home/sinhah/scan_data/CHA_MJFF', 5, 1000),
    ('/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/', 5, 1000),
    ('/home/sinhah/scan_data/CHA_MJFF', 10, 1000),
    ('/media/sinhah/extremessd/ABCD-375/dicom-baseline-subset/', 10, 1000),
    ('/media/sinhah/extremessd/ABCD-375/dicom-baseline/', 10, 1000),
]

DATA_ROOT = Path('/media/sinhah/extremessd/ABCD/active_series/non-recommended/')
# DATA_ROOT = Path('/ocean/projects/med220005p/sinhah/ABCD/active_series/non-recommended') # noqa
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
