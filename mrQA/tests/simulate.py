import tempfile
import zipfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pydicom
from pydicom import dcmread

from mrQA.utils import convert2ascii


def make_test_dataset(num_noncompliant_subjects,
                      repetition_time,
                      echo_train_length,
                      flip_angle):
    src_dir, dest_dir = setup_directories(compliant_dicom_dataset())  # noqa
    dataset_info = defaultdict(set)
    modalities = [s.name for s in src_dir.iterdir() if (s.is_dir() and
                                                        'mrdataset' not in
                                                        s.name)]
    for i, modality in enumerate(modalities):
        subject_paths = [s for s in (src_dir / modality).iterdir()]
        for sub_path in subject_paths:
            for filepath in sub_path.glob('*.dcm'):
                dicom = pydicom.read_file(filepath)
                export_file(dicom, filepath, dest_dir)

    for i, modality in enumerate(modalities):
        count = num_noncompliant_subjects[i]
        subject_paths = [s for s in (src_dir / modality).iterdir()]

        for j in range(count):
            sub_path = subject_paths[j]
            for filepath in sub_path.glob('**/*.dcm'):
                dicom = pydicom.read_file(filepath)
                patient_id = str(dicom.get('PatientID', None))
                dicom.RepetitionTime = repetition_time
                dicom.EchoTrainLength = echo_train_length
                dicom.FlipAngle = flip_angle
                export_file(dicom, filepath, dest_dir)
                modality = dicom.get('SeriesDescription', None).replace(' ',
                                                                        '_')
                dataset_info[modality].add(patient_id)

    return dest_dir, dataset_info


def export_file(dicom, filepath, out_dir):
    patient_id = dicom.get('PatientID', None)
    series_desc = dicom.get('SeriesDescription', None)
    series_number = dicom.get('SeriesNumber', None)
    series_desc = convert2ascii(
        series_desc.replace(' ', '_'))  # + '_' + str(series_number)
    output_path = out_dir / series_desc / patient_id
    number = dicom.get('InstanceNumber', None)
    output_path.mkdir(exist_ok=True, parents=True)
    filename = f'{patient_id}_{number}.dcm'
    dicom.save_as(output_path / filename)


def make_compliant_test_dataset(num_subjects,
                                repetition_time,
                                echo_train_length,
                                flip_angle) -> Path:
    src_dir, dest_dir = setup_directories(sample_dicom_dataset())
    dcm_list = list(src_dir.glob('**/*.dcm'))

    subject_names = set()
    i = 0
    while len(subject_names) < num_subjects:
        filepath = dcm_list[i]
        dicom = pydicom.read_file(filepath)

        dicom.RepetitionTime = repetition_time
        dicom.EchoTrainLength = echo_train_length
        dicom.FlipAngle = flip_angle

        export_file(dicom, filepath, dest_dir)
        subject_names.add(dicom.get('PatientID', None))
        i += 1
    return dest_dir


THIS_DIR = Path(__file__).parent.resolve()


def sample_dicom_dataset(tmp_path='/tmp'):
    DATA_ARCHIVE = THIS_DIR / 'resources/example_dicom_data.zip'
    DATA_ROOT = Path(tmp_path)
    output_dir = DATA_ROOT / 'example_dicom_data'
    if not output_dir.exists():
        with zipfile.ZipFile(DATA_ARCHIVE, 'r') as zip_ref:
            zip_ref.extractall(DATA_ROOT)
    return DATA_ROOT / 'example_dicom_data'


def compliant_dicom_dataset(tmp_path='/tmp'):
    DATA_ARCHIVE = THIS_DIR / 'resources/compliant_dicom_data.zip'
    DATA_ROOT = Path(tmp_path)
    output_dir = DATA_ROOT / 'compliant_dicom_data'
    if not output_dir.exists():
        with zipfile.ZipFile(DATA_ARCHIVE, 'r') as zip_ref:
            zip_ref.extractall(DATA_ROOT)
    return DATA_ROOT / 'compliant_dicom_data'


def setup_directories(src):
    src_dir = Path(src).resolve()
    if not src_dir.exists():
        print(src_dir)
        raise FileNotFoundError("Source Directory {} not found".format(src_dir))

    temp_dir = tempfile.mkdtemp()
    dest_dir = Path(temp_dir).resolve()
    if not dest_dir.exists():
        raise FileNotFoundError("Temporary directory not found")

    return src_dir, dest_dir


def copy2dest(folder, src, dest):
    file_list = []
    date = datetime.now()
    for file in folder.rglob('*'):
        if file.is_file():
            try:
                dicom = dcmread(file)
            except:
                continue
            dicom.ContentDate = date.strftime('%Y%m%d')
            rel_path = file.relative_to(src)
            new_abs_path = dest / rel_path
            parent = new_abs_path.parent
            parent.mkdir(exist_ok=True, parents=True)
            dicom.save_as(new_abs_path)
            file_list.append(file)
    return file_list
