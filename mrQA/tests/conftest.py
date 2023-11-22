import tempfile
import typing as tp
from pathlib import Path
from typing import Tuple

from MRdataset import DicomDataset
from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy

from mrQA.tests.simulate import make_compliant_test_dataset
from mrQA.tests.utils import download

param_strategy: tp.Final[SearchStrategy[Tuple]] = st.tuples(
    st.text(min_size=1, max_size=10),
    st.integers(min_value=1, max_value=10),
    st.floats(allow_nan=False,
              allow_infinity=False),
    st.integers(min_value=-10000000, max_value=10000000),
    st.floats(allow_nan=False,
              allow_infinity=False)
)

THIS_DIR = Path(__file__).parent.resolve()


def sample_protocol():
    """Download a sample protocol from GitHub"""
    # Using an example XML file from the following GitHub repository
    # https://github.com/lrq3000/mri_protocol
    url = 'https://raw.githubusercontent.com/lrq3000/mri_protocol/master/SiemensVidaProtocol/Coma%20Science%20Group.xml'  # noqa
    filename = THIS_DIR / 'coma_science.xml'
    xml_file = Path(filename)

    if not xml_file.is_file():
        download(url, filename)
    return filename


@st.composite
def create_dataset(draw_from: st.DrawFn) -> Tuple:
    name, num_subjects, repetition_time, echo_train_length, flip_angle = draw_from(param_strategy)
    fake_ds_dir = make_compliant_test_dataset(num_subjects,
                                              repetition_time,
                                              echo_train_length,
                                              flip_angle)
    temp_dir = Path(tempfile.mkdtemp())
    ds = DicomDataset(name=name,
                      data_source=fake_ds_dir,
                      config_path=THIS_DIR / 'resources/mri-config.json',
                      output_dir=temp_dir)
    ref_protocol_path = sample_protocol()
    attributes = {
        'name': name,
        'num_subjects': num_subjects,
        'repetition_time': repetition_time,
        'echo_train_length': echo_train_length,
        'flip_angle': flip_angle,
        'fake_ds_dir': fake_ds_dir,
        'config_path': THIS_DIR / 'resources/mri-config.json',
        'ref_protocol_path': ref_protocol_path,
    }
    return ds, attributes


dcm_dataset_strategy: tp.Final[SearchStrategy[Tuple]] = create_dataset()
