import typing as tp
from pathlib import Path
from typing import Tuple

from MRdataset import DicomDataset
from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy

from mrQA.tests.simulate import make_compliant_test_dataset

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


@st.composite
def create_dataset(draw_from: st.DrawFn) -> Tuple:
    name, num_subjects, repetition_time, echo_train_length, flip_angle = draw_from(param_strategy)
    fake_ds_dir = make_compliant_test_dataset(num_subjects,
                                              repetition_time,
                                              echo_train_length,
                                              flip_angle)
    ds = DicomDataset(name=name,
                      data_source=fake_ds_dir,
                      config_path=THIS_DIR / 'resources/mri-config.json')
    attributes = {
        'name': name,
        'num_subjects': num_subjects,
        'repetition_time': repetition_time,
        'echo_train_length': echo_train_length,
        'flip_angle': flip_angle,
        'fake_ds_dir': fake_ds_dir,
        'config_path': THIS_DIR / 'resources/mri-config.json'
    }
    return ds, attributes


dcm_dataset_strategy: tp.Final[SearchStrategy[Tuple]] = create_dataset()
