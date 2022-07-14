import shutil
import hypothesis.strategies as st
from hypothesis import given, settings, assume

from MRdataset import import_dataset
from MRdataset.simulate import make_compliant_test_dataset, \
    make_non_compliant_test_dataset
from compliance import check_compliance
from pathlib import Path
from collections import defaultdict


@settings(max_examples=50, deadline=None)
@given(st.integers(min_value=0, max_value=10),
       st.floats(allow_nan=False,
                 allow_infinity=False),
       st.integers(min_value=-10000000, max_value=10000000),
       st.floats(allow_nan=False,
                 allow_infinity=False))
def test_compliance_all_clean(num_subjects,
                              repetition_time,
                              echo_train_length,
                              flip_angle):
    """pass compliant datasets, and make sure library recognizes them as such"""
    dest_dir = make_compliant_test_dataset(num_subjects,
                                           repetition_time,
                                           echo_train_length,
                                           flip_angle)
    fake_mrd_dataset = import_dataset(dest_dir, include_phantom=True)
    checked_dataset = check_compliance(dataset=fake_mrd_dataset)

    sub_names_by_modality = defaultdict(list)
    for subject_path in Path(dest_dir).iterdir():
        if subject_path.is_dir() and ('.mrdataset' not in str(subject_path)):
            for modality_path in subject_path.iterdir():
                sub_names_by_modality[modality_path.name].append(subject_path.name)

    for modality in checked_dataset.modalities:
        percent_compliant = len(modality.compliant_subject_names) / len(
            modality.subjects)
        assert percent_compliant == 1.

        percent_non_compliant = len(modality.non_compliant_subject_names) / len(
            modality.subjects)
        assert percent_non_compliant == 0

        assert len(modality.non_compliant_subject_names) == 0
        assert len(modality.compliant_subject_names) == len(
            modality._children.keys())

        assert set(sub_names_by_modality[modality.name]) == set(
            modality.compliant_subject_names)
        assert len(modality.reasons_non_compliance) == 0

        assert modality.compliant
        assert not modality.is_multi_echo()

        for subject in modality.subjects:
            for session in subject.sessions:
                for run in session.runs:
                    assert not run.delta
                    assert run.params['tr'] == repetition_time
                    assert run.params[
                               'echo_train_length'] == echo_train_length
                    assert run.params['flip_angle'] == flip_angle
                    assert modality.reference[run.echo_time]['tr'] == \
                           repetition_time
                    assert modality.reference[run.echo_time]['echo_train_length'] == \
                           echo_train_length
                    assert modality.reference[run.echo_time]['flip_angle'] == flip_angle


# def test_non_compliance():
#     """pass non-compliant ds, and ensure library recognizes them as such"""
#
#     # level of chceks
#     # yes or no; specifc parameters

if __name__ == '__main__':
    test_compliance_all_clean(5, 0.0, echo_train_length=0, flip_angle=0)
