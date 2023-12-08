import tempfile
from collections import defaultdict
from pathlib import Path

import hypothesis.strategies as st
from MRdataset import import_dataset
from hypothesis import given, settings, assume
from mrQA import check_compliance
from mrQA.tests.simulate import make_test_dataset
from mrQA.utils import get_config_from_file

THIS_DIR = Path(__file__).parent.resolve()


# @settings(max_examples=50, deadline=None)
# @given(st.integers(min_value=0, max_value=10),
#        st.floats(allow_nan=False,
#                  allow_infinity=False),
#        st.integers(min_value=-10000000, max_value=10000000),
#        st.floats(allow_nan=False,
#                  allow_infinity=False))
# def test_compliance_all_clean(num_subjects,
#                               repetition_time,
#                               echo_train_length,
#                               flip_angle):
#     """pass compliant datasets, and make sure library recognizes them as such"""
#     dest_dir = make_compliant_test_dataset(num_subjects,
#                                            repetition_time,
#                                            echo_train_length,
#                                            flip_angle)
#     fake_mrd_dataset = import_dataset(dest_dir, output_dir=tempdir)
#     checked_dataset = check_compliance(dataset=fake_mrd_dataset)
#
#     sub_names_by_modality = defaultdict(list)
#     for modality_pat in Path(dest_dir).iterdir():
#         if modality_pat.is_dir() and ('.mrdataset' not in str(modality_pat)):
#             for subject_path in modality_pat.iterdir():
#                 sub_names_by_modality[modality_pat.name].append(
#                     subject_path.name)
#
#     for modality in checked_dataset.modalities:
#         percent_compliant = len(modality.compliant_subject_names) / len(
#             modality.subjects)
#         assert percent_compliant == 1.
#
#         percent_non_compliant = len(modality.non_compliant_subject_names) / len(
#             modality.subjects)
#         assert percent_non_compliant == 0
#
#         assert len(modality.non_compliant_subject_names) == 0
#         assert len(modality.compliant_subject_names) == len(
#             modality._children.keys())
#
#         assert set(sub_names_by_modality[modality.name]) == set(
#             modality.compliant_subject_names)
#         assert len(modality.non_compliant_params) == 0
#
#         assert modality.compliant
#         assert not modality.is_multi_echo()
#
#         for subject in modality.subjects:
#             for session in subject.sessions:
#                 for run in session.runs:
#                     assert not run.delta
#                     assert run.params['tr'] == repetition_time
#                     assert run.params[
#                                'echo_train_length'] == echo_train_length
#                     assert run.params['flip_angle'] == flip_angle
#                     assert modality.reference[run.echo_time]['tr'] == \
#                            repetition_time
#                     assert modality.reference[run.echo_time][
#                                'echo_train_length'] == \
#                            echo_train_length
#                     assert modality.reference[run.echo_time][
#                                'flip_angle'] == flip_angle


def assert_list(list1, list2):
    if set(list1) == set(list2):
        if len(list1) == len(list2):
            return True
    return False


@settings(max_examples=5, deadline=None)
@given(st.lists(st.integers(min_value=0, max_value=3), min_size=15,
                max_size=15),
       st.floats(allow_nan=False, allow_infinity=False),
       st.integers(min_value=-10000000, max_value=10000000),
       st.floats(allow_nan=False, allow_infinity=False)
       )
def test_non_compliance(num_noncompliant_subjects,
                        repetition_time,
                        echo_train_length,
                        flip_angle):
    """pass non-compliant ds, and ensure library recognizes them as such"""
    with tempfile.TemporaryDirectory() as tempdir:
        assume(repetition_time != 200)
        assume(echo_train_length != 4000)
        assume(flip_angle != 80)

        fake_ds_dir, dataset_info = \
            make_test_dataset(num_noncompliant_subjects,
                              repetition_time,
                              echo_train_length,
                              flip_angle)
        mrd = import_dataset(fake_ds_dir,
                             config_path=THIS_DIR / 'resources/mri-config.json',
                             output_dir=tempdir)
        hz_audit_results, vt_audit_results, report_path = check_compliance(
            dataset=mrd,
            output_dir=tempdir,
            config_path=THIS_DIR / 'resources/mri-config.json')
        config_dict = get_config_from_file(
            THIS_DIR / 'resources/mri-config.json')
        # include_params = config_dict['include_parameters']
        stratify_by = config_dict.get('stratify_by', None)

        if hz_audit_results is not None:
            # Check on disk, basically the truth
            sub_names_by_modality = defaultdict(list)
            for modality_path in Path(fake_ds_dir).iterdir():
                if modality_path.is_dir() and (
                    '.mrdataset' not in str(modality_path)):
                    for subject_path in modality_path.iterdir():
                        sub_names_by_modality[modality_path.name].append(
                            subject_path.name)

            fully_compliant_ds = hz_audit_results['compliant']
            non_compliant_ds = hz_audit_results['non_compliant']
            reference = hz_audit_results['reference']
            non_compliant_sequences = non_compliant_ds.get_sequence_ids()
            # non_compliant_sequences = [f.split(ATTRIBUTE_SEPARATOR)[0] for f in non_compliant_sequences]
            fully_compliant_sequences = fully_compliant_ds.get_sequence_ids()
            # fully_compliant_sequences = [f.split(ATTRIBUTE_SEPARATOR)[0] for f in fully_compliant_sequences]
            all_sequences = mrd.get_sequence_ids()

            # Check if modalities are   equal
            all_modalities_on_disk = [m for m in sub_names_by_modality.keys() if
                                      not m.startswith('local')]
            non_compliant_modality_on_disk = [m for m in dataset_info if
                                              dataset_info[m] if
                                              not m.startswith('local')]
            compliant_modalities_on_disk = set(all_modalities_on_disk) - set(
                non_compliant_modality_on_disk)

            assert assert_list(all_modalities_on_disk, all_sequences)
            assert assert_list(non_compliant_sequences,
                               non_compliant_modality_on_disk)
            assert assert_list(fully_compliant_sequences,
                               compliant_modalities_on_disk)

            for seq_id in mrd.get_sequence_ids():
                all_subjects = mrd.get_subject_ids(seq_id)
                non_compliant_subjects = non_compliant_ds.get_subject_ids(
                    seq_id)
                compliant_subjects = set(all_subjects) - set(
                    non_compliant_subjects)
                # On diskplu
                all_subjects_on_disk = sub_names_by_modality[seq_id]
                non_compliant_subjects_on_disk = dataset_info[seq_id]
                compliant_subjects_on_disk = set(all_subjects_on_disk) - set(
                    non_compliant_subjects_on_disk)

                # What did you parse
                assert assert_list(all_subjects, all_subjects_on_disk)
                assert assert_list(non_compliant_subjects,
                                   non_compliant_subjects_on_disk)
                assert assert_list(compliant_subjects,
                                   compliant_subjects_on_disk)

                # Check if reference has the right values
                assert reference[seq_id]['RepetitionTime'].get_value() == 200
                assert reference[seq_id]['EchoTrainLength'].get_value() == 4000
                assert reference[seq_id]['FlipAngle'].get_value() == 80

                for subject, session, run, seq in non_compliant_ds.traverse_horizontal(
                    seq_id):
                    assert seq['RepetitionTime'].get_value() == repetition_time
                    assert seq[
                               'EchoTrainLength'].get_value() == echo_train_length
                    assert seq['FlipAngle'].get_value() == flip_angle

                for subject, session, run, seq in fully_compliant_ds.traverse_horizontal(
                    seq_id):
                    assert seq['RepetitionTime'].get_value() == 200
                    assert seq['EchoTrainLength'].get_value() == 4000
                    assert seq['FlipAngle'].get_value() == 80

    # @settings(max_examples=30, deadline=None)
    # @given(st.lists(st.integers(min_value=0, max_value=2), min_size=11,
    #                 max_size=11),
    #        st.floats(allow_nan=False, allow_infinity=False),
    #        st.floats(allow_nan=False, allow_infinity=False),
    #        st.floats(allow_nan=False, allow_infinity=False)
    #        )
    # def modify_test_non_compliance_bids(num_noncompliant_subjects,
    #                                     repetition_time,
    #                                     magnetic_field_strength,
    #                                     flip_angle):
    #     """pass non-compliant ds, and ensure library recognizes them as such"""
    #     assume(repetition_time != const_bids['tr'])
    #     assume(magnetic_field_strength != const_bids['b0'])
    #     assume(flip_angle != const_bids['fa'])
    #
    #     fake_dir, dataset_info = make_bids_test_dataset(num_noncompliant_subjects,
    #                                                     repetition_time,
    #                                                     magnetic_field_strength,
    #                                                     flip_angle)
    #     mrd = import_dataset(fake_dir, include_phantom=True, ds_format='bids')
    #     checked_dataset = check_compliance(dataset=mrd, output_dir=mrd.data_source)
    #
    #     # Check on disk, basically the truth
    #     layout = BIDSLayout(fake_dir)
    #     sub_names_by_modality = defaultdict(set)
    #     subjects = layout.get_subjects()

    # for modality in MRdataset.config.datatypes:
    #     for sub in subjects:
    #         filters = {'datatype': modality,
    #                    'subject': sub,
    #                    'extension': 'json'}
    #         files = layout.get(**filters)
    #         if files:
    #             sub_names_by_modality[modality].add(sub)
    #
    # # Check if modalities are equal
    # non_compliant_modality_names = [m for m in dataset_info if dataset_info[m]]
    # assert assert_list(sub_names_by_modality.keys(),
    #                    checked_dataset._children.keys())
    #
    # assert assert_list(checked_dataset.non_compliant_modality_names,
    #                    non_compliant_modality_names)
    #
    # assert assert_list(checked_dataset.compliant_modality_names,
    #                    set(checked_dataset._children.keys()) - set(
    #                        non_compliant_modality_names))
    #
    # for modality in checked_dataset.modalities:
    #     # GT
    #     all_subjects = sub_names_by_modality[modality.name]
    #     non_compliant_subjects = dataset_info[modality.name]
    #     compliant_subjects = set(all_subjects) - set(non_compliant_subjects)
    #
    #     # What did you parse
    #     assert assert_list(all_subjects, modality._children.keys())
    #     assert assert_list(non_compliant_subjects,
    #                        modality.non_compliant_subject_names)
    #     assert assert_list(compliant_subjects, modality.compliant_subject_names)
    #
    #     # Check if reference has the right values
    #     echo_times = modality.get_echo_times()
    #     for te in echo_times:
    #         reference = modality.get_reference(te)
    #         assert reference['RepetitionTime'] == const_bids['tr']
    #         assert reference['MagneticFieldStrength'] == const_bids['b0']
    #         assert reference['FlipAngle'] == const_bids['fa']
    #
    #     for subject in modality.subjects:
    #         for session in subject.sessions:
    #             for run in session.runs:
    #                 if run.delta:
    #                     assert run.params['RepetitionTime'] == repetition_time
    #                     assert run.params['MagneticFieldStrength'] == \
    #                            magnetic_field_strength
    #                     assert run.params['FlipAngle'] == flip_angle
    #                 else:
    #                     assert run.params['RepetitionTime'] == const_bids['tr']
    #                     assert run.params['MagneticFieldStrength'] == \
    #                            const_bids['b0']
    #                     assert run.params['FlipAngle'] == const_bids['fa']
