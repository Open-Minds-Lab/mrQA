from pathlib import Path

from MRdataset.utils import param_difference
from compliance.formatter import HtmlFormatter
from compliance.utils import timestamp, majority_attribute_values


def check_compliance(dataset,
                     strategy='first',
                     output_dir=None,
                     reference_path=None,
                     reindex=False,
                     verbose=False):
    dataset = compare_with_majority(dataset)
    dataset = partition_by_compliance(dataset)
    generate_report(dataset, output_dir)


# def construct_tree(dataset, root_node):
#     for mode in dataset.modalities:
#         temp_name = '_'.join(mode.split('_')[1:])
#         modality = exists(temp_name, root_node)
#         reloaded = False
#         if modality is None:
#             modality = ModalityNode(path=None)
#             modality.name = temp_name
#         else:
#             reloaded = True
#
#         for subject_id in dataset.modalities[mode]:
#             has_multi_series = len(dataset[subject_id, mode].keys()) > 1
#             for series in dataset[subject_id][mode]:
#                 files = dataset[subject_id][mode][series]
#                 if len(files) == 0:
#                     run_node = SubjectNode(path=None)
#                     run_node.error = True
#                     warnings.warn("Expected at least 1 .dcm files. "
#                                   "Got 0 for Subject : {0} and Modality : {1}"
#                                   "".format(subject_id, mode), stacklevel=2)
#                     # Don't insert this run. Reject it
#                     continue
#                 elif len(files) == 1:
#                     run_node = parse(files[0])
#                     run_node.error = False
#                     if has_multi_series:
#                         run_node.name = '_'.join([subject_id, mode.split('_')[0], series[-2:]])
#                     else:
#                         run_node.name = '_'.join([subject_id, mode.split('_')[0]])
#                     modality.add_subject(run_node)
#                 else:
#                     run_node = parse(files[0])
#                     flag = False
#                     for f in files[1:]:
#                         temp_node = parse(f)
#                         difference = list(dict_diff(dict(run_node.params),
#                                                     dict(temp_node.params)))
#                         if difference:
#                             flag = True
#                             warnings.warn("Expected all .dcm files to have same parameters. "
#                                           "Got different values in Subject : {0}"
#                                           " Modality : {1}".format(subject_id, mode),
#                                           stacklevel=2)
#                             break
#                     run_node.error = flag
#                     run_node.path = Path(files[0]).parent
#                     if has_multi_series:
#                         run_node.name = '_'.join([subject_id, mode.split('_')[0], series[-2:]])
#                     else:
#                         run_node.name = '_'.join([subject_id, mode.split('_')[0]])
#                     # if not run_node.error:
#                     modality.add_subject(run_node)
#         if not reloaded:
#             root_node.add_subject(modality)
#     return root_node


# def partition_sessions_by_first(root_node):
#     for mode in root_node.subjects:
#         i = 0
#         anchor = None
#         if count_zero_children(mode):
#             continue
#         for i, c in enumerate(mode.subjects):
#             if not c.error:
#                 anchor = c
#                 break
#         mode.params = anchor.params.copy()
#         for sub in mode.subjects[i:]:
#             sub.delta = param_diff(sub, anchor)
#             if sub.delta:
#                 sub.fully_compliant = False
#                 mode.non_compliant.append(sub.name)
#             else:
#                 sub.fully_compliant = True
#                 mode.compliant.append(sub.name)
#     return root_node


# def param_diff(sub_a, sub_b):
#     """"""
#
#     return list(dict_diff(dict(sub_a.params), dict(sub_b.params), ignore={'modality'}))


# def count_zero_components(node):
#     """
#     ModalityNode has zero subjects
#     """
#     if len(node.subjects) == 0:
#         warnings.warn("No fully_compliant runs found for node : {0}".format(node.name),
#                       stacklevel=2)
#         return True
#     return False


def compare_with_majority(dataset):
    """Method checking compliance by first inferring the reference protocol/values, and
    then identifying deviations

    """

    for modality in dataset.modalities:
        # Calculate reference for comparing
        run_by_echo = dict()
        for subject in modality.subjects:
            for session in subject.sessions:
                for run in session.runs:
                    # Use defaultdict instead?
                    if run.echo_time not in run_by_echo.keys():
                        run_by_echo[run.echo_time] = []
                    run_by_echo[run.echo_time].append(run.params)

        for echo_time in run_by_echo.keys():
            reference = majority_attribute_values(run_by_echo[echo_time])
            modality.set_reference(reference, echo_time)

        # Start calculating delta for each run
        for subject in modality.subjects:
            for session in subject.sessions:
                for run in session.runs:
                    reference = modality.get_reference(run.echo_time)
                    run.delta = param_difference(run.params, reference)
                    if run.delta:
                        session.compliant = False
                        subject.compliant = False
                        modality.compliant = False

                # Run.delta was always empty, so session.compliant was never set
                if session.compliant is None:
                    session.compliant = True
            if subject.compliant is None:
                subject.compliant = True
        if modality.compliant is None:
            modality.compliant = True
    return dataset


def partition_by_compliance(dataset):
    for modality in dataset.modalities:
        if modality.compliant:
            dataset.add_compliant_modality(modality.name)
        else:
            dataset.add_non_compliant_modality(modality.name)
        for subject in modality.subjects:
            if subject.compliant:
                modality.add_compliant_subject(subject.name)
            else:
                modality.add_non_compliant_subject(subject.name)
            for session in subject.sessions:
                if session.compliant:
                    subject.add_compliant_session(session.name)
                else:
                    subject.add_non_compliant_session(session.name)
    return dataset


def generate_report(dataset, output_dir):
    if output_dir is None:
        output_dir = dataset.data_root
    out_path = Path(output_dir) / '{}_{}.html'.format(dataset.name, timestamp())
    HtmlFormatter(filepath=out_path, params=dataset)


# def create_report(dataset=None,
#                   strategy='first',
#                   output_dir=None,
#                   reference_path=None,
#                   reindex=False,
#                   verbose=False):
#     if output_dir is None:
#         output_dir = dataset.data_root
#
#     check_compliance(dataset=dataset,
#                      strategy=strategy,
#                      output_dir=output_dir,
#                      reference_path=reference_path,
#                      reindex=reindex,
#                      verbose=verbose)
