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
    generate_report(dataset, output_dir)


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
        flag = True
        for subject in modality.subjects:
            for session in subject.sessions:
                for run in session.runs:
                    reference = modality.get_reference(run.echo_time)
                    run.delta = param_difference(run.params, reference, ignore_params=['modality'])
                    if run.delta:
                        subject.add_non_compliant_session(session.name)
                        modality.add_non_compliant_subject(subject.name)
                        dataset.add_non_compliant_modality(modality.name)
                        flag = False
                if flag:
                    subject.add_compliant_session(session.name)
            if flag:
                modality.add_compliant_subject(subject.name)
        if flag:
            modality.compliant = flag
            dataset.add_compliant_modality(modality.name)
    return dataset


def generate_report(dataset, output_dir):
    if output_dir is None:
        output_dir = dataset.data_root
    out_path = Path(output_dir) / '{}_{}.html'.format(dataset.name, timestamp())
    HtmlFormatter(filepath=out_path, params=dataset)
