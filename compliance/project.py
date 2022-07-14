from pathlib import Path
from typing import Union

from MRdataset.base import Project
from MRdataset.utils import param_difference

from compliance.formatter import HtmlFormatter
from compliance.utils import timestamp, majority_attribute_values


def check_compliance(dataset: Project,
                     strategy: str = 'majority',
                     output_dir: Union[Path, str] = None,
                     reference_path: Union[Path, str] = None) -> Project:
    """
    @param dataset
    @param output_dir
    @param strategy
    @param reference_path
    """
    if strategy == 'majority':
        dataset = compare_with_majority(dataset)
    else:
        raise NotImplementedError
    generate_report(dataset, output_dir)

    return dataset


def compare_with_majority(dataset: "Project") -> Project:
    """
    Method checking compliance by first inferring the reference protocol/values,
    and then identifying deviations
    @param dataset

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
        flag_modality = True
        for subject in modality.subjects:
            flag_subject = True
            for session in subject.sessions:
                for run in session.runs:
                    reference = modality.get_reference(run.echo_time)
                    run.delta = param_difference(run.params,
                                                 reference,
                                                 ignore=['modality'])
                    if run.delta:
                        modality.add_non_compliant_subject_name(subject.name)
                        dataset.add_non_compliant_modality_name(modality.name)
                        reasons = extract_reasons(run.delta)
                        modality.reasons_non_compliance.update(reasons)

                        flag_subject = False
                        flag_modality = False
            if flag_subject:
                modality.add_compliant_subject_name(subject.name)
        if flag_modality:
            modality.compliant = flag_modality
            dataset.add_compliant_modality_name(modality.name)
    return dataset


def extract_reasons(data):
    return list(zip(*data))[1]


def generate_report(dataset: Project, output_dir: Union[Path, str]) -> None:
    if output_dir is None:
        output_dir = dataset.data_root
    out_path = Path(output_dir) / '{}_{}.html'.format(dataset.name, timestamp())
    HtmlFormatter(filepath=out_path, params=dataset)
