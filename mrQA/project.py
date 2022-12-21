from itertools import groupby
from pathlib import Path
from typing import Union

from MRdataset.base import Project
from MRdataset.utils import param_difference, is_hashable

from mrQA.config import STRATEGIES_ALLOWED
from mrQA.formatter import HtmlFormatter
from mrQA.utils import timestamp, majority_attribute_values


def check_compliance(dataset: Project,
                     strategy: str = 'majority',
                     output_dir: Union[Path, str] = None):
    """
    Main function for checking compliance. Infers the reference protocol
    according to the user chosen strategy, and then generates a compliance
    report

    Parameters
    ----------
    dataset : Project
        Project instance for the dataset which is to be checked for compliance

    strategy : str
        Strategy employed to specify or automatically infer the
        reference protocol. Allowed options are 'majority'

    output_dir: Union[Path, str]
        Path to save the report

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the input dataset is empty or otherwise invalid

    """

    if not dataset.modalities:
        raise ValueError("Dataset is empty.")

    if strategy == 'majority':
        dataset = compare_with_majority(dataset)
    else:
        raise NotImplementedError(
            'Only the following strategies are allowed : \n\t'
            '{}'.format(STRATEGIES_ALLOWED))

    generate_report(dataset, output_dir)


def _get_runs_by_echo(modality):
    runs_in_modality = []
    for subject in modality.subjects:
        for session in subject.sessions:
            runs_in_modality.extend(session.runs)

    def _sort_key(run):
        return run.echo_time

    runs_in_modality = sorted(runs_in_modality, key=_sort_key)
    runs_by_te = {te: run.params for te, run in groupby(runs_in_modality,
                                                        key=_sort_key)}
    return runs_by_te


def _check_against_reference(modality):
    for subject in modality.subjects:
        for session in subject.sessions:
            for run in session.runs:
                reference = modality.get_reference(run.echo_time)
                run.delta = param_difference(run.params,
                                             reference,
                                             ignore=['modality',
                                                     'phase_encoding_direction'])
                if run.delta:
                    modality.add_non_compliant_subject_name(subject.name)
                    store(modality, run, subject.name, session.name)
                    # If any of the runs are non-compliant, then the
                    # session is non-compliant.
                    session.compliant = False
                    # If any of the sessions are non-compliant, then the
                    # subject is non-compliant.
                    subject.compliant = False
                    # If any of the subjects are non-compliant, then the
                    # modality is non-compliant.
                    modality.compliant = False
                    # If none of the subjects or modalities are found to
                    # be non-compliant, flag will remain True, after the
                    # loop is finished.
            if session.compliant:
                # If after all the runs, session is compliant, then the
                # session is added to the list of compliant sessions.
                subject.add_compliant_session_name(session.name)
        if subject.compliant:
            # If after all the sessions, subject is compliant, then the
            # subject is added to the list of compliant subjects.
            modality.add_compliant_subject_name(subject.name)
    # If after all the subjects, modality is compliant, then the
    # modality should be added to the list of compliant sessions.
    return modality.compliant


def compare_with_majority(dataset: "Project") -> Project:
    """
    Method for post-acquisition compliance. Infers the reference protocol/values
    by looking for the most frequent values, and then identifying deviations

    Parameters
    ----------
    dataset : Project
        MRdataset.base.Project instance for the dataset which is to be checked
        for compliance

    Returns
    -------
    dataset : Project
        Adds the non-compliance information to the same Project instance and
        returns it.
    """
    # TODO: Check for subset, if incomplete dataset throw error and stop

    for modality in dataset.modalities:
        # Infer reference protocol for each echo_time
        run_by_echo = _get_runs_by_echo(modality)

        # For each echo time, find the most common values
        for echo_time in run_by_echo.keys():
            reference = majority_attribute_values(run_by_echo[echo_time])
            modality.set_reference(reference, echo_time)

        modality.compliant = _check_against_reference(modality)
        if modality.compliant:
            dataset.add_compliant_modality_name(modality.name)

    # As we are updating the same dataset by adding non-compliant subject names,
    # and non-compliant modality names, we can return the same dataset
    return dataset


def store(modality, run, subject_name, session_name):
    """
    Store the sources of non-compliance like flip angle, ped, tr, te

    Parameters
    ----------
    modality : MRdataset.base.Modality
        The modality node, in which these sources of non-compliance were found
        so that these values can be stored
    run : MRdataset.base.Run
        Non-compliant which was found to be non-compliant w.r.t. the reference
    subject_name : str
        Non-compliant subject's name
    session_name : str
        Non-compliant session name
    """
    for entry in run.delta:
        if entry[0] != 'change':
            continue
        _, parameter, [new_value, ref_value] = entry

        if not is_hashable(parameter):
            parameter = str(parameter)

        modality.update(parameter, run.echo_time, ref_value, new_value,
                        '{}_{}'.format(subject_name, session_name))


def generate_report(dataset: Project, output_dir: Union[Path, str]) -> None:
    """
    Generates an HTML report aggregating and summarizing the non-compliance
    discovered in the dataset.

    Parameters
    ----------
    dataset : Project
        MRdataset.base.Project instance for the dataset which is to be checked
    output_dir : Union[Path, str]
        Directory in which the generated report should be stored.
    """
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if not Path(output_dir).is_dir():
        raise OSError('Expected valid output_directory, '
                      'Got {0}'.format(output_dir))
    filename = '{}_{}.html'.format(dataset.name, timestamp())
    # Generate the HTML report and save it to the output_path
    output_path = output_dir / filename
    HtmlFormatter(filepath=output_path, params=dataset)
    # Print a small message on the console, about non-compliance of dataset
    print(otg_report(dataset, filename))


def otg_report(dataset, report_name):
    """
    On-the-Go report generator.
    Generate a single line report for the dataset

    Parameters
    ----------
    dataset : Project
        MRdataset.base.Project instance for the dataset which is to be checked
    report_name : str
        Filename for the report

    Returns
    -------

    """
    result = {}
    # For all the modalities calculate the percent of non-compliance
    for modality in dataset.modalities:
        percent_non_compliant = len(modality.non_compliant_subject_names) \
                                / len(modality.subjects)
        if percent_non_compliant > 0:
            result[modality.name] = str(100 * percent_non_compliant)
    # Format the result as a string
    if result:
        ret_string = 'In {0} dataset, modalities "{1}" are non-compliant. ' \
                     'See {2} for report'.format(dataset.name,
                                                 ", ".join(result.keys()),
                                                 report_name)
    else:
        ret_string = 'In {0} dataset, all modalities are compliant. ' \
                     'See {1} for report'.format(dataset.name,
                                                 report_name)
    return ret_string
