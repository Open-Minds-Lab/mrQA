from pathlib import Path
from typing import Union

from MRdataset.base import BaseDataset

from mrQA.config import STRATEGIES_ALLOWED
from mrQA.formatter import HtmlFormatter
from mrQA.utils import timestamp, majority_attribute_values, _get_runs_by_echo, \
    _check_against_reference, _cli_report, _validate_reference, subject_list2txt


def check_compliance(dataset: BaseDataset,
                     strategy: str = 'majority',
                     decimals: int = 3,
                     output_dir: Union[Path, str] = None):
    """
    Main function for checking compliance. Infers the reference protocol
    according to the user chosen strategy, and then generates a compliance
    report

    Parameters
    ----------
    dataset : BaseDataset
        BaseDataset instance for the dataset to be checked for compliance

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
        dataset = compare_with_majority(dataset, decimals)
    else:
        raise NotImplementedError(
            'Only the following strategies are allowed : \n\t'
            '{}'.format(STRATEGIES_ALLOWED))

    report_path = generate_report(dataset, output_dir)
    return report_path


def compare_with_majority(dataset: "BaseDataset",
                          decimals: int = 3) -> BaseDataset:
    """
    Method for post-acquisition compliance. Infers the reference protocol/values
    by looking for the most frequent values, and then identifying deviations

    Parameters
    ----------
    dataset : BaseDataset
        BaseDataset instance for the dataset which is to be checked
        for compliance

    Returns
    -------
    dataset : BaseDataset
        Adds the non-compliance information to the same BaseDataset instance and
        returns it.
    """
    # TODO: Check for subset, if incomplete dataset throw error and stop

    for modality in dataset.modalities:
        # Infer reference protocol for each echo_time
        run_by_echo = _get_runs_by_echo(modality, decimals)

        # For each echo time, find the most common values
        for echo_time in run_by_echo.keys():
            reference = majority_attribute_values(run_by_echo[echo_time])
            if _validate_reference(reference):
                modality.set_reference(reference, echo_time)

        modality.compliant = _check_against_reference(modality, decimals)
        if modality.compliant:
            dataset.add_compliant_modality_name(modality.name)

    # As we are updating the same dataset by adding non-compliant subject names,
    # and non-compliant modality names, we can return the same dataset
    return dataset


def generate_report(dataset: BaseDataset, output_dir: Union[Path, str]):
    """
    Generates an HTML report aggregating and summarizing the non-compliance
    discovered in the dataset.

    Parameters
    ----------
    dataset : BaseDataset
        BaseDataset instance for the dataset which is to be checked
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
    subject_list_dir = output_dir / 'subject_lists'
    subjectlist_files = subject_list2txt(dataset, subject_list_dir)
    args = {
        'ds': dataset,
        'subject_list': subjectlist_files
    }
    HtmlFormatter(filepath=output_path, params=args)
    # Print a small message on the console, about non-compliance of dataset
    print(_cli_report(dataset, str(output_path)))
    return output_path



