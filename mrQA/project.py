from pathlib import Path
from typing import Union

from MRdataset import save_mr_dataset
from MRdataset.base import BaseDataset
from MRdataset.log import logger
from MRdataset.config import DatasetEmptyException

from mrQA.config import STRATEGIES_ALLOWED
from mrQA.formatter import HtmlFormatter
from mrQA.utils import majority_attribute_values, _get_runs_by_echo, \
    _check_against_reference, _cli_report, _validate_reference, \
    export_subject_lists, record_out_paths


def check_compliance(dataset: BaseDataset,
                     strategy: str = 'majority',
                     decimals: int = 3,
                     output_dir: Union[Path, str] = None,
                     verbose: bool = False,
                     tolerance: float = 0.1,) -> Path:
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
    decimals : int
        Number of decimal places to round to (default:3).
    verbose : bool
        print more if true
    tolerance : float
        Tolerance for checking against reference protocol. Default is 0.1

    Returns
    -------
    report_path : Path
        Path to the generated report

    Raises
    ------
    ValueError
        If the input dataset is empty or otherwise invalid
    NotImplementedError
        If the input strategy is not supported
    NotADirectoryError
        If the output directory doesn't exist
    """
    if verbose:
        logger.setLevel('INFO')
    else:
        logger.setLevel('WARNING')

    if not dataset.modalities:
        raise DatasetEmptyException

    if strategy == 'majority':
        dataset = compare_with_majority(dataset, decimals, tolerance=tolerance)
    else:
        raise NotImplementedError(
            f'Only the following strategies are allowed : \n\t'
            f'{STRATEGIES_ALLOWED}')

    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(exist_ok=True, parents=True)
    if not output_dir.is_dir():
        raise NotADirectoryError('Provide a valid output directory')

    report_path, mrds_path, sub_lists_dir_path = record_out_paths(output_dir,
                                                                  dataset.name)
    save_mr_dataset(mrds_path, dataset)
    generate_report(dataset,
                    report_path,
                    sub_lists_dir_path,
                    output_dir)

    # Print a small message on the console, about non-compliance of dataset
    print(_cli_report(dataset, str(report_path)))
    return report_path


def compare_with_majority(dataset: BaseDataset,
                          decimals: int = 3,
                          tolerance: float = 0.1) -> BaseDataset:
    """
    Method for post-acquisition compliance. Infers the reference protocol/values
    by looking for the most frequent values, and then identifying deviations

    Parameters
    ----------
    dataset : BaseDataset
        BaseDataset instance for the dataset which is to be checked
        for compliance
    decimals : int
        Number of decimal places to round to (default:3).
    tolerance : float
        Tolerance for checking against reference protocol. Default is 0.1

    Returns
    -------
    dataset : BaseDataset
        Adds the non-compliance information to the same BaseDataset instance and
        returns it.
    """
    # TODO: Check for subset, if incomplete dataset throw error and stop

    for modality in dataset.modalities:
        # Reset compliance calculation before re-computing it.
        modality.reset_compliance()

        # Infer reference protocol for each echo_time
        # TODO: segregation via echo_time should be deprecated as multiple TE is
        #   part of the same run
        run_by_echo = _get_runs_by_echo(modality, decimals)

        # For each echo time, find the most common values
        for echo_time, run_list in run_by_echo.items():
            reference = majority_attribute_values(run_list, echo_time)
            if _validate_reference(reference):
                modality.set_reference(reference, echo_time)

        modality = _check_against_reference(modality, decimals,
                                            tolerance=tolerance)
        if modality.compliant:
            dataset.add_compliant_modality_name(modality.name)
        else:
            dataset.add_non_compliant_modality_name(modality.name)
    # As we are updating the same dataset by adding non-compliant subject names,
    # and non-compliant modality names, we can return the same dataset
    return dataset


def generate_report(dataset: BaseDataset,
                    report_path: str or Path,
                    sub_lists_dir_path: str,
                    output_dir: Union[Path, str]) -> Path:
    """
    Generates an HTML report aggregating and summarizing the non-compliance
    discovered in the dataset.

    Parameters
    ----------
    dataset : BaseDataset
        BaseDataset instance for the dataset which is to be checked
    report_path : str
        Name of the file to be generated, without extension. Ensures that
        naming is consistent across the report, dataset and record files
    sub_lists_dir_path : str
        Path to the directory in which the subject lists should be stored
    output_dir : Union[Path, str]
        Directory in which the generated report should be stored.

    Returns
    -------
    output_path : Path
        Path to the generated report

    """
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # time_dict = get_timestamps()
    sub_lists_by_modality = export_subject_lists(output_dir,
                                                 dataset,
                                                 sub_lists_dir_path)
    # export_record(output_dir, filename, time_dict)
    # Generate the HTML report and save it to the output_path
    args = {
        'ds': dataset,
        'sub_lists_by_modality': sub_lists_by_modality,
        # 'time': time_dict
    }
    HtmlFormatter(filepath=report_path, params=args)
    return Path(report_path)
