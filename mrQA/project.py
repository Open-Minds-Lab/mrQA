from pathlib import Path
from typing import Union, Dict

from MRdataset import save_mr_dataset
from MRdataset.config import DatasetEmptyException
from MRdataset.base import BaseDataset

from mrQA import logger
from mrQA.base import CompliantDataset, NonCompliantDataset, UndeterminedDataset
from mrQA.config import STRATEGIES_ALLOWED
from mrQA.formatter import HtmlFormatter
from mrQA.utils import _check_against_reference, _cli_report, \
    export_subject_lists, record_out_paths, get_protocol_from_file, \
    compute_majority, _valid_reference
from protocol import BaseMRImagingProtocol, ImagingSequence, SiemensMRImagingProtocol


def check_compliance(dataset: BaseDataset,
                     strategy: str = 'majority',
                     decimals: int = 3,
                     output_dir: Union[Path, str] = None,
                     verbose: bool = False,
                     tolerance: float = 0.1,
                     reference_path: Union[Path, str] = None) -> Path:
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
    reference_path : Union[Path, str]
        Path to the reference protocol file. Required if strategy is
        'reference'

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

    if not dataset.get_sequence_ids():
        raise DatasetEmptyException

    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(exist_ok=True, parents=True)
    if not output_dir.is_dir():
        logger.error(f'Output directory {output_dir} does not exist')
        raise NotADirectoryError('Provide a valid output directory')

    report_path, mrds_path, sub_lists_dir_path = record_out_paths(output_dir,
                                                                  dataset.name)
    save_mr_dataset(mrds_path, dataset)

    if strategy == 'majority':
        compliance_dict = compare_with_majority(dataset, decimals,
                                                tolerance=tolerance)
    elif strategy == 'reference':
        compliance_dict = compare_with_reference(dataset=dataset,
                                                 reference_path=reference_path,
                                                 decimals=decimals,
                                                 tolerance=tolerance)
    else:
        raise NotImplementedError(
            f'Only the following strategies are allowed : \n\t'
            f'{STRATEGIES_ALLOWED}')

    save_mr_dataset(mrds_path, dataset)
    generate_report(compliance_dict,
                    report_path,
                    sub_lists_dir_path,
                    output_dir)

    # Print a small message on the console, about non-compliance of dataset
    print(_cli_report(compliance_dict, str(report_path)))
    return report_path


def compare_with_reference(dataset: BaseDataset,
                           reference_path: Path,
                           decimals: int = 3,
                           tolerance: float = 0.1) -> BaseDataset:
    """
    Method for post-acquisition compliance. Reads the reference protocol/values
    from a file, and then identifies deviations.

    Parameters
    ----------
    dataset: BaseDataset
        BaseDataset instance for the dataset which is to be checked
        for compliance
    reference_path: Path
        Path to the reference protocol file
    decimals: int
        Number of decimal places to round to (default:3).
    tolerance: float
        Tolerance for checking against reference protocol. Default is 0.1

    Returns
    -------
    dataset: BaseDataset
        Adds the non-compliance information to the same BaseDataset instance and
        returns it.
    """

    # Extract reference protocol from file
    if not Path(reference_path).is_file():
        raise FileNotFoundError(f'{reference_path} does not exist')
    ref_protocol = SiemensMRImagingProtocol(reference_path)
    compliant_dataset = CompliantDataset(dataset.name)
    non_compliant_dataset = NonCompliantDataset(dataset.name)
    undetermined_dataset = UndeterminedDataset(dataset.name)

    for seq_name in dataset.get_sequence_ids():
        try:
            ref_sequence = ref_protocol[seq_name]
        except KeyError:
            logger.info(f'No reference protocol for {seq_name} sequence.')
            continue

        for subj, sess, run, seq in dataset.traverse_horizontal(seq_name):
            compliant, non_compliant_tuples = ref_sequence.compliant(seq)

            if compliant:
                compliant_dataset.add(subj, sess, run, seq_name, seq)
            else:
                non_compliant_params = [x[1] for x in non_compliant_tuples]
                non_compliant_dataset.add(subj, sess, run, seq_name, seq)
                non_compliant_dataset.add_non_compliant_params(
                    subj, sess, run, seq_name, non_compliant_params
                )

    return {
        'reference': ref_protocol,
        'compliant': compliant_dataset,
        'non_compliant': non_compliant_dataset,
        'undetermined': undetermined_dataset,
    }



def compare_with_majority(dataset: BaseDataset,
                          decimals: int = 3,
                          tolerance: float = 0.1) -> Dict:
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
    dict
        A dictionary containing the reference protocol, compliant and
        non-compliant datasets
    """

    # TODO: Check for subset, if incomplete dataset throw error and stop
    ref_protocol = BaseMRImagingProtocol(f'reference_for_{dataset.name}')
    compliant_dataset = CompliantDataset(dataset.name)
    non_compliant_dataset = NonCompliantDataset(dataset.name)
    undetermined_dataset = UndeterminedDataset(dataset.name)
    flagged = False

    for seq_name in dataset.get_sequence_ids():
        ref_sequence = ImagingSequence(name=seq_name)
        num_subjects = dataset.get_subject_ids(seq_name)
        if len(num_subjects) > 2:
            flagged = False
            ref_dict = compute_majority(dataset, seq_name)
            ref_sequence.from_dict(ref_dict)
            ref_protocol.add(ref_sequence)
        else:
            logger.info(f'Not enough subjects for {seq_name} sequence.')
            flagged = True

        for subj, sess, run, seq in dataset.traverse_horizontal(seq_name):
            if flagged:
                undetermined_dataset.add(subj, sess, run, seq_name, seq)
            else:
                compliant, non_compliant_tuples = ref_sequence.compliant(seq)

                if compliant:
                    compliant_dataset.add(subj, sess, run, seq_name, seq)
                else:
                    non_compliant_params = [x[1] for x in non_compliant_tuples]
                    non_compliant_dataset.add(subj, sess, run, seq_name, seq)
                    non_compliant_dataset.add_non_compliant_params(
                        subj, sess, run, seq_name, non_compliant_params
                    )

    return {
        'reference': ref_protocol,
        'compliant': compliant_dataset,
        'non_compliant': non_compliant_dataset,
        'undetermined': undetermined_dataset,
    }


def generate_report(compliance_dict: dict,
                    report_path: str or Path,
                    sub_lists_dir_path: str,
                    output_dir: Union[Path, str]) -> Path:
    """
    Generates an HTML report aggregating and summarizing the non-compliance
    discovered in the dataset.

    Parameters
    ----------
    compliance_dict : dict
        Dictionary containing the reference protocol, compliant and
        non-compliant datasets
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

    sub_lists_by_seq = export_subject_lists(output_dir,
                                            compliance_dict,
                                            sub_lists_dir_path)

    # Generate the HTML report and save it to the output_path
    compliance_dict['sub_lists_by_seq'] = sub_lists_by_seq
    HtmlFormatter(filepath=report_path, params=compliance_dict)
    return Path(report_path)
