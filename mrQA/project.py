import json
from pathlib import Path
from typing import Union, Dict, Optional

from MRdataset import save_mr_dataset, BaseDataset, DatasetEmptyException
from protocol import MRImagingProtocol, SiemensMRImagingProtocol

from mrQA import logger
from mrQA.base import CompliantDataset, NonCompliantDataset, UndeterminedDataset
from mrQA.formatter import HtmlFormatter
from mrQA.utils import _cli_report, \
    export_subject_lists, make_output_paths, \
    compute_majority


def check_compliance(dataset: BaseDataset,
                     decimals: int = 3,
                     output_dir: Union[Path, str] = None,
                     verbose: bool = False,
                     tolerance: float = 0.1,
                     config_path: Union[Path, str] = None,
                     reference_path: Union[Path, str] = None) -> Optional[Dict]:
    """
    Main function for checking compliance. Infers the reference protocol
    according to the user chosen strategy, and then generates a compliance
    report

    Parameters
    ----------
    dataset : BaseDataset
        Dataset to be checked for compliance
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
    config_path : Union[Path, str]
        Path to the config file
    Returns
    -------
    compliance_dict : Dict
        Dictionary containing the reference protocol, compliant and
        non-compliant datasets
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
        logger.setLevel('ERROR')

    # Check if dataset is empty
    if not dataset.get_sequence_ids():
        raise DatasetEmptyException

    # Check if output directory exists, create if not
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(exist_ok=True, parents=True)
    if not output_dir.is_dir():
        logger.error(f'Output directory {output_dir} does not exist')
        raise NotADirectoryError('Provide a valid output directory')

    # Create paths for report, mrds pkl file and sub_lists
    report_path, mrds_path, sub_lists_dir_path = make_output_paths(output_dir,
                                                                   dataset)
    # Save the dataset to a pickle file
    save_mr_dataset(mrds_path, dataset)

    # Infer reference protocol if not provided
    if reference_path is None:
        ref_protocol = infer_protocol(dataset, config_path=config_path)
    else:
        ref_protocol = get_protocol_from_file(reference_path)

    # Compare the dataset with reference protocol (inferred or user-defined)
    compliance_summary_dict = compare_with_reference(dataset=dataset,
                                                     reference_protocol=ref_protocol,
                                                     decimals=decimals,
                                                     tolerance=tolerance,
                                                     config_path=config_path)

    # Generate the report if checking compliance was successful
    if compliance_summary_dict:
        generate_report(compliance_summary_dict,
                        report_path,
                        sub_lists_dir_path,
                        output_dir)

        # Print a small message on the console, about non-compliance of dataset
        print(_cli_report(compliance_summary_dict, str(report_path)))
        return compliance_summary_dict
    else:
        logger.error('Could not generate report')
        return None


def get_protocol_from_file(reference_path: Path,
                           vendor: str = 'siemens') -> MRImagingProtocol:
    """
    Extracts the reference protocol from the file. Supports only Siemens
    protocols in xml format. Raises error otherwise.

    Parameters
    ----------
    reference_path : Union[Path, str]
        Path to the reference protocol file
    vendor: str
        Vendor of the scanner. Default is Siemens

    Returns
    -------
    ref_protocol : MRImagingProtocol
        Reference protocol extracted from the file
    """
    # Extract reference protocol from file
    if isinstance(reference_path, str):
        reference_path = Path(reference_path)
    if not isinstance(reference_path, Path):
        raise TypeError(f'Expected Path or str, got {type(reference_path)}')

    if not reference_path.is_file():
        raise FileNotFoundError(f'{reference_path} does not exist')
    # TODO: Add support for other file formats, like json and dcm
    if reference_path.suffix != '.xml':
        raise ValueError(f'Expected xml file, got {reference_path.suffix}')
    # TODO: Add support for other vendors, like GE and Philips
    if vendor != 'siemens':
        raise NotImplementedError(f'Only Siemens protocols are supported')
    ref_protocol = SiemensMRImagingProtocol(filepath=reference_path)
    return ref_protocol


def infer_protocol(dataset: BaseDataset,
                   config_path: Union[Path, str]) -> MRImagingProtocol:
    """
    Infers the reference protocol from the dataset. The reference protocol
    is inferred by computing the majority for each of the parameters for each sequence
    in the dataset.

    Parameters
    ----------
    dataset: BaseDataset
        Dataset to be checked for compliance
    config_path: Union[Path, str]
        Path to the config file
    Returns
    -------
    ref_protocol : MRImagingProtocol
        Reference protocol inferred from the dataset
    """
    config_dict = get_config_from_file(config_path)

    # TODO: Check for subset, if incomplete dataset throw error and stop
    ref_protocol = MRImagingProtocol(f'reference_for_{dataset.name}')
    # create reference protocol for each sequence
    reference_by_seq = {}
    for seq_name in dataset.get_sequence_ids():
        num_subjects = dataset.get_subject_ids(seq_name)
        # If subjects are less than 3, then we can't infer a reference
        if len(num_subjects) > 2:
            reference = compute_majority(dataset=dataset,
                                         seq_name=seq_name,
                                         config_dict=config_dict)
            reference_by_seq[seq_name] = reference
    # update the reference protocol with dictonary
    ref_protocol.add_sequences_from_dict(reference_by_seq)
    return ref_protocol


def compare_with_reference(dataset: BaseDataset,
                           reference_protocol: MRImagingProtocol,
                           decimals: int = 3,
                           tolerance: float = 0.1,
                           config_path: Union[Path, str] = None) -> Optional[Dict]:
    """
    Compares the dataset with the reference protocol (either inferred or
    user-defined). Returns a dictionary containing the reference protocol,
    compliant and non-compliant datasets.

    Parameters
    ----------
    dataset: BaseDataset
        Dataset to be checked for compliance
    reference_protocol: MRImagingProtocol
        Reference protocol to be compared with
    decimals: int
        Number of decimal places to round to (default:3).
    tolerance: float
        Tolerance for checking against reference protocol. Default is 0.1
    config_path: Union[Path, str]
        Path to the config file
    Returns
    -------

    """
    config_dict = get_config_from_file(config_path)
    include_params = config_dict['include_parameters']

    if not reference_protocol:
        logger.error('Reference protocol is empty')
        return None

    compliant_dataset = CompliantDataset(dataset.name)
    non_compliant_dataset = NonCompliantDataset(dataset.name)
    undetermined_dataset = UndeterminedDataset(dataset.name)

    for seq_name in dataset.get_sequence_ids():
        # a temporary placeholder for compliant sequences. It will be
        # merged to compliant dataset if all the subjects are compliant
        temp_dataset = CompliantDataset(dataset.name)
        try:
            ref_sequence = reference_protocol[seq_name]
        except KeyError:
            logger.info(f'No reference protocol for {seq_name} sequence.')
            continue

        compliant_flag = True
        for subj, sess, run, seq in dataset.traverse_horizontal(seq_name):
            compliant, non_compliant_tuples = ref_sequence.compliant(seq, rtol=tolerance,
                                                                     decimals=decimals,
                                                                     include_params=include_params)

            if compliant:
                temp_dataset.add(subject_id=subj, session_id=sess,
                                 run_id=run, seq_id=seq_name, seq=seq)
            else:
                compliant_flag = False
                non_compliant_params = [x[1] for x in non_compliant_tuples]
                non_compliant_dataset.add(subject_id=subj, session_id=sess,
                                          run_id=run, seq_id=seq_name, seq=seq)
                non_compliant_dataset.add_non_compliant_params(
                    subject_id=subj, session_id=sess, run_id=run,
                    seq_id=seq_name, non_compliant_params=non_compliant_params
                )
        # only add the sequence if all the subjects, sessions are compliant
        if compliant_flag:
            compliant_dataset.merge(temp_dataset)

    return {
        'complete_ds': dataset,
        'reference': reference_protocol,
        'compliant': compliant_dataset,
        'non_compliant': non_compliant_dataset,
        'undetermined': undetermined_dataset,
    }


def get_config_from_file(config_path: Union[Path, str]) -> dict:
    if isinstance(config_path, str):
        config_path = Path(config_path)
    if not isinstance(config_path, Path):
        raise TypeError(f'Expected Path or str, got {type(config_path)}')
    if not config_path.is_file():
        raise FileNotFoundError(f'{config_path} does not exist')

    # read json file
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config


def generate_report(compliance_summary_dict: dict,
                    report_path: str or Path,
                    sub_lists_dir_path: str,
                    output_dir: Union[Path, str]) -> Path:
    """
    Generates an HTML report aggregating and summarizing the non-compliance
    discovered in the dataset.

    Parameters
    ----------
    compliance_summary_dict : dict
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
                                            compliance_summary_dict,
                                            sub_lists_dir_path)

    # Generate the HTML report and save it to the output_path
    compliance_summary_dict['sub_lists_by_seq'] = sub_lists_by_seq
    HtmlFormatter(filepath=report_path, params=compliance_summary_dict)
    return Path(report_path)
