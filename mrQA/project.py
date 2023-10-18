from pathlib import Path
from typing import Union, Dict, Optional

from MRdataset import save_mr_dataset, BaseDataset, DatasetEmptyException
from protocol import MRImagingProtocol, SiemensMRImagingProtocol

from mrQA import logger
from mrQA.base import CompliantDataset, NonCompliantDataset, UndeterminedDataset
from mrQA.formatter import HtmlFormatter
from mrQA.utils import _cli_report, \
    export_subject_lists, make_output_paths, \
    compute_majority, modify_sequence_name, get_config_from_file


def check_compliance(dataset: BaseDataset,
                     decimals: int = 3,
                     output_dir: Union[Path, str] = None,
                     verbose: bool = False,
                     tolerance: float = 0.1,
                     config_path: Union[Path, str] = None,
                     reference_path: Union[Path, str] = None):
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

    # Get results of horizontal audit
    hz_audit_results = horizontal_audit(dataset=dataset,
                                        reference_path=reference_path,
                                        decimals=decimals,
                                        tolerance=tolerance,
                                        config_path=config_path)

    # Get results of vertical audit
    vt_audit_results = vertical_audit(dataset=dataset,
                                      decimals=decimals,
                                      tolerance=tolerance,
                                      config_path=config_path)

    # Generate the report if checking compliance was successful
    generate_report(hz_audit=hz_audit_results,
                    vt_audit=vt_audit_results,
                    report_path=report_path,
                    sub_lists_dir_path=sub_lists_dir_path,
                    output_dir=output_dir)

    # Print a small message on the console, about non-compliance of dataset
    print(_cli_report(hz_audit_results, str(report_path)))
    return hz_audit_results, vt_audit_results


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
    if not isinstance(reference_path, Path):
        try:
            reference_path = Path(reference_path)
        except TypeError as e:
            logger.error(f'Expected Path or str for reference protocol path, '
                         f'got {type(reference_path)}')
            raise e

    if not reference_path.is_file():
        raise FileNotFoundError(f'Unable to access {reference_path}. Maybe it'
                                f'does not exist or is not a file')

    # TODO: Add support for other file formats, like json and dcm
    if reference_path.suffix != '.xml':
        raise ValueError(f'Expected xml file, got {reference_path.suffix} file')

    # TODO: Add support for other vendors, like GE and Philips
    if vendor == 'siemens':
        ref_protocol = SiemensMRImagingProtocol(filepath=reference_path)
    else:
        raise NotImplementedError(f'Only Siemens protocols are supported')

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
    for seq_name in dataset.get_sequence_ids():
        num_subjects = dataset.get_subject_ids(seq_name)

        # If subjects are less than 3, then we can't infer a reference protocol
        if len(num_subjects) < 3:
            continue

        # If subjects are more than 3, then we can infer a reference protocol
        reference = compute_majority(dataset=dataset,
                                     seq_name=seq_name,
                                     config_dict=config_dict)

        # Add the inferred reference to the reference protocol
        for seq_id, param_dict in reference.items():
            ref_protocol.add_sequence_from_dict(seq_id, param_dict)

    return ref_protocol


def horizontal_audit(dataset: BaseDataset,
                     reference_path: Union[Path, str],
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
    reference_path: Path | str
        Path to the reference protocol file.
    decimals: int
        Number of decimal places to round to (default:3).
    tolerance: float
        Tolerance for checking against reference protocol. Default is 0.1
    config_path: Union[Path, str]
        Path to the config file
    Returns
    -------

    """
    # Infer reference protocol if not provided
    if reference_path is None:
        ref_protocol = infer_protocol(dataset, config_path=config_path)
    else:
        ref_protocol = get_protocol_from_file(reference_path)

    config_dict = get_config_from_file(config_path)
    hz_audit_config = config_dict["horizontal_audit"]
    include_params = hz_audit_config.get('include_parameters', None)
    stratify_by = hz_audit_config.get('stratify_by', None)

    compliant_ds = CompliantDataset(name=dataset.name,
                                    data_source=dataset.data_source,
                                    ds_format=dataset.format)
    non_compliant_ds = NonCompliantDataset(name=dataset.name,
                                           data_source=dataset.data_source,
                                           ds_format=dataset.format)
    undetermined_ds = UndeterminedDataset(name=dataset.name,
                                          data_source=dataset.data_source,
                                          ds_format=dataset.format)

    eval_dict = {
        'complete_ds'  : dataset,
        'reference'    : ref_protocol,
        'compliant'    : compliant_ds,
        'non_compliant': non_compliant_ds,
        'undetermined' : undetermined_ds,
    }

    if not ref_protocol:
        logger.error('Reference protocol is empty')
        return eval_dict

    for seq_name in dataset.get_sequence_ids():
        # a temporary placeholder for compliant sequences. It will be
        # merged to compliant dataset if all the subjects are compliant
        temp_dataset = CompliantDataset(name=dataset.name,
                                        data_source=dataset.data_source,
                                        ds_format=dataset.format)
        compliant_flag = True
        undetermined_flag = False
        for subj, sess, run, seq in dataset.traverse_horizontal(seq_name):
            sequence_name = modify_sequence_name(seq, stratify_by)

            try:
                ref_sequence = ref_protocol[sequence_name]
            except KeyError:
                logger.info(f'No reference protocol for {seq_name} sequence.')
                undetermined_ds.add(subject_id=subj, session_id=sess,
                                    run_id=run, seq_id=sequence_name, seq=seq)
                undetermined_flag = True
                continue

            is_compliant, non_compliant_tuples = ref_sequence.compliant(
                seq,
                rtol=tolerance,
                decimals=decimals,
                include_params=include_params
            )

            if is_compliant:
                temp_dataset.add(subject_id=subj, session_id=sess,
                                 run_id=run, seq_id=sequence_name, seq=seq)
            else:
                compliant_flag = False
                non_compliant_params = [x[1] for x in non_compliant_tuples]
                non_compliant_ds.add(subject_id=subj, session_id=sess,
                                     run_id=run, seq_id=sequence_name, seq=seq)
                non_compliant_ds.add_non_compliant_params(
                    subject_id=subj, session_id=sess, run_id=run,
                    seq_id=sequence_name,
                    non_compliant_params=non_compliant_params
                )
        # only add the sequence if all the subjects, sessions are compliant
        if compliant_flag and not undetermined_flag:
            compliant_ds.merge(temp_dataset)

    eval_dict['compliant'] = compliant_ds
    eval_dict['non_compliant'] = non_compliant_ds
    eval_dict['undetermined'] = undetermined_ds
    return eval_dict


def vertical_audit(dataset: BaseDataset,
                   decimals: int = 3,
                   tolerance: float = 0.1,
                   config_path: Union[Path, str] = None) -> Optional[Dict]:
    """

    """
    config_dict = get_config_from_file(config_path)
    vt_audit_config = config_dict["vertical_audit"]
    include_params = vt_audit_config.get('include_parameters', None)
    chosen_pairs = vt_audit_config.get('sequences', None)

    compliant_ds = CompliantDataset(name=dataset.name,
                                    data_source=dataset.data_source,
                                    ds_format=dataset.format)
    non_compliant_ds = NonCompliantDataset(name=dataset.name,
                                           data_source=dataset.data_source,
                                           ds_format=dataset.format)

    # TODO: Add option to specify in the config file
    # assuming that sequence_ids are list of 2
    for seq1_name, seq2_name in chosen_pairs:
        for items in dataset.traverse_vertical2(seq1_name, seq2_name):
            subject, session, run1, run2, seq1, seq2 = items
            is_compliant, non_compliant_tuples = seq1.compliant(
                seq2,
                rtol=tolerance,
                decimals=decimals,
                include_params=include_params
            )
            if is_compliant:
                compliant_ds.add(subject_id=subject, session_id=session,
                                 seq_id=seq1_name, seq=seq1)
            else:
                non_compliant_ds.add(subject_id=subject, session_id=session,
                                     run_id=run1, seq_id=seq1_name,
                                     seq=seq1)
                non_compliant_ds.add(subject_id=subject, session_id=session,
                                     run_id=run2, seq_id=seq2_name,
                                     seq=seq2)

                non_compliant_params = [x[0] for x in non_compliant_tuples]
                non_compliant_ds.add_non_compliant_params(
                    subject_id=subject, session_id=session, run_id=run1,
                    seq_id=seq1_name,
                    non_compliant_params=non_compliant_params
                )

                non_compliant_params = [x[1] for x in non_compliant_tuples]
                non_compliant_ds.add_non_compliant_params(
                    subject_id=subject, session_id=session, run_id=run2,
                    seq_id=seq2_name,
                    non_compliant_params=non_compliant_params
                )
    # TODO: add option for num_sequences > 2
    eval_dict = {
        'complete_ds'   : dataset,
        'compliant'     : compliant_ds,
        'non_compliant' : non_compliant_ds,
        'sequence_pairs': chosen_pairs,
        'parameters'    : include_params
    }
    return eval_dict


def generate_report(hz_audit: dict,
                    vt_audit: dict,
                    report_path: str or Path,
                    sub_lists_dir_path: str,
                    output_dir: Union[Path, str]) -> Path:
    """
    Generates an HTML report aggregating and summarizing the non-compliance
    discovered in the dataset.

    Parameters
    ----------
    hz_audit : dict
        Dictionary containing the results of the horizontal audit
    vt_audit : dict
        Dictionary containing the results of the vertical audit
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
        Complete path to the generated report

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
