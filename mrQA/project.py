from collections import namedtuple
from datetime import timedelta
from itertools import combinations
from pathlib import Path
from typing import Union, Dict, Optional

from MRdataset import save_mr_dataset, BaseDataset, DatasetEmptyException
from bokeh.embed import components
from bokeh.plotting import figure
from mrQA import logger
from mrQA.base import CompliantDataset
from mrQA.formatter import HtmlFormatter
from mrQA.utils import _cli_report, \
    export_subject_lists, make_output_paths, \
    modify_sequence_name, _init_datasets, get_reference_protocol, get_config, \
    previous_month, next_month
from protocol import UnspecifiedType


def check_compliance(dataset: BaseDataset,
                     decimals: int = 3,
                     output_dir: Union[Path, str] = None,
                     verbose: bool = False,
                     tolerance: float = 0.1,
                     config_path: Union[Path, str] = None,
                     reference_path: Union[Path, str] = None):
    """
    Main function for checking compliance. It runs horizontal and vertical
    audits on the dataset. Generates a report and saves it to the output
    directory.

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

    # Generate plots/visualization
    plot_results = plot_patterns(
        non_compliant_ds=hz_audit_results['non_compliant'],
        complete_ds=hz_audit_results['complete_ds'],
        config_path=config_path)

    # Generate the report if checking compliance was successful
    generate_report(hz_audit=hz_audit_results,
                    vt_audit=vt_audit_results,
                    report_path=report_path,
                    sub_lists_dir_path=sub_lists_dir_path,
                    output_dir=output_dir,
                    plots=plot_results)

    # Print a small message on the console, about non-compliance of dataset
    print(_cli_report(hz_audit_results, str(report_path)))
    # TODO : print(_cli_report(vt_audit_results, str(report_path)))
    return hz_audit_results, vt_audit_results


def plot_patterns(non_compliant_ds, complete_ds, config_path=None):
    plots = {}

    def _update_dict(attr, _dict):
        if attr not in _dict:
            _dict[attr] = 0
        _dict[attr] += 1
        return _dict

    def plot_components(data, x_range, label, width, legend_label=None):
        colors = ["#c9d9d3", "#718dbf", "#e84d60"][:len(data[label])]
        if legend_label is None:
            p = figure(x_range=x_range, x_axis_label=label,
                       y_axis_label="Number of Deviations (%)",
                       width=800, height=300,  tools="hover")
            p.vbar(x=label, width=width, fill_color="#b3de69", source=data)
            # if label == 'ContentDate':
            #     p.xaxis.major_label_overrides = {
            #         i: date.strftime('%b %d %Y') for i, date in enumerate(x)
            #     }
        else:
            p = figure(x_range=x_range, x_axis_label=label,
                       y_axis_label="Number of Deviations (%)",
                       width=800, height=300,  tools="hover")
            p.vbar_stack(legend_label, x=label, width=width, colors=colors,
                         tools="hover", source=data)
            # if label == 'ContentDate':
            #     p.xaxis.major_label_overrides = {
            #         i: date.strftime('%b %d %Y') for i, date in enumerate(x)
            #     }
        return components(p)

    def get_counter(dataset, parameter, hue=None):
        counter = {}
        for seq_name in dataset.get_sequence_ids():
            for subj, sess, run, seq in dataset.traverse_horizontal(
                    seq_name):
                try:
                    value = seq[parameter].get_value()
                except KeyError:
                    continue

                if isinstance(value, UnspecifiedType):
                    continue
                counter = _update_dict(value, counter)
        return counter

    plots_config = get_config(config_path=config_path, report_type='plots')
    if not plots_config:
        return plots

    include_params = plots_config.get("include_parameters", None)
    Plot = namedtuple('Plot', ['div', 'script'])
    for args in include_params:
        parameter = args['x']
        hue = args['hue']
        nc_counter = get_counter(non_compliant_ds, parameter)
        all_scans_counter = get_counter(complete_ds, parameter)
        normalized_counts = {}
        for key in nc_counter:
            if key in all_scans_counter:
                normalized_counts[key] = 100*nc_counter[key] / all_scans_counter[
                    key]

        if not normalized_counts:
            logger.error(f"Unable to read {parameter}. Skipping plot.")
            continue

        normalized_counts = dict(sorted(normalized_counts.items()))
        y = list(normalized_counts.values())
        x = list(normalized_counts.keys())
        width = 0.9
        x_range = x
        plot_data = {
            parameter: x,

        }
        if parameter == 'ContentDate':
            width = timedelta(days=1)
            x_range = [previous_month(min(x)), next_month(max(x))]
        elif parameter in ('PatientAge', 'PatientWeight'):
            x_range = [min(x) - 0.1, max(x) + 0.1]
            width = 0.1

        try:
            div, script = plot_components(x=x, y=y, width=width,
                                          x_range=x_range, label=parameter)
            plots[parameter] = Plot(div=div, script=script)
        except ValueError as e:
            logger.error(f"Skipping plot. Unable to plot parameter {parameter}."
                         f" Got {e}")
    return plots


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
    hz_audit_config = get_config(config_path=config_path,
                                 report_type='hz')
    ref_protocol = get_reference_protocol(dataset=dataset,
                                          reference_path=reference_path,
                                          config=hz_audit_config)
    compliant_ds, non_compliant_ds, undetermined_ds = _init_datasets(dataset)

    eval_dict = {
        'complete_ds': dataset,
        'reference': ref_protocol,
        'compliant': compliant_ds,
        'non_compliant': non_compliant_ds,
        'undetermined': undetermined_ds,
    }

    if not (ref_protocol and hz_audit_config):
        return eval_dict

    include_params = hz_audit_config.get('include_parameters', None)
    stratify_by = hz_audit_config.get('stratify_by', None)

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
                logger.warning(f'No reference protocol for {seq_name} '
                               f'sequence.')
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
                # a temporary placeholder for compliant sequences. It will be
                # merged to compliant dataset if all the subjects are compliant
                # for a given sequence
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

    # Update the compliance evaluation dict
    eval_dict['compliant'] = compliant_ds
    eval_dict['non_compliant'] = non_compliant_ds
    eval_dict['undetermined'] = undetermined_ds
    return eval_dict


def vertical_audit(dataset: BaseDataset,
                   decimals: int = 3,
                   tolerance: float = 0,
                   config_path: Union[Path, str] = None) -> Optional[Dict]:
    """
    Compares all the sequences of a given subject. For ex, you may want to
    check the field map against the rs-fMRI sequence. Returns a dictionary
    containing the compliant and non-compliant sequences for each subject.

    Parameters
    ----------
    dataset: BaseDataset
        Dataset to be checked for compliance
    decimals: int
        Number of decimal places to round to (default:3).
    tolerance: float
        Tolerance for checking against reference protocol. Default is 0
    config_path: Path | str
        Path to the config file
    """
    vt_audit_config = get_config(config_path=config_path,
                                 report_type='vt')
    compliant_ds, non_compliant_ds, _ = _init_datasets(dataset)
    eval_dict = {
        'complete_ds': dataset,
        'compliant': compliant_ds,
        'non_compliant': non_compliant_ds,
        'sequence_pairs': [],
        'parameters': []
    }
    if not vt_audit_config:
        return eval_dict

    # If include_parameters is not provided, then it will compare all parameters
    include_params = vt_audit_config.get('include_parameters', None)
    chosen_pairs = vt_audit_config.get('sequences', None)
    stratify_by = vt_audit_config.get('stratify_by', None)

    # If no sequence pairs are provided, then compare all possible pairs
    if chosen_pairs is None:
        logger.warn('No sequence pairs provided. Comparing all possible '
                    'sequence pairs.')
        chosen_pairs = list(combinations(dataset.get_sequence_ids(), 2))
    # check pair are queryable, all the pairs are not present
    # throw an error if any of the pair is not present
    used_pairs = set()
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
            seq1_name = modify_sequence_name(seq1, stratify_by)
            seq2_name = modify_sequence_name(seq2, stratify_by)

            non_compliant_ds.add_sequence_pair_names((seq1_name, seq2_name))
            used_pairs.add((seq1_name, seq2_name))

            if is_compliant:
                compliant_ds.add(subject_id=subject, session_id=session,
                                 run_id=run1, seq_id=seq1_name, seq=seq1)
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
                    seq_id=seq1_name, ref_seq=seq2_name,
                    non_compliant_params=non_compliant_params
                )

                non_compliant_params = [x[1] for x in non_compliant_tuples]
                non_compliant_ds.add_non_compliant_params(
                    subject_id=subject, session_id=session, run_id=run2,
                    seq_id=seq2_name, ref_seq=seq1_name,
                    non_compliant_params=non_compliant_params
                )
    # TODO: add option for num_sequences > 2
    eval_dict = {
        'complete_ds': dataset,
        'compliant': compliant_ds,
        'non_compliant': non_compliant_ds,
        'sequence_pairs': used_pairs,
        'parameters': include_params
    }
    return eval_dict


def generate_report(hz_audit: dict,
                    vt_audit: dict,
                    report_path: str or Path,
                    sub_lists_dir_path: str,
                    output_dir: Union[Path, str],
                    plots=None) -> Path:
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
                                            hz_audit['non_compliant'],
                                            sub_lists_dir_path)

    report_formatter = HtmlFormatter(filepath=report_path)
    report_formatter.collect_hz_audit_results(
        complete_ds=hz_audit['complete_ds'],
        compliant_ds=hz_audit['compliant'],
        non_compliant_ds=hz_audit['non_compliant'],
        undetermined_ds=hz_audit['undetermined'],
        subject_lists_by_seq=sub_lists_by_seq,
        ref_protocol=hz_audit['reference']
    )

    report_formatter.collect_vt_audit_results(
        compliant_ds=vt_audit['compliant'],
        non_compliant_ds=vt_audit['non_compliant'],
        complete_ds=vt_audit['complete_ds'],
        sequence_pairs=vt_audit['sequence_pairs'],
        parameters=vt_audit['parameters']
    )

    report_formatter.collect_plots(**plots)
    report_formatter.render()
    # Generate the HTML report and save it to the output_path
    return Path(report_path)
