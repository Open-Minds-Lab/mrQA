from compliance.common import ModalityNode
from compliance.elements.slice import parse
import warnings
import dictdiffer
from dictdiffer import diff as dict_diff
from pathlib import Path
from compliance.templates import formatter
from compliance.utils import functional
import pickle


def check_compliance(dataset,
                     strategy='first',
                     output_dir=None,
                     reference_path=None,
                     reindex=False,
                     verbose=False):
    root_node = ModalityNode(dataset.data_root)
    if dataset.name is None:
        if dataset.projects:
            root_node.name = dataset.projects[0]
    else:
        root_node.name = dataset.name

    metadata_root = Path(dataset.metadata_root)
    if not metadata_root.exists():
        raise FileNotFoundError('Provide a valid /path/to/cache/dir')

    cache_path = metadata_root / "{0}_tree.pkl".format(root_node.name)
    indexed = cache_path.exists()

    # TODO delete this after debugging
    root_node = construct_tree(dataset, root_node)

    if not indexed or reindex:
        root_node = construct_tree(dataset, root_node)
        with open(cache_path, "wb") as f:
            pickle.dump(root_node, f)
    else:
        with open(cache_path, "rb") as f:
            root_node = pickle.load(f)

    root_node = partition_sessions_by_majority(root_node)
    generate_report(root_node, output_dir, dataset.name)


def construct_tree(dataset, root_node):
    for mode in dataset.modalities:
        modality = ModalityNode(path=None)
        modality.name = mode
        for subject_id in dataset.modalities[mode]:
            has_multi_series = len(dataset[subject_id, mode].keys()) > 1
            for series in dataset[subject_id, mode]:
                files = dataset[subject_id, mode][series]
                if len(files) == 0:
                    run_node = ModalityNode(path=None)
                    run_node.error = True
                    warnings.warn("Expected at least 1 .dcm files. "
                                  "Got 0 for Subject : {0} and Modality : {1}"
                                  "".format(subject_id, mode), stacklevel=2)
                    # Don't insert this run. Reject it
                    continue
                elif len(files) == 1:
                    run_node = parse(files[0])
                    run_node.error = False
                    if has_multi_series:
                        run_node.name = subject_id + '_' + series[-2:]
                    else:
                        run_node.name = subject_id
                    modality.add_subject(run_node)
                else:
                    run_node = parse(files[0])
                    flag = False
                    for f in files[1:]:
                        temp_node = parse(f)
                        difference = list(dict_diff(dict(run_node.params),
                                                    dict(temp_node.params)))
                        if difference:
                            flag = True
                            warnings.warn("Expected all .dcm files to have same parameters. "
                                          "Got different values in Subject : {0}"
                                          " Modality : {1}".format(subject_id, mode),
                                          stacklevel=2)
                            break
                    run_node.error = flag
                    run_node.path = Path(files[0]).parent
                    if has_multi_series:
                        run_node.name = subject_id + '_' + series[-2:]
                    else:
                        run_node.name = subject_id
                    # if not run_node.error:
                    modality.add_subject(run_node)
        root_node.add_subject(modality)
    return root_node


def partition_sessions_by_first(root_node):
    for mode in root_node.subjects:
        i = 0
        anchor = None
        if count_zero_children(mode):
            continue
        for i, c in enumerate(mode.subjects):
            if not c.error:
                anchor = c
                break
        mode.params = anchor.params.copy()
        for sub in mode.subjects[i:]:
            sub.delta = diff(sub, anchor)
            if sub.delta:
                sub.fully_compliant = False
                mode.non_compliant.append(sub.name)
            else:
                sub.fully_compliant = True
                mode.compliant.append(sub.name)
    return root_node


def diff(a, b):
    return list(dictdiffer.diff(dict(a.params), dict(b.params)))


def count_zero_children(node):
    """
    ModalityNode has zero subjects
    """
    if len(node.subjects) == 0:
        warnings.warn("No fully_compliant runs found for node : {0}".format(node.name),
                      stacklevel=2)
        return True
    return False


def partition_sessions_by_majority(root_node):
    """Method checking compliance by first inferring the reference protocol/values, and
    then identifying deviations

    """

    for mode in root_node.subjects:
        i = 0
        anchor = None
        if count_zero_children(mode):
            continue
        # Only parse for valid subjects
        mode.params = functional.majority_attribute_values([child for child in mode.subjects if not child.error])
        for sub in mode.subjects:
            if not sub.error:
                sub.delta = diff(sub, mode)
                if sub.delta:
                    sub.fully_compliant = False
                    mode.non_compliant.append(sub.name)
                else:
                    sub.fully_compliant = True
                    mode.compliant.append(sub.name)
            else:
                mode.error_children.append(sub.name)

    return root_node


def generate_report(root_node, output_dir, name):
    formatter.HtmlFormatter(filepath=Path(output_dir) / (
        '{0}_{1}.{2}'.format(name,
                             functional.timestamp(),
                             'html')
    ), params=root_node)


def create_report(dataset=None,
                  strategy='first',
                  output_dir=None,
                  reference_path=None,
                  reindex=False,
                  verbose=False):
    if output_dir is None:
        output_dir = dataset.data_root

    check_compliance(dataset=dataset,
                     strategy=strategy,
                     output_dir=output_dir,
                     reference_path=reference_path,
                     reindex=reindex,
                     verbose=verbose)
