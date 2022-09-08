import time
from collections import Counter, defaultdict
from MRdataset.utils import is_hashable


def timestamp():
    """Generate a timestamp as a string"""
    time_string = time.strftime("%m_%d_%Y_%H_%M")
    return time_string


def majority_attribute_values(iterable, missing=None):
    """
    Given a list of dictionaries, it generates the most common
    values for each key

    Parameters
    ----------
    iterable : list
        a list of dictionaries
    missing : python object, default None
        a default value if the key is missing in any dictionary

    Returns
    -------
    dict
        Key-value pairs specifying the most common values for each key
    """
    counts = {}
    categories = set(counts)
    for length, element in enumerate(iterable):
        categories.update(element)
        for cat in categories:
            try:
                counter = counts[cat]
            except KeyError:
                counts[cat] = counter = Counter({missing: 0})
            value = element.get(cat, missing)
            if not is_hashable(value):
                value = str(value)
            counter[value] += 1
    params = {}
    for k in counts.keys():
        params[k] = counts[k].most_common(1)[0][0]
    return params


def extract_reasons(data):
    """
    Given a list of tuples, extract all the elements at index 1, and return
    as a list

    Parameters
    ----------
    data : List of tuples

    Returns
    -------
    list
        List of values at index 1
    """
    return list(zip(*data))[1]
