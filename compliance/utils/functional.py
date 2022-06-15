import functools
import time


def safe_get(dictionary, keys, default=None):
    return functools.reduce(
        lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys.split("."),
        dictionary
    )


def timestamp():
    time_string = time.strftime("%m_%d_%Y_%H_%M")
    return time_string
