# coding: utf-8
import itertools
from datetime import datetime, timedelta


def dt_to_isoformat_ceil_ms(dt):
    """Helper to account for Mongo storing datetimes with only ms precision."""
    return (dt + timedelta(milliseconds=1)).isoformat(timespec='milliseconds')

# This lu_key prioritizes not duplicating potentially expensive item
# processing on incremental rebuilds at the expense of potentially missing a
# source document updated within 1 ms of a builder get_items call. Ensure
# appropriate builder validation.
LU_KEY_ISOFORMAT = (lambda s: datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f"),
                    dt_to_isoformat_ceil_ms)


def get_mongolike(d, key):
    """
    Grab a dict value using dot-notation like "a.b.c" from dict {"a":{"b":{"c": 3}}}
    Args:
        d (dict): the dictionary to search
        key (str): the key we want to grab with dot notation, e.g., "a.b.c"

    Returns:
        value from desired dict (whatever is stored at the desired key)

    """
    lead_key = key.split(".", 1)[0]
    try:
        lead_key = int(lead_key)  # for searching array data
    except:
        pass

    if "." in key:
        remainder = key.split(".", 1)[1]
        return get_mongolike(d[lead_key], remainder)
    return d[lead_key]


def put_mongolike(key, value):
    """
    Builds a dictionary with a value using mongo dot-notation

    Args:
        key (str): the key to put into using mongo notation, doesn't support arrays
        value: object
    """
    lead_key = key.split(".", 1)[0]

    if "." in key:
        remainder = key.split(".", 1)[1]
        return {lead_key: put_mongolike(remainder, value)}
    return {lead_key: value}


def make_mongolike(d, get_key, put_key):
    """
    Builds a dictionary with a value from another dictionary using mongo dot-notation

    Args:
        d (dict)L the dictionary to search
        get_key (str): the key to grab using mongo notation
        put_key (str): the key to put into using mongo notation, doesn't support arrays
    """
    return put_mongolike(put_key, get_mongolike(d, get_key))


def recursive_update(d, u):
    """
    Recursive updates d with values from u

    Args:
        d (dict): dict to update
        u (dict): updates to propogate
    """

    for k, v in u.items():
        if k in d:
            if isinstance(v, dict) and isinstance(d[k], dict):
                recursive_update(d[k], v)
            else:
                d[k] = v
        else:
            d[k] = v


def grouper(iterable, n, fillvalue=None):
    """
    Collect data into fixed-length chunks or blocks.
    """
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def reload_msonable_object(obj):
    """
    Reload an MSONable object using as_dict and from_dict
    """
    obj_class = obj.__class__
    return obj_class.from_dict(obj.as_dict())

def get_mpi():
    """
    Helper that returns the mpi communicator, rank and size.
    """
    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    return comm, rank, size

