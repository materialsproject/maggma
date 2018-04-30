# coding: utf-8
"""
Utilities to help with maggma functions
"""
import itertools
from datetime import datetime, timedelta
from pydash.utilities import to_path
from pydash.objects import set_, get, has
from pydash.objects import unset as _unset
import logging
import tqdm


class TqdmLoggingHandler(logging.Handler):
    """
    Helper to enable routing tqdm progress around logging
    """

    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


def confirm_field_index(store, fields):
    """Confirm index on store for at least one of fields

    One can't simply ensure an index exists via
    `store.collection.create_index` because a Builder must assume
    read-only access to source Stores. The MongoDB `read` built-in role
    does not include the `createIndex` action.

    Returns:
        True if an index exists for a given field
        False if not

    """
    if not isinstance(fields, list):
        fields = [fields]
    info = store.collection.index_information().values()
    for spec in (index['key'] for index in info):
        for field in fields:
            if spec[0][0] == field:
                return True
    return False


def dt_to_isoformat_ceil_ms(dt):
    """Helper to account for Mongo storing datetimes with only ms precision."""
    return (dt + timedelta(milliseconds=1)).isoformat(timespec='milliseconds')


# This lu_key prioritizes not duplicating potentially expensive item
# processing on incremental rebuilds at the expense of potentially missing a
# source document updated within 1 ms of a builder get_items call. Ensure
# appropriate builder validation.
LU_KEY_ISOFORMAT = (lambda s: datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f"), dt_to_isoformat_ceil_ms)


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


def get_mpi():
    """
    Helper that returns the mpi communicator, rank and size.
    """
    try:
        from mpi4py import MPI

        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        size = comm.Get_size()
    except:
        comm = None
        rank = -1
        size = 0

    return comm, rank, size


def lazy_substitute(d, aliases):
    """
    Simple top level substitute that doesn't dive into mongo like strings
    """
    for alias, key in aliases.items():
        if key in d:
            d[alias] = d[key]
            del d[key]


def substitute(d, aliases):
    """
    Substitutes keys in dictionary
    Accepts multilevel mongo like keys
    """
    for alias, key in aliases.items():
        if has(d, key):
            set_(d, alias, get(d, key))
            unset(d, key)


def unset(d, key):
    """
    Unsets a key
    """
    _unset(d, key)
    path = to_path(key)
    for i in reversed(range(1, len(path))):
        if len(get(d, path[:i])) == 0:
            unset(d, path[:i])
