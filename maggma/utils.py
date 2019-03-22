# coding: utf-8
"""
Utilities to help with maggma functions
"""
import itertools
import signal
import logging


from collections import deque
from datetime import datetime, timedelta
from sys import getsizeof, stderr

from pydash.utilities import to_path
from pydash.objects import set_, get, has
from pydash.objects import unset as _unset

# import tqdm Jupyter widget if running inside Jupyter
try:
    # noinspection PyUnresolvedReferences
    if get_ipython().__class__.__name__ == "ZMQInteractiveShell":
        from tqdm import tqdm_notebook as tqdm
    else:  # likely 'TerminalInteractiveShell'
        from tqdm import tqdm
except NameError:
    from tqdm import tqdm


def primed(iterable):
    """Preprimes an iterator so the first value is calculated immediately
       but not returned until the first iteration
    """
    itr = iter(iterable)
    try:
        first = next(itr)  # itr.next() in Python 2
    except StopIteration:
        return itr
    return itertools.chain([first], itr)


class TqdmLoggingHandler(logging.Handler):
    """
    Helper to enable routing tqdm progress around logging
    """

    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
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
    for spec in (index["key"] for index in info):
        for field in fields:
            if spec[0][0] == field:
                return True
    return False


def dt_to_isoformat_ceil_ms(dt):
    """Helper to account for Mongo storing datetimes with only ms precision."""
    return (dt + timedelta(milliseconds=1)).isoformat(timespec="milliseconds")


def isostr_to_dt(s):
    """Convert an ISO 8601 string to a datetime."""
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")


# This lu_key prioritizes not duplicating potentially expensive item
# processing on incremental rebuilds at the expense of potentially missing a
# source document updated within 1 ms of a builder get_items call. Ensure
# appropriate builder validation.
LU_KEY_ISOFORMAT = (isostr_to_dt, dt_to_isoformat_ceil_ms)


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


def total_size(o, handlers=None, verbose=False):
    """
    Returns the approximate memory footprint (in bytes) of an object.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.

    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    Example usage:
    >>> d = dict(a=1, b=2, c=3, d=[4,5,6,7], e='a string of chars')
    >>> print(total_size(d, verbose=True))

    Based on: https://github.com/ActiveState/code/blob
    /73b09edc1b9850c557a79296655f140ce5e853db
    /recipes/Python/577504_Compute_Memory_footprint_object_its/recipe-577504.py
    """
    all_handlers = {
        tuple: iter,
        list: iter,
        deque: iter,
        dict: (lambda d: itertools.chain.from_iterable(d.items())),
        set: iter,
        frozenset: iter,
    }
    if handlers:
        all_handlers.update(handlers)  # user handlers take precedence
    seen = set()  # track which object id's have already been seen
    default_size = getsizeof(0)  # estimate sizeof object without __sizeof__

    def sizeof(o):
        """Recursively determine size (in bytes) of object."""
        if id(o) in seen:  # do not double count the same object
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        if verbose:
            print(s, type(o), repr(o), file=stderr)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)


def source_keys_updated(source, target, query=None):
    """
    Utility for incremental building. Gets a list of source.key values.

    Get key values for source documents that have been updated with respect to
    corresponding target documents.
    """

    keys_updated = set()  # Handle non-unique keys, e.g. for GroupBuilder.

    props = {target.key: 1, target.lu_field: 1, "_id": 0}
    target_dates = {
        d[target.key]: target.lu_func[0](d[target.lu_field])
        for d in target.query(properties=props)
    }

    props = {source.key: 1, source.lu_field: 1, "_id": 0}
    cursor_source = source.query(criteria=query, properties=props)
    for sdoc in cursor_source:
        key, lu = sdoc[source.key], source.lu_func[0](sdoc[source.lu_field])
        if key not in target_dates:
            keys_updated.add(key)
        elif lu > target_dates[key]:
            keys_updated.add(key)

    return list(keys_updated)


class Timeout:
    # implementation courtesy of https://stackoverflow.com/a/22348885/637562

    def __init__(self, seconds=14, error_message=""):
        """
        Set a maximum running time for functions.

        :param seconds (int): Seconds before TimeoutError raised, set to None to disable,
        default is set assuming a maximum running time of 1 day for 100,000 items
        parallelized across 16 cores, i.e. int(16 * 24 * 60 * 60 / 1e5)
        :param error_message (str): Error message to display with TimeoutError
        """
        self.seconds = int(seconds) if seconds else None
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        if self.seconds:
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        if self.seconds:
            signal.alarm(0)
