# coding: utf-8
"""
Utilities to help with maggma functions
"""
import itertools
import logging
import signal
import uuid
from datetime import datetime, timedelta
from importlib import import_module
from typing import Dict, Iterable, Optional, Union

from bson.json_util import ObjectId
from pydash.objects import get, has, set_
from pydash.objects import unset as _unset
from pydash.utilities import to_path
from pymongo.collection import Collection

# import tqdm Jupyter widget if running inside Jupyter
from tqdm.autonotebook import tqdm


def primed(iterable: Iterable) -> Iterable:
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
        """
        Initialize the Tqdm handler
        """
        super().__init__(level)

    def emit(self, record):
        """
        Emit a record via Tqdm screen
        """
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self.handleError(record)


def confirm_field_index(collection: Collection, field: str) -> bool:
    """Confirm index on store for at least one of fields

    One can't simply ensure an index exists via
    `store.collection.create_index` because a Builder must assume
    read-only access to source Stores. The MongoDB `read` built-in role
    does not include the `createIndex` action.

    Returns:
        True if an index exists for a given field
        False if not

    """
    info = list(collection.index_information().values())
    keys = {spec[0] for index in info for spec in index["key"]}
    return field in keys


def to_isoformat_ceil_ms(dt: Union[datetime, str]) -> str:
    """Helper to account for Mongo storing datetimes with only ms precision."""
    if isinstance(dt, datetime):
        return (dt + timedelta(milliseconds=1)).isoformat(timespec="milliseconds")
    elif isinstance(dt, str):
        return dt


def to_dt(s: Union[datetime, str]) -> datetime:
    """Convert an ISO 8601 string to a datetime."""
    if isinstance(s, str):
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
    elif isinstance(s, datetime):
        return s


# This lu_key prioritizes not duplicating potentially expensive item
# processing on incremental rebuilds at the expense of potentially missing a
# source document updated within 1 ms of a builder get_items call. Ensure
# appropriate builder validation.
LU_KEY_ISOFORMAT = (to_dt, to_isoformat_ceil_ms)


def recursive_update(d: Dict, u: Dict):
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


def grouper(iterable: Iterable, n: int) -> Iterable:
    """
    Collect data into fixed-length chunks or blocks.
    >>> list(grouper(3, 'ABCDEFG'))
    [['A', 'B', 'C'], ['D', 'E', 'F'], ['G']]

    Updated from:
    https://stackoverflow.com/questions/31164731/python-chunking-csv-file-multiproccessing/31170795#31170795
    """
    iterable = iter(iterable)
    return iter(lambda: list(itertools.islice(iterable, n)), [])


def lazy_substitute(d: Dict, aliases: Dict):
    """
    Simple top level substitute that doesn't dive into mongo like strings
    """
    for alias, key in aliases.items():
        if key in d:
            d[alias] = d[key]
            del d[key]


def substitute(d: Dict, aliases: Dict):
    """
    Substitutes keys in dictionary
    Accepts multilevel mongo like keys
    """
    for alias, key in aliases.items():
        if has(d, key):
            set_(d, alias, get(d, key))
            unset(d, key)


def unset(d: Dict, key: str):
    """
    Unsets a key
    """
    _unset(d, key)
    path = to_path(key)
    for i in reversed(range(1, len(path))):
        if len(get(d, path[:i])) == 0:
            unset(d, path[:i])


class Timeout:
    """
    Context manager that provides context.
    implementation courtesy of https://stackoverflow.com/a/22348885/637562
    """

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
        """
        Raises an error on timeout
        """
        raise TimeoutError(self.error_message)

    def __enter__(self):
        """
        Enter context with timeout
        """
        if self.seconds:
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        """
        Exit context with timeout
        """
        if self.seconds:
            signal.alarm(0)


def dynamic_import(abs_module_path: str, class_name: Optional[str] = None):
    """
    Dynamic class importer from: https://www.bnmetrics.com/blog/dynamic-import-in-python3
    """

    if class_name is None:
        class_name = abs_module_path.split(".")[-1]
        abs_module_path = ".".join(abs_module_path.split(".")[:-1])

    module_object = import_module(abs_module_path)
    target_class = getattr(module_object, class_name)
    return target_class


class ReportingHandler(logging.Handler):
    """
    Helper to route reporting messages
    This uses the NOTSET level to send reporting messages
    """

    def __init__(self, reporting_store):
        """
        Initialize the Reporting Logger
        """
        super().__init__(logging.NOTSET)
        self.reporting_store = reporting_store
        self.reporting_store.connect()
        self.errors = 0
        self.warnings = 0
        self.build_id = uuid.uuid4()

    def emit(self, record):
        """
        Emit a record via Tqdm screen
        """
        if "maggma" in record.__dict__:
            maggma_record = record.maggma
            event = maggma_record["event"]

            maggma_record.update(
                {
                    "last_updated": datetime.utcnow(),
                    "machine": uuid.UUID(int=uuid.getnode()),
                }
            )

            if event == "BUILD_STARTED":
                self.errors = 0
                self.warnings = 0
                self.build_id = uuid.uuid4()

            elif event == "BUILD_ENDED":
                maggma_record.update({"errors": self.errors, "warnings": self.warnings})

            maggma_record["_id"] = ObjectId()
            maggma_record["build_id"] = self.build_id
            self.reporting_store.update(maggma_record, key="_id")
