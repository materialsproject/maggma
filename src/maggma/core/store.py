# coding: utf-8
"""
Module containing the core Store definition
"""

import logging
from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import datetime
from enum import Enum
from typing import Dict, Iterator, List, Optional, Tuple, Union

from monty.dev import deprecated
from monty.json import MontyDecoder, MSONable
from pydash import get, has, identity

from maggma.core.validator import Validator
from maggma.utils import LU_KEY_ISOFORMAT


class Sort(Enum):
    """Enumeration for sorting order"""

    Ascending = 1
    Descending = -1


class DateTimeFormat(Enum):
    """Datetime format in store document"""

    DateTime = "datetime"
    IsoFormat = "isoformat"


class Store(MSONable, metaclass=ABCMeta):
    """
    Abstract class for a data Store
    Defines the interface for all data going in and out of a Builder
    """

    def __init__(
        self,
        key: str = "task_id",
        last_updated_field: str = "last_updated",
        last_updated_type: DateTimeFormat = DateTimeFormat("datetime"),
        validator: Optional[Validator] = None,
    ):
        """
        Args:
            key: main key to index on
            last_updated_field: field for date/time stamping the data
            last_updated_type: the date/time format for the last_updated_field.
                                Can be "datetime" or "isoformat"
            validator: Validator to validate documents going into the store
        """
        self.key = key
        self.last_updated_field = last_updated_field
        self.last_updated_type = last_updated_type
        self._lu_func = (
            LU_KEY_ISOFORMAT
            if DateTimeFormat(last_updated_type) == DateTimeFormat.IsoFormat
            else (identity, identity)
        )  # type: Tuple[Callable, Callable]
        self.validator = validator
        self.logger = logging.getLogger(type(self).__name__)
        self.logger.addHandler(logging.NullHandler())

    @abstractproperty
    def _collection(self):
        """
        Returns a handle to the pymongo collection object
        """

    @abstractproperty
    def name(self) -> str:
        """
        Return a string representing this data source
        """

    @abstractmethod
    def connect(self, force_reset: bool = False):
        """
        Connect to the source data

        Args:
            force_reset: whether to reset the connection or not
        """

    @abstractmethod
    def close(self):
        """
        Closes any connections
        """

    @abstractmethod
    def count(self, criteria: Optional[Dict] = None) -> int:
        """
        Counts the number of documents matching the query criteria

        Args:
            criteria: PyMongo filter for documents to count in
        """

    @abstractmethod
    def query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Dict]:
        """
        Queries the Store for a set of documents

        Args:
            criteria: PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip
            limit: limit on total number of documents returned
        """

    @abstractmethod
    def update(self, docs: Union[List[Dict], Dict], key: Union[List, str, None] = None):
        """
        Update documents into the Store

        Args:
            docs: the document or list of documents to update
            key: field name(s) to determine uniqueness for a
                 document, can be a list of multiple fields,
                 a single field, or None if the Store's key
                 field is to be used
        """

    @abstractmethod
    def ensure_index(self, key: str, unique: bool = False) -> bool:
        """
        Tries to create an index and return true if it suceeded

        Args:
            key: single key to index
            unique: Whether or not this index contains only unique keys

        Returns:
            bool indicating if the index exists/was created
        """

    @abstractmethod
    def groupby(
        self,
        keys: Union[List[str], str],
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Tuple[Dict, List[Dict]]]:
        """
        Simple grouping function that will group documents
        by keys.

        Args:
            keys: fields to group documents
            criteria: PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip
            limit: limit on total number of documents returned

        Returns:
            generator returning tuples of (dict, list of docs)
        """

    @abstractmethod
    def remove_docs(self, criteria: Dict):
        """
        Remove docs matching the query dictionary

        Args:
            criteria: query dictionary to match
        """

    def query_one(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
    ):
        """
        Queries the Store for a single document

        Args:
            criteria: PyMongo filter for documents to search
            properties: properties to return in the document
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
        """
        return next(
            self.query(criteria=criteria, properties=properties, sort=sort), None
        )

    def distinct(
        self, field: str, criteria: Optional[Dict] = None, all_exist: bool = False
    ) -> List:
        """
        Get all distinct values for a field

        Args:
            field: the field(s) to get distinct values for
            criteria: PyMongo filter for documents to search in
        """
        criteria = criteria or {}

        results = [
            key for key, _ in self.groupby(field, properties=[field], criteria=criteria)
        ]
        results = [get(r, field) for r in results]
        return results

    @property
    def last_updated(self) -> datetime:
        """
        Provides the most recent last_updated date time stamp from
        the documents in this Store
        """
        doc = next(
            self.query(
                properties=[self.last_updated_field],
                sort={self.last_updated_field: -1},
                limit=1,
            ),
            None,
        )
        if doc and not has(doc, self.last_updated_field):
            raise StoreError(
                f"No field '{self.last_updated_field}' in store document. Please ensure Store.last_updated_field "
                "is a datetime field in your store that represents the time of "
                "last update to each document."
            )
        elif not doc or get(doc, self.last_updated_field) is None:
            # Handle when collection has docs but `NoneType` last_updated_field.
            return datetime.min
        else:
            return self._lu_func[0](get(doc, self.last_updated_field))

    def newer_in(
        self, target: "Store", criteria: Optional[Dict] = None, exhaustive: bool = False
    ) -> List[str]:
        """
        Returns the keys of documents that are newer in the target
        Store than this Store.

        Args:
            target: target Store to
            criteria: PyMongo filter for documents to search in
            exhaustive: triggers an item-by-item check vs. checking
                        the last_updated of the target Store and using
                        that to filter out new items in
        """
        self.ensure_index(self.key)
        self.ensure_index(self.last_updated_field)

        if exhaustive:
            # Get our current last_updated dates for each key value
            props = {self.key: 1, self.last_updated_field: 1, "_id": 0}
            dates = {
                d[self.key]: self._lu_func[0](
                    d.get(self.last_updated_field, datetime.max)
                )
                for d in self.query(properties=props)
            }

            # Get the last_updated for the store we're comparing with
            props = {target.key: 1, target.last_updated_field: 1, "_id": 0}
            target_dates = {
                d[target.key]: target._lu_func[0](
                    d.get(target.last_updated_field, datetime.min)
                )
                for d in target.query(criteria=criteria, properties=props)
            }

            new_keys = set(target_dates.keys()) - set(dates.keys())
            updated_keys = {
                key
                for key, date in dates.items()
                if target_dates.get(key, datetime.min) > date
            }

            return list(new_keys | updated_keys)

        else:
            criteria = {
                self.last_updated_field: {"$gt": self._lu_func[1](self.last_updated)}
            }
            return target.distinct(field=self.key, criteria=criteria)

    @deprecated(message="Please use Store.newer_in")
    def lu_filter(self, targets):
        """Creates a MongoDB filter for new documents.

        By "new", we mean documents in this Store that were last updated later
        than any document in targets.

        Args:
            targets (list): A list of Stores

        """
        if isinstance(targets, Store):
            targets = [targets]

        lu_list = [t.last_updated for t in targets]
        return {self.last_updated_field: {"$gt": self._lu_func[1](max(lu_list))}}

    @deprecated(message="Use Store.newer_in")
    def updated_keys(self, target, criteria=None):
        """
        Returns keys for docs that are newer in the target store in comparison
        with this store when comparing the last updated field (last_updated_field)

        Args:
            target (Store): store to look for updated documents
            criteria (dict): mongo query to limit scope

        Returns:
            list of keys that have been updated in target store
        """
        self.ensure_index(self.key)
        self.ensure_index(self.last_updated_field)

        return self.newer_in(target, criteria=criteria)

    def __ne__(self, other):
        return not self == other

    def __getstate__(self):
        return self.as_dict()

    def __setstate__(self, d):
        d = {k: v for k, v in d.items() if not k.startswith("@")}
        d = MontyDecoder().process_decoded(d)
        self.__init__(**d)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()


class StoreError(Exception):
    """General Store-related error"""

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
