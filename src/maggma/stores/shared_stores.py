from multiprocessing.managers import BaseManager
import time
from monty.json import MontyDecoder
from threading import Lock
from maggma.core.store import Store, Sort, DateTimeFormat
from types import GeneratorType
from typing import Dict, Iterator, List, Optional, Tuple, Union
from pydash import get


class MultiStoreAccessor(Store):

    def __init__(self, store, multistore):
        # We keep this store here to check equality, but will never
        # connect to it
        self.store = store

        self.multistore = multistore

    @property
    def _collection(self):
        """
        Returns a handle to the pymongo collection object
        """
        return self.multistore.store_collection(self.store)

    @property
    def name(self) -> str:
        """
        Return a string representing this data source
        """
        return self.multistore.store_name(self.store)

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data

        Args:
            force_reset: whether to reset the connection or not
        """
        self.multistore.connect(self.store, force_reset=force_reset)

    def close(self):
        """
        Closes any connections
        """
        self.multistore.close(self.store)

    def count(self, criteria: Optional[Dict] = None) -> int:
        """
        Counts the number of documents matching the query criteria

        Args:
            criteria: PyMongo filter for documents to count in
        """
        return self.multistore.count(self.store, criteria=criteria)

    def query(self,
              criteria: Optional[Dict] = None,
              properties: Union[Dict, List, None] = None,
              sort: Optional[Dict[str, Union[Sort, int]]] = None,
              skip: int = 0,
              limit: int = 0,
              **kwargs) -> List[Dict]:
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
        return self.multistore.query(self.store,
                                     criteria=criteria,
                                     properties=properties,
                                     sort=sort,
                                     skip=skip,
                                     limit=limit,
                                     **kwargs)

    def update(self,
               docs: Union[List[Dict], Dict],
               key: Union[List, str, None] = None,
               **kwargs):
        """
        Update documents into the Store

        Args:
            docs: the document or list of documents to update
            key: field name(s) to determine uniqueness for a
                 document, can be a list of multiple fields,
                 a single field, or None if the Store's key
                 field is to be used
        """
        return self.multistore.update(self.store, docs=docs, key=key, **kwargs)

    def ensure_index(self, key: str, unique: bool = False, **kwargs) -> bool:
        """
        Tries to create an index and return true if it suceeded

        Args:
            key: single key to index
            unique: Whether or not this index contains only unique keys

        Returns:
            bool indicating if the index exists/was created
        """
        return self.multistore.ensure_index(self.store,
                                            key=key,
                                            unique=unique,
                                            **kwargs)

    def groupby(self,
                keys: Union[List[str], str],
                criteria: Optional[Dict] = None,
                properties: Union[Dict, List, None] = None,
                sort: Optional[Dict[str, Union[Sort, int]]] = None,
                skip: int = 0,
                limit: int = 0,
                **kwargs) -> Iterator[Tuple[Dict, List[Dict]]]:
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
        return self.multistore.groupby(self.store,
                                       keys=keys,
                                       criteria=criteria,
                                       properties=properties,
                                       sort=sort,
                                       skip=skip,
                                       limit=limit,
                                       **kwargs)

    def remove_docs(self, criteria: Dict, **kwargs):
        """
        Remove docs matching the query dictionary

        Args:
            criteria: query dictionary to match
        """
        return self.multistore.remove_docs(self.store,
                                           criteria=criteria,
                                           **kwargs)

    def query_one(self,
                  criteria: Optional[Dict] = None,
                  properties: Union[Dict, List, None] = None,
                  sort: Optional[Dict[str, Union[Sort, int]]] = None,
                  **kwargs):
        """
        Queries the Store for a single document

        Args:
            criteria: PyMongo filter for documents to search
            properties: properties to return in the document
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
        """
        return self.multistore.query_one(self.store,
                                         criteria=criteria,
                                         properties=properties,
                                         sort=sort,
                                         **kwargs)

    def distinct(self,
                 field: str,
                 criteria: Optional[Dict] = None,
                 all_exist: bool = False,
                 **kwargs) -> List:
        """
        Get all distinct values for a field

        Args:
            field: the field(s) to get distinct values for
            criteria: PyMongo filter for documents to search in
        """
        return self.multistore.distinct(self.store,
                                        field=field,
                                        criteria=criteria,
                                        **kwargs)


class MultiStore():
    """
    This class enables the caching of maggma Store instances. 
    This is useful in conjunction with a multiprocessing.Manager
    to utilize connection pooling.

    This implements the attributes templated in the maggma Store abstract class.
    Use this in 
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._stores = []
        self._multistore_lock = Lock()

    def get_store_index(self, store):
        # check host, port, name, and username
        for i, _store in enumerate(self._stores):
            if store == _store:
                return i
        return None

    def add_store(self, store):
        self._multistore_lock.acquire()

        # Check if the store exists, else just return it
        maybe_store_exists = self.get_store_index(store)
        if maybe_store_exists is None:
            # Make a new instance of it, so it doesn't get
            # modified outside of this process unintentionally
            self._stores.append(MontyDecoder().process_decoded(
                store.as_dict()))
            self._stores[-1].connect()

        self._multistore_lock.release()
        return True

    def ensure_store(self, store):
        if self.get_store_index(store) is None:
            # Store doesn't exist here, we should add it
            return self.add_store(store)
        return True

    def count_stores(self):
        return len(self._stores)

    # These are maggma stores attributes we must override
    @property
    def _collection(self):
        return None

    @property
    def name(self) -> str:
        return None

    def store_collection(self, store):
        store_id = self.get_store_index(store)
        return self._stores[store_id]._collection

    def store_name(self, store) -> str:
        store_id = self.get_store_index(store)
        return self._stores[store_id].name

    def connect(self, store, force_reset: bool = False):
        self._multistore_lock.acquire()
        store_id = self.get_store_index(store)
        self._stores[store_id].connect(force_reset)
        self._multistore_lock.release()

    def close(self, store):
        self._multistore_lock.acquire()
        store_id = self.get_store_index(store)
        self._stores[store_id].close()
        self._multistore_lock.release()

    def count(self,
              store: Store,
              criteria: Optional[Dict] = None,
              **kwargs) -> int:
        """
        Counts the number of documents matching the query criteria

        Args:
            criteria: PyMongo filter for documents to count in
        """
        store_id = self.get_store_index(store)
        return self._stores[store_id].count(criteria=criteria, **kwargs)

    def query(self,
              store: Store,
              criteria: Optional[Dict] = None,
              properties: Union[Dict, List, None] = None,
              sort: Optional[Dict[str, Union[Sort, int]]] = None,
              skip: int = 0,
              limit: int = 0,
              **kwargs) -> List[Dict]:
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
        store_id = self.get_store_index(store)
        # We must return a list, since a generator is not serializable
        return list(self._stores[store_id].query(criteria=criteria,
                                                 properties=properties,
                                                 sort=sort,
                                                 skip=skip,
                                                 limit=limit,
                                                 **kwargs))

    def update(self,
               store: Store,
               docs: Union[List[Dict], Dict],
               key: Union[List, str, None] = None,
               **kwargs):
        """
        Update documents into the Store

        Args:
            docs: the document or list of documents to update
            key: field name(s) to determine uniqueness for a
                 document, can be a list of multiple fields,
                 a single field, or None if the Store's key
                 field is to be used
        """
        store_id = self.get_store_index(store)
        return self._stores[store_id].update(docs=docs, key=key, **kwargs)

    def ensure_index(self,
                     store: Store,
                     key: str,
                     unique: bool = False,
                     **kwargs) -> bool:
        """
        Tries to create an index and return true if it suceeded

        Args:
            key: single key to index
            unique: Whether or not this index contains only unique keys

        Returns:
            bool indicating if the index exists/was created
        """
        store_id = self.get_store_index(store)
        return self._stores[store_id].ensure_index(key=key,
                                                   unique=unique,
                                                   **kwargs)

    def groupby(self,
                store: Store,
                keys: Union[List[str], str],
                criteria: Optional[Dict] = None,
                properties: Union[Dict, List, None] = None,
                sort: Optional[Dict[str, Union[Sort, int]]] = None,
                skip: int = 0,
                limit: int = 0,
                **kwargs) -> Iterator[Tuple[Dict, List[Dict]]]:
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
        store_id = self.get_store_index(store)
        return self._stores[store_id].groupby(keys=keys,
                                              criteria=criteria,
                                              properties=properties,
                                              sort=sort,
                                              skip=skip,
                                              limit=limit,
                                              **kwargs)

    def remove_docs(self, store: Store, criteria: Dict, **kwargs):
        """
        Remove docs matching the query dictionary

        Args:
            criteria: query dictionary to match
        """
        store_id = self.get_store_index(store)
        return self._stores[store_id].remove_docs(criteria=criteria, **kwargs)

    def query_one(self,
                  store: Store,
                  criteria: Optional[Dict] = None,
                  properties: Union[Dict, List, None] = None,
                  sort: Optional[Dict[str, Union[Sort, int]]] = None,
                  **kwargs):
        """
        Queries the Store for a single document

        Args:
            criteria: PyMongo filter for documents to search
            properties: properties to return in the document
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
        """
        store_id = self.get_store_index(store)
        return next(
            self._stores[store_id].query(criteria=criteria,
                                         properties=properties,
                                         sort=sort,
                                         **kwargs), None)

    def distinct(self,
                 store: Store,
                 field: str,
                 criteria: Optional[Dict] = None,
                 all_exist: bool = False,
                 **kwargs) -> List:
        """
        Get all distinct values for a field

        Args:
            field: the field(s) to get distinct values for
            criteria: PyMongo filter for documents to search in
        """
        store_id = self.get_store_index(store)
        self._stores[store_id].distinct(field=field,
                                        properties=[field],
                                        criteria=criteria,
                                        **kwargs)


class StoreServer(BaseManager):

    @classmethod
    def setup(cls, test_object):
        StoreServer.register("MultiStore", callable=lambda: test_object)
        m = StoreServer(address=("127.0.0.1", 0),
                        authkey=b'abc')  # random port
        m.start()
        return m
