from collections.abc import Iterator
from functools import partial
from multiprocessing.managers import BaseManager
from threading import Lock
from typing import Any, Callable, Optional, Union

from monty.json import MontyDecoder

from maggma.core.store import Sort, Store


class StoreFacade(Store):
    """
    This class provides a way to access a single store within a MultiStore with the
    same attributes as any ordinary maggma store.

    ```
    # Create the multistore
    multistore = MultiStore()

    # Add a store to the multistore and create a facade to access it
    first_store = StoreFacade(MongoStore(..., collection_name="collection_one"),
                                   multistore)

    # Add a second store to the multistore and create a facade to access it
    second_store = StoreFacade(MongoStore(..., collection_name="collection_two"),
                                    multistore)

    # Attempt to add a duplicate store and create a facade to access an equivalent store
    third_store = StoreFacade(MongoStore(..., collection_name="collection_two"),
                                   multistore)

    multistore.count_stores() # Returns 2, since only 2 unique stores were added

    # We can then use the stores as we would normally do
    first_store.query(criteria={}, properties=[])
    second_store.update()
    third_store.remove_docs()
    ```
    """

    def __init__(self, store, multistore):
        # Keep track of this store for the purposes of checking
        # equality, but it will never be connected to
        self.store = store

        self.multistore = multistore
        self.multistore.ensure_store(self.store)

    def __getattr__(self, name: str) -> Any:
        if name not in dir(self):
            return self.multistore._proxy_attribute(name, self.store)
        return None

    def __setattr__(self, name: str, value: Any):
        if name not in ["store", "multistore"]:
            self.multistore.set_store_attribute(self.store, name, value)
        else:
            super().__setattr__(name, value)

    @property
    def _collection(self):
        """
        Returns a handle to the pymongo collection object.
        """
        return self.multistore.store_collection(self.store)

    @property
    def name(self) -> str:
        """
        Return a string representing this data source.
        """
        return self.multistore.store_name(self.store)

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data.

        Args:
            force_reset: whether to reset the connection or not when the Store is
                already connected.
        """
        self.multistore.connect(self.store, force_reset=force_reset)

    def close(self):
        """
        Closes any connections.
        """
        self.multistore.close(self.store)

    def count(self, criteria: Optional[dict] = None) -> int:
        """
        Counts the number of documents matching the query criteria.

        Args:
            criteria: PyMongo filter for documents to count in
        """
        return self.multistore.count(self.store, criteria=criteria)

    def query(
        self,
        criteria: Optional[dict] = None,
        properties: Union[dict, list, None] = None,
        sort: Optional[dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[dict]:
        """
        Queries the Store for a set of documents.

        Args:
            criteria: PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip
            limit: limit on total number of documents returned
        """
        return self.multistore.query(
            self.store,
            criteria=criteria,
            properties=properties,
            sort=sort,
            skip=skip,
            limit=limit,
        )

    def update(self, docs: Union[list[dict], dict], key: Union[list, str, None] = None, **kwargs):
        """
        Update documents into the Store.

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
        Tries to create an index and return true if it succeeded.

        Args:
            key: single key to index
            unique: Whether or not this index contains only unique keys

        Returns:
            bool indicating if the index exists/was created
        """
        return self.multistore.ensure_index(self.store, key=key, unique=unique, **kwargs)

    def groupby(
        self,
        keys: Union[list[str], str],
        criteria: Optional[dict] = None,
        properties: Union[dict, list, None] = None,
        sort: Optional[dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
        **kwargs,
    ) -> Iterator[tuple[dict, list[dict]]]:
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
        return self.multistore.groupby(
            self.store, keys=keys, criteria=criteria, properties=properties, sort=sort, skip=skip, limit=limit, **kwargs
        )

    def remove_docs(self, criteria: dict, **kwargs):
        """
        Remove docs matching the query dictionary.

        Args:
            criteria: query dictionary to match
        """
        return self.multistore.remove_docs(self.store, criteria=criteria, **kwargs)

    def query_one(
        self,
        criteria: Optional[dict] = None,
        properties: Union[dict, list, None] = None,
        sort: Optional[dict[str, Union[Sort, int]]] = None,
        **kwargs,
    ):
        """
        Queries the Store for a single document.

        Args:
            criteria: PyMongo filter for documents to search
            properties: properties to return in the document
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
        """
        return self.multistore.query_one(self.store, criteria=criteria, properties=properties, sort=sort, **kwargs)

    def distinct(self, field: str, criteria: Optional[dict] = None, all_exist: bool = False, **kwargs) -> list:
        """
        Get all distinct values for a field.

        Args:
            field: the field(s) to get distinct values for
            criteria: PyMongo filter for documents to search in
        """
        return self.multistore.distinct(self.store, field=field, criteria=criteria, all_exist=all_exist, **kwargs)


class MultiStore:
    """
    A container for multiple maggma stores. When adding stores to a MultiStore,
    a check will be performed to see if the store (or a equivalent) already exists.
    If it does, it will not be added again. This enables the caching of Stores with the
    intent of pooling connections to make sure that the same connection is used
    when accessing as store many times (from different processes).

    Notes:
        1) While this class implements the abstract methods of a Store, it is not a store.
           The additional `store` argument does not conform with the abstract base class.
        2) The stores should not be directly accessed via MultiStore()._stores.
           The MultiStore must be used with the StoreFacade class, which is consistent
           with other Stores.

    An example of usage is as follows:
    ```
    # Create the multistore
    multistore = MultiStore()

    # Add a store to the multistore
    first_store = MongoStore(..., collection_name="collection_one")
    multistore.ensure_store(first_store)
    multistore.count_stores() # Returns 1, since there is one store added

    # Add a second store to the multistore
    second_store = MongoStore(..., collection_name="collection_two")
    multistore.ensure_store(second_store)
    multistore.count_stores() # Returns 2, since there are two stores added

    # Attempt to add a duplicate store
    third_store = MongoStore(..., collection_name="collection_two")
    multistore.ensure_store(second_store) # The store will not be added since it already exists
    multistore.count_stores() # Returns 2
    ```
    """

    def __init__(self, **kwargs):
        """
        Initializes a MultiStore.
        """
        # Keep a list of stores, since there is no way to hash a store (to use a dict)
        self._stores = []
        self._multistore_lock = Lock()
        super().__init__(**kwargs)

    def get_store_index(self, store: Store) -> Optional[int]:
        """
        Gets the index of the store in the list of stores.
        If it doesn't exist, returns None.

        Note: this is not a search for an instance of a store,
            but rather a search for a equivalent store

        Args:
            store: The store to find

        Returns:
            The index of the store in the internal list of cached stores
        """
        # check host, port, name, and username
        for i, _store in enumerate(self._stores):
            if store == _store:
                return i
        return None

    def add_store(self, store: Store):
        """
        Adds a store to the list of cached stores.

        Args:
            store: The store to cache

        Returns:
            True if the store was added or if it already exists
        """
        # Check that the store is actually a store
        if not isinstance(store, Store):
            raise TypeError("store must be a Store")

        # We are writing to the _stores list, so a lock
        # must be used to ensure no simultaneous writes
        with self._multistore_lock:
            # Check if the store exists, just a double check
            # in case another process added it before this lock was acquired
            maybe_store_exists = self.get_store_index(store)
            if maybe_store_exists is None:
                # Make a new instance of it, so it doesn't get
                # modified outside of this process unintentionally
                self._stores.append(MontyDecoder().process_decoded(store.as_dict()))
                self._stores[-1].connect()
                return True

            # Store already exists, we don't need to add it
            return True

    def ensure_store(self, store: Store) -> bool:
        """
        Tries to add the store to the list of cached stores and return true
        if it succeeded.

        Args:
            store: The store to cache

        Returns:
            bool indicating if the store exists/was created
        """
        if self.get_store_index(store) is None:
            # Store doesn't exist here, we should add it
            return self.add_store(store)
        return True

    def count_stores(self) -> int:
        """
        Returns the number of stores in the multistore.

        Returns:
            int indicating the number of stores
        """
        return len(self._stores)

    # These are maggma stores attributes we must provide access to
    def store_collection(self, store):
        store_id = self.get_store_index(store)
        return self._stores[store_id]._collection

    def store_name(self, store) -> str:
        store_id = self.get_store_index(store)
        return self._stores[store_id].name

    def connect(self, store, force_reset: bool = False):
        """
        For a given store, connect to the source data.

        Args:
            store: the store to connect to the source data
            force_reset: whether to reset the connection or not when the Store is
                already connected.
        """
        with self._multistore_lock:
            store_id = self.get_store_index(store)
            self._stores[store_id].connect(force_reset)

    def close(self, store: Store):
        """
        For a given store, close any connections.

        Args:
            store: the store to close connections to
        """
        with self._multistore_lock:
            store_id = self.get_store_index(store)
            self._stores[store_id].close()

    def connect_all(self, force_reset: bool = False):
        """
        Connects to all stores.

        Args:
            force_reset: whether to reset the connection or not when the Store is
                already connected.
        """
        with self._multistore_lock:
            for store in self._stores:
                store.connect(force_reset)

    def close_all(self):
        """
        Closes all connections.
        """
        with self._multistore_lock:
            for store in self._stores:
                store.close()

    def count(self, store: Store, criteria: Optional[dict] = None, **kwargs) -> int:
        """
        Counts the number of documents matching the query criteria.

        Args:
            criteria: PyMongo filter for documents to count in
        """
        store_id = self.get_store_index(store)
        return self._stores[store_id].count(criteria=criteria, **kwargs)

    def query(
        self,
        store: Store,
        criteria: Optional[dict] = None,
        properties: Union[dict, list, None] = None,
        sort: Optional[dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
        **kwargs,
    ) -> list[dict]:
        """
        Queries the Store for a set of documents.

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
        return list(
            self._stores[store_id].query(
                criteria=criteria, properties=properties, sort=sort, skip=skip, limit=limit, **kwargs
            )
        )

    def update(self, store: Store, docs: Union[list[dict], dict], key: Union[list, str, None] = None, **kwargs):
        """
        Update documents into the Store.

        Args:
            docs: the document or list of documents to update
            key: field name(s) to determine uniqueness for a
                 document, can be a list of multiple fields,
                 a single field, or None if the Store's key
                 field is to be used
        """
        store_id = self.get_store_index(store)
        return self._stores[store_id].update(docs=docs, key=key, **kwargs)

    def ensure_index(self, store: Store, key: str, unique: bool = False, **kwargs) -> bool:
        """
        Tries to create an index and return true if it succeeded.

        Args:
            key: single key to index
            unique: Whether or not this index contains only unique keys

        Returns:
            bool indicating if the index exists/was created
        """
        store_id = self.get_store_index(store)
        return self._stores[store_id].ensure_index(key=key, unique=unique, **kwargs)

    def groupby(
        self,
        store: Store,
        keys: Union[list[str], str],
        criteria: Optional[dict] = None,
        properties: Union[dict, list, None] = None,
        sort: Optional[dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
        **kwargs,
    ) -> Iterator[tuple[dict, list[dict]]]:
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
        return self._stores[store_id].groupby(
            keys=keys, criteria=criteria, properties=properties, sort=sort, skip=skip, limit=limit, **kwargs
        )

    def remove_docs(self, store: Store, criteria: dict, **kwargs):
        """
        Remove docs matching the query dictionary.

        Args:
            criteria: query dictionary to match
        """
        store_id = self.get_store_index(store)
        return self._stores[store_id].remove_docs(criteria=criteria, **kwargs)

    def query_one(
        self,
        store: Store,
        criteria: Optional[dict] = None,
        properties: Union[dict, list, None] = None,
        sort: Optional[dict[str, Union[Sort, int]]] = None,
        **kwargs,
    ):
        """
        Queries the Store for a single document.

        Args:
            criteria: PyMongo filter for documents to search
            properties: properties to return in the document
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
        """
        store_id = self.get_store_index(store)
        return next(
            self._stores[store_id].query(criteria=criteria, properties=properties, sort=sort, **kwargs),
            None,
        )

    def distinct(
        self, store: Store, field: str, criteria: Optional[dict] = None, all_exist: bool = False, **kwargs
    ) -> list:
        """
        Get all distinct values for a field.

        Args:
            field: the field(s) to get distinct values for
            criteria: PyMongo filter for documents to search in
        """
        store_id = self.get_store_index(store)
        return self._stores[store_id].distinct(field=field, criteria=criteria, all_exist=all_exist, **kwargs)

    def set_store_attribute(self, store: Store, name: str, value: Any):
        """
        A method to set an attribute of a store.

        Args:
            name: The name of a function or attribute to access
            store: The store to access the attribute of
            value: New value of the attribute
        """
        store_id = self.get_store_index(store)
        setattr(self._stores[store_id], name, value)

    def call_attr(self, name: str, store: Store, **kwargs):
        """
        This class will actually call an attribute/method on the class instance.

        Args:
            name: The name of a function or attribute to access
            store: The store to access the attribute of

        Returns:
            The result of the attribute or function call
        """
        store_id = self.get_store_index(store)
        return getattr(self._stores[store_id], name)(**kwargs)

    def _proxy_attribute(self, name: str, store) -> Union[Any, Callable]:
        """
        This function will take care of the StoreFacade accessing attributes
        or functions of the store that are not required by the Store abstract
        class.

        Args:
            name: The name of a function or attribute to access
            store: The store to access the attribute of

        Returns:
            The attribute or a partial function which gives access to
            the attribute
        """
        store_id = self.get_store_index(store)
        maybe_fn = getattr(self._stores[store_id], name)
        if callable(maybe_fn):
            return partial(self.call_attr, name=name, store=store)
        return maybe_fn


class MultiStoreManager(BaseManager):
    """
    Provide a server that can host shared objects between multiprocessing
    Processes (that normally can't share data). For example, a common MultiStore is
    shared between processes and access is coordinated to limit DB hits.

    # Adapted from fireworks/utilities/fw_utilities.py
    """

    @classmethod
    def setup(cls, multistore):
        """
        Args:
            multistore: A multistore to share between processes.

        Returns:
            A manager
        """
        MultiStoreManager.register("MultiStore", callable=lambda: multistore)
        m = MultiStoreManager(address=("127.0.0.1", 0), authkey=b"abcd")
        m.start()
        return m
