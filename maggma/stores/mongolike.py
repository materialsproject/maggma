# coding: utf-8
"""
Module containing various definitions of Stores.
Stores are a default access pattern to data and provide
various utillities
"""
from __future__ import annotations

import json

from typing import Union, Optional, Dict, List, Iterator, Tuple

import mongomock

from itertools import groupby
from operator import itemgetter
from pymongo import MongoClient
from pydash import set_

from pymongo import ReplaceOne

from monty.json import jsanitize
from monty.io import zopen
from monty.serialization import loadfn
from monty.dev import deprecated
from maggma.utils import confirm_field_index

from maggma.core import Store, Sort, StoreError


class MongoStore(Store):
    """
    A Store that connects to a Mongo collection
    """

    def __init__(
        self,
        database: str,
        collection_name: str,
        host: str = "localhost",
        port: int = 27017,
        username: str = "",
        password: str = "",
        **kwargs,
    ):
        """
        Args:
            database: The database name
            collection: The collection name
            host: Hostname for the database
            port: TCP port to connect to
            username: Username for the collection
            password: Password to connect with
        """
        self.database = database
        self._collection_name = collection_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._collection = None
        self.kwargs = kwargs
        super().__init__(**kwargs)

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data
        """
        if not self._collection or force_reset:
            conn = MongoClient(self.host, self.port)
            db = conn[self.database]
            if self.username != "":
                db.authenticate(self.username, self.password)
            self._collection = db[self._collection_name]

    def __hash__(self):
        return hash((self.database, self._collection_name, self.last_updated_field))

    @classmethod
    def from_db_file(cls, filename: str):
        """
        Convenience method to construct MongoStore from db_file
        from old QueryEngine format
        """
        kwargs = loadfn(filename)
        if "collection" in kwargs:
            kwargs["collection_name"] = kwargs.pop("collection")
        # Get rid of aliases from traditional query engine db docs
        kwargs.pop("aliases", None)
        return cls(**kwargs)

    def groupby(
        self,
        keys: Union[List[str], str],
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Sort]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Tuple[Dict, List[Dict]]]:
        """
        Simple grouping function that will group documents
        by keys.

        Args:
            keys: fields to group documents
            criteria : PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields
            skip: number documents to skip
            limit: limit on total number of documents returned

        Returns:
            generator returning tuples of (key, list of docs)
        """
        pipeline = []
        if criteria is not None:
            pipeline.append({"$match": criteria})

        if properties is not None:
            pipeline.append({"$project": {p: 1 for p in properties}})

        if isinstance(keys, str):
            keys = [keys]

        group_id = {}
        for key in keys:
            set_(group_id, key, "${}".format(key))
        pipeline.append({"$group": {"_id": group_id, "docs": {"$push": "$$ROOT"}}})

        for d in self._collection.aggregate(pipeline, allowDiskUse=True):
            yield (d["_id"], d["docs"])

    @classmethod
    def from_collection(cls, collection):
        """
        Generates a MongoStore from a pymongo collection object
        This is not a fully safe operation as it gives dummy information to the MongoStore
        As a result, this will not serialize and can not reset its connection
        """
        # TODO: How do we make this safer?
        coll_name = collection.name
        db_name = collection.database.name

        store = cls(db_name, coll_name)
        store._collection = collection
        return store

    @property
    @deprecated(message="This will be removed in the future")
    def collection(self):
        if self._collection is None:
            raise StoreError("Must connect Mongo-like store before attemping to use it")
        return self._collection

    def query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Sort]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Dict]:
        """
        Queries the Store for a set of documents

        Args:
            criteria : PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields
            skip: number documents to skip
            limit: limit on total number of documents returned
        """
        if isinstance(properties, list):
            properties = {p: 1 for p in properties}
        for d in self._collection.find(
            filter=criteria, projection=properties, skip=skip, limit=limit
        ):
            yield d

    def ensure_index(self, key: str, unique: Optional[bool] = False) -> bool:
        """
        Tries to create an index and return true if it suceeded
        Args:
            key: single key to index
            unique: Whether or not this index contains only unique keys

        Returns:
            bool indicating if the index exists/was created
        """

        if confirm_field_index(self._collection, key):
            return True
        else:
            try:
                self._collection.create_index(key, unique=unique, background=True)
                return True
            except Exception:
                return False

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

        requests = []

        if not isinstance(docs, list):
            docs = [docs]

        for d in docs:

            d = jsanitize(d, allow_bson=True)

            # document-level validation is optional
            validates = True
            if self.validator:
                validates = self.validator.is_valid(d)
                if not validates:
                    if self.validator.strict:
                        raise ValueError(self.validator.validation_errors(d))
                    else:
                        self.logger.error(self.validator.validation_errors(d))

            if validates:
                key = key or self.key
                if isinstance(key, list):
                    search_doc = {k: d[k] for k in key}
                else:
                    search_doc = {key: d[key]}

                requests.append(ReplaceOne(search_doc, d, upsert=True))

        self._collection.bulk_write(requests, ordered=False)

    def close(self):
        self._collection.database.client.close()


class MemoryStore(MongoStore):
    """
    An in-memory Store that functions similarly
    to a MongoStore
    """

    def __init__(self, name: str = "memory_db", **kwargs):
        self.name = name
        self._collection = None
        self.kwargs = kwargs
        super().__init__(**kwargs)

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data
        """
        if not self._collection or force_reset:
            self._collection = mongomock.MongoClient().db[self.name]

    def __hash__(self):
        return hash((self.name, self.last_updated_field))

    def groupby(
        self,
        keys: Union[List[str], str],
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Sort]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Tuple[Dict, List[Dict]]]:
        """
        Simple grouping function that will group documents
        by keys.

        Args:
            keys: fields to group documents
            criteria : PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields
            skip: number documents to skip
            limit: limit on total number of documents returned

        Returns:
            generator returning tuples of (key, list of elemnts)
        """
        keys = keys if isinstance(keys, list) else [keys]

        input_data = list(self.query(properties=keys, criteria=criteria))

        if len(keys) > 1:
            grouper = itemgetter(*keys)
            for vals, grp in groupby(sorted(input_data, key=grouper), grouper):
                yield {k: v for k, v in zip(keys, vals)}, list(grp)
        else:
            grouper = itemgetter(*keys)
            for val, group in groupby(sorted(input_data, key=grouper), grouper):
                yield {keys[0]: val}, list(group)

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

        for d in docs:

            d = jsanitize(d, allow_bson=True)

            # document-level validation is optional
            validates = True
            if self.validator:
                validates = self.validator.is_valid(d)
                if not validates:
                    if self.validator.strict:
                        raise ValueError(self.validator.validation_errors(d))
                    else:
                        self.logger.error(self.validator.validation_errors(d))

            if validates:
                if isinstance(key, list):
                    search_doc = {k: d[k] for k in key}
                elif key:
                    search_doc = {key: d[key]}
                else:
                    search_doc = {self.key: d[self.key]}

                self._collection.update_one(d, criteria=search_doc)


class JSONStore(MemoryStore):
    """
    A Store for access to a single or multiple JSON files
    """

    def __init__(self, paths, **kwargs):
        """
        Args:
            paths (str or list): paths for json files to
                turn into a Store
        """
        paths = paths if isinstance(paths, (list, tuple)) else [paths]
        self.paths = paths
        self.kwargs = kwargs
        super().__init__("collection", **kwargs)

    def connect(self, force_reset=False):
        super().connect(force_reset=force_reset)
        for path in self.paths:
            with zopen(path) as f:
                data = f.read()
                data = data.decode() if isinstance(data, bytes) else data
                objects = json.loads(data)
                objects = [objects] if not isinstance(objects, list) else objects
                self.update(objects)

    def __hash__(self):
        return hash((*self.paths, self.last_updated_field))


class DatetimeStore(MemoryStore):
    """Utility store intended for use with `Store.lu_filter`."""

    def __init__(self, dt, **kwargs):
        """
        Args:
            dt (Datetime): Datetime to set
        """
        self.__dt = dt
        self.kwargs = kwargs
        super().__init__("date", **kwargs)

    def connect(self, force_reset=False):
        super().connect(force_reset)
        self._collection.insert_one({self.last_updated_field: self.__dt})
