from abc import ABCMeta, abstractmethod
from datetime import datetime
import json

import mongomock
import pymongo
from pymongo import MongoClient
from pydash import identity

from monty.json import MSONable
from monty.io import zopen


class Store(MSONable, metaclass=ABCMeta):
    """
    Abstract class for a data Store
    Defines the interface for all data going in and out of a Builder
    """

    def __init__(self, key="task_id", lu_field='_lu', lu_key=(identity, identity)):
        """
        Args:
            lu_field (str): 'last updated' field name
            lu_key (tuple): A pair of key functions to map
                self.lu_field to a `datetime` and back, respectively.
        """
        self.key = key
        self.lu_field = lu_field
        self.lu_key = lu_key

    @property
    @abstractmethod
    def collection(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def query(self, properties=None, criteria=None, **kwargs):
        pass

    @abstractmethod
    def distinct(self, key, criteria=None, **kwargs):
        pass

    @abstractmethod
    def update(self, docs, update_lu=True):
        pass

    @abstractmethod
    def ensure_index(self, key, unique=False):
        """Wrapper for pymongo.Collection.ensure_index
        """
        pass

    @property
    def last_updated(self):
        doc = next(self.query(properties=[self.lu_field]).sort(
            [(self.lu_field, pymongo.DESCENDING)]).limit(1), None)
        # Handle when collection has docs but `NoneType` lu_field.
        return (doc[self.lu_field] if (doc and doc[self.lu_field])
                else datetime.min)

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
        return {self.lu_field: {"$gt": self.lu_key[1](max(lu_list))}}

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash((self.lu_field,))


class MongoStore(Store):
    """
    A Store that connects to any Mongo collection
    """

    def __init__(self, database, collection_name, host="localhost", port=27017,
                 username="", password="", **kwargs):
        """

        Args:
            database (str): db name
            collection (str): collection name
            host (str):
            port (int):
            username (str):
            password (str):
            lu_field (str): see Store doc
        """
        self.database = database
        self.collection_name = collection_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.__collection = None
        self.kwargs = kwargs
        super(MongoStore, self).__init__(**kwargs)

    @property
    def collection(self):
        return self.__collection

    def connect(self):
        conn = MongoClient(self.host, self.port)
        db = conn[self.database]
        if self.username is not "":
            db.authenticate(self.username, self.password)
        self.__collection = db[self.collection_name]

    def __hash__(self):
        return hash((self.collection_name, self.lu_field))

    def query(self, properties=None, criteria=None, **kwargs):
        if isinstance(properties,list):
            properties = {p: 1 for p in properties}
        return self.collection.find(filter=criteria, projection=properties, **kwargs)

    def distinct(self, key, criteria=None, **kwargs):
        return self.collection.distinct(key, filter=criteria, **kwargs)

    def update(self, docs, update_lu=True):
        bulk = self.collection.initialize_ordered_bulk_op()

        for d in docs:
            if update_lu:
                d[self.lu_field] = datetime.utcnow()
            bulk.find({self.key: d[self.key]}).upsert().replace_one(d)
        bulk.execute()

    def ensure_index(self, key, unique=False):
        """Wrapper for pymongo.Collection.ensure_index
        """
        return self.collection.create_index(key, unique=unique, background=True)

    def close(self):
        self.collection.close()


class MemoryStore(Store):
    """
    An in memory Store
    """

    def __init__(self, name, **kwargs):
        self.name = name
        self.__collection = None
        self.kwargs = kwargs
        super(MemoryStore, self).__init__(**kwargs)

    @property
    def collection(self):
        return self.__collection

    def connect(self):
        self.__collection = mongomock.MongoClient().db[self.name]

    def close(self):
        self.__collection.close()

    def __hash__(self):
        return hash((self.name, self.lu_field))

    def query(self, properties=None, criteria=None, **kwargs):
        if isinstance(properties,list):
            properties = {p: 1 for p in properties}
        return self.collection.find(filter=criteria, projection=properties, **kwargs)

    def distinct(self, key, criteria=None, **kwargs):
        return self.collection.distinct(key, filter=criteria, **kwargs)

    def ensure_index(self, key, unique=False):
        """Wrapper for pymongo.Collection.ensure_index
        """
        return self.collection.ensure_index(key, unique=unique, background=True)

    def update(self, docs, update_lu=True):
        bulk = self.__collection.initialize_ordered_bulk_op()

        for d in docs:
            d[self.lu_field] = datetime.utcnow()
            bulk.find(
                {self.key: d[self.key]}).upsert().replace_one(d)
        bulk.execute()


class JSONStore(MemoryStore):
    """
    A Store for access to a single or multiple JSON files
    """

    def __init__(self, paths, **kwargs):
        paths = paths if isinstance(paths, (list, tuple)) else [paths]
        self.paths = paths
        self.kwargs = kwargs
        super(JSONStore, self).__init__("collection", **kwargs)

    def connect(self):
        super(JSONStore, self).connect()
        for path in self.paths:
            with zopen(path) as f:
                data = f.read()
                data = data if not isinstance(data,bytes) else str(data)
                objects = json.loads(data)
                objects = [objects] if not isinstance(
                    objects, list) else objects
                self.collection.insert_many(objects)

    def __hash__(self):
        return hash((*self.paths, self.lu_field))


class DatetimeStore(MemoryStore):
    """Utility store intended for use with `Store.lu_filter`."""

    def __init__(self, dt, **kwargs):
        self.__dt = dt
        self.kwargs = kwargs
        super(DatetimeStore, self).__init__("date", **kwargs)

    def connect(self):
        super(DatetimeStore, self).connect()
        self.collection.insert_one({self.lu_field: self.__dt})
