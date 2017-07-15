from abc import ABCMeta, abstractmethod, abstractproperty
import datetime
import json

import mongomock
import pymongo
from pymongo import MongoClient
from pydash import identity

from monty.json import MSONable



class Store(MSONable, metaclass=ABCMeta):
    """
    Abstract class for a data Store
    Defines the interface for all data going in and out of a Builder
    """

    def __init__(self, lu_field='_lu', lu_key=(identity, identity)):
        """
        Args:
            lu_field (str): 'last updated' field name
            lu_key (tuple): A pair of key functions to map
                self.lu_field to a `datetime` and back, respectively.
        """
        self.lu_field = lu_field
        self.lu_key = lu_key

    @property
    @abstractmethod
    def collection(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    def __call__(self):
        return self.collection

    @property
    def meta(self):
        return self.collection.db["{}.meta".format(self.collection.name)]

    @property
    def last_updated(self):
        doc = next(self.collection.find({}, {"_id": 0, self.lu_field: 1}).sort(
            [(self.lu_field, pymongo.DESCENDING)]).limit(1), None)
        return (doc[self.lu_field] if doc else datetime.datetime.min)

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
        return {self.lu_field: {"$gte": self.lu_key[1](max(lu_list))}}

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

    def __init__(self, database, collection,
                 host="localhost", port=27017, username="", password="",
                 **kwargs):
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
        self.collection_name = collection
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.__collection = None
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


class MemoryStore(Store):
    """
    An in memory Store
    """

    def __init__(self, name, **kwargs):
        self.name = name
        self.__collection = None
        super(MemoryStore, self).__init__(**kwargs)

    @property
    def collection(self):
        return self.__collection

    def connect(self):
        self.__collection = mongomock.MongoClient().db[self.name]

    def __hash__(self):
        return hash((self.name, self.lu_field))


class JSONStore(MemoryStore):
    """
    A Store for access to a single or multiple JSON files
    """

    def __init__(self, paths, **kwargs):
        paths = paths if isinstance(paths, (list, tuple)) else [paths]
        self.paths = paths
        super(JSONStore, self).__init__("collection", **kwargs)

    def connect(self):
        super(JSONStore, self).connect()
        for path in self.paths:
            with open(path) as f:
                objects = list(json.load(f))
                objects = [objects] if not isinstance(objects, list) else objects
                self.collection.insert_many(objects)

    def __hash__(self):
        return hash((self.path, self.lu_field))


class DatetimeStore(MemoryStore):
    """Utility store intended for use with `Store.lu_filter`."""

    def __init__(self, dt, **kwargs):
        self.__dt = dt
        super(DatetimeStore, self).__init__("date", **kwargs)

    def connect(self):
        super(DatetimeStore, self).connect()
        self.collection.insert_one({self.lu_field: self.__dt})
