from abc import ABCMeta, abstractmethod
import datetime
import json

import mongomock
import pymongo

from monty.json import MSONable


class Store(MSONable, metaclass=ABCMeta):
    """
    Abstract class for a data Store
    Defines the interface for all data going in and out of a Builder
    """

    def __init__(self, lu_field='_lu'):
        """
        Args:
            lu_field (str): 'last updated' field name
        """
        self.lu_field = lu_field

    @property
    @abstractmethod
    def collection(self):
        pass

    @abstractmethod
    def last_updated(self):
        pass

    def lu_fiter(self, targets):
        """
        Creates a filter string that can be applied to the source collection assuming targets is a list of Stores
        
        Args:
            targets
        """
        if isinstance(targets, Store):
            targets = [targets]

        lu_list = [t.last_updated for t in targets]
        return {self.lu_field: {"$gte": max(lu_list)}}

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

    def __init__(self, collection, lu_field='_lu'):
        self._collection = collection
        super(MongoStore, self).__init__(lu_field)

    @property
    def collection(self):
        return self._collection

    def last_updated(self):
        doc = next(self._collection.find({}, {"_id": 0, self.lu_field: 1}).sort(
            [(self.lu_field, pymongo.DESCENDING)]).limit(1), None)
        return doc[self.lu_field] if doc else datetime.datetime.min

    def __hash__(self):
        return hash((self._collection.name, self.lu_field))


class MemoryStore(MongoStore):
    """
    An in memory Store
    """

    def __init__(selfs, name, lu_field='_lu'):
        _collection = mongomock.MongoClient().db[name]
        super().__init__(_collection, lu_field)


class JSONStore(MemoryStore):
    """
    A Store for access to a single or multiple JSON files
    """

    def __init__(self, paths, lu_field='_lu'):
        paths = paths if isinstance(paths, (list, tuple)) else [paths]
        self.paths = paths
        super().__init__("collection", lu_field)

        for path in paths:
            with open(path) as f:
                objects = list(json.load(f))
                objects = [objects] if not isinstance(objects, list) else objects
                self.collection.insert_many(objects)

    def __hash__(self):
        return hash((self.path, self.lu_field))


class DatetimeStore(MongoStore):
    """Utility store intended for use with `Store.lu_filter`."""

    def __init__(self, dt, lu_field='_lu'):
        self._dt = dt
        collection = mongomock.MongoClient().db.collection
        collection.insert_one({lu_field: dt})
        super().__init__(collection, lu_field)
