from abc import ABCMeta, abstractmethod
import datetime
import json

import mongomock
import pymongo

from monty.json import MSONable


class Store(MSONable, metaclass=ABCMeta):

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
        Assuming targets is a list of stores

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


class MongoStore(Store):

    def __init__(self, collection, lu_field='_lu'):
        self._collection = collection
        super(MongoStore, self).__init__(lu_field)

    def collection(self):
        return self._collection

    def last_updated(self):
        doc = next(self._collection.find({}, {"_id": 0, self.lu_field: 1}).sort(
            [(self.lu_field, pymongo.DESCENDING)]).limit(1), None)
        return doc[self.lu_field] if doc else datetime.datetime.min

    def __hash__(self):
        return hash((self._collection.name, self.lu_field))


class JSONStore(MongoStore):
    def __init__(self, path, lu_field='_lu'):
        self.path = path
        _collection = mongomock.MongoCient().db.collection

        with open(path) as f:
            objects = list(json.load(f))
            objects = [objects] if not isinstance(objects, list) else objects
            _collection.insert_many(objects)

        super().__init__(_collection, lu_field)

    def __hash__(self):
        return hash((self.path, self.lu_field))


class DatetimeStore(MongoStore):
    """Utility store intended for use with `Store.lu_filter`."""

    def __init__(self, dt, lu_field='_lu'):
        self._dt = dt
        collection = mongomock.MongoClient().db.collection
        collection.insert_one({lu_field: dt})
        super().__init__(collection, lu_field)
