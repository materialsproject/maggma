from abc import ABCMeta, abstractmethod
from datetime import datetime
import json

import mongomock
import pymongo
from pymongo import MongoClient
from pydash import identity

from monty.json import MSONable
from monty.io import zopen
from monty.serialization import loadfn


class Store(MSONable, metaclass=ABCMeta):
    """
    Abstract class for a data Store
    Defines the interface for all data going in and out of a Builder
    """

    def __init__(self, key="task_id", lu_field='last_updated', lu_key=(identity, identity)):
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
    def update(self, docs, update_lu=True,key=None):
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
            database (str): database name
            collection (str): collection name
            host (str): hostname for mongo db
            port (int): tcp port for mongo db
            username (str): username for mongo db
            password (str): password for mongo db
            lu_field (str): 'last updated' field name
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
        """
        Function that gets data from MongoStore with property focus.

        Args:
            properties (list or dict): list of properties to return
                or dictionary with {"property": 1} type structure
                from standard mongo Collection.find syntax
            criteria (dict): filter for query, matches documents
                against key-value pairs
            **kwargs (kwargs): further kwargs to Collection.find
        """
        if isinstance(properties, list):
            properties = {p: 1 for p in properties}
        return self.collection.find(filter=criteria, projection=properties, **kwargs)

    def distinct(self, key, criteria=None, **kwargs):
        """
        Function get to get
        Args:
            key (mongolike key or list of mongolike keys): key or keys
                for which to find distinct values or sets of values.
            criteria (filter criteria): criteria for filter 
            **kwargs (kwargs): kwargs corresponding to collection.distinct
        """
        if isinstance(key, list):
            agg_pipeline = [{"$match": criteria}] if criteria else []

            # use string ints as keys and replace later to avoid bug where periods
            # can't be in group keys, then reconstruct after
            group_op = {"$group": {"_id": {str(n): "${}".format(k) for n, k in enumerate(key)}}}
            agg_pipeline.append(group_op)
            results = [r['_id'] for r in self.collection.aggregate(agg_pipeline)]
            for result in results:
                for n in list(result.keys()):
                    result[key[int(n)]] = result.pop(n)

            # Return as document as partial matches are included
            return results

        else:
            return self.collection.distinct(key, filter=criteria, **kwargs)

    def update(self, docs, update_lu=True, key=None):
        """
        Function to update associated MongoStore collection.

        Args:
            docs: list of documents
        """
        # TODO: @montoyjh
        # - replaces docs rather than updates, maybe insert/update
        #       functionality should be separate?
        # - upsert maybe should be optional
        # - still a bit slow, not sure it takes full advantages of parallel

        key = key if key else self.key

        bulk = self.collection.initialize_ordered_bulk_op()

        for d in docs:
            if update_lu:
                d[self.lu_field] = datetime.utcnow()
            bulk.find({key: d[key]}).upsert().replace_one(d)
        bulk.execute()

    def ensure_index(self, key, unique=False):
        """Wrapper for pymongo.Collection.ensure_index
        """
        return self.collection.create_index(key, unique=unique, background=True)

    def close(self):
        self.collection.close()

    @classmethod
    def from_db_file(cls, filename):
        """
        Convenience method to construct MongoStore from db_file
        """
        kwargs = loadfn(filename)
        if "collection" in kwargs:
            kwargs["collection_name"] = kwargs.pop("collection")
        # Get rid of aliases from traditional query engine db docs
        kwargs.pop("aliases", None)
        return cls(**kwargs)


class MemoryStore(Store):
    """
    An in memory Store
    """
    # TODO: Isn't this exactly the same thing as a MongoStore,
    #           except for connect?

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
        if isinstance(properties, list):
            properties = {p: 1 for p in properties}
        return self.collection.find(filter=criteria, projection=properties, **kwargs)

    def distinct(self, key, criteria=None, **kwargs):
        if isinstance(key, list):
            agg_pipeline = [{"$match": criteria}] if criteria else []

            # use string ints as keys and replace later to avoid bug where periods
            # can't be in group keys, then reconstruct after
            group_op = {"$group": {"_id": {str(n): "${}".format(k) for n, k in enumerate(key)}}}
            agg_pipeline.append(group_op)
            results = self.collection.aggregate(agg_pipeline)
            results = [r['_id'] for r in results]
            for result in results:
                for n in result.keys():
                    result[key[int(n)]] = result.pop(n)

            # Return as document as partial matches are included
            return results
        else:
            return self.collection.distinct(key, filter=criteria, **kwargs)

    def ensure_index(self, key, unique=False):
        """Wrapper for pymongo.Collection.ensure_index
        """
        return self.collection.ensure_index(key, unique=unique, background=True)

    def update(self, docs, update_lu=True, key=None):
        key = key if key else self.key
        bulk = self.__collection.initialize_ordered_bulk_op()

        for d in docs:
            d[self.lu_field] = datetime.utcnow()
            bulk.find(
                {key: d[key]}).upsert().replace_one(d)
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
                data = data.decode() if isinstance(data, bytes) else data
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
