from abc import ABCMeta, abstractmethod
from datetime import datetime
import json


import mongomock
import pymongo
import gridfs
from pymongo import MongoClient
from pydash import identity

from monty.json import MSONable, jsanitize
from monty.io import zopen
from monty.serialization import loadfn
from maggma.utils import LU_KEY_ISOFORMAT

class Store(MSONable, metaclass=ABCMeta):
    """
    Abstract class for a data Store
    Defines the interface for all data going in and out of a Builder
    """

    def __init__(self, key="task_id", lu_field='last_updated', lu_type="datetime"):
        """
        Args:
            key (str): master key to index on
            lu_field (str): 'last updated' field name
            lu_type (tuple): the date/time format for the lu_field
        """
        self.key = key
        self.lu_field = lu_field
        self.lu_type = lu_type
        self.lu_func = LU_KEY_ISOFORMAT if lu_type == "isoformat" else (identity,identity)

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
    def query(self, properties=None, criteria=None):
        pass

    @abstractmethod
    def query_one(self, properties=None, criteria=None):
        pass

    @abstractmethod
    def distinct(self, key, criteria=None):
        pass

    @abstractmethod
    def update(self, docs, update_lu=True, key=None):
        pass

    @abstractmethod
    def ensure_index(self, key, unique=False):
        pass

    @property
    def last_updated(self):
        doc = next(self.query(properties=[self.lu_field]).sort(
            [(self.lu_field, pymongo.DESCENDING)]).limit(1), None)
        # Handle when collection has docs but `NoneType` lu_field.
        return (self.lu_func[0](doc[self.lu_field]) if (doc and doc[self.lu_field])
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
        return {self.lu_field: {"$gt": self.lu_func[1](max(lu_list))}}

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash((self.lu_field,))


class Mongolike(object):
    """
    Mixin class that allows for basic mongo functionality
    """

    @property
    def collection(self):
        return self._collection

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
        return self.collection.find(filter=criteria, projection=properties,
                                    **kwargs)

    def query_one(self, properties=None, criteria=None, **kwargs):
        """
        Function that gets a single from MongoStore with property focus.
        Returns None if nothing matches

        Args:
            properties (list or dict): list of properties to return
                or dictionary with {"property": 1} type structure
                from standard mongo Collection.find syntax
            criteria (dict): filter for query, matches documents
                against key-value pairs
            **kwargs (kwargs): further kwargs to Collection.find_one
        """
        if isinstance(properties, list):
            properties = {p: 1 for p in properties}
        return self.collection.find_one(filter=criteria, projection=properties,
                                        **kwargs)

    def distinct(self, key, criteria=None, all_exist=False, **kwargs):
        """
        Function get to get all distinct values of a certain key in
        a mongolike store.  May take a single key or a list of keys

        Args:
            key (mongolike key or list of mongolike keys): key or keys
                for which to find distinct values or sets of values.
            criteria (filter criteria): criteria for filter
            all_exist (bool): whether to ensure all keys in list exist
                in each document, defaults to False
            **kwargs (kwargs): kwargs corresponding to collection.distinct
        """
        if isinstance(key, list):
            agg_pipeline = [{"$match": criteria}] if criteria else []
            if all_exist:
                agg_pipeline.append(
                    {"$match": {k: {"$exists": True} for k in key}})
            # use string ints as keys and replace later to avoid bug where periods
            # can't be in group keys, then reconstruct after
            group_op = {"$group": {
                "_id": {str(n): "${}".format(k) for n, k in enumerate(key)}}}
            agg_pipeline.append(group_op)
            results = [r['_id']
                       for r in self.collection.aggregate(agg_pipeline)]
            for result in results:
                for n in list(result.keys()):
                    result[key[int(n)]] = result.pop(n)

            # Return as document as partial matches are included
            return results

        else:
            return self.collection.distinct(key, filter=criteria, **kwargs)

    def ensure_index(self, key, unique=False):
        """
        Wrapper for pymongo.Collection.ensure_index
        """
        return self.collection.create_index(key, unique=unique, background=True)

    def update(self, docs, update_lu=True, key=None):
        """
        Function to update associated MongoStore collection.

        Args:
            docs: list of documents
        """

        bulk = self.collection.initialize_ordered_bulk_op()

        for d in docs:
            search_doc = {}
            if isinstance(key,list):
                search_doc = {k:d[k] for k in key}
            elif key:
                search_doc={key: d[key]}
            else:
                search_doc = {self.key: d[self.key]}
            if update_lu:
                d[self.lu_field] = datetime.utcnow()
            bulk.find(search_doc).upsert().replace_one(d)
        bulk.execute()

    def close(self):
        self.collection.database.client.close()


class MongoStore(Mongolike, Store):
    """
    A Store that connects to a Mongo collection
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
        """
        self.database = database
        self.collection_name = collection_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._collection = None
        self.kwargs = kwargs
        super(MongoStore, self).__init__(**kwargs)

    def connect(self):
        conn = MongoClient(self.host, self.port)
        db = conn[self.database]
        if self.username is not "":
            db.authenticate(self.username, self.password)
        self._collection = db[self.collection_name]

    def __hash__(self):
        return hash((self.database,self.collection_name, self.lu_field))

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


class MemoryStore(Mongolike, Store):
    """
    An in-memory Store that functions similarly
    to a MongoStore
    """

    def __init__(self, name="memory_db", **kwargs):
        self.name = name
        self._collection = None
        self.kwargs = kwargs
        super(MemoryStore, self).__init__(**kwargs)

    def connect(self):
        self._collection = mongomock.MongoClient().db[self.name]

    def __hash__(self):
        return hash((self.name, self.lu_field))


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
        """
        Args:
            dt (Datetime): Datetime to set
        """
        self.__dt = dt
        self.kwargs = kwargs
        super(DatetimeStore, self).__init__("date", **kwargs)

    def connect(self):
        super(DatetimeStore, self).connect()
        self.collection.insert_one({self.lu_field: self.__dt})


class GridFSStore(MongoStore):
    """
    A Store for GrdiFS backend. Provides a common access method consistent with other stores
    """

    def connect(self):
        conn = MongoClient(self.host, self.port)
        db = conn[self.database]
        if self.username is not "":
            db.authenticate(self.username, self.password)

        self._collection = gridfs.GridFS(db,self.collection_name)
        self._files_collection = db["{}.files".format(self.collection_name)]
        self._chunks_collection = db["{}.chunks".format(self.collection_name)]

    @property
    def collection(self):
        # TODO: Should this return the real MongoCollection or the GridFS
        return self._collection

    def query(self, properties=None, criteria=None, **kwargs):
        """
        Function that gets data from GridFS. This store ignores all 
        property projections as its designed for whole document access

        Args:
            properties (list or dict): This will be ignored by the GridFS
                Store
            criteria (dict): filter for query, matches documents
                against key-value pairs
            **kwargs (kwargs): further kwargs to Collection.find
        """
        for f in self.collection.find(filter=criteria,**kwargs):
            yield json.loads(f.read())

    def query_one(self, properties=None, criteria=None, **kwargs):
        """
        Function that gets a single document from GridFS. This store
        ignores all property projections as its designed for whole 
        document access

        Args:
            properties (list or dict): This will be ignored by the GridFS
                Store
            criteria (dict): filter for query, matches documents
                against key-value pairs
            **kwargs (kwargs): further kwargs to Collection.find
        """
        f = self.collection.find_one(filter=criteria, **kwargs)
        if f:
            return json.loads(f.read())
        else:
            return None
        
    def distinct(self, key, criteria=None, all_exist=False, **kwargs):
        """
        Function get to get all distinct values of a certain key in the
        GridFS Store. This searches the .files collection for this data

        Args:
            key (mongolike key or list of mongolike keys): key or keys
                for which to find distinct values or sets of values.
            criteria (filter criteria): criteria for filter
            all_exist (bool): whether to ensure all keys in list exist
                in each document, defaults to False
            **kwargs (kwargs): kwargs corresponding to collection.distinct
        """
        if isinstance(key, list):
            agg_pipeline = [{"$match": criteria}] if criteria else []
            if all_exist:
                agg_pipeline.append(
                    {"$match": {k: {"$exists": True} for k in key}})
            # use string ints as keys and replace later to avoid bug where periods
            # can't be in group keys, then reconstruct after
            group_op = {"$group": {
                "_id": {str(n): "${}".format(k) for n, k in enumerate(key)}}}
            agg_pipeline.append(group_op)
            results = [r['_id']
                       for r in self._files_collection.aggregate(agg_pipeline)]
            for result in results:
                for n in list(result.keys()):
                    result[key[int(n)]] = result.pop(n)

            # Return as document as partial matches are included
            return results

        else:
            return self._files_collection.distinct(key, filter=criteria, **kwargs)

    def ensure_index(self, key, unique=False):
        """
        Wrapper for pymongo.Collection.ensure_index for the files collection
        """
        return self._files_collection.create_index(key, unique=unique, background=True)

    def update(self, docs, update_lu=True, key=None):
        """
        Function to update associated MongoStore collection.

        Args:
            docs: list of documents
        """

        for d in docs:
            search_doc = {}
            if isinstance(key,list):
                search_doc = {k:d[k] for k in key}
            elif key:
                search_doc={key: d[key]}
            else:
                search_doc = {self.key: d[self.key]}

            data = json.dumps(jsanitize(d)).encode("UTF-8")
            self.collection.put(data,**search_doc)

    def close(self):
        self.collection.database.client.close()