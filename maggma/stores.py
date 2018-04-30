# coding: utf-8
"""
Module containing various definitions of Stores.
Stores are a default access pattern to data and provide
various utillities
"""
from abc import ABCMeta, abstractmethod
import copy
from datetime import datetime
import json
import zlib

import mongomock
import pymongo
import gridfs
from itertools import groupby
from operator import itemgetter
from pymongo import MongoClient, DESCENDING
from pydash import identity

from monty.json import MSONable, jsanitize, MontyDecoder
from monty.io import zopen
from monty.serialization import loadfn
from maggma.utils import LU_KEY_ISOFORMAT, confirm_field_index


class Store(MSONable, metaclass=ABCMeta):
    """
    Abstract class for a data Store
    Defines the interface for all data going in and out of a Builder
    """

    def __init__(self, key="task_id", lu_field='last_updated', lu_type="datetime", validator=None):
        """
        Args:
            key (str): master key to index on
            lu_field (str): 'last updated' field name
            lu_type (tuple): the date/time format for the lu_field. Can be "datetime" or "isoformat"
        """
        self.key = key
        self.lu_field = lu_field
        self.lu_type = lu_type
        self.lu_func = LU_KEY_ISOFORMAT if lu_type == "isoformat" else (identity, identity)
        self.validator = validator

    @property
    @abstractmethod
    def collection(self):
        """
        Returns a handle to the pymongo collection object
        Not guaranteed to exist in the future
        """
        pass

    @abstractmethod
    def connect(self, force_reset=False):
        """
        Connect to the source data
        """
        pass

    @abstractmethod
    def close(self):
        """
        Closes any connections
        """
        pass

    @abstractmethod
    def query(self, properties=None, criteria=None, **kwargs):
        """
        Queries the Store for a set of properties
        """
        pass

    @abstractmethod
    def query_one(self, properties=None, criteria=None, **kwargs):
        """
        Get one property from the store
        """
        pass

    @abstractmethod
    def distinct(self, key, criteria=None, **kwargs):
        """
        Get all distinct values for a key
        """
        pass

    @abstractmethod
    def update(self, docs, update_lu=True, key=None, **kwargs):
        """
        Update docs into the store
        """
        pass

    @abstractmethod
    def ensure_index(self, key, unique=False, **kwargs):
        """
        Tries to create and index
        Args:
            key (string): single key to index
            unique (bool): Whether or not this index contains only unique keys

        Returns:
            bool indicating if the index exists/was created
        """
        pass

    @abstractmethod
    def groupby(self, keys, properties=None, criteria=None, **kwargs):
        """
        Simple grouping function that will group documents
        by keys.

        Args:
            keys (list or string): fields to group documents
            properties (list): properties to return in grouped documents
            criteria (dict): filter for documents to group

        Returns:
            command cursor corresponding to grouped documents

            elements of the command cursor have the structure:
            {'_id': {"KEY_1": value_1, "KEY_2": value_2 ...,
             'docs': [list_of_documents corresponding to key values]}

        """
        pass

    @property
    def last_updated(self):
        doc = next(self.query(properties=[self.lu_field]).sort([(self.lu_field, pymongo.DESCENDING)]).limit(1), None)
        # Handle when collection has docs but `NoneType` lu_field.
        return (self.lu_func[0](doc[self.lu_field]) if (doc and doc[self.lu_field]) else datetime.min)

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
        return hash((self.lu_field, ))

    def __getstate__(self):
        return self.as_dict()

    def __setstate__(self, d):
        del d["@class"]
        del d["@module"]
        md = MontyDecoder()
        d = md.process_decoded(d)
        self.__init__(**d)


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
        return self.collection.find(filter=criteria, projection=properties, **kwargs)

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
        return self.collection.find_one(filter=criteria, projection=properties, **kwargs)

    def ensure_index(self, key, unique=False, **kwargs):
        """
        Wrapper for pymongo.Collection.ensure_index
        """
        if "background" not in kwargs:
            kwargs["background"] = True

        if confirm_field_index(self.collection,key):
            return True
        else:
            return self.collection.create_index(key, unique=unique, **kwargs)

    def update(self, docs, update_lu=True, key=None, **kwargs):
        """
        Function to update associated MongoStore collection.

        Args:
            docs: list of documents
        """

        bulk = self.collection.initialize_ordered_bulk_op()

        for d in docs:

            d = jsanitize(d, allow_bson=True)

            # document-level validation is optional
            validates = True
            if self.validator:
                validates = self.validator.is_valid(d)
                if not validates:
                    if self.validator.strict:
                        raise ValueError('Document failed to validate: {}'.format(d))
                    else:
                        self.logger.error('Document failed to validate: {}'.format(d))

            if validates:
                if isinstance(key, list):
                    search_doc = {k: d[k] for k in key}
                elif key:
                    search_doc = {key: d[key]}
                else:
                    search_doc = {self.key: d[self.key]}
                if update_lu:
                    d[self.lu_field] = datetime.utcnow()
                bulk.find(search_doc).upsert().replace_one(d)

        bulk.execute()

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
            criteria = criteria if criteria else {}
            # Update to ensure keys are there
            if all_exist:
                criteria.update({k: {"$exists": True} for k in key if k not in criteria})

            results = []
            for d in self.groupby(key, properties=key, criteria=criteria):
                results.append(d["_id"])
            return results

        else:
            return self.collection.distinct(key, filter=criteria, **kwargs)

    def close(self):
        self.collection.database.client.close()


class MongoStore(Mongolike, Store):
    """
    A Store that connects to a Mongo collection
    """

    def __init__(self, database, collection_name, host="localhost", port=27017, username="", password="", **kwargs):
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

    def connect(self, force_reset=False):
        if not self._collection or force_reset:
            conn = MongoClient(self.host, self.port)
            db = conn[self.database]
            if self.username is not "":
                db.authenticate(self.username, self.password)
            self._collection = db[self.collection_name]

    def __hash__(self):
        return hash((self.database, self.collection_name, self.lu_field))

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

    def groupby(self, keys, properties=None, criteria=None, allow_disk_use=True, **kwargs):
        """
        Simple grouping function that will group documents
        by keys.

        Args:
            keys (list or string): fields to group documents
            properties (list): properties to return in grouped documents
            criteria (dict): filter for documents to group
            allow_disk_use (bool): whether to allow disk use in aggregation

        Returns:
            command cursor corresponding to grouped documents

            elements of the command cursor have the structure:
            {'_id': {"KEY_1": value_1, "KEY_2": value_2 ...,
             'docs': [list_of_documents corresponding to key values]}

        """
        pipeline = []
        if criteria is not None:
            pipeline.append({"$match": criteria})

        if properties is not None:
            pipeline.append({"$project": {p: 1 for p in properties}})

        if isinstance(keys, str):
            keys = [keys]

        group_id = {key: "${}".format(key) for key in keys}
        pipeline.append({"$group": {"_id": group_id, "docs": {"$push": "$$ROOT"}}})

        return self.collection.aggregate(pipeline, allowDiskUse=allow_disk_use)

    @classmethod
    def from_collection(cls, collection, **kwargs):
        """
        Generates a MongoStore from a pymongo collection object
        This is not a fully safe operation as it gives dummy information to the MongoStore
        As a result, this will not serialize and can not reset its connection
        """
        # TODO: How do we make this safer?
        coll_name = collection.name
        db_name = collection.database.name

        store = cls(db_name, coll_name, **kwargs)
        store._collection = collection
        return store


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

    def connect(self, force_reset=False):
        if not self._collection or force_reset:
            self._collection = mongomock.MongoClient().db[self.name]

    def __hash__(self):
        return hash((self.name, self.lu_field))

    def groupby(self, keys, properties=None, criteria=None, **kwargs):
        """
        Simple grouping function that will group documents
        by keys.

        Args:
            keys (list or string): fields to group documents
            properties (list): properties to return in grouped documents
            criteria (dict): filter for documents to group
            allow_disk_use (bool): whether to allow disk use in aggregation

        Returns:
            command cursor corresponding to grouped documents

            elements of the command cursor have the structure:
            {'_id': {"KEY_1": value_1, "KEY_2": value_2 ...,
             'docs': [list_of_documents corresponding to key values]}

        """
        keys = keys if isinstance(keys, list) else [keys]

        input_data = list(self.query(properties=keys, criteria=criteria))

        if len(keys) > 1:
            grouper = itemgetter(*keys)
            for key, grp in groupby(sorted(input_data, key=grouper), grouper):
                temp_dict = {"_id": zip(keys, key), "docs": list(grp)}
                yield temp_dict
        else:
            grouper = itemgetter(*keys)
            for key, grp in groupby(sorted(input_data, key=grouper), grouper):
                temp_dict = {"_id": {keys[0]: key}, "docs": list(grp)}
                yield temp_dict

    def update(self, docs, update_lu=True, key=None, **kwargs):
        """
        Function to update associated MongoStore collection.

        Args:
            docs: list of documents
        """

        for d in docs:

            d = jsanitize(d, allow_bson=True)

            # document-level validation is optional
            validates = True
            if self.validator:
                validates = self.validator.is_valid(d)
                if not validates:
                    if self.validator.strict:
                        raise ValueError('Document failed to validate: {}'.format(d))
                    else:
                        self.logger.error('Document failed to validate: {}'.format(d))

            if validates:
                if isinstance(key, list):
                    search_doc = {k: d[k] for k in key}
                elif key:
                    search_doc = {key: d[key]}
                else:
                    search_doc = {self.key: d[self.key]}
                if update_lu:
                    d[self.lu_field] = datetime.utcnow()
                self.collection.insert_one(d)


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

    def connect(self, force_reset=False):
        super(JSONStore, self).connect(force_reset=force_reset)
        for path in self.paths:
            with zopen(path) as f:
                data = f.read()
                data = data.decode() if isinstance(data, bytes) else data
                objects = json.loads(data)
                objects = [objects] if not isinstance(objects, list) else objects
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

    def connect(self, force_reset=False):
        super(DatetimeStore, self).connect(force_reset)
        self.collection.insert_one({self.lu_field: self.__dt})


class GridFSStore(Store):
    """
    A Store for GrdiFS backend. Provides a common access method consistent with other stores
    """

    # https://github.com/mongodb/specifications/
    #   blob/master/source/gridfs/gridfs-spec.rst#terms
    #   (Under "Files collection document")
    files_collection_fields = (
        "_id", "length", "chunkSize", "uploadDate", "md5", "filename",
        "contentType", "aliases", "metadata")

    def __init__(self, database, collection_name, host="localhost", port=27017, username="", password="", compression=False, **kwargs):

        self.database = database
        self.collection_name = collection_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._collection = None
        self.compression = compression
        self.kwargs = kwargs
        self.meta_keys = set()

        if "key" not in kwargs:
            kwargs["key"] = "_oid"

        super(GridFSStore, self).__init__(**kwargs)

    def connect(self, force_reset=False):
        conn = MongoClient(self.host, self.port)
        if not self._collection or force_reset:
            db = conn[self.database]
            if self.username is not "":
                db.authenticate(self.username, self.password)

            self._collection = gridfs.GridFS(db, self.collection_name)
            self._files_collection = db["{}.files".format(self.collection_name)]
            self._chunks_collection = db["{}.chunks".format(self.collection_name)]

    @property
    def collection(self):
        # TODO: Should this return the real MongoCollection or the GridFS
        return self._collection

    @classmethod
    def transform_criteria(cls, criteria):
        """
        Allow client to not need to prepend 'metadata.' to query fields.
        Args:
            criteria (dict): Query criteria
        """
        for field in criteria:
            if (field not in cls.files_collection_fields
                    and not field.startswith('metadata.')):
                criteria['metadata.' + field] = copy.copy(criteria[field])
                del criteria[field]

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
        if isinstance(criteria, dict):
            self.transform_criteria(criteria)
        for f in self.collection.find(filter=criteria, **kwargs):
            data = f.read()

            metadata = f.metadata
            if metadata.get("compression", "") == "zlib":
                data = zlib.decompress(data).decode("UTF-8")

            try:
                data = json.loads(data)
            except:
                pass
            yield data

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
        return next(self.query(criteria=criteria, **kwargs), None)

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
            criteria = criteria if criteria else {}
            # Update to ensure keys are there
            if all_exist:
                criteria.update({k: {"$exists": True} for k in key if k not in criteria})

            results = []
            for d in self.groupby(key, properties=key, criteria=criteria):
                results.append(d["_id"])
            return results

        else:
            if criteria:
                self.transform_criteria(criteria)
            # Transfor to metadata subfield if not supposed to be in gridfs main fields
            if key not in self.files_collection_fields:
                key = "metadata.{}".format(key)

            return self._files_collection.distinct(key, filter=criteria, **kwargs)

    def groupby(self, keys, properties=None, criteria=None, allow_disk_use=True, **kwargs):
        """
        Simple grouping function that will group documents
        by keys.

        Args:
            keys (list or string): fields to group documents
            properties (list): properties to return in grouped documents
            criteria (dict): filter for documents to group
            allow_disk_use (bool): whether to allow disk use in aggregation

        Returns:
            command cursor corresponding to grouped documents

            elements of the command cursor have the structure:
            {'_id': {"KEY_1": value_1, "KEY_2": value_2 ...,
             'docs': [list_of_documents corresponding to key values]}

        """
        pipeline = []
        if criteria is not None:
            self.transform_criteria(criteria)
            pipeline.append({"$match": criteria})

        if properties is not None:
            properties = [p if p in self.files_collection_fields else "metadata.{}".format(p) for p in properties]
            pipeline.append({"$project": {p: 1 for p in properties}})

        if isinstance(keys, str):
            keys = [keys]

        # ensure propper naming for keys in and outside of metadata
        keys = [k if k in self.files_collection_fields else "metadata.{}".format(k) for k in key]

        group_id = {key: "${}".format(key) for key in keys}
        pipeline.append({"$group": {"_id": group_id, "docs": {"$push": "$$ROOT"}}})

        return self.collection.aggregate(pipeline, allowDiskUse=allow_disk_use)

    def ensure_index(self, key, unique=False):
        """
        Wrapper for pymongo.Collection.ensure_index for the files collection
        """
        if key in self.files_collection_fields:
            return self._files_collection.create_index(key, unique=unique, background=True)
        else:
            # Store this key to put into metadata collection
            self.meta_keys |= key
            return self._files_collection.create_index("metadata.{}".format(key), unique=unique, background=True)

    def update(self, docs, update_lu=True, key=None):
        """
        Function to update associated MongoStore collection.

        Args:
            docs ([dict]): list of documents
            update_lu (bool) : Updat the last_updated field or not
            key (list or str): list or str of important parameters
        """
        if isinstance(key, str):
            key = [key]
        elif not key:
            key = [self.key]

        key = list(set(key) | self.meta_keys - set(self.files_collection_fields))

        for d in docs:

            search_doc = {k: d[k] for k in key}
            if update_lu:
                d[self.lu_field] = datetime.utcnow()

            metadata = {self.lu_field: d[self.lu_field]}
            metadata.update(search_doc)

            data = json.dumps(jsanitize(d)).encode("UTF-8")
            if self.compression:
                data = zlib.compress(data)
                metadata["compression"] = "zlib"

            self.collection.put(data, metadata=metadata)
            self.transform_criteria(search_doc)

            # Cleans up old gridfs entries
            for fdoc in (self._files_collection.find(search_doc, ["_id"])
                         .sort("uploadDate", -1).skip(1)):
                self.collection.delete(fdoc["_id"])

    def close(self):
        self.collection.database.client.close()
