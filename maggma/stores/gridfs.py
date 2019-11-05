# coding: utf-8
"""
Module containing various definitions of Stores.
Stores are a default access pattern to data and provide
various utillities
"""
from __future__ import annotations
import copy
from datetime import datetime
import json
import zlib
import pymongo
import gridfs

from pymongo import MongoClient
from monty.json import jsanitize
from maggma.utils import confirm_field_index
from maggma.core import Store


class GridFSStore(Store):
    """
    A Store for GrdiFS backend. Provides a common access method consistent with other stores
    """

    # https://github.com/mongodb/specifications/
    #   blob/master/source/gridfs/gridfs-spec.rst#terms
    #   (Under "Files collection document")
    files_collection_fields = (
        "_id",
        "length",
        "chunkSize",
        "uploadDate",
        "md5",
        "filename",
        "contentType",
        "aliases",
        "metadata",
    )

    def __init__(
        self,
        database: str,
        collection_name: str,
        host: str = "localhost",
        port: int = 27017,
        username: str = "",
        password: str = "",
        compression: bool = False,
        **kwargs,
    ):
        """
        Initializes a GrdiFS Store for binary data
        Args:
            database: database name
            collection_name: The name of the collection.
                This is the string portion before the GridFS extensions
            host: hostname for the database
            port: port to connec to
            username: username to connect as
            password: password to authenticate as
        """

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
            kwargs["key"] = "_id"

        kwargs["last_updated_field"] = "uploadDate"

        super().__init__(**kwargs)

    def connect(self, force_reset=False):
        conn = MongoClient(self.host, self.port)
        if not self._collection or force_reset:
            db = conn[self.database]
            if self.username != "":
                db.authenticate(self.username, self.password)

            self._collection = gridfs.GridFS(db, self.collection_name)
            self._files_collection = db["{}.files".format(self.collection_name)]
            self._chunks_collection = db["{}.chunks".format(self.collection_name)]

    @property
    def collection(self):
        # TODO: Should this return the real MongoCollection or the GridFS
        return self._collection

    @property
    def last_updated(self):
        doc = next(
            self._files_collection.find(projection=[self.last_updated_field])
            .sort([(self.last_updated_field, pymongo.DESCENDING)])
            .limit(1),
            None,
        )
        if doc and self.last_updated_field not in doc:
            raise StoreError(
                "No field '{}' in store document. Please ensure Store.last_updated_field "
                "is a datetime field in your store that represents the time of "
                "last update to each document.".format(self.last_updated_field)
            )
        # Handle when collection has docs but `NoneType` last_updated_field.
        return (
            self._lu_func[0](doc[self.last_updated_field])
            if (doc and doc[self.last_updated_field])
            else datetime.min
        )

    @classmethod
    def transform_criteria(cls, criteria):
        """
        Allow client to not need to prepend 'metadata.' to query fields.
        Args:
            criteria (dict): Query criteria
        """
        for field in criteria:
            if field not in cls.files_collection_fields and not field.startswith(
                "metadata."
            ):
                criteria["metadata." + field] = copy.copy(criteria[field])
                del criteria[field]

    def query(self, criteria=None, properties=None, **kwargs):
        """
        Function that gets data from GridFS. This store ignores all
        property projections as its designed for whole document access

        Args:
            criteria (dict): filter for query, matches documents
                against key-value pairs
            properties (list or dict): This will be ignored by the GridFS
                Store
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
            except Exception:
                pass
            yield data

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
                criteria.update(
                    {k: {"$exists": True} for k in key if k not in criteria}
                )

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

    def groupby(
        self, keys, criteria=None, properties=None, allow_disk_use=True, **kwargs
    ):
        """
        Simple grouping function that will group documents
        by keys.

        Args:
            keys (list or string): fields to group documents
            criteria (dict): filter for documents to group
            properties (list): properties to return in grouped documents
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
            properties = [
                p if p in self.files_collection_fields else "metadata.{}".format(p)
                for p in properties
            ]
            pipeline.append({"$project": {p: 1 for p in properties}})

        if isinstance(keys, str):
            keys = [keys]

        # ensure propper naming for keys in and outside of metadata
        keys = [
            k if k in self.files_collection_fields else "metadata.{}".format(k)
            for k in keys
        ]

        group_id = {key: "${}".format(key) for key in keys}
        pipeline.append({"$group": {"_id": group_id, "docs": {"$push": "$$ROOT"}}})

        return self.collection.aggregate(pipeline, allowDiskUse=allow_disk_use)

    def ensure_index(self, key, unique=False):
        """
        Wrapper for pymongo.Collection.ensure_index for the files collection
        """
        # Transform key for gridfs first
        if key not in self.files_collection_fields:
            key = "metadata.{}".format(key)

        if confirm_field_index(self.collection, key):
            return True
        else:
            try:
                self.collection.create_index(key, unique=unique, background=True)
                return True
            except Exception:
                return False

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
                d[self.last_updated_field] = datetime.utcnow()

            metadata = {self.last_updated_field: d[self.last_updated_field]}
            metadata.update(search_doc)

            data = json.dumps(jsanitize(d)).encode("UTF-8")
            if self.compression:
                data = zlib.compress(data)
                metadata["compression"] = "zlib"

            self.collection.put(data, metadata=metadata)
            self.transform_criteria(search_doc)

            # Cleans up old gridfs entries
            for fdoc in (
                self._files_collection.find(search_doc, ["_id"])
                .sort("uploadDate", -1)
                .skip(1)
            ):
                self.collection.delete(fdoc["_id"])

    def close(self):
        self.collection.database.client.close()


class StoreError(Exception):
    """General Store-related error."""

    pass
