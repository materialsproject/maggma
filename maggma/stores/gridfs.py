# coding: utf-8
"""
Module containing various definitions of Stores.
Stores are a default access pattern to data and provide
various utillities
"""
from __future__ import annotations

from typing import Union, Optional, Dict, List, Iterator, Tuple

import copy
from datetime import datetime
import json
import zlib
import pymongo
import gridfs

from pymongo import MongoClient
from monty.json import jsanitize
from monty.dev import deprecated
from maggma.utils import confirm_field_index
from maggma.core import Store, Sort


# TODO: Make arguments more specific for this
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
        super().__init__(**kwargs)

    def name(self) -> str:
        """
        Return a string representing this data source
        """
        return self.collection_name

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data
        """
        conn = MongoClient(self.host, self.port)
        if not self._collection or force_reset:
            db = conn[self.database]
            if self.username != "":
                db.authenticate(self.username, self.password)

            self._collection = gridfs.GridFS(db, self.collection_name)
            self._files_collection = db["{}.files".format(self.collection_name)]
            self._chunks_collection = db["{}.chunks".format(self.collection_name)]

    @property
    @deprecated(message="This will be removed in the future")
    def collection(self):
        return self._collection

    @property
    def last_updated(self) -> datetime:
        """
        Provides the most recent last_updated date time stamp from
        the documents in this Store
        """
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
    def transform_criteria(cls, criteria: Dict) -> Dict:
        """
        Allow client to not need to prepend 'metadata.' to query fields.
        Args:
            criteria (dict): Query criteria
        """
        new_criteria = dict(**criteria)
        for field in new_criteria:
            if field not in cls.files_collection_fields and not field.startswith(
                "metadata."
            ):
                new_criteria["metadata." + field] = copy.copy(new_criteria[field])
                del new_criteria[field]

        return new_criteria

    def query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Sort]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Dict]:
        """
        Queries the GridFS Store for a set of documents
        Currently ignores properties

        TODO: If properties wholy in metadata, just query that

        Args:
            criteria : PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields
            skip: number documents to skip
            limit: limit on total number of documents returned
        """
        if isinstance(criteria, dict):
            criteria = self.transform_criteria(criteria)

        for f in self._collection.find(
            filter=criteria, skip=skip, limit=limit, sort=sort
        ):
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
        a GridFs store.

        Currently not implemented
        TODO: If key in metadata or transform to metadata field

        Args:
            key (mongolike key or list of mongolike keys): key or keys
                for which to find distinct values or sets of values.
            criteria (filter criteria): criteria for filter
            all_exist (bool): whether to ensure all keys in list exist
                in each document, defaults to False
            **kwargs (kwargs): kwargs corresponding to collection.distinct
        """
        raise Exception("Can't get distinct values of GridFS Store")

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
            generator returning tuples of (dict, list of docs)
        """
        pipeline = []
        if criteria is not None:
            criteria = self.transform_criteria(criteria)
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

        for doc in self._collection.aggregate(pipeline, allowDiskUse=True):
            yield (doc["_id"], doc["docs"])

    def ensure_index(self, key: str, unique: Optional[bool] = False) -> bool:
        """
        Tries to create an index and return true if it suceeded
        Currently operators on the GridFS files collection
        Args:
            key: single key to index
            unique: Whether or not this index contains only unique keys

        Returns:
            bool indicating if the index exists/was created
        """
        # Transform key for gridfs first
        if key not in self.files_collection_fields:
            key = "metadata.{}".format(key)

        if confirm_field_index(self.collection, key):
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

        if not isinstance(docs, list):
            docs = [docs]

        if isinstance(key, str):
            key = [key]
        elif not key:
            key = [self.key]

        key = list(set(key) | self.meta_keys - set(self.files_collection_fields))

        for d in docs:
            search_doc = {k: d[k] for k in key}

            metadata = {k: d[k] for k in [self.last_updated_field] if k in d}
            metadata.update(search_doc)

            data = json.dumps(jsanitize(d)).encode("UTF-8")
            if self.compression:
                data = zlib.compress(data)
                metadata["compression"] = "zlib"

            self._collection.put(data, metadata=metadata)
            search_doc = self.transform_criteria(search_doc)

            # Cleans up old gridfs entries
            for fdoc in (
                self._files_collection.find(search_doc, ["_id"])
                .sort("uploadDate", -1)
                .skip(1)
            ):
                self._collection.delete(fdoc["_id"])

    def remove_docs(self, criteria: Dict):
        """
        Remove docs matching the query dictionary

        Args:
            criteria: query dictionary to match
        """
        if isinstance(criteria, dict):
            criteria = self.transform_criteria(criteria)
        ids = [cursor._id for cursor in self._collection.find(criteria)]

        for id in ids:
            self._collection.delete(id)

    def close(self):
        self._collection.database.client.close()


class StoreError(Exception):
    """General Store-related error."""

    pass
