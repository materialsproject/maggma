# coding: utf-8
"""
Module containing various definitions of Stores.
Stores are a default access pattern to data and provide
various utillities
"""
from __future__ import annotations

from typing import Union, Optional, Dict, List, Iterator, Tuple, Set, Any

import copy
from datetime import datetime
import json
import zlib
import gridfs
from pydash import get, has

from pymongo import MongoClient
from monty.json import jsanitize
from monty.dev import deprecated
from maggma.utils import confirm_field_index
from maggma.core import Store, Sort
from maggma.stores import MongoStore


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
        self._collection = None  # type: Any
        self.compression = compression
        self.kwargs = kwargs
        self.meta_keys = set()  # type: Set[str]

        if "key" not in kwargs:
            kwargs["key"] = "_id"
        super().__init__(**kwargs)

    def name(self) -> str:
        """
        Return a string representing this data source
        """
        return f"gridfs://{self.host}/{self.database}/{self.collection_name}"

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
            self._files_store = MongoStore.from_collection(self._files_collection)
            self._files_store.last_updated_field = f"metadata.{self.last_updated_field}"
            self._chunks_collection = db["{}.chunks".format(self.collection_name)]

    @property  # type: ignore
    @deprecated(message="This will be removed in the future")
    def collection(self):
        return self._collection

    @property
    def last_updated(self) -> datetime:
        """
        Provides the most recent last_updated date time stamp from
        the documents in this Store
        """
        return self._files_store.last_updated

    @classmethod
    def transform_criteria(cls, criteria: Dict) -> Dict:
        """
        Allow client to not need to prepend 'metadata.' to query fields.
        Args:
            criteria: Query criteria
        """
        new_criteria = dict()
        for field in criteria:
            if field not in cls.files_collection_fields and not field.startswith(
                "metadata."
            ):
                new_criteria["metadata." + field] = copy.copy(criteria[field])
            else:
                new_criteria[field] = copy.copy(criteria[field])

        return new_criteria

    def count(self, criteria: Optional[Dict] = None) -> int:
        """
        Counts the number of documents matching the query criteria

        Args:
            criteria: PyMongo filter for documents to count in
        """
        if isinstance(criteria, dict):
            criteria = self.transform_criteria(criteria)

        return self._files_store.count(criteria)

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
            criteria: PyMongo filter for documents to search in
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

    def distinct(
        self, field: str, criteria: Optional[Dict] = None, all_exist: bool = False
    ) -> List:
        """
        Get all distinct values for a field

        Args:
            field: the field(s) to get distinct values for
            criteria: PyMongo filter for documents to search in
        """
        criteria = (
            self.transform_criteria(criteria)
            if isinstance(criteria, dict)
            else criteria
        )

        field = (
            f"metadata.{field}"
            if field not in self.files_collection_fields
            and not field.startswith("metadata.")
            else field
        )

        return self._files_store.distinct(field=field, criteria=criteria)

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
        by keys. Will only work if the keys are included in the files
        collection for GridFS

        Args:
            keys: fields to group documents
            criteria: PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields
            skip: number documents to skip
            limit: limit on total number of documents returned

        Returns:
            generator returning tuples of (dict, list of docs)
        """

        criteria = (
            self.transform_criteria(criteria)
            if isinstance(criteria, dict)
            else criteria
        )
        keys = [keys] if not isinstance(keys, list) else keys
        keys = [
            f"metadata.{k}"
            if k not in self.files_collection_fields and not k.startswith("metadata.")
            else k
            for k in keys
        ]
        for group, ids in self._files_store.groupby(
            keys, criteria=criteria, properties=[f"metadata.{self.key}"]
        ):
            ids = [
                get(doc, f"metadata.{self.key}")
                for doc in ids
                if has(doc, f"metadata.{self.key}")
            ]

            group = {
                k.replace("metadata.", ""): get(group, k) for k in keys if has(group, k)
            }

            yield group, list(self.query(criteria={self.key: {"$in": ids}}))

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

    def __eq__(self, other: object) -> bool:
        """
        Check equality for GridFSStore
        other: other GridFSStore to compare with
        """
        if not isinstance(other, GridFSStore):
            return False

        fields = ["database", "collection_name", "host", "port"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)
