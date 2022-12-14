# coding: utf-8
"""
Module containing various definitions of Stores.
Stores are a default access pattern to data and provide
various utilities
"""

from pathlib import Path
import yaml
from itertools import chain, groupby
from socket import socket
import warnings
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import mongomock
import orjson
from monty.dev import requires
from monty.io import zopen
from monty.json import MSONable, jsanitize
from monty.serialization import loadfn
from pydash import get, has, set_
from pymongo import MongoClient, ReplaceOne, uri_parser
from pymongo.errors import ConfigurationError, DocumentTooLarge, OperationFailure
from sshtunnel import SSHTunnelForwarder

from maggma.core import Sort, Store, StoreError
from maggma.utils import confirm_field_index, confirm_field_index_async, to_dt

from maggma.stores.mongolike import SSHTunnel

from motor.motor_asyncio import AsyncIOMotorClient

try:
    import montydb
except ImportError:
    montydb = None


class MongoStoreAsync(Store):
    """
    A Store that connects to a Mongo collection
    """

    def __init__(
        self,
        database: str,
        collection_name: str,
        host: str = "localhost",
        port: int = 27017,
        username: str = "",
        password: str = "",
        ssh_tunnel: Optional[SSHTunnel] = None,
        safe_update: bool = False,
        auth_source: Optional[str] = None,
        mongoclient_kwargs: Optional[Dict] = None,
        **kwargs,
    ):
        """
        Args:
            database: The database name
            collection_name: The collection name
            host: Hostname for the database
            port: TCP port to connect to
            username: Username for the collection
            password: Password to connect with
            safe_update: fail gracefully on DocumentTooLarge errors on update
            auth_source: The database to authenticate on. Defaults to the database name.
        """
        self.database = database
        self.collection_name = collection_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssh_tunnel = ssh_tunnel
        self.safe_update = safe_update
        self._coll = None  # type: Any
        self.kwargs = kwargs

        if auth_source is None:
            auth_source = self.database
        self.auth_source = auth_source
        self.mongoclient_kwargs = mongoclient_kwargs or {}

        super().__init__(**kwargs)

    @property
    def name(self) -> str:
        """
        Return a string representing this data source
        """
        return f"mongo://{self.host}/{self.database}/{self.collection_name}"

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data
        """
        if self._coll is None or force_reset:
            if self.ssh_tunnel is None:
                host = self.host
                port = self.port
            else:
                self.ssh_tunnel.start()
                host, port = self.ssh_tunnel.local_address

            conn: AsyncIOMotorClient = (
                AsyncIOMotorClient(
                    host=host,
                    port=port,
                    username=self.username,
                    password=self.password,
                    authSource=self.auth_source,
                    **self.mongoclient_kwargs,
                )
                if self.username != ""
                else AsyncIOMotorClient(host, port, **self.mongoclient_kwargs)
            )
            db = conn[self.database]
            self._coll = db[self.collection_name]

    def __hash__(self) -> int:
        """Hash for MongoStore"""
        return hash((self.database, self.collection_name, self.last_updated_field))

    @classmethod
    def from_db_file(cls, filename: str, **kwargs):
        """
        Convenience method to construct MongoStore from db_file
        from old QueryEngine format
        """
        kwargs = loadfn(filename)
        if "collection" in kwargs:
            kwargs["collection_name"] = kwargs.pop("collection")
        # Get rid of aliases from traditional query engine db docs
        kwargs.pop("aliases", None)
        return cls(**kwargs)

    @classmethod
    def from_launchpad_file(cls, lp_file, collection_name, **kwargs):
        """
        Convenience method to construct MongoStore from a launchpad file

        Note: A launchpad file is a special formatted yaml file used in fireworks

        Returns:
        """
        with open(lp_file, "r") as f:
            lp_creds = yaml.load(f, Loader=yaml.FullLoader)

        db_creds = lp_creds.copy()
        db_creds["database"] = db_creds["name"]
        for key in list(db_creds.keys()):
            if key not in ["database", "host", "port", "username", "password"]:
                db_creds.pop(key)
        db_creds["collection_name"] = collection_name

        return cls(**db_creds, **kwargs)

    async def distinct(
        self, field: str, criteria: Optional[Dict] = None, all_exist: bool = False
    ) -> List:
        """
        Get all distinct values for a field

        Args:
            field: the field(s) to get distinct values for
            criteria: PyMongo filter for documents to search in
        """

        criteria = criteria or {}
        try:
            distinct_vals = await self._collection.distinct(field, criteria)
        except (OperationFailure, DocumentTooLarge):
            distinct_vals = [
                d["_id"]
                async for d in self._collection.aggregate(
                    [{"$match": criteria}, {"$group": {"_id": f"${field}"}}]
                )
            ]
            if all(isinstance(d, list) for d in filter(None, distinct_vals)):  # type: ignore
                distinct_vals = list(chain.from_iterable(filter(None, distinct_vals)))

        return distinct_vals if distinct_vals is not None else []

    async def groupby(
        self,
        keys: Union[List[str], str],
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Tuple[Dict, List[Dict]]]:
        """
        Simple grouping function that will group documents
        by keys.

        Args:
            keys: fields to group documents
            criteria: PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip
            limit: limit on total number of documents returned

        Returns:
            generator returning tuples of (key, list of docs)
        """
        pipeline = []
        if isinstance(keys, str):
            keys = [keys]

        if properties is None:
            properties = []
        if isinstance(properties, dict):
            properties = list(properties.keys())

        if criteria is not None:
            pipeline.append({"$match": criteria})

        if len(properties) > 0:
            pipeline.append({"$project": {p: 1 for p in properties + keys}})

        alpha = "abcdefghijklmnopqrstuvwxyz"
        group_id = {letter: f"${key}" for letter, key in zip(alpha, keys)}
        pipeline.append({"$group": {"_id": group_id, "docs": {"$push": "$$ROOT"}}})
        async for d in self._collection.aggregate(pipeline, allowDiskUse=True):
            id_doc = {}  # type: Dict[str,Any]
            for letter, key in group_id.items():
                if has(d["_id"], letter):
                    set_(id_doc, key[1:], d["_id"][letter])
            yield (id_doc, d["docs"])

    @classmethod
    def from_collection(cls, collection):
        """
        Generates a MongoStore from a pymongo collection object
        This is not a fully safe operation as it gives dummy information to the MongoStore
        As a result, this will not serialize and can not reset its connection

        Args:
            collection: the PyMongo collection to create a MongoStore around
        """
        # TODO: How do we make this safer?
        coll_name = collection.name
        db_name = collection.database.name

        store = cls(db_name, coll_name)
        store._coll = collection
        return store

    @property
    def _collection(self):
        """Property referring to underlying pymongo collection"""
        if self._coll is None:
            raise StoreError("Must connect Mongo-like store before attemping to use it")
        return self._coll

    async def count(
        self,
        criteria: Optional[Dict] = None,
        hint: Optional[Dict[str, Union[Sort, int]]] = None,
    ) -> int:
        """
        Counts the number of documents matching the query criteria

        Args:
            criteria: PyMongo filter for documents to count in
            hint: Dictionary of indexes to use as hints for query optimizer.
                Keys are field names and values are 1 for ascending or -1 for descending.
        """

        criteria = criteria if criteria else {}

        hint_list = (
            [
                (k, Sort(v).value) if isinstance(v, int) else (k, v.value)
                for k, v in hint.items()
            ]
            if hint
            else None
        )

        if hint_list is not None:  # pragma: no cover
            return await self._collection.count_documents(filter=criteria, hint=hint_list)

        return await self._collection.count_documents(filter=criteria)

    async def query(  # type: ignore
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        hint: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
        **kwargs
    ) -> Iterator[Dict]:
        """
        Queries the Store for a set of documents

        Args:
            criteria: PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            hint: Dictionary of indexes to use as hints for query optimizer.
                Keys are field names and values are 1 for ascending or -1 for descending.
            skip: number documents to skip
            limit: limit on total number of documents returned
            mongoclient_kwargs: Dict of extra kwargs to pass to pymongo find.
        """
        if isinstance(properties, list):
            properties = {p: 1 for p in properties}

        sort_list = (
            [
                (k, Sort(v).value) if isinstance(v, int) else (k, v.value)
                for k, v in sort.items()
            ]
            if sort
            else [("_id", 1)]
        )

        hint_list = (
            [
                (k, Sort(v).value) if isinstance(v, int) else (k, v.value)
                for k, v in hint.items()
            ]
            if hint
            else None
        )

        results = self._collection.find(
            filter=criteria,
            projection=properties,
            skip=skip,
            limit=limit,
            sort=sort_list,
            hint=hint_list,
            **kwargs
        )

        async for d in results:
            yield d

    async def ensure_index(self, key: str, unique: Optional[bool] = False) -> bool:
        """
        Tries to create an index and return true if it suceeded
        Args:
            key: single key to index
            unique: Whether or not this index contains only unique keys

        Returns:
            bool indicating if the index exists/was created
        """

        if await confirm_field_index_async(self._collection, key):
            return True
        else:
            try:
                await self._collection.create_index(key, unique=unique, background=True)
                return True
            except Exception:
                return False

    async def update(self, docs: Union[List[Dict], Dict], key: Union[List, str, None] = None):
        """
        Update documents into the Store

        Args:
            docs: the document or list of documents to update
            key: field name(s) to determine uniqueness for a
                 document, can be a list of multiple fields,
                 a single field, or None if the Store's key
                 field is to be used
        """

        requests = []

        if not isinstance(docs, list):
            docs = [docs]

        for d in docs:

            d = jsanitize(d, allow_bson=True)

            # document-level validation is optional
            validates = True
            if self.validator:
                validates = self.validator.is_valid(d)
                if not validates:
                    if self.validator.strict:
                        raise ValueError(self.validator.validation_errors(d))
                    else:
                        self.logger.error(self.validator.validation_errors(d))

            if validates:
                key = key or self.key
                if isinstance(key, list):
                    search_doc = {k: d[k] for k in key}
                else:
                    search_doc = {key: d[key]}

                requests.append(ReplaceOne(search_doc, d, upsert=True))

        if len(requests) > 0:
            try:
                await self._collection.bulk_write(requests, ordered=False)
            except (OperationFailure, DocumentTooLarge) as e:
                if self.safe_update:
                    for req in requests:
                        req._filter
                        try:
                            await self._collection.bulk_write([req], ordered=False)
                        except (OperationFailure, DocumentTooLarge):
                            self.logger.error(
                                f"Could not upload document for {req._filter} as it was too large for Mongo"
                            )
                else:
                    raise e

    async def remove_docs(self, criteria: Dict):
        """
        Remove docs matching the query dictionary

        Args:
            criteria: query dictionary to match
        """
        await self._collection.delete_many(filter=criteria)

    def close(self):
        """Close up all collections"""
        self._collection.database.client.close()
        self._coll = None
        if self.ssh_tunnel is not None:
            self.ssh_tunnel.stop()

    def __eq__(self, other: object) -> bool:
        """
        Check equality for MongoStore
        other: other mongostore to compare with
        """
        if not isinstance(other, MongoStore):
            return False

        fields = ["database", "collection_name", "host", "port", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)

    async def query_one(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
    ):
        """
        Queries the Store for a single document

        Args:
            criteria: PyMongo filter for documents to search
            properties: properties to return in the document
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
        """
        return await anext(
            self.query(criteria=criteria, properties=properties, sort=sort), None
        )

class MongoURIStoreAsync(MongoStoreAsync):
    """
    A Store that connects to a Mongo collection via a URI
    This is expected to be a special mongodb+srv:// URIs that include
    client parameters via TXT records
    """

    def __init__(
        self,
        uri: str,
        collection_name: str,
        database: str = None,
        ssh_tunnel: Optional[SSHTunnel] = None,
        mongoclient_kwargs: Optional[Dict] = None,
        **kwargs,
    ):
        """
        Args:
            uri: MongoDB+SRV URI
            database: database to connect to
            collection_name: The collection name
        """
        self.uri = uri
        self.ssh_tunnel = ssh_tunnel
        self.mongoclient_kwargs = mongoclient_kwargs or {}

        # parse the dbname from the uri
        if database is None:
            d_uri = uri_parser.parse_uri(uri)
            if d_uri["database"] is None:
                raise ConfigurationError(
                    "If database name is not supplied, a database must be set in the uri"
                )
            self.database = d_uri["database"]
        else:
            self.database = database

        self.collection_name = collection_name
        self.kwargs = kwargs
        self._coll = None
        super(MongoStoreAsync, self).__init__(**kwargs)  # lgtm

    @property
    def name(self) -> str:
        """
        Return a string representing this data source
        """
        # TODO: This is not very safe since it exposes the username/password info
        return self.uri

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data
        """
        if self._coll is None or force_reset:  # pragma: no cover
            conn: MongoClient = MongoClient(self.uri, **self.mongoclient_kwargs)
            db = conn[self.database]
            self._coll = db[self.collection_name]



