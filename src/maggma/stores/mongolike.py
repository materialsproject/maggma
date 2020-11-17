# coding: utf-8
"""
Module containing various definitions of Stores.
Stores are a default access pattern to data and provide
various utilities
"""
from __future__ import annotations

import json
from itertools import groupby, chain
from socket import socket
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import mongomock
from monty.dev import deprecated
from monty.io import zopen
from monty.json import jsanitize, MSONable
from monty.serialization import loadfn
from pydash import get, has, set_
from pymongo import MongoClient, ReplaceOne
from pymongo.errors import OperationFailure, DocumentTooLarge
from sshtunnel import SSHTunnelForwarder


from maggma.core import Sort, Store, StoreError
from maggma.utils import confirm_field_index


class MongoStore(Store):
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
        """
        self.database = database
        self.collection_name = collection_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssh_tunnel = ssh_tunnel
        self._collection = None  # type: Any
        self.kwargs = kwargs
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
        if not self._collection or force_reset:
            if self.ssh_tunnel is None:
                conn = MongoClient(self.host, self.port)
            else:
                self.ssh_tunnel.start()
                host, port = self.ssh_tunnel.local_address
                conn = MongoClient(host=host, port=port)
            db = conn[self.database]
            if self.username != "":
                db.authenticate(self.username, self.password)
            self._collection = db[self.collection_name]

    def __hash__(self) -> int:
        """ Hash for MongoStore """
        return hash((self.database, self.collection_name, self.last_updated_field))

    @classmethod
    def from_db_file(cls, filename: str):
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

    def distinct(
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
            distinct_vals = self._collection.distinct(field, criteria)
        except (OperationFailure, DocumentTooLarge):
            distinct_vals = [
                d["_id"]
                for d in self._collection.aggregate([{"$group": {"_id": f"${field}"}}])
            ]
            if all(isinstance(d, list) for d in filter(None, distinct_vals)):  # type: ignore
                distinct_vals = list(chain.from_iterable(filter(None, distinct_vals)))

        return distinct_vals if distinct_vals is not None else []

    def groupby(
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
        for d in self._collection.aggregate(pipeline, allowDiskUse=True):
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
        store._collection = collection
        return store

    @property  # type: ignore
    @deprecated(message="This will be removed in the future")
    def collection(self):
        """ Property referring to underlying pymongo collection """
        if self._collection is None:
            raise StoreError("Must connect Mongo-like store before attemping to use it")
        return self._collection

    def count(self, criteria: Optional[Dict] = None) -> int:
        """
        Counts the number of documents matching the query criteria

        Args:
            criteria: PyMongo filter for documents to count in
        """

        criteria = criteria if criteria else {}
        return self._collection.find(filter=criteria).count()

    def query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Dict]:
        """
        Queries the Store for a set of documents

        Args:
            criteria: PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip
            limit: limit on total number of documents returned
        """
        if isinstance(properties, list):
            properties = {p: 1 for p in properties}

        sort_list = (
            [
                (k, Sort(v).value) if isinstance(v, int) else (k, v.value)
                for k, v in sort.items()
            ]
            if sort
            else None
        )

        for d in self._collection.find(
            filter=criteria,
            projection=properties,
            skip=skip,
            limit=limit,
            sort=sort_list,
        ):
            yield d

    def ensure_index(self, key: str, unique: Optional[bool] = False) -> bool:
        """
        Tries to create an index and return true if it suceeded
        Args:
            key: single key to index
            unique: Whether or not this index contains only unique keys

        Returns:
            bool indicating if the index exists/was created
        """

        if confirm_field_index(self._collection, key):
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
            self._collection.bulk_write(requests, ordered=False)

    def remove_docs(self, criteria: Dict):
        """
        Remove docs matching the query dictionary

        Args:
            criteria: query dictionary to match
        """
        self._collection.delete_many(filter=criteria)

    def close(self):
        """ Close up all collections """
        self._collection.database.client.close()
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


class MongoURIStore(MongoStore):
    """
    A Store that connects to a Mongo collection via a URI
    This is expected to be a special mongodb+srv:// URIs that include
    client parameters via TXT records
    """

    def __init__(self, uri: str, database: str, collection_name: str, **kwargs):
        """
        Args:
            uri: MongoDB+SRV URI
            database: database to connect to
            collection_name: The collection name
        """
        self.uri = uri
        self.database = database
        self.collection_name = collection_name
        self.kwargs = kwargs
        self._collection = None
        super(MongoStore, self).__init__(**kwargs)  # lgtm

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
        if not self._collection or force_reset:
            conn = MongoClient(self.uri)
            db = conn[self.database]
            self._collection = db[self.collection_name]


class MemoryStore(MongoStore):
    """
    An in-memory Store that functions similarly
    to a MongoStore
    """

    def __init__(self, collection_name: str = "memory_db", **kwargs):
        """
        Initializes the Memory Store
        Args:
            collection_name: name for the collection in memory
        """
        self.collection_name = collection_name
        self._collection = None
        self.ssh_tunnel = None  # This is to fix issues with the tunnel on close
        self.kwargs = kwargs
        super(MongoStore, self).__init__(**kwargs)  # noqa

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data
        """

        if not self._collection or force_reset:
            self._collection = mongomock.MongoClient().db[self.name]

    @property
    def name(self):
        """ Name for the store """
        return f"mem://{self.collection_name}"

    def __hash__(self):
        """ Hash for the store """
        return hash((self.name, self.last_updated_field))

    def groupby(
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
            generator returning tuples of (key, list of elemnts)
        """
        keys = keys if isinstance(keys, list) else [keys]
        data = [
            doc
            for doc in self.query(properties=keys, criteria=criteria)
            if all(has(doc, k) for k in keys)
        ]

        def grouping_keys(doc):
            return tuple(get(doc, k) for k in keys)

        for vals, group in groupby(sorted(data, key=grouping_keys), key=grouping_keys):
            doc = {}  # type: Dict[Any,Any]
            for k, v in zip(keys, vals):
                set_(doc, k, v)
            yield doc, list(group)

    def __eq__(self, other: object) -> bool:
        """
        Check equality for MemoryStore
        other: other MemoryStore to compare with
        """
        if not isinstance(other, MemoryStore):
            return False

        fields = ["collection_name", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


class JSONStore(MemoryStore):
    """
    A Store for access to a single or multiple JSON files
    """

    def __init__(self, paths: Union[str, List[str]], **kwargs):
        """
        Args:
            paths: paths for json files to turn into a Store
        """
        paths = paths if isinstance(paths, (list, tuple)) else [paths]
        self.paths = paths
        self.kwargs = kwargs
        super().__init__(collection_name="collection", **kwargs)

    def connect(self, force_reset=False):
        """
        Loads the files into the collection in memory
        """
        super().connect(force_reset=force_reset)
        for path in self.paths:
            with zopen(path) as f:
                data = f.read()
                data = data.decode() if isinstance(data, bytes) else data
                objects = json.loads(data)
                objects = [objects] if not isinstance(objects, list) else objects
                self.update(objects)

    def __hash__(self):
        return hash((*self.paths, self.last_updated_field))

    def __eq__(self, other: object) -> bool:
        """
        Check equality for JSONStore

        Args:
            other: other JSONStore to compare with
        """
        if not isinstance(other, JSONStore):
            return False

        fields = ["paths", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


class SSHTunnel(MSONable):

    __TUNNELS: Dict[str, SSHTunnelForwarder] = {}

    def __init__(
        self,
        tunnel_server_address: str,
        remote_server_address: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        private_key: Optional[str] = None,
        **kwargs,
    ):
        """
        Args:
            tunnel_server_address: string address with port for the SSH tunnel server
            remote_server_address: string address with port for the server to connect to
            username: optional username for the ssh tunnel server
            password: optional password for the ssh tunnel server; If a private_key is
                supplied this password is assumed to be the private key password
            private_key: ssh private key to authenticate to the tunnel server
            kwargs: any extra args passed to the SSHTunnelForwarder
        """

        self.tunnel_server_address = tunnel_server_address
        self.remote_server_address = remote_server_address
        self.username = username
        self.password = password
        self.private_key = private_key
        self.kwargs = kwargs

        if remote_server_address in SSHTunnel.__TUNNELS:
            self.tunnel = SSHTunnel.__TUNNELS[remote_server_address]
        else:
            open_port = _find_free_port("127.0.0.1")
            local_bind_address = ("127.0.0.1", open_port)

            ssh_address, ssh_port = tunnel_server_address.split(":")
            ssh_port = int(ssh_port)  # type: ignore

            remote_bind_address, remote_bind_port = remote_server_address.split(":")
            remote_bind_port = int(remote_bind_port)  # type: ignore

            if private_key is not None:
                ssh_password = None
                ssh_private_key_password = password
            else:
                ssh_password = password
                ssh_private_key_password = None

            self.tunnel = SSHTunnelForwarder(
                ssh_address_or_host=(ssh_address, ssh_port),
                local_bind_address=local_bind_address,
                remote_bind_address=(remote_bind_address, remote_bind_port),
                ssh_username=username,
                ssh_password=ssh_password,
                ssh_private_key_password=ssh_private_key_password,
                ssh_pkey=private_key,
                **kwargs,
            )

    def start(self):
        if not self.tunnel.is_active:
            self.tunnel.start()

    def stop(self):
        if self.tunnel.tunnel_is_up:
            self.tunnel.stop()

    @property
    def local_address(self) -> Tuple[str, int]:
        return self.tunnel.local_bind_address


def _find_free_port(address="0.0.0.0"):
    s = socket()
    s.bind((address, 0))  # Bind to a free port provided by the host.
    return s.getsockname()[1]  # Return the port number assigned.
