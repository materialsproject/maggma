"""
Module containing various definitions of Stores.
Stores are a default access pattern to data and provide
various utilities.
"""

import warnings
import weakref
from collections.abc import Callable, Iterator
from itertools import chain, groupby
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import bson
import mongomock_ng as mongomock
import orjson
from monty.dev import requires
from monty.io import zopen
from monty.json import jsanitize
from monty.serialization import loadfn
from pydash import get, has, set_
from pymongo import MongoClient, ReplaceOne, uri_parser
from pymongo.errors import ConfigurationError, DocumentTooLarge, OperationFailure
from ruamel.yaml import YAML

from maggma.core import Sort, Store, StoreError
from maggma.stores.ssh_tunnel import SSHTunnel
from maggma.utils import confirm_field_index, to_dt

try:
    from montydb import MontyClient, set_storage  # type: ignore
except ImportError:
    MontyClient = None


class MongoStore(Store):
    """
    A Store that connects to a Mongo collection.
    """

    def __init__(
        self,
        database: str,
        collection_name: str,
        host: str = "localhost",
        port: int = 27017,
        username: str = "",
        password: str = "",
        ssh_tunnel: SSHTunnel | None = None,
        safe_update: bool = False,
        auth_source: str | None = None,
        mongoclient_kwargs: dict | None = None,
        default_sort: dict[str, Sort | int] | None = None,
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
            default_sort: Default sort field and direction to use when querying. Can be used to
                ensure determinacy in query results.
        """
        self.database = database
        self.collection_name = collection_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssh_tunnel = ssh_tunnel
        self.safe_update = safe_update
        self.default_sort = default_sort
        self._coll = None  # type: ignore
        self.kwargs = kwargs

        if auth_source is None:
            auth_source = self.database
        self.auth_source = auth_source
        self.mongoclient_kwargs = mongoclient_kwargs or {}

        super().__init__(**kwargs)

    @property
    def name(self) -> str:
        """
        Return a string representing this data source.
        """
        return f"mongo://{self.host}/{self.database}/{self.collection_name}"

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data.

        Args:
            force_reset: whether to reset the connection or not when the Store is
                already connected.
        """
        if self._coll is None or force_reset:
            if self.ssh_tunnel is None:
                host = self.host
                port = self.port
            else:
                self.ssh_tunnel.start()
                host, port = self.ssh_tunnel.local_address

            conn: MongoClient = (
                MongoClient(
                    host=host,
                    port=port,
                    username=self.username,
                    password=self.password,
                    authSource=self.auth_source,
                    **self.mongoclient_kwargs,
                )
                if self.username != ""
                else MongoClient(host, port, **self.mongoclient_kwargs)
            )
            db = conn[self.database]
            self._coll = db[self.collection_name]  # type: ignore

    def __hash__(self) -> int:
        """Hash for MongoStore."""
        return hash((self.database, self.collection_name, self.last_updated_field))

    @classmethod
    def from_db_file(cls, filename: str, **kwargs):
        """
        Convenience method to construct MongoStore from db_file
        from old QueryEngine format.
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
        Convenience method to construct MongoStore from a launchpad file.

        Note: A launchpad file is a special formatted yaml file used in fireworks

        Returns:
        """
        with open(lp_file) as f:
            yaml = YAML(typ="safe", pure=True)
            lp_creds = yaml.load(f.read())

        db_creds = lp_creds.copy()
        db_creds["database"] = db_creds["name"]
        for key in list(db_creds.keys()):
            if key not in ["database", "host", "port", "username", "password"]:
                db_creds.pop(key)
        db_creds["collection_name"] = collection_name

        return cls(**db_creds, **kwargs)

    def distinct(self, field: str, criteria: dict | None = None, all_exist: bool = False) -> list:
        """
        Get all distinct values for a field.

        Args:
            field: the field(s) to get distinct values for
            criteria: PyMongo filter for documents to search in
        """
        criteria = criteria or {}
        try:
            distinct_vals = self._collection.distinct(field, criteria)
        except (OperationFailure, DocumentTooLarge):
            distinct_vals = [
                d["_id"] for d in self._collection.aggregate([{"$match": criteria}, {"$group": {"_id": f"${field}"}}])
            ]
            if all(isinstance(d, list) for d in filter(None, distinct_vals)):  # type: ignore
                distinct_vals = list(chain.from_iterable(filter(None, distinct_vals)))

        return distinct_vals if distinct_vals is not None else []

    def groupby(
        self,
        keys: list[str] | str,
        criteria: dict | None = None,
        properties: dict | list | None = None,
        sort: dict[str, Sort | int] | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[tuple[dict, list[dict]]]:
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
        group_id = {letter: f"${key}" for letter, key in zip(alpha, keys, strict=False)}
        pipeline.append({"$group": {"_id": group_id, "docs": {"$push": "$$ROOT"}}})
        for d in self._collection.aggregate(pipeline, allowDiskUse=True):
            id_doc = {}  # type: ignore
            for letter, key in group_id.items():
                if has(d["_id"], letter):
                    set_(id_doc, key[1:], d["_id"][letter])
            yield (id_doc, d["docs"])

    @classmethod
    def from_collection(cls, collection):
        """
        Generates a MongoStore from a pymongo collection object
        This is not a fully safe operation as it gives dummy information to the MongoStore
        As a result, this will not serialize and can not reset its connection.

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
        """Property referring to underlying pymongo collection."""
        if self._coll is None:
            raise StoreError("Must connect Mongo-like store before attempting to use it")
        return self._coll

    def count(
        self,
        criteria: dict | None = None,
        hint: dict[str, Sort | int] | None = None,
    ) -> int:
        """
        Counts the number of documents matching the query criteria.

        Args:
            criteria: PyMongo filter for documents to count in
            hint: Dictionary of indexes to use as hints for query optimizer.
                Keys are field names and values are 1 for ascending or -1 for descending.
        """
        criteria = criteria if criteria else {}

        hint_list = (
            [(k, Sort(v).value) if isinstance(v, int) else (k, v.value) for k, v in hint.items()] if hint else None
        )

        if hint_list is not None:  # pragma: no cover
            return self._collection.count_documents(filter=criteria, hint=hint_list)

        return (
            self._collection.count_documents(filter=criteria)
            if criteria
            else self._collection.estimated_document_count()
        )

    def query(  # type: ignore
        self,
        criteria: dict | None = None,
        properties: dict | list | None = None,
        sort: dict[str, Sort | int] | None = None,
        hint: dict[str, Sort | int] | None = None,
        skip: int = 0,
        limit: int = 0,
        **kwargs,
    ) -> Iterator[dict]:
        """
        Queries the Store for a set of documents.

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

        default_sort_formatted = None

        if self.default_sort is not None:
            default_sort_formatted = [
                (k, Sort(v).value) if isinstance(v, int) else (k, v.value) for k, v in self.default_sort.items()
            ]

        sort_list = (
            [(k, Sort(v).value) if isinstance(v, int) else (k, v.value) for k, v in sort.items()]
            if sort
            else default_sort_formatted
        )

        hint_list = (
            [(k, Sort(v).value) if isinstance(v, int) else (k, v.value) for k, v in hint.items()] if hint else None
        )

        yield from self._collection.find(
            filter=criteria,
            projection=properties,
            skip=skip,
            limit=limit,
            sort=sort_list,
            hint=hint_list,
            **kwargs,
        )

    def ensure_index(self, key: str, unique: bool | None = False) -> bool:
        """
        Tries to create an index and return true if it succeeded.

        Args:
            key: single key to index
            unique: Whether or not this index contains only unique keys.

        Returns:
            bool indicating if the index exists/was created
        """
        if confirm_field_index(self._collection, key):
            return True

        try:
            self._collection.create_index(key, unique=unique, background=True)
            return True
        except Exception:
            return False

    def update(self, docs: list[dict] | dict, key: list | str | None = None):
        """
        Update documents into the Store.

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

        for d in (jsanitize(x, allow_bson=True, recursive_msonable=True) for x in docs):
            # document-level validation is optional
            validates = True
            if self.validator:
                validates = self.validator.is_valid(d)
                if not validates:
                    if self.validator.strict:
                        raise ValueError(self.validator.validation_errors(d))
                    self.logger.error(self.validator.validation_errors(d))

            if validates:
                key = key or self.key
                search_doc = {k: d[k] for k in key} if isinstance(key, list) else {key: d[key]}

                requests.append(ReplaceOne(search_doc, d, upsert=True))

        if len(requests) > 0:
            try:
                self._collection.bulk_write(requests, ordered=False)
            except (OperationFailure, DocumentTooLarge) as e:
                if self.safe_update:
                    for req in requests:
                        try:
                            self._collection.bulk_write([req], ordered=False)
                        except (OperationFailure, DocumentTooLarge):
                            self.logger.error(
                                f"Could not upload document for {req._filter} as it was too large for Mongo"
                            )
                else:
                    raise e

    def remove_docs(self, criteria: dict):
        """
        Remove docs matching the query dictionary.

        Args:
            criteria: query dictionary to match
        """
        self._collection.delete_many(filter=criteria)

    def close(self):
        """Close up all collections."""
        self._collection.database.client.close()
        self._coll = None
        if self.ssh_tunnel is not None:
            self.ssh_tunnel.stop()

    def __eq__(self, other: object) -> bool:
        """
        Check equality for MongoStore
        other: other mongostore to compare with.
        """
        if not isinstance(other, MongoStore):
            return False

        fields = ["database", "collection_name", "host", "port", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


class MongoURIStore(MongoStore):
    """
    A Store that connects to a Mongo collection via a URI
    This is expected to be a special mongodb+srv:// URIs that include
    client parameters via TXT records.
    """

    def __init__(
        self,
        uri: str,
        collection_name: str,
        database: str | None = None,
        ssh_tunnel: SSHTunnel | None = None,
        safe_update: bool = False,
        mongoclient_kwargs: dict | None = None,
        default_sort: dict[str, Sort | int] | None = None,
        **kwargs,
    ):
        """
        Args:
            uri: MongoDB+SRV URI
            database: database to connect to
            collection_name: The collection name
            default_sort: Default sort field and direction to use when querying. Can be used to
                ensure determinacy in query results.
        """
        self.uri = uri
        if ssh_tunnel:
            raise ValueError(f"At the moment ssh_tunnel is not supported for {self.__class__.__name__}")
        self.ssh_tunnel = None
        self.default_sort = default_sort
        self.safe_update = safe_update
        self.mongoclient_kwargs = mongoclient_kwargs or {}

        # parse the dbname from the uri
        if database is None:
            d_uri = uri_parser.parse_uri(uri)
            if d_uri["database"] is None:
                raise ConfigurationError("If database name is not supplied, a database must be set in the uri")
            self.database = d_uri["database"]
        else:
            self.database = database

        self.collection_name = collection_name
        self.kwargs = kwargs
        self._coll = None
        super(MongoStore, self).__init__(**kwargs)  # lgtm

    @property
    def name(self) -> str:
        """
        Return a string representing this data source.
        """
        # TODO: This is not very safe since it exposes the username/password info
        return self.uri

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data.

        Args:
            force_reset: whether to reset the connection or not when the Store is
                already connected.
        """
        if self._coll is None or force_reset:  # pragma: no cover
            conn: MongoClient = MongoClient(self.uri, **self.mongoclient_kwargs)
            db = conn[self.database]
            self._coll = db[self.collection_name]  # type: ignore


class MemoryStore(MongoStore):
    """
    An in-memory Store that functions similarly to a MongoStore.

    If a MongoDB server is reachable (by default on ``localhost:27017``), the
    data is stored in a real, ephemeral MongoDB database for full MongoDB
    compatibility and performance. That database is namespaced uniquely per
    Store instance and is dropped automatically when the Store is garbage
    collected or the interpreter exits, so the user never has to create or
    clean up a database manually. If no server is reachable, the Store
    transparently falls back to an in-process ``mongomock`` database, so it
    works even with no MongoDB installed.
    """

    #: Set to True by connect() when a real MongoDB backend is in use. Defined
    #: at class level so close() is safe on subclasses (e.g. MontyStore) that
    #: do not call MemoryStore.__init__.
    _using_real_mongo: bool = False
    _finalizer = None

    def __init__(
        self,
        collection_name: str = "memory_db",
        host: str = "localhost",
        port: int = 27017,
        mongoclient_kwargs: dict | None = None,
        server_selection_timeout_ms: int = 500,
        **kwargs,
    ):
        """
        Initializes the Memory Store.

        Args:
            collection_name: name for the collection in memory.
            host: hostname to probe for a running MongoDB server to back the
                Store with. The data is written to an ephemeral database that
                is dropped when the Store is closed.
            port: TCP port to probe for a running MongoDB server.
            mongoclient_kwargs: Dict of extra kwargs to pass to MongoClient when
                a real MongoDB backend is used.
            server_selection_timeout_ms: how long, in milliseconds, to wait when
                probing ``host:port`` for a MongoDB server before falling back
                to an in-process ``mongomock`` database. Set to 0 (or less) to
                skip the probe entirely and always use ``mongomock``.
        """
        self.collection_name = collection_name
        self.host = host
        self.port = port
        self.mongoclient_kwargs = mongoclient_kwargs or {}
        self.server_selection_timeout_ms = server_selection_timeout_ms
        # unique, ephemeral database name so that multiple MemoryStore instances
        # backed by the same real MongoDB server do not clobber one another
        self._database = f"maggma_memory_{uuid4().hex}"
        self.default_sort = None
        self._coll = None
        self.kwargs = kwargs
        super(MongoStore, self).__init__(**kwargs)

    def _get_memory_client(self) -> MongoClient:
        """
        Return a client backing the in-memory Store.

        Attempts to connect to a real MongoDB server at ``host:port`` for full
        MongoDB compatibility and performance. If none is reachable within
        ``server_selection_timeout_ms``, falls back to an in-process
        ``mongomock`` client so the Store works without a MongoDB server.
        """
        if self.server_selection_timeout_ms > 0:
            mongoclient_kwargs = dict(self.mongoclient_kwargs)
            mongoclient_kwargs.setdefault("serverSelectionTimeoutMS", self.server_selection_timeout_ms)
            try:
                client = MongoClient(host=self.host, port=self.port, **mongoclient_kwargs)
                # force server selection to confirm a server is actually reachable
                client.admin.command("ping")
                self._using_real_mongo = True
                # ensure the ephemeral database is dropped when this Store is
                # garbage collected or the interpreter exits, so nothing is left
                # behind on the server
                if self._finalizer is None:
                    self._finalizer = weakref.finalize(
                        self,
                        self._drop_ephemeral_database,
                        self.host,
                        self.port,
                        self._database,
                        dict(mongoclient_kwargs),
                    )
                self.logger.debug(f"{self.name} using real MongoDB backend at {self.host}:{self.port}")
                return client
            except Exception:
                self.logger.debug(f"{self.name}: no MongoDB server reachable, falling back to mongomock")

        self._using_real_mongo = False
        return mongomock.MongoClient()  # type: ignore

    @staticmethod
    def _drop_ephemeral_database(host: str, port: int, database: str, mongoclient_kwargs: dict):
        """Drop an ephemeral MongoDB database. Used as a weakref finalizer, so it
        must not hold a reference to the Store."""
        mongoclient_kwargs.setdefault("serverSelectionTimeoutMS", 500)
        try:
            client = MongoClient(host=host, port=port, **mongoclient_kwargs)
            client.drop_database(database)
            client.close()
        except Exception:
            pass

    def _connect_collection(self, force_reset: bool = False):
        """Establish (or re-establish) the underlying in-memory collection."""
        if force_reset and self._coll is not None:
            old_client = self._coll.database.client
            # on a forced reset, discard the previous contents (matching the
            # historical mongomock behavior of starting from a fresh client)
            if getattr(self, "_using_real_mongo", False):
                try:
                    old_client.drop_database(self._database)
                except Exception:
                    pass
            old_client.close()
        client = self._get_memory_client()
        self._coll = client[self._database][self.collection_name]  # type: ignore

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data.

        Args:
            force_reset: whether to reset the connection or not when the Store is
                already connected.
        """
        if self._coll is None or force_reset:
            self._connect_collection(force_reset=force_reset)

    def close(self):
        """Close up all collections.

        For an in-memory Store this is intentionally a no-op that leaves the
        Store usable, matching the historical ``mongomock`` behavior (its
        ``close()`` did nothing) that callers such as the builders rely on when
        they query a Store after it has been closed. Resources are released and
        any ephemeral MongoDB database backing the Store is dropped when the
        Store is garbage collected or at interpreter exit (see
        ``_drop_ephemeral_database``). Use ``force_reset=True`` on ``connect()``
        to explicitly discard the contents and start fresh.
        """

    @property
    def name(self):
        """Name for the store."""
        return f"mem://{self.collection_name}"

    def __hash__(self):
        """Hash for the store."""
        return hash((self.name, self.last_updated_field))

    def groupby(
        self,
        keys: list[str] | str,
        criteria: dict | None = None,
        properties: dict | list | None = None,
        sort: dict[str, Sort | int] | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[tuple[dict, list[dict]]]:
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
            generator returning tuples of (key, list of elements)
        """
        keys = keys if isinstance(keys, list) else [keys]

        if properties is None:
            properties = []
        if isinstance(properties, dict):
            properties = list(properties.keys())

        data = [
            doc for doc in self.query(properties=keys + properties, criteria=criteria) if all(has(doc, k) for k in keys)
        ]

        def grouping_keys(doc):
            return tuple(get(doc, k) for k in keys)

        for vals, group in groupby(sorted(data, key=grouping_keys), key=grouping_keys):
            doc = {}  # type: ignore
            for k, v in zip(keys, vals, strict=False):
                set_(doc, k, v)
            yield doc, list(group)

    def __eq__(self, other: object) -> bool:
        """
        Check equality for MemoryStore
        other: other MemoryStore to compare with.
        """
        if not isinstance(other, MemoryStore):
            return False

        fields = ["collection_name", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


class JSONStore(MemoryStore):
    """
    A Store for access to a single or multiple JSON files.
    """

    def __init__(
        self,
        paths: str | list[str],
        read_only: bool = True,
        serialization_option: int | None = None,
        serialization_default: Callable[[Any], Any] | None = None,
        encoding: str | None = None,
        **kwargs,
    ):
        """
        Args:
            paths: paths for json files to turn into a Store
            read_only: whether this JSONStore is read only. When read_only=True,
                       the JSONStore can still apply MongoDB-like writable operations
                       (e.g. an update) because it behaves like a MemoryStore,
                       but it will not write those changes to the file. On the other hand,
                       if read_only=False (i.e., it is writeable), the JSON file
                       will be automatically updated every time a write-like operation is
                       performed.

                       Note that when read_only=False, JSONStore only supports a single JSON
                       file. If the file does not exist, it will be automatically created
                       when the JSONStore is initialized.
            serialization_option:
                option that will be passed to the orjson.dump when saving to the json the file.
            serialization_default:
                default that will be passed to the orjson.dump when saving to the json the file.
            encoding: Character encoding of files to be tracked by the store. The default
                (None) follows python's default behavior, which is to determine the character
                encoding from the platform. This should work in the great majority of cases.
                However, if you encounter a UnicodeDecodeError, consider setting the encoding
                explicitly to 'utf8' or another encoding as appropriate.
        """
        paths = paths if isinstance(paths, list | tuple) else [paths]
        self.paths = paths
        self.encoding = encoding

        # file_writable overrides read_only for compatibility reasons
        if "file_writable" in kwargs:
            file_writable = kwargs.pop("file_writable")
            warnings.warn(
                "file_writable is deprecated; use read only instead.",
                DeprecationWarning,
            )
            self.read_only = not file_writable
            if self.read_only != read_only:
                warnings.warn(
                    f"Received conflicting keyword arguments file_writable={file_writable}"
                    f" and read_only={read_only}. Setting read_only={file_writable}.",
                    UserWarning,
                )
        else:
            self.read_only = read_only
        self.kwargs = kwargs

        if not self.read_only and len(paths) > 1:
            raise RuntimeError("Cannot instantiate file-writable JSONStore with multiple JSON files.")

        self.default_sort = None
        self.serialization_option = serialization_option
        self.serialization_default = serialization_default

        super().__init__(**kwargs)

    def connect(self, force_reset: bool = False):
        """
        Loads the files into the collection in memory.

        Args:
            force_reset: whether to reset the connection or not. If False (default) and .connect()
            has been called previously, the .json file will not be read in again. This can improve performance
            on systems with slow storage when multiple connect / disconnects are performed.
        """
        if self._coll is None or force_reset:
            self._connect_collection(force_reset=force_reset)

            # create the .json file if it does not exist
            if not self.read_only and not Path(self.paths[0]).exists():
                with zopen(self.paths[0], mode="wt", encoding=self.encoding) as f:
                    data: list[dict] = []
                    bytesdata = orjson.dumps(data)
                    f.write(bytesdata.decode("utf-8"))

            for path in self.paths:
                objects = self.read_json_file(path)
                try:
                    self.update(objects)
                except KeyError:
                    raise KeyError(
                        f"""
                        Key field '{self.key}' not found in {path.name}. This
                        could mean that this JSONStore was initially created with a different key field.
                        The keys found in the .json file are {list(objects[0].keys())}. Try
                        re-initializing your JSONStore using one of these as the key arguments.
                        """
                    )

    def read_json_file(self, path) -> list:
        """
        Helper method to read the contents of a JSON file and generate
        a list of docs.

        Args:
            path: Path to the JSON file to be read
        """
        with zopen(path, mode="rt", encoding=self.encoding) as f:
            data = f.read()
            data = data.decode() if isinstance(data, bytes) else data
            objects = bson.json_util.loads(data) if "$oid" in data else orjson.loads(data)
            objects = [objects] if not isinstance(objects, list) else objects
            # datetime objects deserialize to str. Try to convert the last_updated
            # field back to datetime.
            # # TODO - there may still be problems caused if a JSONStore is init'ed from
            # documents that don't contain a last_updated field
            # See Store.last_updated in store.py.
            for obj in objects:
                if obj.get(self.last_updated_field):
                    obj[self.last_updated_field] = to_dt(obj[self.last_updated_field])

        return objects

    def update(self, docs: list[dict] | dict, key: list | str | None = None):
        """
        Update documents into the Store.

        For a file-writable JSONStore, the json file is updated.

        Args:
            docs: the document or list of documents to update
            key: field name(s) to determine uniqueness for a
                 document, can be a list of multiple fields,
                 a single field, or None if the Store's key
                 field is to be used
        """
        super().update(docs=docs, key=key)
        if not self.read_only:
            self.update_json_file()

    def remove_docs(self, criteria: dict):
        """
        Remove docs matching the query dictionary.

        For a file-writable JSONStore, the json file is updated.

        Args:
            criteria: query dictionary to match
        """
        super().remove_docs(criteria=criteria)
        if not self.read_only:
            self.update_json_file()

    def update_json_file(self):
        """
        Updates the json file when a write-like operation is performed.
        """
        with zopen(self.paths[0], mode="wt", encoding=self.encoding) as f:
            data = list(self.query())
            for d in data:
                d.pop("_id")
            bytesdata = orjson.dumps(
                data,
                option=self.serialization_option,
                default=self.serialization_default,
            )
            f.write(bytesdata.decode("utf-8"))

    def __hash__(self):
        return hash((*self.paths, self.last_updated_field))

    def __eq__(self, other: object) -> bool:
        """
        Check equality for JSONStore.

        Args:
            other: other JSONStore to compare with
        """
        if not isinstance(other, JSONStore):
            return False

        fields = ["paths", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


@requires(
    MontyClient is not None,
    "MontyStore requires MontyDB to be installed. See the MontyDB repository for more "
    "information: https://github.com/davidlatwe/montydb",
)
class MontyStore(MemoryStore):
    """
    A MongoDB compatible store that uses on disk files for storage.

    This is handled under the hood using MontyDB. A number of on-disk storage options
    are available but MontyDB provides a mongo style interface for all options. The
    options include:

    - sqlite: Uses an sqlite database to store documents.
    - lightning: Uses Lightning Memory-Mapped Database (LMDB) for storage. This can
      provide fast read and write times but requires lmdb to be installed (in most cases
      this can be achieved using ``pip install lmdb``).
    - flatfile: Uses a system of flat json files. This is not recommended as multiple
      simultaneous connections to the store will not work correctly.

    Note that MontyDB (and, therefore, MontyStore) will write out a new database to
    the disk but cannot be used to read an existing (e.g. SQLite) database that wasn't
    formatted by MontyDB.

    See the MontyDB repository for more information: https://github.com/davidlatwe/montydb
    """

    def __init__(
        self,
        collection_name,
        database_path: str | None = None,
        database_name: str = "db",
        storage: Literal["sqlite", "flatfile", "lightning"] = "sqlite",
        storage_kwargs: dict | None = None,
        client_kwargs: dict | None = None,
        **kwargs,
    ):
        """
        Initializes the Monty Store.

        Args:
            collection_name: Name for the collection.
            database_path: Path to on-disk database files. If None, the current working
                directory will be used.
            database_name: The database name.
            storage: The storage type. Options include "sqlite", "lightning", "flatfile". Note that
            although MontyDB supports in memory storage, this capability is disabled in maggma to avoid unintended
            behavior, since multiple in-memory MontyStore would actually point to the same data.
            storage_kwargs: Keyword arguments passed to ``montydb.set_storage``.
            client_kwargs: Keyword arguments passed to the ``montydb.MontyClient``
                constructor.
            **kwargs: Additional keyword arguments passed to the Store constructor.
        """
        if database_path is None:
            database_path = str(Path.cwd())

        self.database_path = database_path
        self.database_name = database_name
        self.collection_name = collection_name
        self._coll = None  # type: ignore
        self.default_sort = None
        self.ssh_tunnel = None  # This is to fix issues with the tunnel on close
        self.kwargs = kwargs
        self.storage = storage
        self.storage_kwargs = storage_kwargs or {
            "use_bson": True,  # import pymongo's BSON; do not use montydb's
            "mongo_version": "4.0",
        }
        self.client_kwargs = client_kwargs or {}
        super(MongoStore, self).__init__(**kwargs)

    def connect(self, force_reset: bool = False):
        """
        Connect to the database store.

        Args:
            force_reset: whether to reset the connection or not when the Store is
                already connected.
        """
        if not self._coll or force_reset:
            # TODO - workaround, may be obviated by a future montydb update
            if self.database_path != ":memory:":
                set_storage(self.database_path, storage=self.storage, **self.storage_kwargs)
            client = MontyClient(self.database_path, **self.client_kwargs)
            self._coll = client[self.database_name][self.collection_name]

    def close(self):
        """Close up the MontyDB client."""
        if self._coll is not None:
            self._coll.database.client.close()

    @property
    def name(self) -> str:
        """Return a string representing this data source."""
        return f"monty://{self.database_path}/{self.database_name}/{self.collection_name}"

    def count(
        self,
        criteria: dict | None = None,
        hint: dict[str, Sort | int] | None = None,
    ) -> int:
        """
        Counts the number of documents matching the query criteria.

        Args:
            criteria: PyMongo filter for documents to count in
            hint: Dictionary of indexes to use as hints for query optimizer.
                Keys are field names and values are 1 for ascending or -1 for descending.
        """
        criteria = criteria if criteria else {}

        hint_list = (
            [(k, Sort(v).value) if isinstance(v, int) else (k, v.value) for k, v in hint.items()] if hint else None
        )

        if hint_list is not None:  # pragma: no cover
            return self._collection.count_documents(filter=criteria, hint=hint_list)

        return self._collection.count_documents(filter=criteria)

    def update(self, docs: list[dict] | dict, key: list | str | None = None):
        """
        Update documents into the Store.

        Args:
            docs: The document or list of documents to update.
            key: Field name(s) to determine uniqueness for a document, can be a list of
                multiple fields, a single field, or None if the Store's key field is to be
                used.
        """
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
                    self.logger.error(self.validator.validation_errors(d))

            if validates:
                key = key or self.key
                search_doc = {k: d[k] for k in key} if isinstance(key, list) else {key: d[key]}

                self._collection.replace_one(search_doc, d, upsert=True)
