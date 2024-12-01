"""
Module containing various definitions of Stores.
Stores are a default access pattern to data and provide
various utilities.
"""

import warnings
from collections.abc import Iterator
from itertools import chain, groupby
from pathlib import Path
from typing import Any, Callable, Literal, Optional, Union

import bson
import mongomock
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
        ssh_tunnel: Optional[SSHTunnel] = None,
        safe_update: bool = False,
        auth_source: Optional[str] = None,
        mongoclient_kwargs: Optional[dict] = None,
        default_sort: Optional[dict[str, Union[Sort, int]]] = None,
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

    def distinct(self, field: str, criteria: Optional[dict] = None, all_exist: bool = False) -> list:
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
        keys: Union[list[str], str],
        criteria: Optional[dict] = None,
        properties: Union[dict, list, None] = None,
        sort: Optional[dict[str, Union[Sort, int]]] = None,
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
        group_id = {letter: f"${key}" for letter, key in zip(alpha, keys)}
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
        criteria: Optional[dict] = None,
        hint: Optional[dict[str, Union[Sort, int]]] = None,
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
        criteria: Optional[dict] = None,
        properties: Union[dict, list, None] = None,
        sort: Optional[dict[str, Union[Sort, int]]] = None,
        hint: Optional[dict[str, Union[Sort, int]]] = None,
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

    def ensure_index(self, key: str, unique: Optional[bool] = False) -> bool:
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

    def update(self, docs: Union[list[dict], dict], key: Union[list, str, None] = None):
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
        database: Optional[str] = None,
        ssh_tunnel: Optional[SSHTunnel] = None,
        safe_update: bool = False,
        mongoclient_kwargs: Optional[dict] = None,
        default_sort: Optional[dict[str, Union[Sort, int]]] = None,
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
        self.ssh_tunnel = ssh_tunnel
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
    An in-memory Store that functions similarly
    to a MongoStore.
    """

    def __init__(self, collection_name: str = "memory_db", **kwargs):
        """
        Initializes the Memory Store.

        Args:
            collection_name: name for the collection in memory.
        """
        self.collection_name = collection_name
        self.default_sort = None
        self._coll = None
        self.kwargs = kwargs
        super(MongoStore, self).__init__(**kwargs)

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data.

        Args:
            force_reset: whether to reset the connection or not when the Store is
                already connected.
        """
        if self._coll is None or force_reset:
            self._coll = mongomock.MongoClient().db[self.name]  # type: ignore

    def close(self):
        """Close up all collections."""
        self._coll.database.client.close()

    @property
    def name(self):
        """Name for the store."""
        return f"mem://{self.collection_name}"

    def __hash__(self):
        """Hash for the store."""
        return hash((self.name, self.last_updated_field))

    def groupby(
        self,
        keys: Union[list[str], str],
        criteria: Optional[dict] = None,
        properties: Union[dict, list, None] = None,
        sort: Optional[dict[str, Union[Sort, int]]] = None,
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
            for k, v in zip(keys, vals):
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
        paths: Union[str, list[str]],
        read_only: bool = True,
        serialization_option: Optional[int] = None,
        serialization_default: Optional[Callable[[Any], Any]] = None,
        encoding: Optional[str] = None,
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
        paths = paths if isinstance(paths, (list, tuple)) else [paths]
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
            self._coll = mongomock.MongoClient().db[self.name]  # type: ignore

            # create the .json file if it does not exist
            if not self.read_only and not Path(self.paths[0]).exists():
                with zopen(self.paths[0], "w", encoding=self.encoding) as f:
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
        with zopen(path) as f:
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

    def update(self, docs: Union[list[dict], dict], key: Union[list, str, None] = None):
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
        with zopen(self.paths[0], "w", encoding=self.encoding) as f:
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
        database_path: Optional[str] = None,
        database_name: str = "db",
        storage: Literal["sqlite", "flatfile", "lightning"] = "sqlite",
        storage_kwargs: Optional[dict] = None,
        client_kwargs: Optional[dict] = None,
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

    @property
    def name(self) -> str:
        """Return a string representing this data source."""
        return f"monty://{self.database_path}/{self.database_name}/{self.collection_name}"

    def count(
        self,
        criteria: Optional[dict] = None,
        hint: Optional[dict[str, Union[Sort, int]]] = None,
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

    def update(self, docs: Union[list[dict], dict], key: Union[list, str, None] = None):
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
