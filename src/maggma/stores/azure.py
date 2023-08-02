"""
Advanced Stores for connecting to Microsoft Azure data
"""
import os
import threading
import warnings
import zlib
from concurrent.futures import wait
from concurrent.futures.thread import ThreadPoolExecutor
from hashlib import sha1
from json import dumps
from typing import Dict, Iterator, List, Optional, Tuple, Union

import msgpack  # type: ignore
from monty.msgpack import default as monty_default

from maggma.core import Sort, Store
from maggma.utils import grouper, to_isoformat_ceil_ms

try:
    import azure
    import azure.storage.blob as azure_blob
    from azure.core.exceptions import ResourceExistsError
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient, ContainerClient
except (ImportError, ModuleNotFoundError):
    azure_blob = None  # type: ignore
    ContainerClient = None


AZURE_KEY_SANITIZE = {"-": "_", ".": "_"}


class AzureBlobStore(Store):
    """
    GridFS like storage using Azure Blob and a regular store for indexing.

    Requires azure-storage-blob and azure-identity modules to be installed.
    """

    def __init__(
        self,
        index: Store,
        container_name: str,
        azure_client_info: Optional[Union[str, dict]] = None,
        compress: bool = False,
        sub_dir: Optional[str] = None,
        workers: int = 1,
        azure_resource_kwargs: Optional[dict] = None,
        key: str = "fs_id",
        store_hash: bool = True,
        unpack_data: bool = True,
        searchable_fields: Optional[List[str]] = None,
        key_sanitize_dict: Optional[dict] = None,
        create_container: bool = False,
        **kwargs,
    ):
        """
        Initializes an AzureBlob Store

        Args:
            index: a store to use to index the Azure blob
            container_name: name of the container
            azure_client_info: connection_url of the BlobServiceClient if a string.
                Assumes that the access is passwordless in that case.
                Otherwise, if a dictionary, options to instantiate the
                BlobServiceClient.
                Currently supported keywords:
                    - connection_string: a connection string for the Azure blob
            compress: compress files inserted into the store
            sub_dir: (optional)  subdirectory of the container to store the data.
                When defined, a final "/" will be added if not already present.
            workers: number of concurrent Azure puts to run
            store_hash: store the sha1 hash right before insertion to the database.
            unpack_data: whether to decompress and unpack byte data when querying from
                the container.
            searchable_fields: fields to keep in the index store
            key_sanitize_dict: a dictionary that allows to customize the sanitization
                of the keys in metadata, since they should adhere to the naming rules
                for C# identifiers. If None the AZURE_KEY_SANITIZE default will be used
                to handle the most common cases.
            create_container: if True the Store creates the container, in case it does
                not exist.
            kwargs: keywords for the base Store.
        """
        if azure_blob is None:
            raise RuntimeError("azure-storage-blob and azure-identity are required for AzureBlobStore")

        self.index = index
        self.container_name = container_name
        self.azure_client_info = azure_client_info
        self.compress = compress
        self.sub_dir = sub_dir.rstrip("/") + "/" if sub_dir else ""
        self.service: Optional[BlobServiceClient] = None
        self.container: Optional[ContainerClient] = None
        self.workers = workers
        self.azure_resource_kwargs = azure_resource_kwargs if azure_resource_kwargs is not None else {}
        self.unpack_data = unpack_data
        self.searchable_fields = searchable_fields if searchable_fields is not None else []
        self.store_hash = store_hash
        if key_sanitize_dict is None:
            key_sanitize_dict = AZURE_KEY_SANITIZE
        self.key_sanitize_dict = key_sanitize_dict
        self.create_container = create_container

        # Force the key to be the same as the index
        assert isinstance(
            index.key, str
        ), "Since we are using the key as a file name in Azure Blob, the key must be a string"
        if key != index.key:
            warnings.warn(
                f'The desired AzureBlobStore key "{key}" does not match the index key "{index.key},"'
                "the index key will be used",
                UserWarning,
            )
        kwargs["key"] = str(index.key)

        self._thread_local = threading.local()
        super().__init__(**kwargs)

    @property
    def name(self) -> str:
        """
        Returns:
            a string representing this data source
        """
        return f"container://{self.container_name}"

    def connect(self, *args, **kwargs):  # lgtm[py/conflicting-attributes]
        """
        Connect to the source data
        """

        service_client = self._get_service_client()

        if not self.service:
            self.service = service_client
            container = service_client.get_container_client(self.container_name)
            if not container.exists():
                if self.create_container:
                    # catch the exception to avoid errors if already created
                    try:
                        container.create_container()
                    except ResourceExistsError:
                        pass
                else:
                    raise RuntimeError(f"Container not present on Azure: {self.container_name}")

            self.container = container
        self.index.connect(*args, **kwargs)

    def close(self):
        """
        Closes any connections
        """
        self.index.close()
        self.service = None
        self.container = None

    @property
    def _collection(self):
        """
        Returns:
            a handle to the pymongo collection object

        Important:
            Not guaranteed to exist in the future
        """
        # For now returns the index collection since that is what we would "search" on
        return self.index._collection

    def count(self, criteria: Optional[Dict] = None) -> int:
        """
        Counts the number of documents matching the query criteria

        Args:
            criteria: PyMongo filter for documents to count in
        """

        return self.index.count(criteria)

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

        if self.container is None or self.service is None:
            raise RuntimeError("The store has not been connected")

        prop_keys = set()
        if isinstance(properties, dict):
            prop_keys = set(properties.keys())
        elif isinstance(properties, list):
            prop_keys = set(properties)

        for doc in self.index.query(criteria=criteria, sort=sort, limit=limit, skip=skip):
            if properties is not None and prop_keys.issubset(set(doc.keys())):
                yield {p: doc[p] for p in properties if p in doc}
            else:
                try:
                    data = self.container.download_blob(self.sub_dir + str(doc[self.key])).readall()
                except azure.core.exceptions.ResourceNotFoundError:
                    self.logger.error(f"Could not find Blob object {doc[self.key]}")

                if self.unpack_data:
                    data = self._unpack(data=data, compressed=doc.get("compression", "") == "zlib")

                    if self.last_updated_field in doc:
                        data[self.last_updated_field] = doc[self.last_updated_field]  # type: ignore

                yield data  # type: ignore

    @staticmethod
    def _unpack(data: bytes, compressed: bool):
        if compressed:
            data = zlib.decompress(data)
        # requires msgpack-python to be installed to fix string encoding problem
        # https://github.com/msgpack/msgpack/issues/121
        # During recursion
        # msgpack.unpackb goes as deep as possible during reconstruction
        # MontyDecoder().process_decode only goes until it finds a from_dict
        # as such, we cannot just use msgpack.unpackb(data, object_hook=monty_object_hook, raw=False)
        # Should just return the unpacked object then let the user run process_decoded
        return msgpack.unpackb(data, raw=False)

    def distinct(self, field: str, criteria: Optional[Dict] = None, all_exist: bool = False) -> List:
        """
        Get all distinct values for a field

        Args:
            field: the field(s) to get distinct values for
            criteria: PyMongo filter for documents to search in
        """
        # Index is a store so it should have its own distinct function
        return self.index.distinct(field, criteria=criteria)

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
            generator returning tuples of (dict, list of docs)
        """
        return self.index.groupby(
            keys=keys,
            criteria=criteria,
            properties=properties,
            sort=sort,
            skip=skip,
            limit=limit,
        )

    def ensure_index(self, key: str, unique: bool = False) -> bool:
        """
        Tries to create an index and return true if it succeeded

        Args:
            key: single key to index
            unique: Whether or not this index contains only unique keys

        Returns:
            bool indicating if the index exists/was created
        """
        return self.index.ensure_index(key, unique=unique)

    def update(
        self,
        docs: Union[List[Dict], Dict],
        key: Union[List, str, None] = None,
        additional_metadata: Union[str, List[str], None] = None,
    ):
        """
        Update documents into the Store

        Args:
            docs: the document or list of documents to update
            key: field name(s) to determine uniqueness for a
                 document, can be a list of multiple fields,
                 a single field, or None if the Store's key
                 field is to be used
            additional_metadata: field(s) to include in the blob store's metadata
        """

        if self.container is None or self.service is None:
            raise RuntimeError("The store has not been connected")

        if not isinstance(docs, list):
            docs = [docs]

        if isinstance(key, str):
            key = [key]
        elif not key:
            key = [self.key]

        if additional_metadata is None:
            additional_metadata = []
        elif isinstance(additional_metadata, str):
            additional_metadata = [additional_metadata]
        else:
            additional_metadata = list(additional_metadata)

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            fs = {
                pool.submit(
                    self.write_doc_to_blob,
                    doc=itr_doc,
                    search_keys=key + additional_metadata + self.searchable_fields,
                )
                for itr_doc in docs
            }
            fs, _ = wait(fs)

            search_docs = [sdoc.result() for sdoc in fs]

        # Use store's update to remove key clashes
        self.index.update(search_docs, key=self.key)

    def _get_service_client(self):
        if not hasattr(self._thread_local, "container"):
            if isinstance(self.azure_client_info, str):
                # assume it is the account_url and that the connection is passwordless
                default_credential = DefaultAzureCredential()
                return BlobServiceClient(self.azure_client_info, credential=default_credential)

            if isinstance(self.azure_client_info, dict):
                connection_string = self.azure_client_info.get("connection_string")
                if connection_string:
                    return BlobServiceClient.from_connection_string(conn_str=connection_string)

            msg = f"Could not instantiate BlobServiceClient from azure_client_info: {self.azure_client_info}"
            raise RuntimeError(msg)
        return None

    def _get_container(self) -> Optional[ContainerClient]:
        """
        If on the main thread return the container created above, else create a new
        container on each thread.
        """
        if threading.current_thread().name == "MainThread":
            return self.container
        if not hasattr(self._thread_local, "container"):
            service_client = self._get_service_client()
            container = service_client.get_container_client(self.container_name)
            self._thread_local.container = container
        return self._thread_local.container

    def write_doc_to_blob(self, doc: Dict, search_keys: List[str]):
        """
        Write the data to an Azure blob and return the metadata to be inserted into the index db

        Args:
            doc: the document
            search_keys: list of keys to pull from the docs and be inserted into the
            index db
        """
        container = self._get_container()
        if container is None:
            raise RuntimeError("The store has not been connected")

        search_doc = {k: doc[k] for k in search_keys}
        search_doc[self.key] = doc[self.key]  # Ensure key is in metadata
        if self.sub_dir != "":
            search_doc["sub_dir"] = self.sub_dir

        # Remove MongoDB _id from search
        if "_id" in search_doc:
            del search_doc["_id"]

        # to make hashing more meaningful, make sure last updated field is removed
        lu_info = doc.pop(self.last_updated_field, None)
        data = msgpack.packb(doc, default=monty_default)

        if self.compress:
            # Compress with zlib if chosen
            search_doc["compression"] = "zlib"
            data = zlib.compress(data)

        if self.last_updated_field in doc:
            # need this conversion for metadata insert
            search_doc[self.last_updated_field] = str(to_isoformat_ceil_ms(doc[self.last_updated_field]))

        # keep a record of original keys, in case these are important for the individual researcher
        # it is not expected that this information will be used except in disaster recovery
        blob_to_mongo_keys = {k: self._sanitize_key(k) for k in search_doc}
        blob_to_mongo_keys["blob_to_mongo_keys"] = "blob_to_mongo_keys"  # inception
        # encode dictionary since values have to be strings
        search_doc["blob_to_mongo_keys"] = dumps(blob_to_mongo_keys)

        container.upload_blob(
            name=self.sub_dir + str(doc[self.key]),
            data=data,
            metadata={blob_to_mongo_keys[k]: str(v) for k, v in search_doc.items()},
            overwrite=True,
        )

        if lu_info is not None:
            search_doc[self.last_updated_field] = lu_info

        if self.store_hash:
            hasher = sha1()
            hasher.update(data)
            obj_hash = hasher.hexdigest()
            search_doc["obj_hash"] = obj_hash
        return search_doc

    def _sanitize_key(self, key):
        """
        Sanitize keys to store metadata.
        The metadata keys should adhere to the naming rules for C# identifiers.
        """

        new_key = str(key)
        for k, v in self.key_sanitize_dict.items():
            new_key = new_key.replace(k, v)

        return new_key

    def remove_docs(self, criteria: Dict, remove_blob_object: bool = False):
        """
        Remove docs matching the query dictionary

        Args:
            criteria: query dictionary to match
            remove_blob_object: whether to remove the actual blob Object or not
        """
        if self.container is None or self.service is None:
            raise RuntimeError("The store has not been connected")

        if not remove_blob_object:
            self.index.remove_docs(criteria=criteria)
        else:
            to_remove = self.index.distinct(self.key, criteria=criteria)
            self.index.remove_docs(criteria=criteria)

            # Can remove up to 256 items at a time
            to_remove_chunks = list(grouper(to_remove, n=256))
            for chunk_to_remove in to_remove_chunks:
                objlist = [{"name": f"{self.sub_dir}{obj}"} for obj in chunk_to_remove]
                self.container.delete_blobs(*objlist)

    @property
    def last_updated(self):
        return self.index.last_updated

    def newer_in(self, target: Store, criteria: Optional[Dict] = None, exhaustive: bool = False) -> List[str]:
        """
        Returns the keys of documents that are newer in the target
        Store than this Store.

        Args:
            target: target Store
            criteria: PyMongo filter for documents to search in
            exhaustive: triggers an item-by-item check vs. checking
                        the last_updated of the target Store and using
                        that to filter out new items in
        """
        if hasattr(target, "index"):
            return self.index.newer_in(target=target.index, criteria=criteria, exhaustive=exhaustive)

        return self.index.newer_in(target=target, criteria=criteria, exhaustive=exhaustive)

    def __hash__(self):
        return hash((self.index.__hash__, self.container_name))

    def rebuild_index_from_blob_data(self, **kwargs):
        """
        Rebuilds the index Store from the data in Azure
        Relies on the index document being stores as the metadata for the file
        This can help recover lost databases
        """

        objects = self.container.list_blobs(name_starts_with=self.sub_dir)
        for obj in objects:
            # handle the case where there are subdirs in the chosen container
            # but are below the level of the current subdir
            dir_name = os.path.dirname(obj.name)
            if dir_name != self.sub_dir:
                continue

            data = self.container.download_blob(obj.name).readall()

            if self.compress:
                data = zlib.decompress(data)
            unpacked_data = msgpack.unpackb(data, raw=False)
            # TODO maybe it can be avoided to reupload the data, since it is paid
            self.update(unpacked_data, **kwargs)

    def rebuild_metadata_from_index(self, index_query: Optional[Dict] = None):
        """
        Read data from the index store and populate the metadata of the Azure Blob.
        Force all of the keys to be lower case to be Minio compatible
        Args:
            index_query: query on the index store
        """
        if self.container is None or self.service is None:
            raise RuntimeError("The store has not been connected")

        qq = {} if index_query is None else index_query
        for index_doc in self.index.query(qq):
            key_ = self.sub_dir + index_doc[self.key]
            blob = self.container.get_blob_client(key_)
            properties = blob.get_blob_properties()
            new_meta = {self._sanitize_key(k): v for k, v in properties.metadata.items()}
            for k, v in index_doc.items():
                new_meta[str(k).lower()] = v
            new_meta.pop("_id")
            if self.last_updated_field in new_meta:
                new_meta[self.last_updated_field] = str(to_isoformat_ceil_ms(new_meta[self.last_updated_field]))
            blob.set_blob_metadata(new_meta)

    def __eq__(self, other: object) -> bool:
        """
        Check equality for AzureBlobStore
        other: other AzureBlobStore to compare with
        """
        if not isinstance(other, AzureBlobStore):
            return False

        fields = ["index", "container_name", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)
