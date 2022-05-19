# coding: utf-8
"""
Advanced Stores for connecting to AWS data
"""
import threading
import warnings
import zlib
from concurrent.futures import wait
from concurrent.futures.thread import ThreadPoolExecutor
from hashlib import sha1
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from json import dumps

import msgpack  # type: ignore
from monty.msgpack import default as monty_default

from maggma.core import Sort, Store
from maggma.utils import grouper, to_isoformat_ceil_ms

try:
    import boto3
    import botocore
    from boto3.session import Session
    from botocore.exceptions import ClientError
except (ImportError, ModuleNotFoundError):
    boto3 = None  # type: ignore


class S3Store(Store):
    """
    GridFS like storage using Amazon S3 and a regular store for indexing
    Assumes Amazon AWS key and secret key are set in environment or default config file
    """

    def __init__(
        self,
        index: Store,
        bucket: str,
        s3_profile: Optional[Union[str, dict]] = None,
        compress: bool = False,
        endpoint_url: str = None,
        sub_dir: str = None,
        s3_workers: int = 1,
        s3_resource_kwargs: Optional[dict] = None,
        key: str = "fs_id",
        store_hash: bool = True,
        unpack_data: bool = True,
        searchable_fields: Optional[List[str]] = None,
        **kwargs,
    ):
        """
        Initializes an S3 Store

        Args:
            index: a store to use to index the S3 Bucket
            bucket: name of the bucket
            s3_profile: name of aws profile containing credentials for role.
                Alternatively you can pass in a dictionary with the full credentials:
                    aws_access_key_id (string) -- AWS access key ID
                    aws_secret_access_key (string) -- AWS secret access key
                    aws_session_token (string) -- AWS temporary session token
                    region_name (string) -- Default region when creating new connections
            compress: compress files inserted into the store
            endpoint_url: endpoint_url to allow interface to minio service
            sub_dir: (optional)  subdirectory of the s3 bucket to store the data
            s3_workers: number of concurrent S3 puts to run
            store_hash: store the sha1 hash right before insertion to the database.
            unpack_data: whether to decompress and unpack byte data when querying from the bucket.
            searchable_fields: fields to keep in the index store
        """
        if boto3 is None:
            raise RuntimeError("boto3 and botocore are required for S3Store")
        self.index = index

        self.bucket = bucket
        self.s3_profile = s3_profile
        self.compress = compress
        self.endpoint_url = endpoint_url
        self.sub_dir = sub_dir.strip("/") + "/" if sub_dir else ""
        self.s3 = None  # type: Any
        self.s3_bucket = None  # type: Any
        self.s3_workers = s3_workers
        self.s3_resource_kwargs = (
            s3_resource_kwargs if s3_resource_kwargs is not None else {}
        )
        self.unpack_data = unpack_data
        self.searchable_fields = (
            searchable_fields if searchable_fields is not None else []
        )
        self.store_hash = store_hash

        # Force the key to be the same as the index
        assert isinstance(
            index.key, str
        ), "Since we are using the key as a file name in S3, they key must be a string"
        if key != index.key:
            warnings.warn(
                f'The desired S3Store key "{key}" does not match the index key "{index.key},"'
                "the index key will be used",
                UserWarning,
            )
        kwargs["key"] = str(index.key)

        self._thread_local = threading.local()
        super(S3Store, self).__init__(**kwargs)

    @property
    def name(self) -> str:
        """
        Returns:
            a string representing this data source
        """
        return f"s3://{self.bucket}"

    def connect(self, *args, **kwargs):  # lgtm[py/conflicting-attributes]
        """
        Connect to the source data
        """

        session = self._get_session()
        resource = session.resource(
            "s3", endpoint_url=self.endpoint_url, **self.s3_resource_kwargs
        )

        if not self.s3:
            self.s3 = resource
            try:
                self.s3.meta.client.head_bucket(Bucket=self.bucket)
            except ClientError:
                raise RuntimeError("Bucket not present on AWS: {}".format(self.bucket))

            self.s3_bucket = resource.Bucket(self.bucket)
        self.index.connect(*args, **kwargs)

    def close(self):
        """
        Closes any connections
        """
        self.index.close()
        self.s3 = None
        self.s3_bucket = None

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
        prop_keys = set()
        if isinstance(properties, dict):
            prop_keys = set(properties.keys())
        elif isinstance(properties, list):
            prop_keys = set(properties)

        for doc in self.index.query(
            criteria=criteria, sort=sort, limit=limit, skip=skip
        ):
            if properties is not None and prop_keys.issubset(set(doc.keys())):
                yield {p: doc[p] for p in properties if p in doc}
            else:
                try:
                    # TODO: THis is ugly and unsafe, do some real checking before pulling data
                    data = (
                        self.s3_bucket.Object(self.sub_dir + str(doc[self.key]))
                        .get()["Body"]
                        .read()
                    )
                except botocore.exceptions.ClientError as e:
                    # If a client error is thrown, then check that it was a 404 error.
                    # If it was a 404 error, then the object does not exist.
                    error_code = int(e.response["Error"]["Code"])
                    if error_code == 404:
                        self.logger.error(
                            "Could not find S3 object {}".format(doc[self.key])
                        )
                        break
                    else:
                        raise e

                if self.unpack_data:
                    data = self._unpack(
                        data=data, compressed=doc.get("compression", "") == "zlib"
                    )

                    if self.last_updated_field in doc:
                        data[self.last_updated_field] = doc[self.last_updated_field]

                yield data

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
        unpacked_data = msgpack.unpackb(data, raw=False)
        return unpacked_data

    def distinct(
        self, field: str, criteria: Optional[Dict] = None, all_exist: bool = False
    ) -> List:
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
        Tries to create an index and return true if it suceeded

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
            additional_metadata: field(s) to include in the s3 store's metadata
        """
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

        with ThreadPoolExecutor(max_workers=self.s3_workers) as pool:
            fs = {
                pool.submit(
                    self.write_doc_to_s3,
                    doc=itr_doc,
                    search_keys=key + additional_metadata + self.searchable_fields,
                )
                for itr_doc in docs
            }
            fs, _ = wait(fs)

            search_docs = [sdoc.result() for sdoc in fs]

        # Use store's update to remove key clashes
        self.index.update(search_docs, key=self.key)

    def _get_session(self):
        if not hasattr(self._thread_local, "s3_bucket"):
            if isinstance(self.s3_profile, dict):
                return Session(**self.s3_profile)
            else:
                return Session(profile_name=self.s3_profile)

    def _get_bucket(self):
        """
        If on the main thread return the bucket created above, else create a new bucket on each thread
        """
        if threading.current_thread().name == "MainThread":
            return self.s3_bucket
        if not hasattr(self._thread_local, "s3_bucket"):
            session = self._get_session()
            resource = session.resource("s3", endpoint_url=self.endpoint_url)
            self._thread_local.s3_bucket = resource.Bucket(self.bucket)
        return self._thread_local.s3_bucket

    def write_doc_to_s3(self, doc: Dict, search_keys: List[str]):
        """
        Write the data to s3 and return the metadata to be inserted into the index db

        Args:
            doc: the document
            search_keys: list of keys to pull from the docs and be inserted into the
            index db
        """
        s3_bucket = self._get_bucket()

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
            # need this conversion for aws metadata insert
            search_doc[self.last_updated_field] = str(
                to_isoformat_ceil_ms(doc[self.last_updated_field])
            )

        # keep a record of original keys, in case these are important for the individual researcher
        # it is not expected that this information will be used except in disaster recovery
        s3_to_mongo_keys = {k: self._sanitize_key(k) for k in search_doc.keys()}
        s3_to_mongo_keys["s3-to-mongo-keys"] = "s3-to-mongo-keys"  # inception
        # encode dictionary since values have to be strings
        search_doc["s3-to-mongo-keys"] = dumps(s3_to_mongo_keys)
        s3_bucket.put_object(
            Key=self.sub_dir + str(doc[self.key]),
            Body=data,
            Metadata={s3_to_mongo_keys[k]: str(v) for k, v in search_doc.items()},
        )

        if lu_info is not None:
            search_doc[self.last_updated_field] = lu_info

        if self.store_hash:
            hasher = sha1()
            hasher.update(data)
            obj_hash = hasher.hexdigest()
            search_doc["obj_hash"] = obj_hash
        return search_doc

    @staticmethod
    def _sanitize_key(key):
        """
        Sanitize keys to store in S3/MinIO metadata.
        """

        # Any underscores are encoded as double dashes in metadata, since keys with
        # underscores may be result in the corresponding HTTP header being stripped
        # by certain server configurations (e.g. default nginx), leading to:
        # `botocore.exceptions.ClientError: An error occurred (AccessDenied) when
        # calling the PutObject operation: There were headers present in the request
        # which were not signed`
        # Metadata stored in the MongoDB index (self.index) is stored unchanged.

        # Additionally, MinIO requires lowercase keys
        return str(key).replace("_", "-").lower()

    def remove_docs(self, criteria: Dict, remove_s3_object: bool = False):
        """
        Remove docs matching the query dictionary

        Args:
            criteria: query dictionary to match
            remove_s3_object: whether to remove the actual S3 Object or not
        """
        if not remove_s3_object:
            self.index.remove_docs(criteria=criteria)
        else:
            to_remove = self.index.distinct(self.key, criteria=criteria)
            self.index.remove_docs(criteria=criteria)

            # Can remove up to 1000 items at a time via boto
            to_remove_chunks = list(grouper(to_remove, n=1000))
            for chunk_to_remove in to_remove_chunks:
                objlist = [{"Key": f"{self.sub_dir}{obj}"} for obj in chunk_to_remove]
                self.s3_bucket.delete_objects(Delete={"Objects": objlist})

    @property
    def last_updated(self):
        return self.index.last_updated

    def newer_in(
        self, target: Store, criteria: Optional[Dict] = None, exhaustive: bool = False
    ) -> List[str]:
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
            return self.index.newer_in(
                target=target.index, criteria=criteria, exhaustive=exhaustive
            )
        else:
            return self.index.newer_in(
                target=target, criteria=criteria, exhaustive=exhaustive
            )

    def __hash__(self):
        return hash((self.index.__hash__, self.bucket))

    def rebuild_index_from_s3_data(self, **kwargs):
        """
        Rebuilds the index Store from the data in S3
        Relies on the index document being stores as the metadata for the file
        This can help recover lost databases
        """
        bucket = self.s3_bucket
        objects = bucket.objects.filter(Prefix=self.sub_dir)
        for obj in objects:
            key_ = self.sub_dir + obj.key
            data = self.s3_bucket.Object(key_).get()["Body"].read()

            if self.compress:
                data = zlib.decompress(data)
            unpacked_data = msgpack.unpackb(data, raw=False)
            self.update(unpacked_data, **kwargs)

    def rebuild_metadata_from_index(self, index_query: dict = None):
        """
        Read data from the index store and populate the metadata of the S3 bucket
        Force all of the keys to be lower case to be Minio compatible
        Args:
            index_query: query on the index store
        """

        qq = {} if index_query is None else index_query
        for index_doc in self.index.query(qq):
            key_ = self.sub_dir + index_doc[self.key]
            s3_object = self.s3_bucket.Object(key_)
            new_meta = {self._sanitize_key(k): v for k, v in s3_object.metadata.items()}
            for k, v in index_doc.items():
                new_meta[str(k).lower()] = v
            new_meta.pop("_id")
            if self.last_updated_field in new_meta:
                new_meta[self.last_updated_field] = str(
                    to_isoformat_ceil_ms(new_meta[self.last_updated_field])
                )
            # s3_object.metadata.update(new_meta)
            s3_object.copy_from(
                CopySource={"Bucket": self.s3_bucket.name, "Key": key_},
                Metadata=new_meta,
                MetadataDirective="REPLACE",
            )

    def __eq__(self, other: object) -> bool:
        """
        Check equality for S3Store
        other: other S3Store to compare with
        """
        if not isinstance(other, S3Store):
            return False

        fields = ["index", "bucket", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)
