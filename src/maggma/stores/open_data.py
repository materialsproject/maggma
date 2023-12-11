import gzip
from io import BytesIO
from typing import Any, Callable, Dict, List, Optional

import orjson
from boto3 import Session
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from bson import json_util

from maggma.stores.aws import S3Store
from maggma.stores.mongolike import MemoryStore


class S3IndexStore(MemoryStore):
    """
    A store that loads the index of the collection from an S3 file.

    S3IndexStore can still apply MongoDB-like writable operations
    (e.g. an update) because it behaves like a MemoryStore,
    but it will not write those changes to S3.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        endpoint_url: Optional[str] = None,
        manifest_key: str = "manifest.json",
        **kwargs,
    ):
        """Initializes an S3IndexStore

        Args:
            bucket (str): Name of the bucket where the index is stored.
            prefix (str, optional): The prefix to add to the name of the index, i.e. the manifest key.
                Defaults to "".
            endpoint_url (Optional[str], optional): S3-compatible endpoint URL.
                Defaults to None, indicating to use the default configured AWS S3.
            manifest_key (str, optional): The name of the index. Defaults to "manifest.json".
        """
        self.bucket = bucket
        self.prefix = prefix
        self.endpoint_url = endpoint_url
        self.client: Any = None
        self.session: Any = None
        self.s3_session_kwargs = {}
        self.manifest_key = manifest_key

        super().__init__(**kwargs)

    def _get_full_key_path(self) -> str:
        """Produces the full path for the index."""
        return f"{self.prefix}{self.manifest_key}"

    def _retrieve_manifest(self) -> List[Dict]:
        """Retrieves the contents of the index stored in S3.

        Returns:
            List[Dict]: The index contents with each item representing a document.
        """
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._get_full_key_path())
            json_data = orjson.loads(response["Body"].read().decode("utf-8"))
            return json_data if isinstance(json_data, list) else [json_data]
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "NoSuchKey":
                return []
            raise

    def _load_index(self, force_reset: bool = False) -> None:
        """Load the contents of the index stored in S3 into memory.

        Args:
            force_reset: whether to force a reset of the memory store prior to load
        """
        super().connect(force_reset=force_reset)
        super().update(self._retrieve_manifest())

    def store_manifest(self, data: List[Dict]) -> None:
        """Stores the provided data into the index stored in S3.
        This overwrites and fully replaces all of the contents of the previous index stored in S3.
        It also rewrites the memory index with the provided data.

        Args:
            data (List[Dict]): The data to store in the index.
        """
        self.client.put_object(
            Bucket=self.bucket,
            Body=orjson.dumps(data, default=json_util.default),
            Key=self._get_full_key_path(),
        )
        super().connect(force_reset=True)
        super().update(data)

    def connect(self, force_reset: bool = False):
        """
        Sets up the S3 client and loads the contents of the index stored in S3 into memory.

        Args:
            force_reset: whether to force a reset of the memory store prior to load
        """
        # set up the S3 client
        if not self.session:
            self.session = Session(**self.s3_session_kwargs)

        self.client = self.session.client("s3", endpoint_url=self.endpoint_url)

        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            raise RuntimeError(f"Bucket not present on AWS: {self.bucket}")

        # load index
        self._load_index(force_reset=force_reset)

    def __hash__(self):
        return hash((self.collection_name, self.bucket, self.prefix))

    def __eq__(self, other: object) -> bool:
        """
        Check equality for S3Store
        other: other S3Store to compare with.
        """
        if not isinstance(other, S3IndexStore):
            return False

        fields = ["collection_name", "bucket", "prefix", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


class OpenDataStore(S3Store):
    """
    Data is stored on S3 compatible storage using the format used by Materials Project on OpenData.
    The index is loaded from S3 compatible storage into memory.

    Note that updates will only affect the in-memory representation of the index - they will not be persisted.
    To persist writes utilize the rebuild_index_* functions.

    This Store should not be used for applications that are distributed and rely on reading updated
    values from the index as data inconsistencied will arise.
    """

    def __init__(
        self,
        index: S3IndexStore,
        object_file_extension: str = ".json.gz",
        access_as_public_bucket: bool = False,
        **kwargs,
    ):
        """Initializes an OpenDataStore

        Args:
            index (S3IndexStore): The store that'll be used as the index, ie for queries pertaining to this store.
            object_file_extension (str, optional): The extension used for the data stored in S3. Defaults to ".json.gz".
            access_as_public_bucket (bool, optional): If True, the S3 bucket will be accessed without signing, ie as if it's a public bucket.
                This is useful for end users. Defaults to False.
        """
        self.index = index
        self.object_file_extension = object_file_extension
        self.access_as_public_bucket = access_as_public_bucket
        if access_as_public_bucket:
            kwargs["s3_resource_kwargs"] = kwargs["s3_resource_kwargs"] if "s3_resource_kwargs" in kwargs else {}
            kwargs["s3_resource_kwargs"]["config"] = Config(signature_version=UNSIGNED)

        kwargs["index"] = index
        kwargs["unpack_data"] = True
        super().__init__(**kwargs)

    def _get_full_key_path(self, id: str) -> str:
        return f"{self.sub_dir}{id}{self.object_file_extension}"

    def _get_id_from_full_key_path(self, key: str) -> str:
        prefix, suffix = self.sub_dir, self.object_file_extension
        if prefix in key and suffix in key:
            start_idx = key.index(prefix) + len(prefix)
            end_idx = key.index(suffix, start_idx)
            return key[start_idx:end_idx]
        return ""

    def _get_compression_function(self) -> Callable:
        return gzip.compress

    def _get_decompression_function(self) -> Callable:
        return gzip.decompress

    def _read_data(self, data: bytes, compress_header: str = "gzip") -> List[Dict]:
        if compress_header is not None:
            data = self._get_decompression_function()(data)
        return orjson.loads(data)

    def _gather_indexable_data(self, doc: Dict, search_keys: List[str]) -> Dict:
        index_doc = {k: doc[k] for k in search_keys}
        index_doc[self.key] = doc[self.key]  # Ensure key is in metadata
        # Ensure last updated field is in metada if it's present in the data
        if self.last_updated_field in doc:
            index_doc[self.last_updated_field] = doc[self.last_updated_field]
        return index_doc

    def write_doc_to_s3(self, doc: Dict, search_keys: List[str]) -> Dict:
        search_doc = self._gather_indexable_data(doc, search_keys)

        data = orjson.dumps(doc, default=json_util.default)
        data = self._get_compression_function()(data)
        self._get_bucket().upload_fileobj(
            Fileobj=BytesIO(data),
            Key=self._get_full_key_path(str(doc[self.key])),
        )
        return search_doc

    def _index_for_doc_from_s3(self, bucket, key: str) -> Dict:
        response = bucket.Object(key).get()
        doc = self._read_data(response["Body"].read())
        return self._gather_indexable_data(doc, self.searchable_fields)

    def rebuild_index_from_s3_data(self) -> List[Dict]:
        """
        Rebuilds the index Store from the data in S3
        Stores only the key, last_updated_field and searchable_fields in the index.

        Returns:
            List[Dict]: The set of docs representing the index data.
        """
        bucket = self._get_bucket()
        paginator = bucket.meta.client.get_paginator("list_objects_v2")

        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=self.sub_dir.strip("/"))

        all_index_docs = []
        for page in page_iterator:
            for file in page["Contents"]:
                key = file["Key"]
                if key != self.index._get_full_key_path():
                    index_doc = self._index_for_doc_from_s3(bucket, key)
                    all_index_docs.append(index_doc)
        self.index.store_manifest(all_index_docs)
        return all_index_docs

    def rebuild_index_from_data(self, docs: List[Dict]) -> List[Dict]:
        """
        Rebuilds the index Store from the provided data.
        The provided data needs to include all of the documents in this data set.
        Stores only the key, last_updated_field and searchable_fields in the index.

        Args:
            docs (List[Dict]): The data to build the index from.

        Returns:
            List[Dict]: The set of docs representing the index data.
        """
        all_index_docs = []
        for doc in docs:
            index_doc = self._gather_indexable_data(doc, self.searchable_fields)
            all_index_docs.append(index_doc)
        self.index.store_manifest(all_index_docs)
        return all_index_docs
