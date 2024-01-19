import gzip
from datetime import datetime
from io import BytesIO
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import orjson
import pandas as pd
from boto3 import Session
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from bson import json_util

from maggma.core.store import Sort, Store
from maggma.stores.aws import S3Store


class PandasMemoryStore(Store):
    """
    A store that is backed by Pandas DataFrame.

    """

    def __init__(
        self,
        **kwargs,
    ):
        self._data = None
        super().__init__(**kwargs)

    @property
    def _collection(self):
        """
        Returns a handle to the pymongo collection object

        Raises:
            NotImplementedError: always as this concept does not make sense for this type of store
        """
        raise NotImplementedError("Index memory store cannot be used with this property")

    @property
    def name(self) -> str:
        """
        Return a string representing this data source
        """
        return "imem://"

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data.
        Not necessary for this type of store but here for compatibility.

        Args:
            force_reset: whether to reset the connection or not
        """
        return

    def close(self):
        """
        Closes any connections
        Not necessary for this type of store but here for compatibility.
        """
        return

    def count(self, criteria: Optional[Dict] = None) -> int:
        """
        Counts the number of documents matching the query criteria

        Returns:
            int: the number of documents matching the query criteria

        Args:
            criteria: the value of the `query` key will be used as the string expression to filter;
                NotImplmentedError will be thrown if it's not None and this key/value pair does not exist
        """
        query_string = None
        if criteria and "query" not in criteria:
            raise NotImplementedError("Pandas memory store cannot handle PyMongo filters")
        if criteria:
            query_string = criteria["query"]

        if self._data is None:
            return 0
        if query_string is None:
            return len(self._data)
        return len(self._data.query(query_string))

    def _query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> pd.DataFrame:
        query_string = ""
        if criteria and "query" not in criteria:
            raise AttributeError("Pandas memory store cannot handle PyMongo filters")
        if criteria:
            query_string = criteria["query"]

        if isinstance(properties, dict):
            properties = [key for key, value in properties.items() if value == 1]

        if self._data is None:
            return iter([])

        ret = self._data

        if query_string:
            ret = ret.query(query_string)

        if properties:
            ret = ret[properties]

        if sort:
            sort_keys, sort_ascending = zip(*[(k, v == 1) for k, v in sort.items()])
            ret = ret.sort_values(by=list(sort_keys), ascending=list(sort_ascending))

        ret = ret[skip:]
        if limit > 0:
            ret = ret[:limit]
        return ret

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
            criteria: the value of the `query` key will be used as the Pandas string expression to query with;
                all other data will be ignored
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip (from the start of the result set)
            limit: limit on total number of documents returned

        Returns:
            Iterator[Dict]: an iterator over the documents that match the query parameters

        Raises:
            AttributeError: if criteria exists and does not include a query key
        """
        ret = self._query(criteria=criteria, properties=properties, sort=sort, skip=skip, limit=limit)
        return (row.to_dict() for _, row in ret.iterrows())

    def update(self, docs: Union[List[Dict], Dict], key: Union[List, str, None] = None, clear_first: bool = False):
        """
        Update documents into the Store

        Args:
            docs: the document or list of documents to update
            key: field name(s) to determine uniqueness for a
                 document, can be a list of multiple fields,
                 a single field, or None if the Store's key
                 field is to be used
            clear_first: if True clears the underlying data first, fully replacing the data with docs;
                if False performs an upsert based on the parameters
        """
        df = pd.DataFrame(docs)
        if self._data is None or clear_first:
            if not df.empty:
                self._data = df
            return

        if key is None:
            key = self.key

        merged = self._data.merge(df, on=key, how="left", suffixes=("", "_B"))
        for column in df.columns:
            if column not in key:
                merged[column] = merged[column + "_B"].combine_first(merged[column])
        merged = merged[self._data.columns]
        non_matching = df[~df.set_index(key).index.isin(self._data.set_index(key).index)]
        self._data = pd.concat([merged, non_matching], ignore_index=True)

    def ensure_index(self, key: str, unique: bool = False) -> bool:
        """
        Tries to create an index and return true if it succeeded

        Raises:
            NotImplementedError: always as this concept does not make sense for this type of store
        """
        raise NotImplementedError("Pandas memory store does not support this function")

    def _field_exists(self, key: str) -> bool:
        return key in self._data

    def newer_in(self, target: "Store", criteria: Optional[Dict] = None, exhaustive: bool = False) -> List[str]:
        """
        Returns the keys of documents that are newer in the target
        Store than this Store.

        Args:
            target: target Store to compare with
            criteria: the value of the `query` key will be used as the Pandas string expression to query with;
                all other data will be ignored; only valid when exhaustive is True
            exhaustive: triggers an item-by-item check vs. checking
                        the last_updated of the target Store and using
                        that to filter out new items in

        Returns:
            List[str]: if no criteria is provided a list of the keys of documents in the target store
                whose last updated field value is greater than the 'newest' document in this store;
                otherwise a list of the keys of documents in the target store that additionally meet the criteria

        Raises:
            AttributeError: if the key and last updated fields are not both present in this store or
                if criteria is provided when exhaustive is not set to True
        """
        if not (self._field_exists(self.key) and self._field_exists(self.last_updated_field)):
            raise AttributeError("This index store does not contain data with both key and last updated fields")

        if criteria is not None and not exhaustive:
            raise AttributeError("Criteria is only considered when doing an item-by-item check")

        if exhaustive:
            # Get our current last_updated dates for each key value
            props = {self.key: 1, self.last_updated_field: 1, "_id": 0}
            dates = {
                d[self.key]: self._lu_func[0](d.get(self.last_updated_field, datetime.max))
                for d in self.query(properties=props)
            }

            # Get the last_updated for the store we're comparing with
            props = {target.key: 1, target.last_updated_field: 1, "_id": 0}
            target_dates = {
                d[target.key]: target._lu_func[0](d.get(target.last_updated_field, datetime.min))
                for d in target.query(criteria=criteria, properties=props)
            }

            new_keys = set(target_dates.keys()) - set(dates.keys())
            updated_keys = {key for key, date in dates.items() if target_dates.get(key, datetime.min) > date}

            return list(new_keys | updated_keys)

        criteria = {"query": f"{self.last_updated_field} > '{self._lu_func[1](self.last_updated)}'"}
        return target.distinct(field=self.key, criteria=criteria)

    def distinct(self, field: str, criteria: Optional[Dict] = None, all_exist: bool = False) -> List:
        """
        Get all distinct values for a field

        Args:
            field: the field(s) to get distinct values for
            criteria: the value of the `query` key will be used as the Pandas string expression to query with;
                all other data will be ignored

        Returns:
            List: a list of all the distinct values for the provided field (after filtering by the provided criteria)
        """
        criteria = criteria or {}

        return [key for key, _ in self.groupby(field, properties=[field], criteria=criteria)]

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
            criteria: the value of the `query` key will be used as the
                Pandas string expression to query with;
                all other data will be ignored
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip
            limit: limit on total number of documents returned

        Returns:
            Iterator[Tuple[Dict, List[Dict]]]: iterator returning tuples of (dict, list of docs)
        """
        ret = self._query(criteria=criteria, properties=properties, sort=sort, skip=skip, limit=limit)
        grouped_tuples = [(name, group) for name, group in ret.groupby(keys)]
        return iter(grouped_tuples)

    def remove_docs(self, criteria: Dict):
        """
        Remove docs matching the query dictionary

        Args:
            criteria: query dictionary to match

        Raises:
            NotImplementedError: always as this concept is not used
        """
        raise NotImplementedError("Not implemented for this store")

    def __hash__(self):
        """Hash for the store"""
        return hash((self.key, self.last_updated_field))

    def __eq__(self, other: object) -> bool:
        """
        Check equality for PandasMemoryStore
        other: other PandasMemoryStore to compare with
        """
        if not isinstance(other, PandasMemoryStore):
            return False

        fields = ["key", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


class S3IndexStore(PandasMemoryStore):
    """
    A store that loads the index of the collection from an S3 file.

    S3IndexStore can still apply MongoDB-like writable operations
    (e.g. an update) because it behaves like a MemoryStore,
    but it will not write those changes to S3.
    """

    def __init__(
        self,
        collection_name: str,
        bucket: str,
        prefix: str = "",
        endpoint_url: Optional[str] = None,
        manifest_key: str = "manifest.json",
        **kwargs,
    ):
        """Initializes an S3IndexStore

        Args:
            collection_name (str): name of the collection
            bucket (str): Name of the bucket where the index is stored.
            prefix (str, optional): The prefix to add to the name of the index, i.e. the manifest key.
                Defaults to "".
            endpoint_url (Optional[str], optional): S3-compatible endpoint URL.
                Defaults to None, indicating to use the default configured AWS S3.
            manifest_key (str, optional): The name of the index. Defaults to "manifest.json".
        """
        self.collection_name = collection_name
        self.bucket = bucket
        self.prefix = prefix
        self.endpoint_url = endpoint_url
        self.client: Any = None
        self.session: Any = None
        self.s3_session_kwargs = {}
        self.manifest_key = manifest_key
        self.kwargs = kwargs

        super().__init__(**kwargs)

    def _get_full_key_path(self) -> str:
        """Produces the full path for the index."""
        return f"{self.prefix}{self.manifest_key}"

    def _retrieve_manifest(self) -> pd.DataFrame:
        """Retrieves the contents of the index stored in S3.

        Returns:
            List[Dict]: The index contents with each item representing a document.
        """
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._get_full_key_path())
            return pd.read_json(response["Body"], orient="records")
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "NoSuchKey":
                return []
            raise

    def _load_index(self, force_reset: bool = False) -> None:
        """Load the contents of the index stored in S3 into memory.

        Args:
            force_reset: whether to force a reset of the memory store prior to load
        """
        super().update(self._retrieve_manifest(), clear_first=True)

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
        super().update(data, clear_first=True)

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
        return hash((self.collection_name, self.bucket, self.prefix, self.endpoint_url, self.manifest_key))

    def __eq__(self, other: object) -> bool:
        """
        Check equality for S3Store
        other: other S3Store to compare with.
        """
        if not isinstance(other, S3IndexStore):
            return False

        fields = ["collection_name", "bucket", "prefix", "endpoint_url", "manifest_key", "last_updated_field"]
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
        bucket: str,
        compress: bool = True,
        endpoint_url: Optional[str] = None,
        sub_dir: Optional[str] = None,
        key: str = "fs_id",
        searchable_fields: Optional[List[str]] = None,
        object_file_extension: str = ".json.gz",
        access_as_public_bucket: bool = False,
        **kwargs,
    ):
        """Initializes an OpenDataStore

        Args:
            index (S3IndexStore): The store that'll be used as the index, ie for queries pertaining to this store.
            bucket: name of the bucket.
            compress: compress files inserted into the store.
            endpoint_url: this allows the interface with minio service
            sub_dir: subdirectory of the S3 bucket to store the data.
            key: main key to index on.
            searchable_fields: fields to keep in the index store.
            object_file_extension (str, optional): The extension used for the data stored in S3. Defaults to ".json.gz".
            access_as_public_bucket (bool, optional): If True, the S3 bucket will be accessed without signing,
            ie as if it's a public bucket.
                This is useful for end users. Defaults to False.
        """
        self.sub_dir = sub_dir.strip("/") + "/" if sub_dir else ""
        self.searchable_fields = searchable_fields if searchable_fields is not None else []
        self.object_file_extension = object_file_extension
        self.access_as_public_bucket = access_as_public_bucket
        if access_as_public_bucket:
            kwargs["s3_resource_kwargs"] = kwargs["s3_resource_kwargs"] if "s3_resource_kwargs" in kwargs else {}
            kwargs["s3_resource_kwargs"]["config"] = Config(signature_version=UNSIGNED)

        kwargs["index"] = index
        kwargs["bucket"] = bucket
        kwargs["compress"] = compress
        kwargs["endpoint_url"] = endpoint_url
        kwargs["sub_dir"] = sub_dir
        kwargs["key"] = key
        kwargs["searchable_fields"] = searchable_fields
        kwargs["unpack_data"] = True
        self.kwargs = kwargs
        super().__init__(**kwargs)

    def _get_full_key_path(self, id: str) -> str:
        if self.index.collection_name == "thermo" and self.key == "thermo_id":
            material_id, thermo_type = id.split("_", 1)
            return f"{self.sub_dir}{thermo_type}/{material_id}{self.object_file_extension}"
        if self.index.collection_name == "xas" and self.key == "spectrum_id":
            material_id, spectrum_type, absorbing_element, edge = id.rsplit("-", 3)
            return f"{self.sub_dir}{edge}/{spectrum_type}/{absorbing_element}/{material_id}{self.object_file_extension}"
        if self.index.collection_name == "synth_descriptions" and self.key == "doi":
            return f"{self.sub_dir}{id.replace('/', '_')}{self.object_file_extension}"
        return f"{self.sub_dir}{id}{self.object_file_extension}"

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

    def __hash__(self):
        return hash(
            (
                self.bucket,
                self.compress,
                self.endpoint_url,
                self.key,
                self.sub_dir,
            )
        )

    def __eq__(self, other: object) -> bool:
        """
        Check equality for OpenDataStore.

        other: other OpenDataStore to compare with.
        """
        if not isinstance(other, OpenDataStore):
            return False

        fields = [
            "index",
            "bucket",
            "compress",
            "endpoint_url",
            "key",
            "searchable_fields",
            "sub_dir",
            "last_updated_field",
        ]
        return all(getattr(self, f) == getattr(other, f) for f in fields)
