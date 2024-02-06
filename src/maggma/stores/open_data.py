import gzip
from datetime import datetime
from io import BytesIO, StringIO
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import jsonlines
import pandas as pd
from boto3 import Session
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from bson import json_util

from maggma.core.store import Sort, Store
from maggma.stores.aws import S3Store
from maggma.utils import grouper


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
        Returns a handle to the pymongo collection object.

        Raises:
            NotImplementedError: always as this concept does not make sense for this type of store
        """
        raise NotImplementedError("Index memory store cannot be used with this property")

    @property
    def name(self) -> str:
        """
        Return a string representing this data source.
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

    @staticmethod
    def add_missing_items(to_dt: pd.DataFrame, from_dt: pd.DataFrame, on: List[str]) -> pd.DataFrame:
        orig_columns = to_dt.columns
        merged = to_dt.merge(from_dt, on=on, how="left", suffixes=("", "_B"))
        for column in from_dt.columns:
            if column not in on:
                merged[column].update(merged.pop(column + "_B"))
        return merged[orig_columns]

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
        if key is not None:
            raise NotImplementedError("updating store based on a key different than the store key is not supported")

        df = pd.DataFrame(docs)
        if self._data is None or clear_first:
            if not df.empty:
                self._data = df
            return
        key = [self.key]

        merged = PandasMemoryStore.add_missing_items(to_dt=self._data, from_dt=df, on=key)
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
            df = pd.read_json(response["Body"], orient="records", lines=True)
            if self.last_updated_field in df.columns:
                df[self.last_updated_field] = df[self.last_updated_field].apply(
                    lambda x: datetime.fromisoformat(x["$date"].rstrip("Z"))
                    if isinstance(x, dict) and "$date" in x
                    else x
                )
            return df

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

    def store_manifest(self, data: pd.DataFrame) -> None:
        """Stores the provided data into the index stored in S3.
        This overwrites and fully replaces all of the contents of the previous index stored in S3.
        It also rewrites the memory index with the provided data.

        Args:
            data (List[Dict]): The data to store in the index.
        """
        string_io = StringIO()
        with jsonlines.Writer(string_io, dumps=json_util.dumps) as writer:
            for _, row in data.iterrows():
                writer.write(row.to_dict())

        self.client.put_object(
            Bucket=self.bucket,
            Body=BytesIO(string_io.getvalue().encode("utf-8")),
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
        object_file_extension: str = ".jsonl.gz",
        access_as_public_bucket: bool = False,
        object_grouping: Optional[List[str]] = None,
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
        self.object_grouping = object_grouping if object_grouping is not None else ["nelements", "symmetry_number"]

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
        self.searchable_fields = list(
            set(self.object_grouping) | set(self.searchable_fields) | {self.key, self.last_updated_field}
        )

    def query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Dict]:
        """
        Queries the Store for a set of documents.

        Args:
            criteria: PyMongo filter for documents to search in.
            properties: properties to return in grouped documents.
            sort: Dictionary of sort order for fields. Keys are field names and values
                are 1 for ascending or -1 for descending.
            skip: number documents to skip.
            limit: limit on total number of documents returned.

        """
        prop_keys = set()
        if isinstance(properties, dict):
            prop_keys = set(properties.keys())
        elif isinstance(properties, list):
            prop_keys = set(properties)

        for _, docs in self.index.groupby(
            keys=self.object_grouping, criteria=criteria, sort=sort, limit=limit, skip=skip
        ):
            group_doc = None  # S3 backed group doc
            for _, doc in docs.iterrows():
                data = doc
                if properties is None or not prop_keys.issubset(set(doc.keys())):
                    if not group_doc:
                        group_doc = self._read_doc_from_s3(self._get_full_key_path(docs))
                    if group_doc.empty:
                        continue
                    data = group_doc.query(f"{self.key} == '{doc[self.key]}'")
                    data = data.to_dict(orient="index")[0]
                if properties is None:
                    yield data
                else:
                    yield {p: data[p] for p in prop_keys if p in data}

    def _read_doc_from_s3(self, file_id: str) -> pd.DataFrame:
        try:
            response = self.s3_bucket.Object(file_id).get()
            return pd.read_json(response["Body"], orient="records", lines=True, compression={"method": "gzip"})
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "NoSuchKey":
                return pd.DataFrame()
            raise

    def _get_full_key_path(self, id: str) -> str:
        raise NotImplementedError("Not implemented for this store")

    def _get_full_key_path(self, index: pd.DataFrame) -> str:
        id = ""
        for group in self.object_grouping:
            id = f"{id}{group}={index[group].iloc[0]}/"
        id = id.rstrip("/")
        return f"{self.sub_dir}{id}{self.object_file_extension}"

    def _get_compression_function(self) -> Callable:
        return gzip.compress

    def _get_decompression_function(self) -> Callable:
        return gzip.decompress

    def _gather_indexable_data(self, df: pd.DataFrame, search_keys: List[str]) -> pd.DataFrame:
        return df[search_keys]

    def update(
        self,
        docs: Union[List[Dict], Dict],
        key: Union[List, str, None] = None,
        additional_metadata: Union[str, List[str], None] = None,
    ):
        if additional_metadata is not None:
            raise NotImplementedError("updating store with additional metadata is not supported")
        super().update(docs=docs, key=key)

    @staticmethod
    def json_normalize_and_filter(docs: List[Dict], object_grouping: List[str]) -> pd.DataFrame:
        dfs = []
        for chunk in grouper(iterable=docs, n=1000):
            dfs.append(pd.json_normalize(chunk, sep="_")[object_grouping])
        return pd.concat(dfs)

    def _write_to_s3_and_index(self, docs: List[Dict], search_keys: List[str]):
        """Implements updating of the provided documents in S3 and the index.

        Args:
            docs (List[Dict]): The documents to update
            search_keys (List[str]): The keys of the information to be updated in the index
        """
        # group docs to update by object grouping
        og = list(set(self.object_grouping) | set(search_keys))
        df = OpenDataStore.json_normalize_and_filter(docs=docs, object_grouping=og)
        df_grouped = df.groupby(self.object_grouping)
        existing = self.index._data
        docs_df = pd.DataFrame(docs)
        for group, _ in df_grouped:
            query_str = " and ".join([f"{col} == {val!r}" for col, val in zip(self.object_grouping, group)])
            sub_df = df.query(query_str)
            sub_docs_df = docs_df[docs_df[self.key].isin(sub_df[self.key].unique())]
            merged_df = sub_df
            if existing is not None:
                # fetch subsection of existing and docs_df and do outer merge with indicator=True
                sub_existing = existing.query(query_str)
                merged_df = sub_existing.merge(sub_df, on=og, how="outer", indicator=True)
                # if there's any rows in existing only
                if not merged_df[merged_df["_merge"] == "left_only"].empty:
                    ## fetch the S3 data and populate those rows in sub_docs_df
                    s3_df = self._read_doc_from_s3(self._get_full_key_path(sub_existing))
                    # sub_docs
                    sub_docs_df = sub_docs_df.merge(merged_df[[self.key, "_merge"]], on=self.key, how="right")
                    sub_docs_df.update(s3_df, overwrite=False)
                    sub_docs_df = sub_docs_df.drop("_merge", axis=1)

                merged_df = merged_df.drop("_merge", axis=1)
            # write doc based on subsection
            self._write_doc_and_update_index(sub_docs_df, merged_df)

    def _write_doc_and_update_index(self, items: pd.DataFrame, index: pd.DataFrame) -> None:
        self.write_doc_to_s3(items, index)
        self.index.update(index)

    def write_doc_to_s3(self, doc, search_keys):
        if not isinstance(doc, pd.DataFrame):
            raise NotImplementedError("doc parameter must be a Pandas DataFrame for the implementation for this store")
        if not isinstance(search_keys, pd.DataFrame):
            raise NotImplementedError(
                "search_keys parameter must be a Pandas DataFrame for the implementation for this store"
            )
        # def write_doc_to_s3(self, items: pd.DataFrame, index: pd.DataFrame) -> None:
        string_io = StringIO()
        with jsonlines.Writer(string_io, dumps=json_util.dumps) as writer:
            for _, row in doc.iterrows():
                writer.write(row.to_dict())

        data = self._get_compression_function()(string_io.getvalue().encode("utf-8"))

        self._get_bucket().upload_fileobj(
            Fileobj=BytesIO(data),
            Key=self._get_full_key_path(search_keys),
        )

    def _index_for_doc_from_s3(self, key: str) -> pd.DataFrame:
        doc = self._read_doc_from_s3(key)
        return self._gather_indexable_data(doc, self.searchable_fields)

    def rebuild_index_from_s3_data(self) -> pd.DataFrame:
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
                    all_index_docs.append(self._index_for_doc_from_s3(key))
        ret = pd.concat(all_index_docs, ignore_index=True)
        self.index.store_manifest(ret)
        return ret

    def rebuild_index_from_data(self, docs: pd.DataFrame) -> pd.DataFrame:
        """
        Rebuilds the index Store from the provided data.
        The provided data needs to include all of the documents in this data set.
        Stores only the key, last_updated_field and searchable_fields in the index.

        Args:
            docs (List[Dict]): The data to build the index from.

        Returns:
            List[Dict]: The set of docs representing the index data.
        """
        all_index_docs = self._gather_indexable_data(docs, self.searchable_fields)
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
                tuple(self.object_grouping),
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
            "object_grouping",
        ]
        return all(getattr(self, f) == getattr(other, f) for f in fields)
