import gzip
import logging
import re
from collections.abc import Generator
from datetime import datetime
from io import BytesIO, StringIO
from typing import Optional, Union

import jsonlines
import numpy as np
import pandas as pd
from boto3 import client as boto_client
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from bson import json_util

from maggma.core.store import Sort
from maggma.utils import LU_KEY_ISOFORMAT


def chunker(df: pd.DataFrame, chunk_size: int) -> Generator[pd.DataFrame, None, None]:
    """
    Creates a generator for a DataFrame to allow chunk processing.

    Args:
        df: the DataFrame to chunk
        chunk_size: size of the chunks

    Returns:
        Generator[pd.DataFrame, None, None]: a generator for a DataFrame
            to allow chunk processing.

    """
    return (df.iloc[pos : pos + chunk_size] for pos in range(0, len(df), chunk_size))


class PandasMemoryStore:
    """
    A store that is backed by Pandas DataFrame.

    """

    def __init__(
        self,
        key: str = "task_id",
        last_updated_field: str = "last_updated",
    ):
        """
        Args:
            key: main key to index on
            last_updated_field: field for date/time stamping the data.
        """
        self._data = None
        self.key = key
        self.last_updated_field = last_updated_field
        self.logger = logging.getLogger(type(self).__name__)
        self.logger.addHandler(logging.NullHandler())
        self.logger.warning(
            "Use all open data stores with caution as they are deprecated and may be incompatible with numpy 2.0+."
        )

    @property
    def index_data(self):
        return self._data

    def set_index_data(self, new_index: pd.DataFrame):
        self._data = new_index

    def _verify_criteria(self, criteria: dict) -> tuple[str, str, list]:
        query_string, is_in_key, is_in_list = "", None, None
        if criteria and "query" not in criteria and "is_in" not in criteria:
            raise AttributeError("Pandas memory store only support query or is_in")
        if criteria and "query" in criteria and "is_in" in criteria:
            raise AttributeError("Pandas memory store cannot mix query and is_in; please just use one or the other")
        if criteria:
            if "is_in" in criteria:
                is_in_key, is_in_list = criteria["is_in"]
                query_string = None
            elif "query" in criteria:
                query_string = criteria["query"]
        return query_string, is_in_key, is_in_list

    def query(
        self,
        criteria: Optional[dict] = None,
        properties: Union[list, None] = None,
        sort: Optional[dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
        criteria_fields: Union[list, None] = None,
    ) -> pd.DataFrame:
        """
        Queries the Store for a set of documents.

        Args:
            criteria: if there's a `query` key, it's value will be used as the
                Pandas string expression to query with; if there's a
                'is_in' key, it's value will be used to perform an isin call
                using the first item in that tuple for the column name and
                the second item as the list across which to filter on; only
                one valid key is accepted; all other data in the criteria
                will be ignored
            properties: subset of properties to return
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip (from the start of the result set)
            limit: limit on total number of documents returned
            criteria_fields: if this value is not None, the in-memory index will
                be used for the query if all the "criteria_fields" and "properties"
                are present in the in-memory index; otherwise will default to
                querying store type dependent implementation

        Returns:
            pd.DataFrame: DataFrame that contains all the documents that match
                the query parameters

        Raises:
            AttributeError: if criteria exists and does not include a valid key;
                also if more than one valid key is present
        """
        query_string, is_in_key, is_in_list = self._verify_criteria(criteria=criteria)
        if properties is not None and not isinstance(properties, list):
            raise AttributeError(f"Pandas query expects properties must be a list and not a {type(properties)}")

        if self._data is None:
            return pd.DataFrame()

        return PandasMemoryStore._query(
            index=self._data,
            query_string=query_string,
            is_in_key=is_in_key,
            is_in_list=is_in_list,
            properties=properties,
            sort=sort,
            skip=skip,
            limit=limit,
        )

    @staticmethod
    def _query(
        index: pd.DataFrame,
        query_string: str,
        is_in_key: str,
        is_in_list: list,
        properties: Union[list, None] = None,
        sort: Optional[dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> pd.DataFrame:
        ret = index

        if query_string:
            ret = ret.query(query_string)
        elif is_in_key is not None:
            ret = ret[ret[is_in_key].isin(is_in_list)]

        if sort:
            sort_keys, sort_ascending = zip(*[(k, v == 1) for k, v in sort.items()])
            ret = ret.sort_values(by=list(sort_keys), ascending=list(sort_ascending))

        if properties:
            ret = ret[properties]

        ret = ret[skip:]
        if limit > 0:
            ret = ret[:limit]
        return ret

    def count(self, criteria: Optional[dict] = None, criteria_fields: Union[list, None] = None) -> int:
        """
        Counts the number of documents matching the query criteria.

        Returns:
            int: the number of documents matching the query criteria

        Args:
            criteria: see `query` method for details on how to construct
            criteria_fields: see `query` method for details
        """
        return len(self.query(criteria=criteria, criteria_fields=criteria_fields))

    def distinct(
        self, field: str, criteria: Optional[dict] = None, criteria_fields: Union[list, None] = None
    ) -> pd.Series:
        """
        Get all distinct values for a field.

        Args:
            field: the field(s) to get distinct values for
            criteria: see `query` method for details on how to construct
            criteria_fields: see `query` method for details

        Returns:
            pd.Series: Series of all the distinct values for the provided field
                (after filtering by the provided criteria)
        """
        ret = self.query(criteria=criteria, properties=[field], criteria_fields=criteria_fields)
        return ret[field].drop_duplicates()

    @property
    def last_updated(self) -> datetime:
        """
        Provides the most recent last_updated date time stamp from
        the documents in this Store.
        """
        if self._data is None:
            return datetime.min

        max = self._data[self.last_updated_field].max()
        if max is None:
            return datetime.min

        return LU_KEY_ISOFORMAT[0](max)

    def newer_in(
        self,
        target: "PandasMemoryStore",
        criteria: Optional[dict] = None,
        exhaustive: bool = False,
        criteria_fields: Union[list, None] = None,
    ) -> pd.Series:
        """
        Returns the keys of documents that are newer in the target
        Store than this Store.

        Args:
            target: target Store to compare with
            criteria: see `query` method for details on how to construct
            exhaustive: triggers an item-by-item check vs. checking
                        the last_updated of the target Store and using
                        that to filter out new items in
            criteria_fields: see `query` method for details

        Returns:
            pd.Series: if no criteria is provided a Series of the keys of documents in the target store
                whose last updated field value is greater than the 'newest' document in this store;
                otherwise a list of the keys of documents in the target store that additionally meet the criteria

        Raises:
            AttributeError: if the key and last updated fields are not both present in this store or
                if criteria is provided when exhaustive is not set to True
        """
        if self._data is None:
            return target.query()

        if not (self._field_exists(self.key) and self._field_exists(self.last_updated_field)):
            raise AttributeError("This index store does not contain data with both key and last updated fields")

        if criteria is not None and not exhaustive:
            raise AttributeError("Criteria is only considered when doing an item-by-item check")

        if exhaustive:
            # Get our current last_updated dates for each key value
            props = [self.key, self.last_updated_field]
            dates = {
                d[self.key]: LU_KEY_ISOFORMAT[0](d.get(self.last_updated_field, datetime.max))
                for _, d in self.query(properties=props, criteria_fields=criteria_fields).iterrows()
            }
            # Get the last_updated for the store we're comparing with
            props = [target.key, target.last_updated_field]
            target_dates = {
                d[target.key]: LU_KEY_ISOFORMAT[0](d.get(target.last_updated_field, datetime.min))
                for _, d in target.query(
                    criteria=criteria, properties=props, criteria_fields=criteria_fields
                ).iterrows()
            }
            new_keys = set(target_dates.keys()) - set(dates.keys())
            updated_keys = {key for key, date in dates.items() if target_dates.get(key, datetime.min) > date}
            return pd.Series(data=list(new_keys | updated_keys), name=self.key)

        criteria = {"query": f"{self.last_updated_field} > '{LU_KEY_ISOFORMAT[1](self.last_updated)}'"}
        return target.distinct(field=self.key, criteria=criteria, criteria_fields=[self.last_updated_field])

    def get_merged_items(self, to_dt: pd.DataFrame, from_dt: pd.DataFrame) -> pd.DataFrame:
        orig_columns = to_dt.columns
        merged = to_dt.merge(from_dt, on=self.key, how="left", suffixes=("", "_B"))
        for column in from_dt.columns:
            if column not in self.key:
                oc_dtype = merged[column].dtype
                s = merged.pop(column + "_B")
                s.name = column
                merged.update(s)
                merged[column].astype(oc_dtype)
        return pd.concat(
            (merged[orig_columns], from_dt[~from_dt.set_index(self.key).index.isin(to_dt.set_index(self.key).index)]),
            ignore_index=True,
        )

    def update(self, docs: pd.DataFrame) -> pd.DataFrame:
        """
        Update documents into the Store.

        Args:
            docs: the document or list of documents to update

        Returns:
            pd.DataFrame the updated documents
        """
        if self._data is None:
            if docs is not None and not docs.empty:
                self._data = docs
            return docs

        self._data = self.get_merged_items(to_dt=self._data, from_dt=docs)
        return docs

    def _field_exists(self, key: str) -> bool:
        return key in self._data

    def __hash__(self):
        """Hash for the store."""
        return hash((self.key, self.last_updated_field))

    def __eq__(self, other: object) -> bool:
        """
        Check equality for PandasMemoryStore
        other: other PandasMemoryStore to compare with.
        """
        if not isinstance(other, PandasMemoryStore):
            return False

        fields = ["key", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


class S3IndexStore(PandasMemoryStore):
    """
    A store that loads the index of the collection from an S3 file.

    Note that `update` calls will not write changes to S3, only to memory.
    You must call `store_manifest` to store any updates applied during the session.
    """

    def __init__(
        self,
        collection_name: str,
        bucket: str,
        prefix: str = "",
        endpoint_url: Optional[str] = None,
        manifest_key: str = "manifest.jsonl",
        **kwargs,
    ):
        """Initializes an S3IndexStore.

        Args:
            collection_name (str): name of the collection
            bucket (str): Name of the bucket where the index is stored.
            prefix (str, optional): The prefix to add to the name of the index, i.e. the manifest key.
                Defaults to "".
            endpoint_url (Optional[str], optional): S3-compatible endpoint URL.
                Defaults to None, indicating to use the default configured AWS S3.
            manifest_key (str, optional): The name of the index. Defaults to "manifest.jsonl".
        """
        self.collection_name = collection_name
        self.bucket = bucket
        self.prefix = prefix if prefix == "" else prefix.rstrip("/") + "/"
        self.endpoint_url = endpoint_url
        self.manifest_key = manifest_key
        self.kwargs = kwargs
        self._s3_client = None

        super().__init__(**kwargs)

    @property
    def s3_client(self):
        if self._s3_client is None:
            self._s3_client = boto_client("s3", endpoint_url=self.endpoint_url)
        return self._s3_client

    def connect(self):
        """
        Sets up the S3 client and loads the contents of the index stored in S3 into memory.
        This will overwrite the local memory with the S3 data.

        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except ClientError:
            raise RuntimeError(f"Bucket not present on AWS: {self.bucket}")

        # load index
        self.set_index_data(self.retrieve_manifest())

    def close(self):
        """Closes any connections."""
        if self._s3_client is not None:
            self._s3_client.close()
            self._s3_client = None

    def retrieve_manifest(self) -> pd.DataFrame:
        """Retrieves the contents of the index stored in S3.

        Returns:
            pd.DataFrame: The index contents read from the manifest file.
                Returns None if a manifest file does not exist.
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=self._get_manifest_full_key_path())
            df = pd.read_json(response["Body"], orient="records", lines=True)
            return df.map(
                lambda x: datetime.fromisoformat(x["$date"].rstrip("Z")) if isinstance(x, dict) and "$date" in x else x
            )

        except ClientError as ex:
            if ex.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def _get_manifest_full_key_path(self) -> str:
        """Produces the full path for the index."""
        return f"{self.prefix}{self.manifest_key}"

    def store_manifest(self) -> None:
        """Stores the existing data into the index stored in S3.
        This overwrites and fully replaces all of the contents
        of the previous index stored in S3 with the current contents
        of the memory index.
        """
        string_io = StringIO()
        with jsonlines.Writer(string_io, dumps=json_util.dumps) as writer:
            for _, row in self._data.iterrows():
                writer.write(row.to_dict())

        self.s3_client.put_object(
            Bucket=self.bucket,
            Body=BytesIO(string_io.getvalue().encode("utf-8")),
            Key=self._get_manifest_full_key_path(),
        )

    def __getstate__(self):
        # Return the object's state excluding the _s3_client attribute
        state = self.__dict__.copy()
        state["_s3_client"] = None  # Exclude the client from serialization
        return state

    def __setstate__(self, state):
        # Restore instance attributes (excluding the client)
        self.__dict__.update(state)
        # Initialize the client as None; it will be recreated on demand
        self._s3_client = None

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


class OpenDataStore(S3IndexStore):
    """
    Data is stored on S3 compatible storage using the format used by Materials Project on OpenData.
    The index is loaded from S3 compatible storage into memory.

    Note that updates will only affect the in-memory representation of the index - they will not be persisted.
    To persist index writes utilize the `store_manifest` function.

    This Store should not be used for applications that are distributed and rely on reading updated
    values from the index as data inconsistencied will arise.
    """

    def __init__(
        self,
        index: S3IndexStore = None,  # set _index to this and create property
        searchable_fields: Optional[list[str]] = None,
        object_file_extension: str = ".jsonl.gz",
        access_as_public_bucket: bool = False,
        object_grouping: Optional[list[str]] = None,
        **kwargs,
    ):
        """Initializes an OpenDataStore.

        Args:
            index (S3IndexStore): The store that'll be used as the index,
                ie for queries pertaining to this store. If None, will create
                index from manifest located in same location as the data.
            searchable_fields: additional fields to keep in the index store.
                `key`, `last_updated_field` and the fields in `object_grouping`
                are already added to the index by default
            object_file_extension (str, optional): The extension used for the data
                stored in S3. Defaults to ".jsonl.gz".
            access_as_public_bucket (bool, optional): If True, the S3 bucket will
                be accessed without signing, ie as if it's a public bucket.
                This is useful for end users. Defaults to False.
        """
        self._index = index
        self.searchable_fields = searchable_fields if searchable_fields is not None else []
        self.object_file_extension = object_file_extension
        self.access_as_public_bucket = access_as_public_bucket
        self.object_grouping = object_grouping if object_grouping is not None else ["nelements", "symmetry_number"]

        if access_as_public_bucket:
            kwargs["s3_resource_kwargs"] = kwargs.get("s3_resource_kwargs", {})
            kwargs["s3_resource_kwargs"]["config"] = Config(signature_version=UNSIGNED)
        super().__init__(**kwargs)
        self.searchable_fields = list(
            set(self.object_grouping) | set(self.searchable_fields) | {self.key, self.last_updated_field}
        )

    @property
    def index(self):
        if self._index is None:
            return super()
        return self._index

    def update(
        self,
        docs: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Update documents in S3 and local in-memory index.

        Args:
            docs: the documents to update

        Returns:
            pd.DataFrame the index for the updated documents
        """
        # group docs to update by object grouping
        docs_by_group = self._json_normalize_and_filter(docs=docs).groupby(self.object_grouping)
        existing_index = self.index.index_data
        ret = []
        for group, group_docs_index in docs_by_group:
            query_str = " and ".join([f"{col} == {val!r}" for col, val in zip(self.object_grouping, group)])
            group_docs = docs[docs[self.key].isin(group_docs_index[self.key].to_list())]
            merged_docs, merged_index = group_docs, group_docs_index
            if existing_index is not None:
                # fetch subsection of existing and docs_df and do outer merge with indicator=True
                sub_existing = existing_index.query(query_str)
                merged_index = self.get_merged_items(to_dt=sub_existing, from_dt=group_docs_index)
                # if there's any rows in existing only need to fetch the S3 data and merge that in
                if (~sub_existing[self.key].isin(group_docs_index[self.key])).any():
                    ## fetch the S3 data and populate those rows in sub_docs_df
                    s3_docs = self._read_doc_from_s3(self._get_full_key_path(sub_existing))
                    merged_docs = self.get_merged_items(to_dt=s3_docs, from_dt=group_docs)
            # write doc based on subsection
            self._write_doc_and_update_index(merged_docs, merged_index)
            ret.append(merged_index)
        return pd.concat(ret)

    def query(
        self,
        criteria: Optional[dict] = None,
        properties: Union[list, None] = None,
        sort: Optional[dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
        criteria_fields: Union[list, None] = None,
    ) -> pd.DataFrame:
        """
        Queries the Store for a set of documents.

        Args:
            criteria: if there's a `query` key, it's value will be used as the
                Pandas string expression to query with; if there's a
                'is_in' key, it's value will be used to perform an isin call
                using the first item in that tuple for the column name and
                the second item as the list across which to filter on; only
                one valid key is accepted; all other data in the criteria
                will be ignored
            properties: subset of properties to return
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip (from the start of the result set)
            limit: limit on total number of documents returned
            criteria_fields: if this value is not None, the in-memory index will
                be used for the query if all the "criteria_fields" and "properties"
                are present in the in-memory index; otherwise will default to
                querying against the S3 docs

        Returns:
            pd.DataFrame: DataFrame that contains all the documents that match
                the query parameters

        Raises:
            AttributeError: if criteria exists and does not include a valid key;
                also if more than one valid key is present
        """
        query_string, is_in_key, is_in_list = self._verify_criteria(criteria=criteria)
        if properties is not None and not isinstance(properties, list):
            raise AttributeError(f"OpenData query expects properties must be a list and not a {type(properties)}")

        if self.index.index_data is None:
            return pd.DataFrame()

        # optimization if all required fields are in the index
        if criteria_fields is not None and properties is not None:
            query_fields = set(criteria_fields) | set(properties)
            if all(item in self.index.index_data.columns for item in list(query_fields)):
                return self.index.query(criteria=criteria, properties=properties, sort=sort, skip=skip, limit=limit)

        results = []
        for _, docs in self.index.index_data.groupby(self.object_grouping):
            results.append(
                PandasMemoryStore._query(
                    index=self._read_doc_from_s3(self._get_full_key_path(docs)),
                    query_string=query_string,
                    is_in_key=is_in_key,
                    is_in_list=is_in_list,
                    properties=properties,
                    sort=sort,
                    skip=skip,
                    limit=limit,
                )
            )
        return pd.concat(results, ignore_index=True)

    def _get_full_key_path(self, index: pd.DataFrame) -> str:
        id = ""
        for group in self.object_grouping:
            id = f"{id}{group}={index[group].iloc[0]}/"
        id = id.rstrip("/")
        return f"{self.prefix}{id}{self.object_file_extension}"

    def _gather_indexable_data(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._json_normalize_and_filter(df)

    def _json_normalize_and_filter(self, docs: pd.DataFrame) -> pd.DataFrame:
        dfs = []
        for chunk in chunker(df=docs, chunk_size=1000):
            dfs.append(pd.json_normalize(chunk.to_dict(orient="records"), sep="_")[self.searchable_fields])
        return pd.concat(dfs, ignore_index=True)

    def _write_doc_and_update_index(self, items: pd.DataFrame, index: pd.DataFrame) -> None:
        self._write_doc_to_s3(items, index)
        self.index.update(index)

    def _write_doc_to_s3(self, doc: pd.DataFrame, index: pd.DataFrame) -> None:
        doc = doc.replace({pd.NaT: None}).replace({"NaT": None}).replace({np.nan: None})

        string_io = StringIO()
        with jsonlines.Writer(string_io, dumps=json_util.dumps) as writer:
            for _, row in doc.iterrows():
                writer.write(row.to_dict())

        data = gzip.compress(string_io.getvalue().encode("utf-8"))

        self.s3_client.upload_fileobj(
            Bucket=self.bucket,
            Fileobj=BytesIO(data),
            Key=self._get_full_key_path(index),
        )

    def _read_doc_from_s3(self, file_id: str) -> pd.DataFrame:
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=file_id)
            df = pd.read_json(response["Body"], orient="records", lines=True, compression={"method": "gzip"})

            def replace_nested_date_dict(obj):
                if isinstance(obj, dict):
                    if "$date" in obj:
                        # Return the datetime string or convert it to a datetime object
                        return datetime.fromisoformat(obj["$date"].rstrip("Z"))
                    # Recursively process each key-value pair in the dictionary
                    for key, value in obj.items():
                        obj[key] = replace_nested_date_dict(value)
                elif isinstance(obj, list):
                    # Process each item in the list
                    return [replace_nested_date_dict(item) for item in obj]
                return obj

            return df.map(replace_nested_date_dict)

        except ClientError as ex:
            if ex.response["Error"]["Code"] == "NoSuchKey":
                return pd.DataFrame()
            raise

    def _index_for_doc_from_s3(self, key: str) -> pd.DataFrame:
        doc = self._read_doc_from_s3(key)
        return self._gather_indexable_data(doc)

    def rebuild_index_from_s3_data(self) -> pd.DataFrame:
        """
        Rebuilds the index Store from the data in S3
        Stores only the searchable_fields in the index.
        Only updates the in-memory index and does not persist the index;
        please call `store_manifest` with the returned values to persist.

        Returns:
            pd.DataFrame: The set of docs representing the index data.
        """
        paginator = self.s3_client.get_paginator("list_objects_v2")

        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)

        all_index_docs = []
        for page in page_iterator:
            for file in page["Contents"]:
                key = file["Key"]
                if key != self.index._get_manifest_full_key_path() and key.endswith(self.object_file_extension):
                    all_index_docs.append(self._index_for_doc_from_s3(key))
        ret = pd.concat(all_index_docs, ignore_index=True)
        self.index.set_index_data(ret)
        return ret

    def rebuild_index_from_data(self, docs: pd.DataFrame) -> pd.DataFrame:
        """
        Rebuilds the index Store from the provided data.
        The provided data needs to include all of the documents in this data set.
        Stores only the searchable_fields in the index.
        Only updates the in-memory index and does not persist the index;
        please call `store_manifest` with the returned values to persist.

        Returns:
            pd.DataFrame: The set of docs representing the index data.
        """
        ret = self._gather_indexable_data(docs)
        self.index.set_index_data(ret)
        return ret

    def __getstate__(self):
        # Return the object's state excluding the _s3_client attribute
        state = self.__dict__.copy()
        state["_s3_client"] = None  # Exclude the client from serialization
        return state

    def __setstate__(self, state):
        # Restore instance attributes (excluding the client)
        self.__dict__.update(state)
        # Initialize the client as None; it will be recreated on demand
        self._s3_client = None

    def __hash__(self):
        return hash(
            (
                self.bucket,
                self.endpoint_url,
                self.key,
                self.prefix,
                tuple(self.object_grouping),
                tuple(self.searchable_fields),
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
            "_index",
            "bucket",
            "endpoint_url",
            "key",
            "searchable_fields",
            "prefix",
            "last_updated_field",
            "object_grouping",
        ]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


class TasksOpenDataStore(OpenDataStore):
    """
    Task data is stored on S3 compatible storage using the format used by Materials Project on OpenData.
    The index is loaded from S3 compatible storage into memory.

    Note that updates will only affect the in-memory representation of the index - they will not be persisted.
    To persist index writes utilize the `store_manifest` function.

    This Store should not be used for applications that are distributed and rely on reading updated
    values from the index as data inconsistencied will arise.
    """

    def __init__(
        self,
        **kwargs,
    ):
        """Initializes a TaskOpenDataStore."""
        super().__init__(**kwargs)

    def _index_for_doc_from_s3(self, key: str) -> pd.DataFrame:
        doc = self._read_doc_from_s3(key)
        # create an entry for the trailing object grouping field
        col = self.object_grouping[-1]
        val = re.search(rf"{col}=(.+)\.jsonl\.gz", key).group(1)
        doc[col] = val
        return self._gather_indexable_data(doc)

    def update(self, docs: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("update is not supported for this store")
