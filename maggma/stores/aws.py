# coding: utf-8
"""
Advanced Stores for connecting to AWS data
"""

import json
import zlib

from typing import Union, Optional, Dict, List, Iterator, Tuple

from monty.json import jsanitize
from monty.dev import deprecated

from maggma.core import Store, Sort
from maggma.utils import grouper

try:
    import boto3
    import botocore

    boto_import = True
except ImportError:
    boto_import = False


class AmazonS3Store(Store):
    """
    GridFS like storage using Amazon S3 and a regular store for indexing
    Assumes Amazon AWS key and secret key are set in environment or default config file
    """

    def __init__(self, index: Store, bucket: str, compress: bool = False, **kwargs):
        """
        Initializes an S3 Store
        Args:
            index (Store): a store to use to index the S3 Bucket
            bucket (str) : name of the bucket
            compress (bool): compress files inserted into the store
        """
        if not boto_import:
            raise ValueError(
                "boto not available, please install boto3 to " "use AmazonS3Store"
            )
        self.index = index
        self.bucket = bucket
        self.compress = compress
        self.s3 = None
        self.s3_bucket = None
        # Force the key to be the same as the index
        kwargs["key"] = index.key
        super(AmazonS3Store, self).__init__(**kwargs)

    def name(self) -> str:
        """
        Return a string representing this data source
        """
        return self.bucket

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data
        """
        self.index.connect(force_reset=force_reset)
        if not self.s3:
            self.s3 = boto3.resource("s3")

            if self.bucket not in [bucket.name for bucket in self.s3.buckets.all()]:
                raise Exception("Bucket not present on AWS: {}".format(self.bucket))

            self.s3_bucket = self.s3.Bucket(self.bucket)

    def close(self):
        """
        Closes any connections
        """
        self.index.close()
        self.s3 = None
        self.s3_bucket = None

    @property
    @deprecated(message="This will be removed in the future")
    def collection(self):
        """
        Returns a handle to the pymongo collection object
        Not guaranteed to exist in the future
        """
        # For now returns the index collection since that is what we would "search" on
        return self.index

    def query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Sort]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Dict]:
        """
        Queries the Store for a set of documents

        Args:
            criteria : PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields
            skip: number documents to skip
            limit: limit on total number of documents returned
        """
        for doc in self.index.query(
            criteria=criteria, sort=sort, limit=limit, skip=skip
        ):
            try:
                # TODO : THis is ugly and unsafe, do some real checking before pulling data
                data = self.s3_bucket.Object(doc[self.key]).get()["Body"].read()
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

            if doc.get("compression", "") == "zlib":
                data = zlib.decompress(data)
            yield json.loads(data)

    def distinct(
        self,
        field: Union[List[str], str],
        criteria: Optional[Dict] = None,
        all_exist: bool = False,
    ) -> Union[List[Dict], List]:
        """
        Get all distinct values for a field(s)
        For a single field, this returns a list of values
        For multiple fields, this return a list of of dictionaries for each unique combination

        Args:
            field: the field(s) to get distinct values for
            criteria : PyMongo filter for documents to search in
            all_exist : ensure all fields exist for the distinct set
        """
        # Index is a store so it should have its own distinct function
        return self.index.distinct(field, criteria=criteria, all_exist=all_exist)

    def groupby(
        self,
        keys: Union[List[str], str],
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Sort]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Tuple[Dict, List[Dict]]]:
        """
        Simple grouping function that will group documents
        by keys.

        Args:
            keys: fields to group documents
            criteria : PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields
            skip: number documents to skip
            limit: limit on total number of documents returned

        Returns:
            generator returning tuples of (dict, list of docs)
        """
        self.index.groupby(
            keys=keys,
            criteria=criteria,
            properties=properties,
            sort=sort,
            skip=skip,
            limit=limit,
        )

    def ensure_index(self, key: str, unique: Optional[bool] = False) -> bool:
        """
        Tries to create an index and return true if it suceeded
        Args:
            key: single key to index
            unique: Whether or not this index contains only unique keys

        Returns:
            bool indicating if the index exists/was created
        """
        return self.index.ensure_index(key, unique=unique, background=True)

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
        search_docs = []
        search_keys = []

        if isinstance(key, list):
            search_keys = key
        elif key:
            search_keys = [key]
        else:
            search_keys = [self.key]

        for d in docs:
            search_doc = {k: d[k] for k in search_keys}
            search_doc[self.key] = d[self.key]  # Ensure key is in metadata

            # Remove MongoDB _id from search
            if "_id" in search_doc:
                del search_doc["_id"]

            data = json.dumps(jsanitize(d)).encode()

            # Compress with zlib if chosen
            if self.compress:
                search_doc["compression"] = "zlib"
                data = zlib.compress(data)

            self.s3_bucket.put_object(Key=d[self.key], Body=data, Metadata=search_doc)
            search_docs.append(search_doc)

        # Use store's update to remove key clashes
        self.index.update(search_docs)

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
            to_remove_chunks = list(grouper(to_remove, N=1000))
            for chunk_to_remove in to_remove_chunks:
                self.s3_bucket.delete_objects()

    @property
    def last_updated(self):
        return self.index.last_updated

    def newer_in(
        self,
        target: Store,
        key: Union[str, None] = None,
        criteria: Optional[Dict] = None,
        exhaustive: bool = False,
    ) -> List[str]:
        """
        Returns the keys of documents that are newer in the target
        Store than this Store.

        Args:
            key: a single key field to return, defaults to Store.key
            criteria : PyMongo filter for documents to search in
            exhaustive: triggers an item-by-item check vs. checking
                        the last_updated of the target Store and using
                        that to filter out new items in
        """
        self.index.newer_in(
            target=target, key=key, criteria=criteria, exhaustive=exhaustive
        )

    def __hash__(self):
        return hash((self.index.__hash__, self.bucket))

    def rebuild_index_from_s3_data(self):
        """
        Rebuilds the index Store from the data in S3
        Relies on the index document being stores as the metadata for the file
        """
        index_docs = []
        for file in self.s3_bucket.objects.all():
            # TODO: Transform the data back from strings and remove AWS S3 specific keys
            index_docs.append(file.metadata)

        self.index.update(index_docs)
