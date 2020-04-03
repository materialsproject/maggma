# coding: utf-8
"""
Advanced Stores for connecting to AWS data
"""

import json
import zlib

from typing import Union, Optional, Dict, List, Iterator, Tuple, Any

from monty.json import jsanitize
from monty.dev import deprecated

from maggma.core import Store, Sort
from maggma.utils import grouper

try:
    import botocore
    import boto3
    from boto3.session import Session
except ImportError:
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
        s3_profile: str = None,
        compress: bool = False,
        endpoint_url: str = None,
        sub_dir: str = None,
        **kwargs,
    ):
        """
        Initializes an S3 Store

        Args:
            index: a store to use to index the S3 Bucket
            bucket: name of the bucket
            s3_profile: name of aws profile containing credentials for role
            compress: compress files inserted into the store
            endpoint_url: endpoint_url to allow interface to minio service
            sub_dir: (optional)  subdirectory of the s3 bucket to store the data
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
        # Force the key to be the same as the index
        kwargs["key"] = str(index.key)
        super(S3Store, self).__init__(**kwargs)

    def name(self) -> str:
        """
        Returns:
            a string representing this data source
        """
        return f"s3://{self.bucket}"

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data
        """
        self.index.connect(force_reset=force_reset)

        session = Session(profile_name=self.s3_profile)
        resource = session.resource("s3", endpoint_url=self.endpoint_url)

        if not self.s3:
            self.s3 = resource
            if self.bucket not in [bucket.name for bucket in self.s3.buckets.all()]:
                raise Exception("Bucket not present on AWS: {}".format(self.bucket))

            self.s3_bucket = resource.Bucket(self.bucket)

    def close(self):
        """
        Closes any connections
        """
        self.index.close()
        self.s3 = None
        self.s3_bucket = None

    @property  # type: ignore
    @deprecated(message="This will be removed in the future")
    def collection(self):
        """
        Returns:
            a handle to the pymongo collection object

        Important:
            Not guaranteed to exist in the future
        """
        # For now returns the index collection since that is what we would "search" on
        return self.index

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
        sort: Optional[Dict[str, Sort]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Dict]:
        """
        Queries the Store for a set of documents

        Args:
            criteria: PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields
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
                        self.s3_bucket.Object(self.sub_dir + doc[self.key])
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

                if doc.get("compression", "") == "zlib":
                    data = zlib.decompress(data)
                yield json.loads(data)

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
        sort: Optional[Dict[str, Sort]] = None,
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
            sort: Dictionary of sort order for fields
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
            if self.sub_dir != "":
                search_doc["sub_dir"] = self.sub_dir

            # Remove MongoDB _id from search
            if "_id" in search_doc:
                del search_doc["_id"]

            data = json.dumps(jsanitize(d)).encode()

            # Compress with zlib if chosen
            if self.compress:
                search_doc["compression"] = "zlib"
                data = zlib.compress(data)

            self.s3_bucket.put_object(
                Key=self.sub_dir + str(d[self.key]), Body=data, Metadata=search_doc
            )
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
            to_remove_chunks = list(grouper(to_remove, n=1000))
            for chunk_to_remove in to_remove_chunks:
                objlist = [{"Key": self.sub_dir + obj} for obj in chunk_to_remove]
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
        return self.index.newer_in(
            target=target, criteria=criteria, exhaustive=exhaustive
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

    def __eq__(self, other: object) -> bool:
        """
        Check equality for S3Store
        other: other S3Store to compare with
        """
        if not isinstance(other, S3Store):
            return False

        fields = ["index", "bucket", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)
