# coding: utf-8
"""
Advanced Stores for behavior outside normal access patterns
"""
import os
import hvac
import json
import zlib

from datetime import datetime
from maggma.stores import Store, MongoStore
from maggma.utils import lazy_substitute, substitute, unset
from pymongo import DESCENDING, MongoClient
from monty.json import jsanitize

try:
    import boto3
    import botocore
    boto_import = True
except:
    boto_import = False



class VaultStore(MongoStore):
    """
    Extends MongoStore to read credentials out of Vault server
    and uses these values to initialize MongoStore instance
    """

    def __init__(self, collection_name, vault_secret_path):
        """
        collection (string): name of mongo collection
        vault_secret_path (string): path on vault server with mongo creds object

        Environment (must be set prior to invocation):
        VAULT_ADDR - URL of vault server (eg. https://matgen8.lbl.gov:8200)
        VAULT_TOKEN or GITHUB_TOKEN - token used to authenticate to vault
        """
        vault_addr = os.getenv("VAULT_ADDR")

        if not vault_addr:
            raise RuntimeError("VAULT_ADDR not set")

        client = hvac.Client(vault_addr)

        # If we have a vault token use this
        token = os.getenv("VAULT_TOKEN")

        # Look for a github token instead
        if not token:
            github_token = os.getenv("GITHUB_TOKEN")

            if github_token:
                client.auth_github(github_token)
            else:
                raise RuntimeError("VAULT_TOKEN or GITHUB_TOKEN not set")
        else:
            client.token = token
            if not client.is_authenticated():
                raise RuntimeError("Bad token")

        # Read the vault secret
        json_db_creds = client.read(vault_secret_path)
        db_creds = json.loads(json_db_creds['data']['value'])

        database = db_creds.get("db")
        host = db_creds.get("host", "localhost")
        port = db_creds.get("port", 27017)
        username = db_creds.get("username", "")
        password = db_creds.get("password", "")

        super(VaultStore, self).__init__(database, collection_name, host, port, username, password)


class AliasingStore(Store):
    """
    Special Store that aliases for the primary accessors
    """

    def __init__(self, store, aliases, **kwargs):
        """
        store (Store): the store to wrap around
        aliases (dict): dict of aliases of the form external key: internal key
        """
        self.store = store
        self.aliases = aliases
        self.reverse_aliases = {v: k for k, v in aliases.items()}
        self.kwargs = kwargs

        kwargs.update({"lu_field": store.lu_field, "lu_type": store.lu_type})
        super(AliasingStore, self).__init__(**kwargs)

    def query(self, properties=None, criteria=None, **kwargs):

        if isinstance(properties, list):
            properties = {p: 1 for p in properties}

        criteria = criteria if criteria else {}
        substitute(properties, self.reverse_aliases)
        lazy_substitute(criteria, self.reverse_aliases)
        for d in self.store.query(properties, criteria, **kwargs):
            substitute(d, self.aliases)
            yield d

    def query_one(self, properties=None, criteria=None, **kwargs):

        if isinstance(properties, list):
            properties = {p: 1 for p in properties}

        criteria = criteria if criteria else {}
        substitute(properties, self.reverse_aliases)
        lazy_substitute(criteria, self.reverse_aliases)
        d = self.store.query_one(properties, criteria, **kwargs)
        substitute(d, self.aliases)
        return d

    def distinct(self, key, criteria=None, **kwargs):
        if key in self.aliases:
            key = self.aliases[key]
        criteria = criteria if criteria else {}
        lazy_substitute(criteria, self.aliases)
        return self.store.distinct(key, criteria, **kwargs)

    def update(self, docs, update_lu=True, key=None):
        key = key if key else self.key

        for d in docs:
            substitute(d, self.reverse_aliases)

        if key in self.aliases:
            key = self.aliases[key]

        self.store.update(docs, update_lu=update_lu, key=key)

    def ensure_index(self, key, unique=False):
        if key in self.aliases:
            key = self.aliases
        return self.store.ensure_index(key, unique)

    def close(self):
        self.store.close()

    @property
    def collection(self):
        return self.store.collection

    def connect(self, force_reset=False):
        self.store.connect(force_reset=force_reset)


class AmazonS3Store(Store):
    """
    GridFS like storage using Amazon S3 and a regular store for indexing
    Assumes Amazon AWS key and secret key are set in environment or default config file
    """

    def __init__(self, index, bucket, **kwargs):
        """
        Initializes an S3 Store
        Args:
            index (Store): a store to use to index the S3 Bucket
            bucket (str) : name of the bucket
        """
        if not boto_import:
            raise ValueError("boto not available, please install boto3 to "
                             "use AmazonS3Store")
        self.index = index
        self.bucket = bucket
        self.s3 = None
        self.s3_bucket = None
        # Force the key to be the same as the index
        kwargs["key"] = index.key
        super(AmazonS3Store, self).__init__(**kwargs)

    def connect(self, force_reset=False):
        self.index.connect(force_reset=force_reset)
        if not self.s3:
            self.s3 = boto3.resource("s3")
            # TODO: Provide configuration variable to create bucket if not present
            if self.bucket not in self.s3.list_buckets():
                raise Exception("Bucket not present on AWS: {}".format(self.bucket))
            self.s3_bucket = self.s3.Bucket(self.bucket)

    def close(self):
        self.index.close()

    @property
    def collection(self):
        # For now returns the index collection since that is what we would "search" on
        return self.index

    def query(self, properties=None, criteria=None, **kwargs):
        """
        Function that gets data from Amazon S3. This store ignores all
        property projections as its designed for whole document access

        Args:
            properties (list or dict): This will be ignored by the S3
                Store
            criteria (dict): filter for query, matches documents
                against key-value pairs
            **kwargs (kwargs): further kwargs to Collection.find
        """
        for f in self.index.find(filter=criteria, **kwargs):
            try:
                data = self.s3_bucket.Object(f[self.key]).get()
            except botocore.exceptions.ClientError as e:
                # If a client error is thrown, then check that it was a 404 error.
                # If it was a 404 error, then the object does not exist.
                error_code = int(e.response['Error']['Code'])
                if error_code == 404:
                    self.logger.error("Could not find S3 object {}".format(f[self.key]))
                    break

            if f.get("compression", "") is "zlib":
                data = zlib.decompress(data)

            yield json.loads(data)

    def query_one(self, properties=None, criteria=None, **kwargs):
        """
        Function that gets a single document from Amazon S3. This store
        ignores all property projections as its designed for whole
        document access

        Args:
            properties (list or dict): This will be ignored by the S3
                Store
            criteria (dict): filter for query, matches documents
                against key-value pairs
            **kwargs (kwargs): further kwargs to Collection.find
        """
        f = self.index.find_one(filter=criteria, **kwargs)
        if f:
            try:
                data = self.s3_bucket.Object(f[self.key]).get()
            except botocore.exceptions.ClientError as e:
                # If a client error is thrown, then check that it was a 404 error.
                # If it was a 404 error, then the object does not exist.
                error_code = int(e.response['Error']['Code'])
                if error_code == 404:
                    self.logger.error("Could not find S3 object {}".format(f[self.key]))
                    return None

            if f.get("compression", "") is "zlib":
                data = zlib.decompress(data)

            return json.loads(data)
        else:
            return None

    def distinct(self, key, criteria=None, all_exist=False, **kwargs):
        """
        Function get to get all distinct values of a certain key in the
        GridFS Store. This searches the .files collection for this data

        Args:
            key (mongolike key or list of mongolike keys): key or keys
                for which to find distinct values or sets of values.
            criteria (filter criteria): criteria for filter
            all_exist (bool): whether to ensure all keys in list exist
                in each document, defaults to False
            **kwargs (kwargs): kwargs corresponding to collection.distinct
        """
        # Index is a store so it should have its own distinct function
        return self.index.distinct(key, filter=criteria, **kwargs)

    def ensure_index(self, key, unique=False):
        """
        Wrapper for pymongo.Collection.ensure_index for the files collection
        """
        return self.index.ensure_index(key, unique=unique, background=True)

    def update(self, docs, update_lu=True, key=None, compress=False):
        """
        Function to update associated MongoStore collection.

        Args:
            docs ([dict]): list of documents
            key ([str] or str): keys to use to build search doc
            compress (bool): compress the document or not
        """
        now = datetime.now()
        search_docs = []
        for d in docs:
            search_doc = {}
            if isinstance(key, list):
                search_doc = {k: d[k] for k in key}
            elif key:
                search_doc = {key: d[key]}

            # Always include our main key
            search_doc = {self.key: d[self.key]}

            # Remove MongoDB _id from search
            if "_id" in search_doc:
                del search_doc["_id"]

            # Add a timestamp
            if update_lu:
                search_doc[self.lu_key] = now
                d[self.lu_key] = now

            data = json.dumps(jsanitize(d)).encode()

            # Compress with zlib if chosen
            if compress:
                search_doc["compression"] = "zlib"
                data = zlib.compress(data)

            self.s3_bucket.put_object(Key=d[self.key], Body=data, Metadata=search_doc)
            search_docs.append(search_doc)

        # Use store's update to remove key clashes
        self.index.update(search_docs)

    @property
    def last_updated(self):
        self.index.last_updated

    def lu_filter(self, targets):
        """Creates a MongoDB filter for new documents.

        By "new", we mean documents in this Store that were last updated later
        than any document in targets.

        Args:
            targets (list): A list of Stores

        """
        self.index.last_updated(targets)

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


class JointStore(Store):
    """Store corresponding to multiple collections, uses lookup to join"""
    def __init__(self, database, collection_names, host="localhost",
                 port=27017, username="", password="", master=None, **kwargs):
        self.database = database
        self.collection_names = collection_names
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._collection = None
        self.master = master or collection_names[0]
        self.kwargs = kwargs
        super(JointStore, self).__init__(**kwargs)

    def connect(self, force_reset=False):
        conn = MongoClient(self.host, self.port)
        db = conn[self.database]
        if self.username is not "":
            db.authenticate(self.username, self.password)
        self._collection = db[self.master]

    def close(self):
        self.collection.database.client.close()

    @property
    def collection(self):
        return self._collection

    @property
    def nonmaster_names(self):
        return list(set(self.collection_names) - {self.master})

    def query(self, properties=None, criteria=None, **kwargs):
        pipeline = []
        for cname in self.nonmaster_names:
            pipeline.append({
                "$lookup": {"from": cname, "localField": self.key,
                            "foreignField": self.key, "as": cname}})
            pipeline.append({
                "$unwind": {"path": "${}".format(cname),
                            "preserveNullAndEmptyArrays": True}})

        # Do projection for max last_updated
        lu_proj = {self.lu_field: {"$max": ["${}.{}".format(cname, self.lu_field)
                                            for cname in self.collection_names]}}
        pipeline.append({"$addFields": lu_proj})

        if criteria:
            pipeline.append({"$match": criteria})
        if isinstance(properties, list):
            properties = {k: 1 for k in properties}
        if properties:
            pipeline.append({"$project": properties})
        return self.collection.aggregate(pipeline)

    @property
    def last_updated(self):
        lus = []
        for cname in self.collection_names:
            lu = MongoStore.from_collection(
                self.collection.database[cname],
                lu_field=self.lu_field).last_updated
            lus.append(lu)
        return max(lus)

    def update(self, docs, update_lu=True, key=None):
        # TODO: implement update?
        raise NotImplementedError("No update method for JointStore")

    def _get_store_by_name(self, name):
        return MongoStore.from_collection(self.collection.database[name])

    def distinct(self, key, criteria=None, **kwargs):
        # TODO: implement multi-key distinct, I think this can be implemented
        #       similarly as standard multi-key distinct
        # TODO: actually, this this needs to be implemented with an agg pipeline
        #       to work for criteria filtering on sub-collections
        if isinstance(key, list):
            raise NotImplementedError("list-key distinct not implemented for "
                                      "JointStore")
        skey = key.split('.', 1)
        if skey[0] in self.nonmaster_names:
            return self._get_store_by_name(skey[0]).distinct(skey[1], **kwargs)
        else:
            return self.collection.distinct(key, **kwargs)

    def ensure_index(self):
        pass

    def _get_pipeline(self, properties=None, criteria=None):
        """
        Gets the aggregation pipeline for query and query_one

        Args:
            properties: properties to be returned
            criteria: criteria to filter by

        Returns:
            list of aggregation operators
        """
        pipeline = []
        for cname in self.collection_names:
            if cname is not self.master:
                pipeline.append({
                    "$lookup": {"from": cname, "localField": self.key,
                                "foreignField": self.key, "as": cname}})
                pipeline.append({
                    "$unwind": {"path": "${}".format(cname),
                                "preserveNullAndEmptyArrays": True}})

        # Do projection for max last_updated
        lu_proj = {self.lu_field: {"$max": ["${}.{}".format(cname, self.lu_field)
                                            for cname in self.collection_names]}}
        pipeline.append({"$addFields": lu_proj})

        if criteria:
            pipeline.append({"$match": criteria})
        if isinstance(properties, list):
            properties = {k: 1 for k in properties}
        if properties:
            pipeline.append({"$project": properties})
        return pipeline

    def query_one(self, properties=None, criteria=None, **kwargs):
        """
        Get one document

        Args:
            properties([str] or {}): properties to return in query
            criteria ({}): filter for matching
            **kwargs: kwargs for collection.aggregate

        Returns:
            single document
        """
        # TODO: maybe adding explicit limit in agg pipeline is better as below?
        # pipeline = self._get_pipeline(properties, criteria)
        # pipeline.append({"$limit": 1})
        query = self.query(properties, criteria, **kwargs)
        try:
            doc = query.next()
            return doc
        except StopIteration:
            return None
