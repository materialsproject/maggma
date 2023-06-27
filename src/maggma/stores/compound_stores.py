""" Special stores that combine underlying Stores together """
from datetime import datetime
from itertools import groupby
from typing import Dict, Iterator, List, Optional, Tuple, Union

from pydash import set_
from pymongo import MongoClient

from maggma.core import Sort, Store, StoreError
from maggma.stores.mongolike import MongoStore


class JointStore(Store):
    """
    Store that implements a on-the-fly join across multiple collections all in the same MongoDB database.
    This is a Read-Only Store designed to combine data from multiple collections.
    """

    def __init__(
        self,
        database: str,
        collection_names: List[str],
        host: str = "localhost",
        port: int = 27017,
        username: str = "",
        password: str = "",
        main: Optional[str] = None,
        merge_at_root: bool = False,
        mongoclient_kwargs: Optional[Dict] = None,
        **kwargs,
    ):
        """
        Args:
            database: The database name
            collection_names: list of all collections
                to join
            host: Hostname for the database
            port: TCP port to connect to
            username: Username for the collection
            password: Password to connect with
            main: name for the main collection
                if not specified this defaults to the first
                in collection_names list
        """
        self.database = database
        self.collection_names = collection_names
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._coll = None  # type: Any
        self.main = main or collection_names[0]
        self.merge_at_root = merge_at_root
        self.mongoclient_kwargs = mongoclient_kwargs or {}
        self.kwargs = kwargs

        super(JointStore, self).__init__(**kwargs)

    @property
    def name(self) -> str:
        """
        Return a string representing this data source
        """
        compound_name = ",".join(self.collection_names)
        return f"Compound[{self.host}/{self.database}][{compound_name}]"

    def connect(self, force_reset: bool = False):
        """
        Connects the underlying Mongo database and
        all collection connections

        Args:
            force_reset: whether to forcibly reset the connection
        """
        conn: MongoClient = (
            MongoClient(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                **self.mongoclient_kwargs,
            )
            if self.username != ""
            else MongoClient(self.host, self.port, **self.mongoclient_kwargs)
        )
        db = conn[self.database]
        self._coll = db[self.main]
        self._has_merge_objects = (
            self._collection.database.client.server_info()["version"] > "3.6"
        )

    def close(self):
        """
        Closes underlying database connections
        """
        self._collection.database.client.close()

    @property
    def _collection(self):
        """Property referring to the root pymongo collection"""
        if self._coll is None:
            raise StoreError("Must connect Mongo-like store before attemping to use it")
        return self._coll

    @property
    def nonmain_names(self) -> List:
        """
        alll non-main collection names
        """
        return list(set(self.collection_names) - {self.main})

    @property
    def last_updated(self) -> datetime:
        """
        Special last_updated for this JointStore
        that checks all underlying collections
        """
        lus = []
        for cname in self.collection_names:
            store = MongoStore.from_collection(self._collection.database[cname])
            store.last_updated_field = self.last_updated_field
            lu = store.last_updated
            lus.append(lu)
        return max(lus)

    # TODO: implement update?
    def update(self, docs, update_lu=True, key=None, **kwargs):
        """
        Update documents into the underlying collections
        Not Implemented for JointStore
        """
        raise NotImplementedError("JointStore is a read-only store")

    def _get_store_by_name(self, name) -> MongoStore:
        """
        Gets an underlying collection as a mongoStore
        """
        if name not in self.collection_names:
            raise ValueError("Asking for collection not referenced in this Store")
        return MongoStore.from_collection(self._collection.database[name])

    def ensure_index(self, key, unique=False, **kwargs):
        """
        Can't ensure index for JointStore
        """
        raise NotImplementedError("No ensure_index method for JointStore")

    def _get_pipeline(self, criteria=None, properties=None, skip=0, limit=0):
        """
        Gets the aggregation pipeline for query and query_one

        Args:
            properties: properties to be returned
            criteria: criteria to filter by
            skip: docs to skip
            limit: limit results to N docs
        Returns:
            list of aggregation operators
        """
        pipeline = []
        collection_names = list(set(self.collection_names) - set(self.main))
        for cname in collection_names:
            pipeline.append(
                {
                    "$lookup": {
                        "from": cname,
                        "localField": self.key,
                        "foreignField": self.key,
                        "as": cname,
                    }
                }
            )

            if self.merge_at_root:
                if not self._has_merge_objects:
                    raise Exception(
                        "MongoDB server version too low to use $mergeObjects."
                    )

                pipeline.append(
                    {
                        "$replaceRoot": {
                            "newRoot": {
                                "$mergeObjects": [
                                    {"$arrayElemAt": ["${}".format(cname), 0]},
                                    "$$ROOT",
                                ]
                            }
                        }
                    }
                )
            else:
                pipeline.append(
                    {
                        "$unwind": {
                            "path": "${}".format(cname),
                            "preserveNullAndEmptyArrays": True,
                        }
                    }
                )

        # Do projection for max last_updated
        lu_max_fields = ["${}".format(self.last_updated_field)]
        lu_max_fields.extend(
            [
                "${}.{}".format(cname, self.last_updated_field)
                for cname in self.collection_names
            ]
        )
        lu_proj = {self.last_updated_field: {"$max": lu_max_fields}}
        pipeline.append({"$addFields": lu_proj})

        if criteria:
            pipeline.append({"$match": criteria})
        if isinstance(properties, list):
            properties = {k: 1 for k in properties}
        if properties:
            pipeline.append({"$project": properties})

        if skip > 0:
            pipeline.append({"$skip": skip})

        if limit > 0:
            pipeline.append({"$limit": limit})
        return pipeline

    def count(self, criteria: Optional[Dict] = None) -> int:
        """
        Counts the number of documents matching the query criteria

        Args:
            criteria: PyMongo filter for documents to count in
        """
        pipeline = self._get_pipeline(criteria=criteria)
        pipeline.append({"$count": "count"})
        agg = list(self._collection.aggregate(pipeline))
        return agg[0].get("count", 0) if len(agg) > 0 else 0

    def query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Dict]:
        pipeline = self._get_pipeline(
            criteria=criteria, properties=properties, skip=skip, limit=limit
        )
        agg = self._collection.aggregate(pipeline)
        for d in agg:
            yield d

    def groupby(
        self,
        keys: Union[List[str], str],
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Tuple[Dict, List[Dict]]]:
        pipeline = self._get_pipeline(
            criteria=criteria, properties=properties, skip=skip, limit=limit
        )
        if not isinstance(keys, list):
            keys = [keys]
        group_id = {}  # type: Dict[str,Any]
        for key in keys:
            set_(group_id, key, "${}".format(key))
        pipeline.append({"$group": {"_id": group_id, "docs": {"$push": "$$ROOT"}}})

        agg = self._collection.aggregate(pipeline)

        for d in agg:
            yield d["_id"], d["docs"]

    def query_one(self, criteria=None, properties=None, **kwargs):
        """
        Get one document

        Args:
            properties: properties to return in query
            criteria: filter for matching
            kwargs: kwargs for collection.aggregate

        Returns:
            single document
        """
        # TODO: maybe adding explicit limit in agg pipeline is better as below?
        # pipeline = self._get_pipeline(properties, criteria)
        # pipeline.append({"$limit": 1})
        query = self.query(criteria=criteria, properties=properties, **kwargs)
        try:
            doc = next(query)
            return doc
        except StopIteration:
            return None

    def remove_docs(self, criteria: Dict):
        """
        Remove docs matching the query dictionary

        Args:
            criteria: query dictionary to match
        """
        raise NotImplementedError("No remove_docs method for JointStore")

    def __eq__(self, other: object) -> bool:
        """
        Check equality for JointStore
        Args:
            other: other JointStore to compare with
        """
        if not isinstance(other, JointStore):
            return False

        fields = [
            "database",
            "collection_names",
            "host",
            "port",
            "main",
            "merge_at_root",
        ]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


class ConcatStore(Store):
    """Store concatting multiple stores"""

    def __init__(self, stores: List[Store], **kwargs):
        """
        Initialize a ConcatStore that concatenates multiple stores together
        to appear as one store

        Args:
            stores: list of stores to concatenate together
        """
        self.stores = stores
        self.kwargs = kwargs
        super(ConcatStore, self).__init__(**kwargs)

    @property
    def name(self) -> str:
        """
        A string representing this data source
        """
        compound_name = ",".join([store.name for store in self.stores])
        return f"Concat[{compound_name}]"

    def connect(self, force_reset: bool = False):
        """
        Connect all stores in this ConcatStore

        Args:
            force_reset: Whether to forcibly reset the connection for all stores
        """
        for store in self.stores:
            store.connect(force_reset)

    def close(self):
        """
        Close all connections in this ConcatStore
        """
        for store in self.stores:
            store.close()

    @property
    def _collection(self):
        raise NotImplementedError("No collection property for ConcatStore")

    @property
    def last_updated(self) -> datetime:
        """
        Finds the most recent last_updated across all the stores.
        This might not be the most usefull way to do this for this type of Store
        since it could very easily over-estimate the last_updated based on what stores
        are used
        """
        lus = []
        for store in self.stores:
            lu = store.last_updated
            lus.append(lu)
        return max(lus)

    def update(self, docs: Union[List[Dict], Dict], key: Union[List, str, None] = None):
        """
        Update documents into the Store
        Not implemented in ConcatStore

        Args:
            docs: the document or list of documents to update
            key: field name(s) to determine uniqueness for a
                 document, can be a list of multiple fields,
                 a single field, or None if the Store's key
                 field is to be used
        """
        raise NotImplementedError("No update method for ConcatStore")

    def distinct(
        self, field: str, criteria: Optional[Dict] = None, all_exist: bool = False
    ) -> List:
        """
        Get all distinct values for a field

        Args:
            field: the field(s) to get distinct values for
            criteria: PyMongo filter for documents to search in
        """
        distincts = []
        for store in self.stores:
            distincts.extend(store.distinct(field=field, criteria=criteria))

        return list(set(distincts))

    def ensure_index(self, key: str, unique: bool = False) -> bool:
        """
        Ensure an index is properly set. Returns whether all stores support this index or not

        Args:
            key: single key to index
            unique: Whether or not this index contains only unique keys

        Returns:
            bool indicating if the index exists/was created on all stores
        """
        return all([store.ensure_index(key, unique) for store in self.stores])

    def count(self, criteria: Optional[Dict] = None) -> int:
        """
        Counts the number of documents matching the query criteria

        Args:
            criteria: PyMongo filter for documents to count in
        """
        counts = [store.count(criteria) for store in self.stores]

        return sum(counts)

    def query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Dict]:
        """
        Queries across all Store for a set of documents

        Args:
            criteria: PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip
            limit: limit on total number of documents returned
        """
        # TODO: skip, sort and limit are broken. implement properly
        for store in self.stores:
            for d in store.query(criteria=criteria, properties=properties):
                yield d

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
        if isinstance(keys, str):
            keys = [keys]

        docs = []
        for store in self.stores:
            temp_docs = list(
                store.groupby(
                    keys=keys,
                    criteria=criteria,
                    properties=properties,
                    sort=sort,
                    skip=skip,
                    limit=limit,
                )
            )
            for key, group in temp_docs:
                docs.extend(group)

        def key_set(d: Dict) -> Tuple:
            "index function based on passed in keys"
            test_d = tuple(d.get(k, None) for k in keys)
            return test_d

        sorted_docs = sorted(docs, key=key_set)
        for vals, group_iter in groupby(sorted_docs, key=key_set):
            id_dict = {key: val for key, val in zip(keys, vals)}
            yield id_dict, list(group_iter)

    def remove_docs(self, criteria: Dict):
        """
        Remove docs matching the query dictionary

        Args:
            criteria: query dictionary to match
        """
        raise NotImplementedError("No remove_docs method for JointStore")

    def __eq__(self, other: object) -> bool:
        """
        Check equality for ConcatStore

        Args:
            other: other JointStore to compare with
        """
        if not isinstance(other, ConcatStore):
            return False

        fields = ["stores"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)
