from typing import List, Iterator, Tuple, Optional, Union, Dict
from pydash import get, set_
from pymongo import MongoClient
from maggma.core import Store, Sort
from maggma.stores import MongoStore


class JointStore(Store):
    """Store corresponding to multiple collections, uses lookup to join"""

    def __init__(
        self,
        database: str,
        collection_names: List[str],
        host: str = "localhost",
        port: int = 27017,
        username: str = "",
        password: str = "",
        master: Optional[str] = None,
        merge_at_root: bool = False,
        **kwargs
    ):
        self.database = database
        self.collection_names = collection_names
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._collection = None
        self.master = master or collection_names[0]
        self.merge_at_root = merge_at_root
        self.kwargs = kwargs
        super(JointStore, self).__init__(**kwargs)

    def connect(self, force_reset: bool = False):
        conn = MongoClient(self.host, self.port)
        db = conn[self.database]
        if self.username != "":
            db.authenticate(self.username, self.password)
        self._collection = db[self.master]
        self._has_merge_objects = (
            self._collection.database.client.server_info()["version"] > "3.6"
        )

    def close(self):
        self.collection.database.client.close()

    @property
    def collection(self):
        return self._collection

    @property
    def nonmaster_names(self):
        return list(set(self.collection_names) - {self.master})

    @property
    def last_updated(self):
        lus = []
        for cname in self.collection_names:
            lu = MongoStore.from_collection(
                self.collection.database[cname],
                last_updated_field=self.last_updated_field,
            ).last_updated
            lus.append(lu)
        return max(lus)

    # TODO: implement update?
    def update(self, docs, update_lu=True, key=None, **kwargs):
        raise NotImplementedError("No update method for JointStore")

    def _get_store_by_name(self, name):
        return MongoStore.from_collection(self.collection.database[name])

    def distinct(
        self,
        field: Union[List[str], str],
        criteria: Optional[Dict] = None,
        all_exist: bool = False,
    ) -> List:
        """
        Get all distinct values for a key

        Args:
            field: the field(s) to get distinct values for
            criteria : PyMongo filter for documents to search in
            all_exist : ensure all fields exist for the distinct set
        """
        g_field = field if isinstance(field, list) else [field]
        if all_exist:
            criteria = criteria or {}
            criteria.update(
                {k: {"$exists": True} for k in g_field if k not in criteria}
            )
        cursor = self.groupby(g_field, criteria=criteria)
        if isinstance(field, list):
            return [d[0] for d in cursor]
        else:
            return [get(d[0], field) for d in cursor]

    def ensure_index(self, key, unique=False, **kwargs):
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
        for cname in self.collection_names:
            if cname is not self.master:
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

    def query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Sort]] = None,
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
        sort: Optional[Dict[str, Sort]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Tuple[Dict, List[Dict]]]:
        pipeline = self._get_pipeline(
            criteria=criteria, properties=properties, skip=skip, limit=limit
        )
        if not isinstance(keys, list):
            keys = [keys]
        group_id = {}
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
            properties([str] or {}): properties to return in query
            criteria ({}): filter for matching
            **kwargs: kwargs for collection.aggregate
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


class ConcatStore(Store):
    """Store concatting multiple stores"""

    def __init__(self, *stores, **kwargs):
        """
        Initialize a ConcatStore that concatenates multiple stores together
        to appear as one store
        """
        self.stores = stores
        super(ConcatStore, self).__init__(**kwargs)

    def connect(self, force_reset=False):
        """
        Connect all stores in this ConcatStore
        Args:
            force_reset (bool): Whether to forcibly reset the connection for
            all stores
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
    def collection(self):
        raise NotImplementedError("No collection property for ConcatStore")

    @property
    def last_updated(self):
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

    # TODO: implement update?
    def update(self, docs, update_lu=True, key=None, **kwargs):
        raise NotImplementedError("No update method for JointStore")

    def distinct(self, key, criteria=None, all_exist=True, **kwargs):
        """
        Return all distinct values for a key within the stores
        Args:
            key (str): key to find distinct values
            criteria (dict): criteria dictionary to reduce the documents to search on
            all_exist (bool): ensure the key exists in the doc or not
        """
        distincts = []
        for store in self.stores:
            distincts.extend(store.distinct(key, criteria, all_exist, **kwargs))
        return list(set(distincts))

    def ensure_index(self, key, unique=False, **kwargs):
        """
        Ensure an index is properly set. Returns whether all stores support this index or not
        Args:
            key (str or [str]): single key or list of keys to group by
        """
        return all([store.ensure_index(key, unique, **kwargs) for store in self.stores])

    def query(self, criteria=None, properties=None, **kwargs):
        """
        Queries across all the stores.
        Args:
            criteria (dict): mongo style query to reduce the docs to group
            properties (str or [str]): properties to project
        """
        for store in self.stores:
            for d in store.query(criteria=criteria, properties=properties, **kwargs):
                yield d

    def query_one(self, criteria=None, properties=None, **kwargs):
        return next(self.query(criteria=criteria, properties=properties, **kwargs))

    def groupby(self, keys, criteria=None, properties=None, **kwargs):
        """
        Group documents by a key. This version is highly inefficient since it performs
        post-grouping in python across all of its stores
        Args:
            keys (str or [str]): single key or list of keys to group by
            criteria (dict): mongo style query to reduce the docs to group
            properties (str or [str]): properties to project
        """
        if isinstance(keys, str):
            keys = [keys]

        docs = []
        for store in self.stores:
            temp_docs = list(
                store.groupby(keys, criteria=criteria, properties=properties, **kwargs)
            )
            for group in temp_docs:
                docs.extend(group["docs"])

        def key_set(d):
            "index function based on passed in keys"
            test_d = tuple(d.get(k, "") for k in keys)
            return test_d

        for k, group in groupby(docs, key=key_set):
            yield list(group)
