from maggma.api.query_operator import QueryOperator
from maggma.api.utils import STORE_PARAMS
from typing import Optional
from fastapi import Query, HTTPException

from maggma.stores.mongolike import MongoStore


class VersionQuery(QueryOperator):
    """
    Method to generate a query on a specific collection version
    """

    def __init__(self, default_version=None):
        """
        Args:
            default_version: the default collection version
        """
        self.default_version = default_version

        def query(
            version: Optional[str] = Query(
                default_version, description="Database version to query on formatted as YYYY_MM_DD",
            ),
        ) -> STORE_PARAMS:

            crit = {}

            if version:
                crit.update({"version": version})

            return {"criteria": crit}

        self.query = query  # type: ignore

    def query(self):
        " Stub query function for abstract class "
        pass

    def versioned_store_setup(self, store: MongoStore, version: str):
        prefix = "_".join(store.collection_name.split("_")[0:-3])
        versioned_collection = f"{prefix}_{version}"
        store.connect()
        if versioned_collection in store._collection.database.list_collection_names():
            store.collection_name = f"{prefix}_{version}"
            store.close()
            return store
        else:
            store.close()
            raise HTTPException(
                status_code=404, detail=f"Data with version {version} not found",
            )
