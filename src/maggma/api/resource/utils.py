from typing import Callable, Dict, List

from fastapi import Depends, Request, Response

from maggma.api.query_operator import QueryOperator
from maggma.api.utils import STORE_PARAMS, attach_signature
from maggma.core.store import Store


def attach_query_ops(
    function: Callable[[List[STORE_PARAMS]], Dict], query_ops: List[QueryOperator]
) -> Callable[[List[STORE_PARAMS]], Dict]:
    """
    Attach query operators to API compliant function
    The function has to take a list of STORE_PARAMs as the only argument

    Args:
        function: the function to decorate
    """
    attach_signature(
        function,
        annotations={
            **{f"dep{i}": STORE_PARAMS for i, _ in enumerate(query_ops)},
            "request": Request,
            "temp_response": Response,
        },
        defaults={f"dep{i}": Depends(dep.query) for i, dep in enumerate(query_ops)},
    )
    return function


def generate_query_pipeline(query: dict, store: Store):
    """
    Generate the generic aggregation pipeline used in GET endpoint queries

    Args:
        query: Query parameters
        store: Store containing endpoint data
    """
    pipeline = [
        {"$match": query["criteria"]},
    ]

    sorting = query.get("sort", False)

    if sorting:
        sort_dict = {"$sort": {}}  # type: dict
        sort_dict["$sort"].update(query["sort"])
        sort_dict["$sort"].update({store.key: 1})  # Ensures sort by key is last in dict to fix determinacy

    projection_dict = {"_id": 0}  # Do not return _id by default

    if query.get("properties", False):
        projection_dict.update({p: 1 for p in query["properties"]})

    if sorting:
        pipeline.append({"$project": {**projection_dict, store.key: 1}})
        pipeline.append(sort_dict)

    pipeline.append({"$project": projection_dict})
    pipeline.append({"$skip": query["skip"] if "skip" in query else 0})

    if query.get("limit", False):
        pipeline.append({"$limit": query["limit"]})

    return pipeline
