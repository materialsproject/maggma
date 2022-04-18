from inspect import signature
from typing import Any, Dict, List, Optional, Type

from fastapi import HTTPException, Request
from pydantic import BaseModel

from maggma.api.models import Meta, Response
from maggma.api.query_operator import PaginationQuery, QueryOperator, SparseFieldsQuery
from maggma.api.resource import Resource
from maggma.api.resource.utils import attach_query_ops
from maggma.api.utils import STORE_PARAMS, merge_queries
from maggma.core import Store


class PostOnlyResource(Resource):
    """
    Implements a REST Compatible Resource as a POST URL endpoint
    """

    def __init__(
        self,
        store: Store,
        model: Type[BaseModel],
        tags: Optional[List[str]] = None,
        query_operators: Optional[List[QueryOperator]] = None,
        key_fields: Optional[List[str]] = None,
        query: Optional[Dict] = None,
        include_in_schema: Optional[bool] = True,
        sub_path: Optional[str] = "/",
    ):
        """
        Args:
            store: The Maggma Store to get data from
            model: The pydantic model this Resource represents
            tags: List of tags for the Endpoint
            query_operators: Operators for the query language
            key_fields: List of fields to always project. Default uses SparseFieldsQuery
                to allow user to define these on-the-fly.
            include_in_schema: Whether the endpoint should be shown in the documented schema.
            sub_path: sub-URL path for the resource.
        """
        self.store = store
        self.tags = tags or []
        self.query = query or {}
        self.key_fields = key_fields
        self.versioned = False

        self.include_in_schema = include_in_schema
        self.sub_path = sub_path
        self.response_model = Response[model]  # type: ignore

        self.query_operators = (
            query_operators
            if query_operators is not None
            else [
                PaginationQuery(),
                SparseFieldsQuery(
                    model,
                    default_fields=[self.store.key, self.store.last_updated_field],
                ),
            ]
        )

        super().__init__(model)

    def prepare_endpoint(self):
        """
        Internal method to prepare the endpoint by setting up default handlers
        for routes
        """

        self.build_dynamic_model_search()

    def build_dynamic_model_search(self):

        model_name = self.model.__name__

        async def search(**queries: Dict[str, STORE_PARAMS]) -> Dict:
            request: Request = queries.pop("request")  # type: ignore
            queries.pop("temp_response")  # type: ignore

            query_params = [
                entry
                for _, i in enumerate(self.query_operators)
                for entry in signature(i.query).parameters
            ]

            overlap = [
                key for key in request.query_params.keys() if key not in query_params
            ]
            if any(overlap):
                raise HTTPException(
                    status_code=400,
                    detail="Request contains query parameters which cannot be used: {}".format(
                        ", ".join(overlap)
                    ),
                )

            query: Dict[Any, Any] = merge_queries(list(queries.values()))  # type: ignore
            query["criteria"].update(self.query)

            self.store.connect()

            count = self.store.count(query["criteria"])
            data = list(self.store.query(**query))
            operator_meta = {}

            for operator in self.query_operators:
                data = operator.post_process(data, query)
                operator_meta.update(operator.meta())

            meta = Meta(total_doc=count)
            response = {"data": data, "meta": {**meta.dict(), **operator_meta}}
            return response

        self.router.post(
            self.sub_path,
            tags=self.tags,
            summary=f"Post {model_name} documents",
            response_model=self.response_model,
            response_description=f"Post {model_name} data",
            response_model_exclude_unset=True,
        )(attach_query_ops(search, self.query_operators))
