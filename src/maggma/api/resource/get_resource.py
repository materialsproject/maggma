from inspect import signature
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Request
from monty.json import MontyDecoder, MSONable
from pydantic import BaseModel
from starlette.responses import RedirectResponse

from maggma.api.models import Meta, Response
from maggma.api.query_operator import PaginationQuery, QueryOperator, SparseFieldsQuery
from maggma.api.resource import Resource
from maggma.api.utils import STORE_PARAMS, attach_signature, merge_queries
from maggma.core import Store
from maggma.utils import dynamic_import


class GetResource(Resource):
    """
    Implements a REST Compatible Resource as a GET URL endpoint
    This class provides a number of convenience features
    including full pagination, field projection
    """

    def __init__(
        self,
        store: Store,
        model: BaseModel,
        tags: Optional[List[str]] = None,
        query_operators: Optional[List[QueryOperator]] = None,
        query: Optional[Dict] = None,
        enable_get_by_key: bool = True,
        enable_default_search: bool = True,
    ):
        """
        Args:
            store: The Maggma Store to get data from
            model: the pydantic model this Resource represents
            tags: list of tags for the Endpoint
            query_operators: operators for the query language
            description: an explanation of wht does this resource do
        """

        super().__init__(model)

        self.store = store
        self.tags = tags or []
        self.query = query or {}
        self.enable_get_by_key = enable_get_by_key
        self.enable_default_search = enable_default_search

        self.query_operators = (
            query_operators
            if query_operators is not None
            else [
                PaginationQuery(),
                SparseFieldsQuery(
                    self.model,
                    default_fields=[self.store.key, self.store.last_updated_field],
                ),
            ]
        )

        self.response_model = Response[self.model]

    def prepare_endpoint(self):
        """
        Internal method to prepare the endpoint by setting up default handlers
        for routes
        """
        if self.enable_get_by_key:
            self.build_get_by_key()

        if self.enable_default_search:
            self.set_dynamic_model_search()

    def build_get_by_key(self):
        key_name = self.store.key
        model_name = self.model.__name__

        async def get_by_key(
            key: str = Path(
                ..., alias=key_name, title=f"The {key_name} of the {model_name} to get"
            ),
            fields: STORE_PARAMS = Depends(
                SparseFieldsQuery(self.model, [self.store.key]).query
            ),
        ):
            f"""
            Get's a document by the primary key in the store

            Args:
                {key_name}: the id of a single {model_name}

            Returns:
                a single {model_name} document
            """
            self.store.connect()

            item = self.store.query_one(
                criteria={self.store.key: key, **self.query},
                properties=fields["properties"],
            )

            if item is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Item with {self.store.key} = {key} not found",
                )

            response = {"data": [item]}
            return response

        self.router.get(
            f"/{{{key_name}}}/",
            response_description=f"Get an {model_name} by {key_name}",
            response_model=self.response_model,
            response_model_exclude_unset=True,
            tags=self.tags,
        )(get_by_key)

    def set_dynamic_model_search(self):

        model_name = self.model.__name__

        async def search(**queries: STORE_PARAMS):
            self.store.connect()

            query: Dict[Any, Any] = merge_queries(list(queries.values()))
            query["criteria"].update(self.query)

            count_query = query["criteria"]
            count = self.store.count(count_query)
            data = list(self.store.query(**query))
            meta = Meta(total=count)
            response = {"data": data, "meta": meta.dict()}
            return response

        attach_signature(
            search,
            annotations={
                f"dep{i}": STORE_PARAMS for i, _ in enumerate(self.query_operators)
            },
            defaults={
                f"dep{i}": Depends(dep.query)
                for i, dep in enumerate(self.query_operators)
            },
        )

        self.router.get(
            "/",
            tags=self.tags,
            summary=f"Get {model_name} documents",
            response_model=self.response_model,
            response_description=f"Search for a {model_name}",
            response_model_exclude_unset=True,
        )(search)
