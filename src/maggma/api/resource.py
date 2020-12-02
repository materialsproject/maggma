from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path
from monty.json import MSONable
from pydantic import BaseModel

from maggma.api.models import Meta, Response
from maggma.api.query_operator import (
    DefaultDynamicQuery,
    PaginationQuery,
    QueryOperator,
    SparseFieldsQuery,
)
from maggma.api.util import (
    STORE_PARAMS,
    attach_signature,
    dynamic_import,
    merge_queries,
)
from maggma.core import Store


class Resource(MSONable):
    """
    Implements a REST Compatible Resource as a URL endpoint
    This class provides a number of convenience features
    including full pagination, field projection, and the
    MAPI query lanaugage
    """

    def __init__(
        self,
        store: Store,
        model: Union[BaseModel, str],
        tags: Optional[List[str]] = None,
        query_operators: Optional[List[QueryOperator]] = None,
        description: str = None,
    ):
        """
        Args:
            store: The Maggma Store to get data from
            model: the pydantic model to apply to the documents from the Store
                This can be a string with a full python path to a model or
                an actuall pydantic Model if this is being instantied in python
                code. Serializing this via Monty will autoconvert the pydantic model
                into a python path string
            tags: list of tags for the Endpoint
            query_operators: operators for the query language
            description: an explanation of wht does this resource do
        """
        self.store = store
        self.tags = tags or []
        self.description = description
        self.model: BaseModel = BaseModel()
        if isinstance(model, str):
            module_path = ".".join(model.split(".")[:-1])
            class_name = model.split(".")[-1]
            self.model = dynamic_import(module_path, class_name)
        else:
            self.model = model

        self.query_operators = (
            query_operators
            if query_operators is not None
            else [
                PaginationQuery(),
                SparseFieldsQuery(self.model, default_fields=[self.store.key]),
                DefaultDynamicQuery(self.model),
            ]
        )

        self.response_model = Response[self.model]  # type: ignore
        self.router = APIRouter()
        self.prepare_endpoint()

    def prepare_endpoint(self):
        """
        Internal method to prepare the endpoint by setting up default handlers
        for routes
        """
        self.build_get_by_key()
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
                criteria={self.store.key: key}, properties=fields["properties"]
            )

            if item is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Item with {self.store.key} = {key} not found",
                )

            response = {"data": [item]}  # , "meta": Meta()}
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

    def run(self):  # pragma: no cover
        """
        Runs the Endpoint cluster locally
        This is intended for testing not production
        """
        import uvicorn

        app = FastAPI()
        app.include_router(self.router, prefix="")
        uvicorn.run(app)

    def as_dict(self) -> Dict:
        """
        Special as_dict implemented to convert pydantic models into strings
        """

        d = super().as_dict()  # Ensures sub-classes serialize correctly
        d["model"] = f"{self.model.__module__}.{self.model.__name__}"  # type: ignore
        return d
