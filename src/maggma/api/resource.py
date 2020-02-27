from typing import List, Dict, Union, Optional
from pydantic import BaseModel
from monty.json import MSONable
from maggma.api.util import (
    dynamic_import,
    STORE_PARAMS,
    merge_queries,
    attach_signature,
)
from maggma.core import Store
from maggma.api.query_operator import (
    QueryOperator,
    PaginationQuery,
    SparseFieldsQuery,
    DefaultDynamicQuery,
)
from fastapi import FastAPI, APIRouter, Path, HTTPException, Depends
from maggma.api.models import Response, Meta
from starlette.responses import RedirectResponse


class Resource(MSONable):
    """
        Implements a REST Compatible Resource as a URL endpoint
        This class provides a number of convenience features
        including full pagination, field projection, and the
        MAPI query lanaugage

        - implements custom error handlers to provide MAPI Responses
        - implement standard metadata response for class
        - JSON Configuration
        """

    def __init__(
        self,
        store: Store,
        model: Union[BaseModel, str],
        tags: Optional[List[str]] = None,
        query_operators: Optional[List[QueryOperator]] = None,
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
        """
        self.store = store
        self.tags = tags or []

        if isinstance(model, str):
            module_path = ".".join(model.split(".")[:-1])
            class_name = model.split(".")[-1]
            self.model = dynamic_import(module_path, class_name)
            assert issubclass(
                self.model, BaseModel
            ), "The resource model has to be a PyDantic Model"
        else:
            self.model = model

        # print(self.store.key)

        self.query_operators = (
            query_operators
            if query_operators is not None
            else [
                PaginationQuery(),
                SparseFieldsQuery(self.model, default_fields=[self.store.key],),
                DefaultDynamicQuery(self.model),
            ]
        )

        self.router = APIRouter()
        self.response_model = Response[self.model]
        self.prepare_endpoint()

    def prepare_endpoint(self):
        """
        Internal method to prepare the endpoint by setting up default handlers
        for routes
        """
        self.set_root()
        # self.build_get_by_key()
        self.set_dynamic_model_search()

    def set_root(self):
        async def get_root():
            return RedirectResponse("/docs")

        self.router.get(
            "/", response_description="Root level of this resource", tags=self.tags,
        )(get_root)

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

            query = {self.store.key: key}

            elements = self.store.distinct("elements", query)
            item = self.store.query_one(
                criteria={self.store.key: key}, properties=fields["properties"]
            )

            if item is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Item with {self.store.key} = {key} not found",
                )

            response = Response(data=[item])
            response.meta.elements = elements

            return response.dict()

        self.router.get(
            f"/{{{key_name}}}",
            response_description=f"Get an {model_name} by {key_name}",
            response_model=self.response_model,
            response_model_exclude_unset=True,
            tags=self.tags,
        )(get_by_key)

    def set_dynamic_model_search(self):

        model_name = self.model.__name__

        async def search(**queries: STORE_PARAMS):
            self.store.connect()

            query = merge_queries(list(queries.values()))

            count_query = query["criteria"]
            count = self.store.count(count_query)
            elements = self.store.distinct("elements", count_query)
            data = [self.model(**d) for d in self.store.query(**query)]
            meta = Meta(total=count, elements=elements)
            response = Response[self.model](data=data, meta=meta.dict())
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
            "/search",
            tags=self.tags,
            summary=f"Get {model_name} documents",
            # response_model=self.response_model,
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
        d["model"] = f"{self.model.__module__}.{self.model.__name__}"
        return d
