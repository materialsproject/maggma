import pathlib
import copy
from inspect import isclass
from typing import List, Dict, Union, Optional
from pydantic import BaseModel
from monty.json import MSONable
from monty.serialization import loadfn
from fastapi import FastAPI, APIRouter, Path, HTTPException, Depends
from maggma.core import Store
from maggma.utils import dynamic_import

default_responses = loadfn(pathlib.Path(__file__).parent / "default_responses.yaml")

class PaginationParams:
    def __init__(self, skip: int = 0, limit: int = 10):
        self.skip = skip
        self.limit = limit

class EndpointCluster(MSONable):
    """
    Implements an endpoint cluster which is a REST Compatible Resource as
    a URL endpoint
    """

    def __init__(
        self,
        store: Store,
        model: Union[BaseModel, str],
        tags: Optional[List[str]] = None,
        responses: Optional[Dict] = None,
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
            responses: default responses for error codes
        """
        self.store = store
        self.router = APIRouter()
        self.tags = tags
        self.responses = responses

        if isinstance(model, str):
            module_path = ".".join(model.split(".")[:-1])
            class_name = model.split(".")[-1]
            self.model = dynamic_import(module_path, class_name)
        elif isclass(model) and issubclass(model, BaseModel):  # type: ignore
            self.model = model
        else:
            raise ValueError(
                "Model has to be a pydantic model or python path to a pydantic model"
            )

        self.prepare_endpoint()

    def prepare_endpoint(self):
        """
        Internal method to prepare the endpoint by setting up default handlers
        for routes
        """
        key_name = self.store.key
        model_name = self.model.__name__
        responses = copy.copy(default_responses)
        if self.responses:
            responses.update(self.responses)

        tags = self.tags or []

        async def get_by_key(
            key: str = Path(..., title=f"The {key_name} of the {model_name} to get"),
        ):
            f"""
            Get's a document by the primary key in the store

            Args:
                {key_name}: the id of a single

            Returns:
                a single document that satisfies the {model_name} model
            """
            item = self.store.query_one(criteria={self.store.key: key})

            if item is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Item with {self.store.key} = {key} not found",
                )
            else:
                model_item = self.model(**item)
                return model_item

        self.router.get(
            f"/{key_name}/{{key}}",
            response_description=f"Get an {model_name} by {key_name}",
            response_model=self.model,
            tags=tags,
            responses=responses,
        )(get_by_key)

        async def root():
            return {"Result": "Root reached"}

        async def generic_search(
                query_param: str = Path(..., title="The generic search query that user requsted"),
                paginationParam: PaginationParams = Depends()):
            """
            Automatically re-route to the correct endpoint for that functionality
            Args:
                query_param: the user specified query

            Returns:
                document(s) that relates to that search
            """
            skip, limit = paginationParam.skip, paginationParam.limit
            return {"Result": "NOT IMPLEMENTED YET"}

        self.router.get("/",
                        response_description="Default endpoint root")(root)

        self.router.get("/{query_param}",
                        response_description="Default generic search endpoint")(generic_search)

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

        for field in ["tags", "responses"]:
            if not d.get(field, None):
                del d[field]
        return d
