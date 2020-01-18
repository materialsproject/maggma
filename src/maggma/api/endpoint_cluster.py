import pathlib
import copy
from inspect import isclass
from typing import List, Dict, Union, Optional, Set
from pydantic import BaseModel
from monty.json import MSONable
from monty.serialization import loadfn
from fastapi import FastAPI, APIRouter, Path, HTTPException, Depends
from maggma.core import Store
from maggma.utils import dynamic_import
import ast

default_responses = loadfn(pathlib.Path(__file__).parent / "default_responses.yaml")


class CommonParams:
    def __init__(self, projection: str = None, skip: int = 0, limit: int = 10, all_include:bool=True):
        self.skip = skip
        self.limit = limit
        self.all_includes = all_include

        ## TODO this part is buggie
        try:
            if projection is None:
                self.projection = set()
            else:
                self.projection = set(ast.literal_eval(ast.literal_eval(projection))) ## idk why it is like this
        except:
            raise Exception("Cannot parse projection field")

        if self.all_includes and self.projection != set():
            raise Exception("projection and all_includes does not match")




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
            default_projection: Optional[Set[str]] = None,
            # TODO also do checking here to make sure that the fields passed in are actually in the model
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

        self.store = store
        self.router = APIRouter()
        self.tags = tags
        self.responses = responses
        try:
            model_fields = set(self.model.__dict__["__fields__"].keys())

            self.default_projection = model_fields if default_projection is None else default_projection
            if not self.default_projection.issubset(model_fields):
                raise Exception("default projection contains some fields that are not in the model fields")
        except:
            raise Exception("Cannot set default_filter")

        self.prepare_endpoint()

        self.router.get("/",
                        response_description="Default endpoint root, listing possible Paths")(self.root)

        self.router.get("/search",
                        response_description="Default generic search endpoint",
                        response_model=List[self.model],
                        tags=tags,
                        responses=responses,
                        )(
            self.generic_search)

    async def root(self, commonParams: CommonParams = Depends()) -> List[str]:
        """
        Args:
            commonParams: default paging requirements
        Return:
            a list of child endpoints
        """
        # Per discussion on Stackoverflow[https://stackoverflow.com/questions/2894723/what-are-the-best-practices-for
        # -the-root-page-of-a-rest-api] and example from github[https://api.github.com/], it seems like the root
        # should return a list of child endpoints. In this case, i think we should display a set of supported paths

        projection, skip, limit, all_includes = commonParams.projection, commonParams.skip, commonParams.limit, commonParams.all_includes

        result = [route.path for route in self.router.routes]
        return result[skip:skip + limit]

    async def generic_search(self, query: str, commonParams: CommonParams = Depends()):
        """
        Sample generic search, need to build a query language, but this query is used for testing purpose:
        '{"age":12}'
        '{"weight":150}'
        Example:

        http://127.0.0.1:8000/search?query=%27%7B%22weight%22%3A150%7D%27&limit=10&all_include=false
        http://127.0.0.1:8000/search?query=%27%7B%22weight%22%3A150%7D%27&projection=%27%5B%22name%22%2C%22age%22%5D%27&limit=10&all_include=false
        http://127.0.0.1:8000/search?query=%27%7B%22weight%22%3A150%7D%27&limit=10&all_include=true
        Args:
            query: input query
            commonParams: default paging requirements
        Return:
            A list of items that matches the input query

        """
        projection, skip, limit, all_includes = commonParams.projection, commonParams.skip, commonParams.limit, commonParams.all_includes
        try:
            query_dictionary = ast.literal_eval(ast.literal_eval(query)) ## idk why it is like this
        except:
            raise HTTPException(status_code=500, detail="Unable to parse query")

        result = []
        for key, value in query_dictionary.items():
            r = self.store.query(criteria={key:value})
            result.extend(list(r))
        result = [self.model(**r) for r in result][skip:skip+limit]
        if all_includes:
            return result[skip:skip+limit]
        elif projection:
            return [r.dict(include=projection) for r in result]
        else:
            return [r.dict(include=self.default_projection) for r in result]




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
                commonParams: CommonParams = Depends()
        ):
            f"""
            Get's a document by the primary key in the store

            Args:
                {key_name}: the id of a single

            Returns:
                a single document that satisfies the {model_name} model
            """
            projection, skip, limit, all_includes = commonParams.projection, commonParams.skip, commonParams.limit, commonParams.all_includes
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
