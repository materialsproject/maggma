from abc import ABCMeta, abstractmethod
from typing import Dict, List, Optional, Union

from fastapi import APIRouter, FastAPI
from monty.json import MontyDecoder, MSONable
from pydantic import BaseModel
from starlette.responses import RedirectResponse

from maggma.api.models import Response
from maggma.api.query_operator import QueryOperator
from maggma.api.utils import STORE_PARAMS, attach_signature, merge_queries
from maggma.core import Store
from maggma.utils import dynamic_import


class Resource(MSONable, metaclass=ABCMeta):
    """
    Base class for a REST Compatible Resource
    """

    def __init__(
        self,
        store: Store,
        model: Union[BaseModel, str] = None,
        tags: Optional[List[str]] = None,
        query_operators: Optional[List[QueryOperator]] = None,
    ):
        """
        Args:
            store: The Maggma Store to get data from
            model: the pydantic model to apply to the documents from the Store
                This can be a string with a full python path to a model or
                an actual pydantic Model if this is being instantiated in python
                code. Serializing this via Monty will auto-convert the pydantic model
                into a python path string
            tags: list of tags for the Endpoint
            query_operators: operators for the query language
        """
        self.store = store
        self.tags = tags or []
        self.query_operators = query_operators or []

        if isinstance(model, type) and issubclass(model, BaseModel):
            self.model = model
        else:
            raise ValueError("The resource model has to be a PyDantic Model")

        self.router = APIRouter()
        self.response_model = Response[self.model]  # type: ignore
        self.setup_redirect()
        self.prepare_endpoint()

    @abstractmethod
    def prepare_endpoint(self):
        """
        Internal method to prepare the endpoint by setting up default handlers
        for routes
        """
        pass

    def setup_redirect(self):
        @self.router.get("", include_in_schema=False)
        def redirect_unslashes():
            """
            Redirects unforward slashed url to resource
            url with the forward slash
            """

            url = self.router.url_path_for("/")
            return RedirectResponse(url=url, status_code=301)

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

    @classmethod
    def from_dict(cls, d):

        if isinstance(d["model"], str):
            d["model"] = dynamic_import(d["model"])

        return cls(**MontyDecoder().process_decoded(d))
