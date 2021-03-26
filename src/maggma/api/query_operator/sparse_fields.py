import inspect
import warnings
from typing import Dict, List, Optional

from fastapi import Query
from pydantic import BaseModel

from maggma.api.util import STORE_PARAMS
from maggma.utils import dynamic_import
from maggma.api.query_operator import QueryOperator


class SparseFieldsQuery(QueryOperator):
    def __init__(self, model: BaseModel, default_fields: Optional[List[str]] = None):
        """
        Args:
            model: PyDantic Model that represents the underlying data source
            default_fields: default fields to return in the API response if no fields are explicitly requested
        """

        self.model = model

        model_fields = list(self.model.__fields__.keys())
        self.default_fields = (
            model_fields if default_fields is None else list(default_fields)
        )

        assert set(self.default_fields).issubset(
            model_fields
        ), "default projection contains some fields that are not in the model fields"

        default_fields_string = ",".join(self.default_fields)  # type: ignore

        def query(
            fields: str = Query(
                default_fields_string,
                description=f"Fields to project from {model.__name__} "  # type: ignore
                f"as a list of comma seperated strings",
            ),
            all_fields: bool = Query(False, description="Include all fields."),
        ) -> STORE_PARAMS:
            """
            Projection parameters for the API Endpoint
            """

            projection_fields: List[str] = [s.strip() for s in fields.split(",")]

            # need to strip to avoid input such as "name, weight" to be parsed into ["name", " weight"]
            # we need ["name", "weight"]
            if all_fields:
                projection_fields = model_fields
            return {"properties": projection_fields}

        self.query = query  # type: ignore

    def meta(self) -> Dict:
        """
        Returns metadata for the Sparse field set
        """
        return {"default_fields": self.default_fields}

    def as_dict(self) -> Dict:
        """
        Special as_dict implemented to convert pydantic models into strings
        """

        d = super().as_dict()  # Ensures sub-classes serialize correctly
        d["model"] = f"{self.model.__module__}.{self.model.__name__}"  # type: ignore
        return d

    @classmethod
    def from_dict(cls, d):
        """
        Special from_dict to autoload the pydantic model from the location string
        """
        model = d.get("model")
        if isinstance(model, str):
            model = dynamic_import(model)

        assert issubclass(
            model, BaseModel
        ), "The resource model has to be a PyDantic Model"
        d["model"] = model

        cls(**d)
