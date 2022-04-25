import inspect
import sys
from typing import Any, Callable, Dict, List, Optional, Type
from bson.objectid import ObjectId
import base64

from monty.json import MSONable
from pydantic import BaseModel
from pydantic.schema import get_flat_models_from_model
from pydantic.utils import lenient_issubclass
from typing_extensions import Literal

if sys.version_info >= (3, 8):
    from typing import get_args
else:
    from typing_extensions import get_args  # pragma: no cover


QUERY_PARAMS = ["criteria", "properties", "skip", "limit"]
STORE_PARAMS = Dict[
    Literal["criteria", "properties", "sort", "skip", "limit", "request", "pipeline", "hint"], Any,
]


def merge_queries(queries: List[STORE_PARAMS]) -> STORE_PARAMS:

    criteria: STORE_PARAMS = {}
    properties: List[str] = []

    for sub_query in queries:
        if "criteria" in sub_query:
            criteria.update(sub_query["criteria"])
        if "properties" in sub_query:
            properties.extend(sub_query["properties"])

    remainder = {k: v for query in queries for k, v in query.items() if k not in ["criteria", "properties"]}

    return {
        "criteria": criteria,
        "properties": properties if len(properties) > 0 else None,
        **remainder,
    }


def attach_signature(function: Callable, defaults: Dict, annotations: Dict):
    """
    Attaches signature for defaults and annotations for parameters to function

    Args:
        function: callable function to attach the signature to
        defaults: dictionary of parameters -> default values
        annotations: dictionary of type annoations for the parameters
    """

    required_params = [
        inspect.Parameter(
            param,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=defaults.get(param, None),
            annotation=annotations.get(param, None),
        )
        for param in annotations.keys()
        if param not in defaults.keys()
    ]

    optional_params = [
        inspect.Parameter(
            param,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=defaults.get(param, None),
            annotation=annotations.get(param, None),
        )
        for param in defaults.keys()
    ]

    setattr(function, "__signature__", inspect.Signature(required_params + optional_params))


def api_sanitize(
    pydantic_model: Type[BaseModel], fields_to_leave: Optional[List[str]] = None, allow_dict_msonable=False,
):
    """
    Function to clean up pydantic models for the API by:
        1.) Making fields optional
        2.) Allowing dictionaries in-place of the objects for MSONable quantities

    WARNING: This works in place, so it mutates the model and all sub-models

    Args:
        fields_to_leave: list of strings for model fields as "model__name__.field"
    """

    models = [
        model for model in get_flat_models_from_model(pydantic_model) if issubclass(model, BaseModel)
    ]  # type: List[Type[BaseModel]]

    fields_to_leave = fields_to_leave or []
    fields_tuples = [f.split(".") for f in fields_to_leave]
    assert all(len(f) == 2 for f in fields_tuples)

    for model in models:
        model_fields_to_leave = {f[1] for f in fields_tuples if model.__name__ == f[0]}
        for name, field in model.__fields__.items():
            field_type = field.type_

            if name not in model_fields_to_leave:
                field.required = False
                field.field_info.default = None

            if field_type is not None and allow_dict_msonable:
                if lenient_issubclass(field_type, MSONable):
                    field.type_ = allow_msonable_dict(field_type)
                else:
                    for sub_type in get_args(field_type):
                        if lenient_issubclass(sub_type, MSONable):
                            allow_msonable_dict(sub_type)
                field.populate_validators()

    return pydantic_model


def allow_msonable_dict(monty_cls: Type[MSONable]):
    """
    Patch Monty to allow for dict values for MSONable
    """

    def validate_monty(cls, v):
        """
        Stub validator for MSONable as a dictionary only
        """
        if isinstance(v, cls):
            return v
        elif isinstance(v, dict):
            # Just validate the simple Monty Dict Model
            errors = []
            if v.get("@module", "") != monty_cls.__module__:
                errors.append("@module")

            if v.get("@class", "") != monty_cls.__name__:
                errors.append("@class")

            if len(errors) > 0:
                raise ValueError("Missing Monty seriailzation fields in dictionary: {errors}")

            return v
        else:
            raise ValueError(f"Must provide {cls.__name__} or MSONable dictionary")

    setattr(monty_cls, "validate_monty", classmethod(validate_monty))

    return monty_cls


def serialization_helper(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode("utf-8")
    raise TypeError
