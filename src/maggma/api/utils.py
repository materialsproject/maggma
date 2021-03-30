import inspect
from copy import deepcopy
from importlib import import_module
from typing import Any, Callable, Dict, List, Optional, Type, Union

from monty.json import MSONable
from pydantic import BaseModel, create_model
from typing_extensions import Literal

QUERY_PARAMS = ["criteria", "properties", "skip", "limit"]
STORE_PARAMS = Dict[Literal["criteria", "properties", "skip", "limit"], Any]


def merge_queries(queries: List[STORE_PARAMS]) -> STORE_PARAMS:

    criteria: STORE_PARAMS = {}
    properties: List[str] = []

    for sub_query in queries:
        if "criteria" in sub_query:
            criteria.update(sub_query["criteria"])
        if "properties" in sub_query:
            properties.extend(sub_query["properties"])

    remainder = {
        k: v
        for query in queries
        for k, v in query.items()
        if k not in ["criteria", "properties"]
    }

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

    setattr(
        function, "__signature__", inspect.Signature(required_params + optional_params)
    )


def api_sanitize(
    pydantic_model: BaseModel,
    fields_to_leave: Optional[List[str]] = None,
    allow_dict_msonable=False,
):
    """
    Function to clean up pydantic models for the API by:
        1.) Making fields optional
        2.) Allowing dictionaries in-place of the objects for MSONable quantities
    """
    fields_to_leave = fields_to_leave or []
    fields_to_leave = set(tuple(field.split(".")) for field in fields_to_leave)

    props = {}
    for name, field in pydantic_model.__fields__.items():
        field_type = field.type_
        field_info = deepcopy(field.field_info)

        sub_fields_to_leave = [
            sub_field[1:] for sub_field in fields_to_leave if sub_field[0] == name
        ]
        sub_fields_to_leave = [
            sub_field for sub_field in sub_fields_to_leave if len(sub_field) > 0
        ]

        if field_type is not None:
            if issubclass(field_type, BaseModel):
                field_type = api_sanitize(
                    pydantic_model=field_type,
                    fields_to_leave=sub_fields_to_leave,
                    allow_dict_msonable=allow_dict_msonable,
                )
            elif issubclass(field_type, MSONable) and allow_dict_msonable:
                field_type = allow_msonable_dict(deepcopy(field_type))
            elif hasattr(field_type, "__pydantic_model__"):
                field_type.__pydantic_model__ = api_sanitize(
                    pydantic_model=deepcopy(field_type.__pydantic_model__),
                    fields_to_leave=sub_fields_to_leave,
                    allow_dict_msonable=allow_dict_msonable,
                )

        if name not in {fields[0] for fields in fields_to_leave}:
            field_info.default = None

        props[name] = (field_type, field_info)

    model = create_model(pydantic_model.__name__, **props)
    model.__doc__ = pydantic_model.__doc__

    return model


def allow_msonable_dict(monty_cls: Type[MSONable]):
    """
    Patch Monty to allow for dict values for MSONable
    """

    def __get_validators__(cls):
        yield cls.validate_monty

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
                raise ValueError(
                    "Missing Monty seriailzation fields in dictionary: {errors}"
                )

            return v
        else:
            raise ValueError(f"Must provide {cls.__name__} or MSONable dictionary")

    setattr(monty_cls, "validate_monty", classmethod(validate_monty))
    setattr(monty_cls, "__get_validators__", classmethod(__get_validators__))

    return monty_cls
