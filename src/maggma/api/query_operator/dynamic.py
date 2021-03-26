import inspect
import warnings
from typing import Any, Dict, List, Mapping, Optional

from fastapi import Query
from monty.json import MSONable
from pydantic import BaseModel
from pydantic.fields import ModelField

from maggma.api.util import STORE_PARAMS
from maggma.core import Store
from maggma.utils import dynamic_import
from maggma.api.query_operator import QueryOperator


class DefaultDynamicQuery(QueryOperator):
    def __init__(
        self,
        model: BaseModel,
        additional_signature_fields: Mapping[str, List] = None,
    ):
        """
        This function will take query, parse it, and output the mongo criteria.

        About the query:
        The format of the input query will come from two sources: the data model (which we will sometimes refer to as
        default values, and the additional paremeters that users can choose to pass in)
        additional_signature_field must be int the shape of NAME_OF_THE_QUERY -> [DEFAULT_VALUE, QUERY]
        where QUERY is a FastAPI params.Query object

        About how does this script parse the query:
        it will first generate default queries from input data model
        Then it will merge with the optional additional_signature_fields

        About mongo criteria output:
        current implementation only supports 6 operations, namely equal, not equal, less than, greater than, in,
        and not in. Therefore, the criteria output will be also limited to those operations.


        Args:
            model: PyDantic Model to base the query language on
            additional_signature_fields: mapping of NAME_OF_THE_FIELD -> [DEFAULT_VALUE, QUERY]
        """
        self.model = model
        default_mapping = {
            "eq": "$eq",
            "not_eq": "$ne",
            "lt": "$lt",
            "gt": "$gt",
            "in": "$in",
            "not_in": "$nin",
        }
        mapping: dict = default_mapping

        self.additional_signature_fields: Mapping[str, List] = (
            dict()
            if additional_signature_fields is None
            else additional_signature_fields
        )

        # construct fields
        # find all fields in data_object
        all_fields: Dict[str, ModelField] = model.__fields__

        # turn fields into operators, also do type checking
        params = self.fields_to_operator(all_fields)

        # combine with additional_fields
        # user's input always have higher priority than the the default data model's
        params.update(self.additional_signature_fields)

        def query(**kwargs) -> STORE_PARAMS:
            crit = dict()
            for k, v in kwargs.items():
                if v is not None:
                    if "_" not in k:
                        k = k + "_eq"
                    name, operator = k.split("_", 1)
                    try:
                        crit[name] = {mapping[operator]: v}
                    except KeyError:
                        raise KeyError(
                            f"Cannot find key {k} in current query to database mapping"
                        )

            return {"criteria": crit}

        # building the signatures for FastAPI Swagger UI
        signatures: List[Any] = []
        signatures.extend(
            inspect.Parameter(
                param,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=query[1],
                annotation=query[0],
            )
            for param, query in params.items()
        )
        setattr(query, "__signature__", inspect.Signature(signatures))

        self.query = query  # type: ignore

    def fields_to_operator(self, all_fields: Dict[str, ModelField]) -> Dict[str, list]:
        """
        Getting a list of tuple of fields

        turn them into a mapping of

        FIELD_[operator] -> query


        Args:
            all_fields: dictionary of str -> ModelField

        Returns:
            a dictionary of FIELD_[operator] -> query
        """
        params = dict()
        for name, model_field in all_fields.items():
            if model_field.type_ in [str, int, float]:
                t: Any = model_field.type_
                name = model_field.name
                params[f"{name}"] = [
                    t,
                    Query(
                        default=model_field.default,
                        description=f"Querying if {model_field.name} is equal to another",
                    ),
                ]  # supporting both with and with out explicit _eq
                params[f"{name}_eq"] = [
                    t,
                    Query(
                        default=model_field.default,
                        description=f"Querying if {model_field.name} is equal to another",
                    ),
                ]
                params[f"{name}_not_eq"] = [
                    t,
                    Query(
                        default=model_field.default,
                        description=f"Querying if {model_field.name} is not equal to another",
                    ),
                ]
                params[f"{name}_in"] = [
                    List[t],
                    Query(
                        default=model_field.default,
                        description=f"Querying if item is in {model_field.name}",
                    ),
                ]
                params[f"{name}_not_in"] = [
                    List[t],
                    Query(
                        default=model_field.default,
                        description=f"Querying if item is not in {model_field.name} ",
                    ),
                ]

                if model_field.type_ == int or model_field == float:
                    params[f"{name}_lt"] = [
                        model_field.type_,
                        Query(
                            model_field.default,
                            description=f"Querying if {model_field.name} is less than to another",
                        ),
                    ]
                    params[f"{name}_gt"] = [
                        model_field.type_,
                        Query(
                            model_field.default,
                            description=f"Querying if {model_field.name} is greater than to another",
                        ),
                    ]
            else:
                warnings.warn(
                    f"Field name {name} with {model_field.type_} not implemented"
                )
        return params
