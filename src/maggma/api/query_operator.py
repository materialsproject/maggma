import inspect
import warnings
from typing import Any, Dict, List, Mapping, Optional

from fastapi import Query
from monty.json import MSONable
from pydantic import BaseModel
from pydantic.fields import ModelField

from maggma.api.util import STORE_PARAMS, dynamic_import
from maggma.core import Store


class QueryOperator(MSONable):
    """
    Base Query Operator class for defining powerfull query language
    in the Materials API
    """

    def query(self) -> STORE_PARAMS:
        """
        The query function that does the work for this query operator
        """
        raise NotImplementedError("Query operators must implement query")

    def meta(self, store: Store, query: Dict) -> Dict:
        """
        Returns meta data to return with the Response

        Args:
            store: the Maggma Store that the resource uses
            query: the query being executed in this API call
        """
        return {}

    def post_process(self, doc: Dict) -> Dict:
        """
        An optional post-processing function for the data
        """
        return doc


class PaginationQuery(QueryOperator):
    """Query opertators to provides Pagination in the Materials API"""

    def __init__(
        self, default_skip: int = 0, default_limit: int = 10, max_limit: int = 100
    ):
        """
        Args:
            default_skip: the default number of documents to skip
            default_limit: the default number of documents to return
            max_limit: max number of documents to return
        """
        self.default_skip = default_skip
        self.default_limit = default_limit
        self.max_limit = max_limit

        def query(
            skip: int = Query(
                default_skip, description="Number of entries to skip in the search"
            ),
            limit: int = Query(
                default_limit,
                description="Max number of entries to return in a single query."
                f" Limited to {max_limit}",
            ),
        ) -> STORE_PARAMS:
            """
            Pagination parameters for the API Endpoint
            """
            if limit > max_limit:
                raise Exception(
                    "Requested more data per query than allowed by this endpoint."
                    f"The max limit is {max_limit} entries"
                )
            return {"skip": skip, "limit": limit}

        self.query = query  # type: ignore

    def meta(self, store: Store, query: Dict) -> Dict:
        """
        Metadata for the pagination params
        """
        return {"max_limit": self.max_limit}


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

    """
    Factory function to generate a dependency for sparse field sets in FastAPI
    """

    def meta(self, store: Store, query: Dict) -> Dict:
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
            module_path = ".".join(model.split(".")[:-1])
            class_name = model.split(".")[-1]
            model = dynamic_import(module_path, class_name)

        assert issubclass(
            model, BaseModel
        ), "The resource model has to be a PyDantic Model"
        d["model"] = model

        cls(**d)


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
