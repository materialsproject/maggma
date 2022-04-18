from datetime import datetime
from enum import Enum
from inspect import signature
from typing import Any, List, Optional, Type
from uuid import uuid4

from fastapi import HTTPException, Path, Request
from pydantic import BaseModel, Field, create_model

from maggma.api.models import Meta, Response
from maggma.api.query_operator import QueryOperator, SubmissionQuery
from maggma.api.resource import Resource
from maggma.api.resource.utils import attach_query_ops
from maggma.api.utils import STORE_PARAMS, merge_queries
from maggma.core import Store


class SubmissionResource(Resource):
    """
    Implements a REST Compatible Resource as POST and/or GET URL endpoints
    for submitted data.
    """

    def __init__(
        self,
        store: Store,
        model: Type[BaseModel],
        post_query_operators: List[QueryOperator],
        get_query_operators: List[QueryOperator],
        tags: Optional[List[str]] = None,
        include_in_schema: Optional[bool] = True,
        duplicate_fields_check: Optional[List[str]] = None,
        enable_default_search: Optional[bool] = True,
        state_enum: Optional[Enum] = None,
        default_state: Optional[Any] = None,
        calculate_submission_id: Optional[bool] = False,
        get_sub_path: Optional[str] = "/",
        post_sub_path: Optional[str] = "/",
    ):
        """
        Args:
            store: The Maggma Store to get data from
            model: The pydantic model this resource represents
            tags: List of tags for the Endpoint
            post_query_operators: Operators for the query language for post data
            get_query_operators: Operators for the query language for get data
            include_in_schema: Whether to include the submission resource in the documented schema
            duplicate_fields_check: Fields in model used to check for duplicates for POST data
            enable_default_search: Enable default endpoint search behavior.
            state_enum: State Enum defining possible data states
            default_state: Default state value in provided state Enum
            calculate_submission_id: Whether to calculate and use a submission ID as primary data key.
                If False, the store key is used instead.
            get_sub_path: GET sub-URL path for the resource.
            post_sub_path: POST sub-URL path for the resource.
        """

        if isinstance(state_enum, Enum) and default_state not in [
            entry.value for entry in state_enum  # type: ignore
        ]:
            raise RuntimeError(
                "If data is stateful a state enum and valid default value must be provided"
            )

        self.state_enum = state_enum
        self.default_state = default_state
        self.store = store
        self.tags = tags or []
        self.post_query_operators = post_query_operators
        self.get_query_operators = (
            [op for op in get_query_operators if op is not None]  # type: ignore
            + [SubmissionQuery(state_enum)]
            if state_enum is not None
            else get_query_operators
        )
        self.include_in_schema = include_in_schema
        self.duplicate_fields_check = duplicate_fields_check
        self.enable_default_search = enable_default_search
        self.calculate_submission_id = calculate_submission_id
        self.get_sub_path = get_sub_path
        self.post_sub_path = post_sub_path

        new_fields = {}  # type: dict
        if self.calculate_submission_id:
            new_fields["submission_id"] = (
                str,
                Field(..., description="Unique submission ID"),
            )

        if state_enum is not None:
            new_fields["state"] = (
                List[state_enum],  # type: ignore
                Field(..., description="List of data status descriptions"),
            )

            new_fields["updated"] = (
                List[datetime],
                Field(..., description="List of status update datetimes"),
            )

        if new_fields:
            model = create_model(model.__name__, __base__=model, **new_fields)

        self.response_model = Response[model]  # type: ignore

        super().__init__(model)

    def prepare_endpoint(self):
        """
        Internal method to prepare the endpoint by setting up default handlers
        for routes
        """

        if self.enable_default_search:
            self.build_search_data()

        self.build_get_by_key()

        self.build_post_data()

    def build_get_by_key(self):
        model_name = self.model.__name__

        key_name = "submission_id" if self.calculate_submission_id else self.store.key

        async def get_by_key(
            key: str = Path(
                ...,
                alias=key_name,
                description=f"The {key_name} of the {model_name} to get",
            ),
        ):
            f"""
            Get a document using the {key_name}

            Args:
                {key_name}: the id of a single {model_name}

            Returns:
                a single {model_name} document
            """

            self.store.connect()

            crit = {key_name: key}

            item = [self.store.query_one(criteria=crit)]

            if item == [None]:
                raise HTTPException(
                    status_code=404,
                    detail=f"Item with submission ID = {key} not found",
                )

            for operator in self.get_query_operators:  # type: ignore
                item = operator.post_process(item, {})

            response = {"data": item}

            return response

        self.router.get(
            f"{self.get_sub_path}{{{key_name}}}/",
            response_description=f"Get an {model_name} by {key_name}",
            response_model=self.response_model,
            response_model_exclude_unset=True,
            tags=self.tags,
            include_in_schema=self.include_in_schema,
        )(get_by_key)

    def build_search_data(self):

        model_name = self.model.__name__

        async def search(**queries: STORE_PARAMS):

            request: Request = queries.pop("request")  # type: ignore
            queries.pop("temp_response")  # type: ignore

            query: STORE_PARAMS = merge_queries(list(queries.values()))

            query_params = [
                entry
                for _, i in enumerate(self.get_query_operators)  # type: ignore
                for entry in signature(i.query).parameters
            ]

            overlap = [
                key for key in request.query_params.keys() if key not in query_params
            ]
            if any(overlap):
                raise HTTPException(
                    status_code=404,
                    detail="Request contains query parameters which cannot be used: {}".format(
                        ", ".join(overlap)
                    ),
                )

            self.store.connect(force_reset=True)

            count = self.store.count(query["criteria"])
            data = list(self.store.query(**query))  # type: ignore
            meta = Meta(total_doc=count)

            for operator in self.get_query_operators:  # type: ignore
                data = operator.post_process(data, query)

            response = {"data": data, "meta": meta.dict()}

            return response

        self.router.get(
            self.get_sub_path,
            tags=self.tags,
            summary=f"Get {model_name} data",
            response_model=self.response_model,
            response_description="Search for {model_name} data",
            response_model_exclude_unset=True,
            include_in_schema=self.include_in_schema,
        )(attach_query_ops(search, self.get_query_operators))

    def build_post_data(self):
        model_name = self.model.__name__

        async def post_data(**queries: STORE_PARAMS):

            request: Request = queries.pop("request")  # type: ignore
            queries.pop("temp_response")  # type: ignore

            query: STORE_PARAMS = merge_queries(list(queries.values()))

            query_params = [
                entry
                for _, i in enumerate(self.post_query_operators)  # type: ignore
                for entry in signature(i.query).parameters
            ]

            overlap = [
                key for key in request.query_params.keys() if key not in query_params
            ]
            if any(overlap):
                raise HTTPException(
                    status_code=404,
                    detail="Request contains query parameters which cannot be used: {}".format(
                        ", ".join(overlap)
                    ),
                )

            self.store.connect(force_reset=True)

            # Check for duplicate entry
            if self.duplicate_fields_check:
                duplicate = self.store.query_one(
                    criteria={
                        field: query["criteria"][field]
                        for field in self.duplicate_fields_check
                    }
                )

                if duplicate:
                    raise HTTPException(
                        status_code=400,
                        detail="Submission already exists. Duplicate data found for fields: {}".format(
                            ", ".join(self.duplicate_fields_check)
                        ),
                    )

            if self.calculate_submission_id:
                query["criteria"]["submission_id"] = str(uuid4())

            if self.state_enum is not None:
                query["criteria"]["state"] = [self.default_state]
                query["criteria"]["updated"] = [datetime.utcnow()]

            try:
                self.store.update(docs=query["criteria"])  # type: ignore
            except Exception:
                raise HTTPException(
                    status_code=400, detail="Problem when trying to post data.",
                )

            response = {
                "data": query["criteria"],
                "meta": "Submission successful",
            }

            return response

        self.router.post(
            self.post_sub_path,
            tags=self.tags,
            summary=f"Post {model_name} data",
            response_model=None,
            response_description=f"Post {model_name} data",
            response_model_exclude_unset=True,
            include_in_schema=self.include_in_schema,
        )(attach_query_ops(post_data, self.post_query_operators))
