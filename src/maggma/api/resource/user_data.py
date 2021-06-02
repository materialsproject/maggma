from typing import List, Optional, Type
from inspect import signature
from datetime import datetime
from uuid import uuid4

from fastapi import HTTPException, Path, Request

from maggma.api.models import (
    Response,
    UserSubmissionDataModel,
    UserSubmissionDataStatus,
    Meta,
)
from maggma.api.query_operator import QueryOperator, UserSubmissionQuery

from maggma.api.resource import Resource
from maggma.api.resource.utils import attach_query_ops
from maggma.api.utils import (
    STORE_PARAMS,
    merge_queries,
)
from maggma.core import Store


class UserSubmissionResource(Resource):
    """
    Implements a REST Compatible Resource as POST and/or GET URL endpoints
    for user submitted data.
    """

    def __init__(
        self,
        store: Store,
        model: Type[UserSubmissionDataModel],
        tags: Optional[List[str]] = None,
        post_query_operators: Optional[List[QueryOperator]] = None,
        get_query_operators: Optional[List[QueryOperator]] = None,
        include_in_schema: Optional[bool] = True,
        duplicate_fields_check: Optional[List[str]] = None,
        enable_default_search: bool = True,
        get_path: Optional[str] = "/",
        post_path: Optional[str] = "/",
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
            get_path: GET URL path for the resource.
            post_path: POST URL path for the resource.
        """

        self.store = store
        self.tags = tags or []
        self.post_query_operators = post_query_operators
        self.get_query_operators = [
            op for op in get_query_operators if op is not None  # type: ignore
        ] + [UserSubmissionQuery()]
        self.include_in_schema = include_in_schema
        self.duplicate_fields_check = duplicate_fields_check
        self.enable_default_search = enable_default_search
        self.get_path = get_path
        self.post_path = post_path
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

        async def get_by_key(
            key: str = Path(
                ...,
                alias="submission_id",
                title=f"The submission ID of the {model_name} to get",
            ),
        ):
            f"""
            Get a document using the submission ID

            Args:
                submission_id: the submission ID of a single {model_name}
                status_update: the status update for the retrieved {model_name}

            Returns:
                a single {model_name} document
            """

            self.store.connect()

            crit = {"submission_id": key}

            item = [self.store.query_one(criteria=crit)]

            if item == [None]:
                raise HTTPException(
                    status_code=404,
                    detail=f"Item with submission ID = {key} not found",
                )

            for operator in self.get_query_operators:  # type: ignore
                item = operator.post_process(item)

            response = {"data": item}

            return response

        self.router.get(
            f"{self.get_path}{{submission_id}}/",
            response_description=f"Get an {model_name} by submission ID",
            response_model=self.response_model,
            response_model_exclude_unset=False,
            tags=self.tags,
            include_in_schema=self.include_in_schema,
        )(get_by_key)

    def build_search_data(self):

        model_name = self.model.__name__

        async def search(**queries: STORE_PARAMS):

            request: Request = queries.pop("request")  # type: ignore

            query: STORE_PARAMS = merge_queries(list(queries.values()))

            query_params = [
                entry
                for _, i in enumerate(self.get_query_operators)
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
                data = operator.post_process(data)

            response = {"data": data, "meta": meta.dict()}

            return response

        self.router.get(
            self.get_path,
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

            query["criteria"]["submission_id"] = str(uuid4())
            query["criteria"]["status"] = [UserSubmissionDataStatus.submitted.value]
            query["criteria"]["updated"] = [datetime.utcnow()]

            print(query)

            try:
                self.store.update(docs=query["criteria"])  # type: ignore
            except Exception:
                raise HTTPException(
                    status_code=400, detail="Problem when trying to post data.",
                )

            response = {
                "data": query["criteria"],
                "meta": "Submission successful!",
            }

            return response

        self.router.post(
            self.post_path,
            tags=self.tags,
            summary=f"Post {model_name} data",
            response_model=None,
            response_description=f"Post {model_name} data",
            response_model_exclude_unset=True,
            include_in_schema=self.include_in_schema,
        )(attach_query_ops(post_data, self.post_query_operators))
