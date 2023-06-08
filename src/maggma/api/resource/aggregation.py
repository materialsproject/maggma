from typing import Any, Dict, List, Optional, Type

from fastapi import HTTPException, Response, Request
from pydantic import BaseModel
from pymongo import timeout as query_timeout
from pymongo.errors import NetworkTimeout, PyMongoError

from maggma.api.models import Meta
from maggma.api.models import Response as ResponseModel
from maggma.api.query_operator import QueryOperator
from maggma.api.resource import Resource, HeaderProcessor
from maggma.api.resource.utils import attach_query_ops
from maggma.api.utils import STORE_PARAMS, merge_queries
from maggma.core import Store


class AggregationResource(Resource):
    """
    Implements a REST Compatible Resource as a GET URL endpoint
    """

    def __init__(
        self,
        store: Store,
        model: Type[BaseModel],
        pipeline_query_operator: QueryOperator,
        timeout: Optional[int] = None,
        tags: Optional[List[str]] = None,
        include_in_schema: Optional[bool] = True,
        sub_path: Optional[str] = "/",
        header_processor: Optional[HeaderProcessor] = None,
    ):
        """
        Args:
            store: The Maggma Store to get data from
            model: The pydantic model this Resource represents
            tags: List of tags for the Endpoint
            pipeline_query_operator: Operator for the aggregation pipeline
            timeout: Time in seconds Pymongo should wait when querying MongoDB
                before raising a timeout error
            include_in_schema: Whether the endpoint should be shown in the documented schema.
            sub_path: sub-URL path for the resource.
        """
        self.store = store
        self.tags = tags or []

        self.include_in_schema = include_in_schema
        self.sub_path = sub_path
        self.response_model = ResponseModel[model]  # type: ignore

        self.pipeline_query_operator = pipeline_query_operator
        self.header_processor = header_processor
        self.timeout = timeout

        super().__init__(model)

    def prepare_endpoint(self):
        """
        Internal method to prepare the endpoint by setting up default handlers
        for routes
        """

        self.build_dynamic_model_search()

    def build_dynamic_model_search(self):

        model_name = self.model.__name__

        def search(**queries: Dict[str, STORE_PARAMS]) -> Dict:
            request: Request = queries.pop("request")  # type: ignore
            temp_response: Response = queries.pop("temp_response")  # type: ignore

            query: Dict[Any, Any] = merge_queries(list(queries.values()))  # type: ignore

            self.store.connect()

            try:
                with query_timeout(self.timeout):
                    data = list(self.store._collection.aggregate(query["pipeline"]))
            except (NetworkTimeout, PyMongoError) as e:
                if e.timeout:
                    raise HTTPException(
                        status_code=504,
                        detail="Server timed out trying to obtain data. Try again with a smaller request.",
                    )
                else:
                    raise HTTPException(
                        status_code=500,
                    )

            count = len(data)

            data = self.pipeline_query_operator.post_process(data, query)
            operator_meta = self.pipeline_query_operator.meta()

            meta = Meta(total_doc=count)
            response = {"data": data, "meta": {**meta.dict(), **operator_meta}}

            if self.header_processor is not None:
                self.header_processor.process_header(temp_response, request)

            return response

        self.router.get(
            self.sub_path,
            tags=self.tags,
            summary=f"Get {model_name} documents",
            response_model=self.response_model,
            response_description=f"Get {model_name} data",
            response_model_exclude_unset=True,
        )(attach_query_ops(search, [self.pipeline_query_operator]))
