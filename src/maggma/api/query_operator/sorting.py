from typing import Optional
from maggma.api.query_operator import QueryOperator
from maggma.api.utils import STORE_PARAMS
from fastapi import HTTPException, Query


class SortQuery(QueryOperator):
    """
    Method to generate the sorting portion of a query
    """

    def query(
        self,
        field: Optional[str] = Query(None, description="Field to sort with"),
        ascending: Optional[bool] = Query(None, description="Whether the sorting should be ascending",),
    ) -> STORE_PARAMS:

        sort = {}

        if field and ascending is not None:
            sort.update({field: 1 if ascending else -1})

        elif field or ascending is not None:
            raise HTTPException(
                status_code=400, detail="Must specify both a field and order for sorting.",
            )

        return {"sort": sort}
