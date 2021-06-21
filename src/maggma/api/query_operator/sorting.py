from typing import Optional

from fastapi import HTTPException, Query

from maggma.api.query_operator import QueryOperator
from maggma.api.utils import STORE_PARAMS


class SortQuery(QueryOperator):
    """
    Method to generate the sorting portion of a query
    """

    def query(
        self,
        sort_field: Optional[str] = Query(None, description="Field to sort with"),
        ascending: Optional[bool] = Query(None, description="Whether the sorting should be ascending"),
    ) -> STORE_PARAMS:

        sort = {}

        if sort_field:
            if ascending is not None:
                sort.update({sort_field: 1 if ascending else -1})
            else:
                raise HTTPException(
                    status_code=400, detail="Must specify both a field and order for sorting.",
                )

        return {"sort": sort}
