from typing import Optional

from fastapi import Query

from maggma.api.query_operator import QueryOperator
from maggma.api.utils import STORE_PARAMS


class SortQuery(QueryOperator):
    """
    Method to generate the sorting portion of a query
    """

    def query(
        self,
        _sort_fields: Optional[str] = Query(
            None,
            description="Comma delimited fields to sort with.\
 Prefixing '-' to a field will force a sort in descending order.",
        ),
    ) -> STORE_PARAMS:
        sort = {}

        if _sort_fields:
            for sort_field in _sort_fields.split(","):
                if sort_field[0] == "-":
                    sort.update({sort_field[1:]: -1})
                else:
                    sort.update({sort_field: 1})

        return {"sort": sort}
