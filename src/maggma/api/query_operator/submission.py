from typing import Optional
from maggma.api.query_operator import QueryOperator
from maggma.api.utils import STORE_PARAMS
from fastapi import Query
from datetime import datetime


class SubmissionQuery(QueryOperator):
    """
    Method to generate a query for submission data using status and datetime
    """

    def __init__(self, status_enum):
        def query(
            state: Optional[status_enum] = Query(
                None, description="Latest status of the submission"
            ),
            last_updated: Optional[datetime] = Query(
                None, description="Minimum datetime of status update for submission",
            ),
        ) -> STORE_PARAMS:

            crit = {}  # type: dict

            if state:
                crit.update(
                    {"$expr": {"$eq": [{"$arrayElemAt": ["$state", -1]}, state.value]}}  # type: ignore
                )

            if last_updated:
                crit.update(
                    {
                        "$expr": {
                            "$gt": [{"$arrayElemAt": ["$updated", -1]}, last_updated]
                        }
                    }
                )

            return {"criteria": crit}

        self.query = query

    def query(self):
        " Stub query function for abstract class "
        pass
