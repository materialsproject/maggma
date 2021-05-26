from typing import Optional
from maggma.api.models import UserSubmissionDataStatus
from maggma.api.query_operator import QueryOperator
from maggma.api.utils import STORE_PARAMS
from fastapi import Query
from datetime import datetime


class UserSubmissionQuery(QueryOperator):
    """
    Method to generate a query for user submission data using status and datetime
    """

    def query(
        self,
        status: Optional[UserSubmissionDataStatus] = Query(
            None, description="Latest status of the submission"
        ),
        last_updated: Optional[datetime] = Query(
            None, description="Minimum datetime of status update for submission",
        ),
    ) -> STORE_PARAMS:

        crit = {}  # type: dict

        if status:
            crit.update(
                {"$expr": {"$eq": [{"$arrayElemAt": ["$status", -1]}, status.value]}}
            )

        if last_updated:
            crit.update(
                {"$expr": {"$gt": [{"$arrayElemAt": ["$updated", -1]}, last_updated]}}
            )

        return {"criteria": crit}
