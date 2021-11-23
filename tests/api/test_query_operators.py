import pytest
from enum import Enum
from maggma.api.query_operator import (
    NumericQuery,
    PaginationQuery,
    SparseFieldsQuery,
    NumericQuery,
    SortQuery,
)

from pydantic import BaseModel, Field
from fastapi import HTTPException, Query
from datetime import datetime

from monty.serialization import loadfn, dumpfn
from monty.tempfile import ScratchDir

from maggma.api.query_operator.submission import SubmissionQuery


class Owner(BaseModel):
    name: str = Field(..., title="Owner's name")
    age: int = Field(None, title="Owne'r Age")
    weight: float = Field(None, title="Owner's weight")
    last_updated: datetime = Field(None, title="Last updated date for this record")


def test_pagination_functionality():

    op = PaginationQuery()

    assert op.query(skip=10, limit=20) == {"limit": 20, "skip": 10}

    with pytest.raises(HTTPException):
        op.query(limit=10000)


def test_pagination_serialization():

    op = PaginationQuery()

    with ScratchDir("."):
        dumpfn(op, "temp.json")
        new_op = loadfn("temp.json")
        assert new_op.query(skip=10, limit=20) == {"limit": 20, "skip": 10}


def test_sparse_query_functionality():

    op = SparseFieldsQuery(model=Owner)

    assert op.meta()["default_fields"] == ["name", "age", "weight", "last_updated"]
    assert op.query() == {"properties": ["name", "age", "weight", "last_updated"]}


def test_sparse_query_serialization():

    op = SparseFieldsQuery(model=Owner)

    with ScratchDir("."):
        dumpfn(op, "temp.json")
        new_op = loadfn("temp.json")
        assert new_op.query() == {"properties": ["name", "age", "weight", "last_updated"]}


def test_numeric_query_functionality():

    op = NumericQuery(model=Owner)

    assert op.meta() == {}
    assert op.query(age_max=10, age_min=1, age_not_eq=[2, 3], weight_min=120) == {
        "criteria": {"age": {"$lte": 10, "$gte": 1, "$ne": [2, 3]}, "weight": {"$gte": 120}}
    }


def test_numeric_query_serialization():

    op = NumericQuery(model=Owner)

    with ScratchDir("."):
        dumpfn(op, "temp.json")
        new_op = loadfn("temp.json")
        assert new_op.query(age_max=10) == {"criteria": {"age": {"$lte": 10}}}


def test_sort_query_functionality():

    op = SortQuery()

    assert op.query(sort_fields="volume,-density") == {"sort": {"volume": 1, "density": -1}}


def test_sort_serialization():

    op = SortQuery()

    with ScratchDir("."):
        dumpfn(op, "temp.json")
        new_op = loadfn("temp.json")
        assert new_op.query(sort_fields="volume,-density") == {"sort": {"volume": 1, "density": -1}}


@pytest.fixture
def status_enum():
    class StatusEnum(Enum):
        state_A = "A"
        state_B = "B"

    return StatusEnum


def test_submission_functionality(status_enum):

    op = SubmissionQuery(status_enum)
    dt = datetime.utcnow()

    assert op.query(state=status_enum.state_A, last_updated=dt) == {
        "criteria": {
            "$and": [
                {"$expr": {"$eq": [{"$arrayElemAt": ["$state", -1]}, "A"]}},
                {"$expr": {"$gt": [{"$arrayElemAt": ["$last_updated", -1]}, dt]}},
            ]
        }
    }
