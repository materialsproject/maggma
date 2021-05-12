from maggma.api.query_operator import (
    PaginationQuery,
    SparseFieldsQuery,
    NumericQuery,
    StringQueryOperator,
)
from datetime import datetime
import pytest

from pydantic import BaseModel, Field
from fastapi import HTTPException


from monty.serialization import loadfn, dumpfn
from monty.tempfile import ScratchDir


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
        assert new_op.query() == {
            "properties": ["name", "age", "weight", "last_updated"]
        }


def test_numeric_query_functionality():

    op = NumericQuery(model=Owner)

    assert op.meta() == {}
    assert op.query(age_lt=10) == {"criteria": {"age": {"$lt": 10}}}


def test_numeric_query_serialization():

    op = NumericQuery(model=Owner)

    with ScratchDir("."):
        dumpfn(op, "temp.json")
        new_op = loadfn("temp.json")
        assert new_op.query(age_lt=10) == {"criteria": {"age": {"$lt": 10}}}
