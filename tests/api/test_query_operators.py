from maggma.api.query_operator import (
    PaginationQuery,
    SparseFieldsQuery,
    NumericQuery,
    StringQueryOperator,
    SortQuery,
    VersionQuery,
    version,
)
from datetime import datetime
import pytest

from pydantic import BaseModel, Field
from fastapi import HTTPException, Query


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
    assert op.query(age_max=10, age_min=1, age_not_eq=[2, 3], weight_min=120) == {
        "criteria": {
            "age": {"$lte": 10, "$gte": 1, "$ne": [2, 3]},
            "weight": {"$gte": 120},
        }
    }


def test_numeric_query_serialization():

    op = NumericQuery(model=Owner)

    with ScratchDir("."):
        dumpfn(op, "temp.json")
        new_op = loadfn("temp.json")
        assert new_op.query(age_max=10) == {"criteria": {"age": {"$lte": 10}}}


def test_sort_query_functionality():

    op = SortQuery()

    assert op.query(field="volume", ascending=True) == {"sort": {"volume": 1}}
    assert op.query(field="density", ascending=False) == {"sort": {"density": -1}}


def test_sort_serialization():

    op = SortQuery()

    with ScratchDir("."):
        dumpfn(op, "temp.json")
        new_op = loadfn("temp.json")
        assert new_op.query(field="volume", ascending=True) == {"sort": {"volume": 1}}


def test_version_query_functionality():

    op = VersionQuery(default_version="1111_11_11")

    assert op.query()["criteria"]["version"].default == "1111_11_11"
    assert op.query(version="2222_22_22") == {"criteria": {"version": "2222_22_22"}}


def test_version_serialization():

    op = VersionQuery(default_version="1111_11_11")

    with ScratchDir("."):
        dumpfn(op, "temp.json")
        new_op = loadfn("temp.json")
        assert new_op.query(version="2222_22_22") == {
            "criteria": {"version": "2222_22_22"}
        }
