import inspect
from datetime import datetime
from random import randint
from urllib.parse import urlencode

import pytest
from fastapi import FastAPI
from maggma.api.query_operator import NumericQuery, SparseFieldsQuery, StringQueryOperator
from maggma.api.resource import ReadOnlyResource
from maggma.api.resource.core import HintScheme
from maggma.stores import AliasingStore, MemoryStore
from pydantic import BaseModel, Field
from requests import Response
from starlette.testclient import TestClient


class Owner(BaseModel):
    name: str = Field(..., title="Owner's name")
    age: int = Field(None, title="Owne'r Age")
    weight: float = Field(None, title="Owner's weight")
    last_updated: datetime = Field(None, title="Last updated date for this record")


owners = (
    [Owner(name=f"Person{i}", age=i + 3, weight=100 + i) for i in list(range(10))]
    + [Owner(name="PersonAge9", age=9, weight=float(randint(155, 195)))]
    + [Owner(name="PersonWeight150", age=randint(10, 15), weight=float(150))]
    + [Owner(name="PersonAge20Weight200", age=20, weight=float(200))]
)

total_owners = len(owners)


@pytest.fixture()
def owner_store():
    store = MemoryStore("owners", key="name")
    store.connect()
    store.update([d.dict() for d in owners])
    return store


def test_init(owner_store):
    resource = ReadOnlyResource(store=owner_store, model=Owner)
    assert len(resource.router.routes) == 3

    resource = ReadOnlyResource(store=owner_store, model=Owner, enable_get_by_key=False)
    assert len(resource.router.routes) == 2

    resource = ReadOnlyResource(store=owner_store, model=Owner, enable_default_search=False)
    assert len(resource.router.routes) == 2


def test_msonable(owner_store):
    owner_resource = ReadOnlyResource(store=owner_store, model=Owner)
    endpoint_dict = owner_resource.as_dict()

    for k in ["@class", "@module", "store", "model"]:
        assert k in endpoint_dict

    assert isinstance(endpoint_dict["model"], str)
    assert endpoint_dict["model"] == "tests.api.test_read_resource.Owner"


def test_get_by_key(owner_store):
    endpoint = ReadOnlyResource(owner_store, Owner, disable_validation=True)
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)

    assert client.get("/").status_code == 200

    assert client.get("/Person1/").status_code == 200
    assert client.get("/Person1/").json()["data"][0]["name"] == "Person1"


def test_key_fields(owner_store):
    endpoint = ReadOnlyResource(owner_store, Owner, key_fields=["name"])
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)

    assert client.get("/Person1/").status_code == 200
    assert client.get("/Person1/").json()["data"][0]["name"] == "Person1"


@pytest.mark.xfail()
def test_problem_query_params(owner_store):
    endpoint = ReadOnlyResource(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)

    client.get("/?param=test").status_code


@pytest.mark.xfail()
def test_problem_hint_scheme(owner_store):
    class TestHintScheme(HintScheme):
        def generate_hints(query):
            return {"hint": "test"}

    test_store = AliasingStore(owner_store, {"owners": "test"}, key="name")

    ReadOnlyResource(test_store, Owner, hint_scheme=TestHintScheme())


def search_helper(payload, base: str = "/?", debug=True) -> Response:
    """
    Helper function to directly query search endpoints
    Args:
        store: store f
        base: base of the query, default to /query?
        client: TestClient generated from FastAPI
        payload: query in dictionary format
        debug: True = print out the url, false don't print anything

    Returns:
        request.Response object that contains the response of the corresponding payload
    """
    store = MemoryStore("owners", key="name")
    store.connect()
    store.update([d.dict() for d in owners])

    endpoint = ReadOnlyResource(
        store,
        Owner,
        query_operators=[
            StringQueryOperator(model=Owner),
            NumericQuery(model=Owner),
            SparseFieldsQuery(model=Owner),
        ],
        disable_validation=True,
    )
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)

    print(inspect.signature(NumericQuery(model=Owner).query))

    url = base + urlencode(payload)
    if debug:
        print(url)
    res = client.get(url)
    json = res.json()
    return res, json.get("data", [])  # type: ignore


def test_numeric_query_operator():
    # Checking int
    payload = {"age": 20, "_all_fields": True}
    res, data = search_helper(payload=payload, base="/?", debug=True)
    assert res.status_code == 200
    assert len(data) == 1
    assert data[0]["age"] == 20

    payload = {"age_not_eq": 9, "_all_fields": True}
    res, data = search_helper(payload=payload, base="/?", debug=True)
    assert res.status_code == 200
    assert len(data) == 11

    payload = {"age_max": 9}
    res, data = search_helper(payload=payload, base="/?", debug=True)
    assert res.status_code == 200
    assert len(data) == 8

    payload = {"age_min": 0}
    res, data = search_helper(payload=payload, base="/?", debug=True)
    assert res.status_code == 200
    assert len(data) == 13


def test_string_query_operator():
    payload = {"name": "PersonAge9", "_all_fields": True}
    res, data = search_helper(payload=payload, base="/?", debug=True)
    assert res.status_code == 200
    assert len(data) == 1
    assert data[0]["name"] == "PersonAge9"

    payload = {"name_not_eq": "PersonAge9", "_all_fields": True}
    res, data = search_helper(payload=payload, base="/?", debug=True)
    assert res.status_code == 200
    assert len(data) == 12


def test_resource_compound():
    payload = {
        "name": "PersonAge20Weight200",
        "_all_fields": True,
        "weight_min": 199.1,
        "weight_max": 201.4,
        "age": 20,
    }
    res, data = search_helper(payload=payload, base="/?", debug=True)
    assert res.status_code == 200
    assert len(data) == 1
    assert data[0]["name"] == "PersonAge20Weight200"

    payload = {
        "name": "PersonAge20Weight200",
        "_all_fields": False,
        "_fields": "name,age",
        "weight_min": 199.3,
        "weight_max": 201.9,
        "age": 20,
    }
    res, data = search_helper(payload=payload, base="/?", debug=True)
    assert res.status_code == 200
    assert len(data) == 1
    assert data[0]["name"] == "PersonAge20Weight200"
    assert "weight" not in data[0]
