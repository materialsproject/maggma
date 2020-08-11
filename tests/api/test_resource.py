from random import randint
from urllib.parse import urlencode

import pytest
from fastapi import FastAPI
from pydantic import BaseModel, Field
from requests import Response
from starlette.testclient import TestClient

from maggma.api.resource import Resource
from maggma.stores import MemoryStore


class Owner(BaseModel):
    name: str = Field(..., title="Owner's name")
    age: int = Field(None, title="Owne'r Age")
    weight: float = Field(None, title="Owner's weight")


@pytest.fixture(scope="session")
def owners():
    return (
        [
            Owner(
                name=f"Person{i}", age=randint(10, 100), weight=float(randint(100, 100))
            )
            for i in list(range(10)[1:])
        ]  # there are 8 owners here
        + [Owner(name="PersonAge9", age=9, weight=float(randint(155, 195)))]
        + [Owner(name="PersonWeight150", age=randint(10, 15), weight=float(150))]
        + [Owner(name="PersonAge20Weight200", age=20, weight=float(200))]
    )


@pytest.fixture
def owner_store(owners):
    store = MemoryStore("owners", key="name")
    store.connect()
    owners = [d.dict() for d in owners]
    store.update(owners)
    return store


def test_init_resource(owner_store):
    num_default_api_routes = 2
    endpoint = Resource(owner_store, Owner)
    assert len(endpoint.router.routes) == num_default_api_routes
    assert endpoint.router.routes[0]

    endpoint = Resource(owner_store, "tests.api.test_resource.Owner")
    assert len(endpoint.router.routes) == num_default_api_routes
    assert endpoint.router.routes[0]


def test_resource_msonable(owner_store):
    endpoint = Resource(owner_store, Owner)
    endpoint_dict = endpoint.as_dict()

    for k in ["@class", "@module", "store", "model"]:
        assert k in endpoint_dict

    assert isinstance(endpoint_dict["model"], str)
    assert endpoint_dict["model"] == "tests.api.test_resource.Owner"


def test_resource_get_by_key(owner_store):
    endpoint = Resource(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)

    assert client.get("/").status_code == 200

    assert client.get("/Person1/").status_code == 200
    assert client.get("/Person1/").json()["data"][0]["name"] == "Person1"


def dynamic_model_search_response_helper(
    client, payload, base: str = "/?", debug=False
) -> Response:
    """
    Helper function for dynamicModelSearch function, to reduce redundant code
    Args:
        base: base of the query, default to /query?
        client: TestClient generated from FastAPI
        payload: query in dictionary format
        debug: True = print out the url, false don't print anything

    Returns:
        request.Response object that contains the response of the correspoding payload
    """
    url = base + urlencode(payload)
    if debug:
        print(url)
    res = client.get(url)
    return res


def get_fields(res: Response):
    json = res.json()
    return json.get("data", []), json.get("meta", {}), json.get("error", [])


def test_resource_dynamic_model_search_eq_noteq(owner_store):
    endpoint = Resource(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)
    payload = {"name_eq": "PersonAge9", "all_fields": True}
    res = dynamic_model_search_response_helper(
        client=client, payload=payload, base="/?"
    )
    data, meta, err = get_fields(res)

    # String eq
    assert res.status_code == 200
    assert len(data) == 1
    assert data[0]["name"] == "PersonAge9"

    # int Eq
    assert data[0]["age"] == 9

    # float Eq
    payload = {"weight_eq": 150.0, "all_fields": True}
    res = dynamic_model_search_response_helper(
        client=client, payload=payload, base="/?"
    )
    data, meta, err = get_fields(res)
    assert res.status_code == 200
    assert len(data) == 1
    assert data[0]["weight"] == 150.0

    # Str Not eq
    payload = {"name_not_eq": "PersonAge9", "all_fields": True}
    res = dynamic_model_search_response_helper(
        client=client, payload=payload, base="/?"
    )
    data, meta, err = get_fields(res)
    assert res.status_code == 200
    assert len(data) == 10

    # int Not Eq
    payload = {"age_not_eq": 9, "all_fields": True}
    res = dynamic_model_search_response_helper(
        client=client, payload=payload, base="/?"
    )
    data, meta, err = get_fields(res)
    assert res.status_code == 200
    assert len(data) == 10

    # float Not Eq
    payload = {"weight_not_eq": 150.0, "all_fields": True}
    res = dynamic_model_search_response_helper(
        client=client, payload=payload, base="/?"
    )
    data, meta, err = get_fields(res)
    assert res.status_code == 200
    assert len(data) == 10


def test_resource_dynamic_model_search_lt_gt(owner_store):
    endpoint = Resource(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)
    payload = {
        "age_lt": 101  # see if there are 10 entries, all age should be less than 100
    }

    res = dynamic_model_search_response_helper(
        client=client, payload=payload, base="/?"
    )
    data, meta, err = get_fields(res)

    assert res.status_code == 200
    assert len(data) == 10

    payload = {"age_gt": 0}  # there should be 10 entries, all age are greater than 10
    res = dynamic_model_search_response_helper(
        client=client, payload=payload, base="/?"
    )
    data, meta, err = get_fields(res)
    assert res.status_code == 200
    assert len(data) == 10


def test_resource_dynamic_model_search_in_notin(owner_store):
    endpoint = Resource(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)

    # In
    url = "/?age_in=9&age_in=101&all_fields=true&limit=10"
    res = client.get(url)
    data, meta, err = get_fields(res)
    assert res.status_code == 200
    assert len(data) == 1

    # Not In
    url = "/?limit=10&all_fields=true&age_not_in=9&age_not_in=10"
    res = client.get(url)
    data, meta, err = get_fields(res)
    assert res.status_code == 200
    assert len(data) == 10


def test_resource_no_explicit_eq(owner_store):
    endpoint = Resource(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)

    payload = {"name": "PersonAge9", "all_fields": True}
    res = dynamic_model_search_response_helper(
        client=client, payload=payload, base="/?", debug=True
    )
    data, meta, err = get_fields(res)

    # String eq with no explicit eq
    assert res.status_code == 200
    assert len(data) == 1
    assert data[0]["name"] == "PersonAge9"


def test_resource_compound(owner_store):
    endpoint = Resource(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)

    payload = {
        "name": "PersonAge20Weight200",
        "all_fields": True,
        "weight": 200,
        "age": 20,
    }
    res = dynamic_model_search_response_helper(
        client=client, payload=payload, base="/?", debug=True
    )
    data, meta, err = get_fields(res)

    assert res.status_code == 200
    assert len(data) == 1
    assert data[0]["name"] == "PersonAge20Weight200"

    payload = {
        "name": "PersonAge20Weight200",
        "all_fields": False,
        "fields": "name, age",
        "weight": 200,
        "age": 20,
    }
    res = dynamic_model_search_response_helper(
        client=client, payload=payload, base="/?", debug=True
    )
    data, meta, err = get_fields(res)
    assert res.status_code == 200
    assert len(data) == 1
    assert data[0]["name"] == "PersonAge20Weight200"
    assert data[0]["age"] == 20
    assert "weight" not in data[0]
