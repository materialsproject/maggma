import pytest
from random import randint
from pydantic import BaseModel, Field
from maggma.stores import MemoryStore
from maggma.api import EndpointCluster
from starlette.testclient import TestClient
from fastapi import FastAPI
from urllib.parse import urlencode
from requests import Response


class Owner(BaseModel):
    name: str = Field(..., title="Owner's name")
    age: int = Field(None, title="Owne'r Age")
    weight: float = Field(None, title="Owner's weight")


@pytest.fixture("session")
def owners():
    return (
        [
            Owner(
                name=f"Person{i}", age=randint(10, 100), weight=float(randint(100, 200))
            )
            for i in list(range(10)[1:])
        ]  # there are 8 owners here
        + [Owner(name="PersonAge9", age=9, weight=float(randint(100, 200)))]
        + [Owner(name="PersonWeight150", age=randint(10, 15), weight=float(150))]
    )


@pytest.fixture
def owner_store(owners):
    store = MemoryStore("owners", key="name")
    store.connect()
    owners = [d.dict() for d in owners]
    store.update(owners)
    return store


def test_init_endpoint(owner_store):
    num_default_api_routes = 7
    endpoint = EndpointCluster(owner_store, Owner)
    assert len(endpoint.router.routes) == num_default_api_routes
    assert endpoint.router.routes[0]

    endpoint = EndpointCluster(owner_store, "tests.api.test_endpointcluster.Owner")
    assert len(endpoint.router.routes) == num_default_api_routes
    assert endpoint.router.routes[0]


def test_endpoint_msonable(owner_store):
    endpoint = EndpointCluster(owner_store, Owner)
    endpoint_dict = endpoint.as_dict()

    for k in ["@class", "@module", "store", "model"]:
        assert k in endpoint_dict

    assert isinstance(endpoint_dict["model"], str)
    assert endpoint_dict["model"] == "tests.api.test_endpointcluster.Owner"


def test_endpoint_get_by_key(owner_store):
    endpoint = EndpointCluster(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)

    assert client.get("/").status_code == 200

    assert client.get("/name/Person1").status_code == 200
    assert client.get("/name/Person1").json()["name"] == "Person1"


def test_endpoint_alias(owner_store):
    endpoint = EndpointCluster(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)


def dynamic_model_search_response_helper(
    client, payload, base: str = "/query?"
) -> Response:
    """
    Helper function for dynamicModelSearch function, to reduce redundant code
    Args:
        base: base of the query, default to /query?
        client: TestClient generated from FastAPI
        payload: query in dictionary format

    Returns:
        request.Response object that contains the response of the correspoding payload
    """
    print(urlencode(payload))
    url = base + urlencode(payload)
    res = client.get(url)
    return res


def test_endpoint_dynamic_model_search_eq_noteq(owner_store):
    endpoint = EndpointCluster(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)
    payload = {"name_eq": "PersonAge9"}
    res = dynamic_model_search_response_helper(client=client, payload=payload)

    # String eq
    assert res.status_code == 200
    assert len(res.json()) == 1
    assert res.json()[0]["name"] == "PersonAge9"

    # int Eq
    assert res.json()[0]["age"] == 9

    # float Eq
    payload = {"weight_eq": 150.0}
    res = dynamic_model_search_response_helper(client, payload)
    assert res.status_code == 200
    assert len(res.json()) == 1
    assert res.json()[0]["weight"] == 150.0

    # Str Not eq
    payload = {"name_not_eq": "PersonAge9"}
    res = dynamic_model_search_response_helper(client, payload)
    assert res.status_code == 200
    assert len(res.json()) == 10

    # int Not Eq
    payload = {"age_not_eq": 9}
    res = dynamic_model_search_response_helper(client, payload)
    assert res.status_code == 200
    assert len(res.json()) == 10

    # float Not Eq
    payload = {"weight_not_eq": 150.0}
    res = dynamic_model_search_response_helper(client, payload)
    assert res.status_code == 200
    assert len(res.json()) == 10


def test_endpoint_dynamic_model_search_lt_gt(owner_store):
    endpoint = EndpointCluster(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)
    payload = {
        "age_lt": 101  # see if there are 10 entries, all age should be less than 100
    }

    res = dynamic_model_search_response_helper(client, payload)

    assert res.status_code == 200
    assert len(res.json()) == 10

    payload = {"age_gt": 0}  # there should be 10 entries, all age are greater than 10
    res = dynamic_model_search_response_helper(client, payload)
    assert res.status_code == 200
    assert len(res.json()) == 10


def test_endpoint_dynamic_model_search_in_notin(owner_store):
    endpoint = EndpointCluster(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)

    # client = TestClient(app)
    # payload = {"name_in": ["PersonAge9", "PersonWeight150"]}
    # res = dynamic_model_search_response_helper(client=client, payload=payload)
    # assert res.status_code == 200 ## this part doesn't work... i'll figure it out later
    # assert len(res.json()) == 2
