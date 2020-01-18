import pytest
from random import randint
from pydantic import BaseModel, Schema, Field
from maggma.stores import MemoryStore
from maggma.api import EndpointCluster
from starlette.testclient import TestClient
from fastapi import FastAPI


class Owner(BaseModel):
    name: str = Field(..., title="Owner's name")
    age: int = Field(None, title="Owne'r Age")
    weight: int = Field(None, title="Owner's weight")


@pytest.fixture("session")
def owners():
    return [
        Owner(name=f"Person{i}", age=randint(10, 100), weight=randint(100, 200))
        for i in list(range(10)[1:])
    ] + [Owner(name="PersonAge12", age=12, weight=randint(100,200))] + \
        [Owner(name="PersonWeight150", age=randint(10, 15), weight=150)]


@pytest.fixture
def owner_store(owners):
    store = MemoryStore("owners", key="name")
    store.connect()
    owners = [d.dict() for d in owners]
    store.update(owners)
    return store


def test_init_endpoint(owner_store):
    num_default_api_routes = 3
    endpoint = EndpointCluster(owner_store, Owner)
    assert len(endpoint.router.routes) == num_default_api_routes
    assert endpoint.router.routes[0]

    endpoint = EndpointCluster(owner_store, "tests.api.test_endpointcluster.Owner")
    assert len(endpoint.router.routes) == num_default_api_routes
    assert endpoint.router.routes[0]

    with pytest.raises(ValueError):
        endpoint = EndpointCluster(owner_store, 3)


def test_endpoint_msonable(owner_store):

    endpoint = EndpointCluster(owner_store, Owner)
    endpoint_dict = endpoint.as_dict()

    for k in ["@class", "@module", "store", "model"]:
        assert k in endpoint_dict

    assert isinstance(endpoint_dict["model"], str)
    assert endpoint_dict["model"] == "tests.api.test_endpointcluster.Owner"


def test_endpoint_function(owner_store):
    endpoint = EndpointCluster(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)

    assert client.get("/").status_code == 200

    assert client.get("/name/Person1").status_code == 200
    assert client.get("/name/Person1").json()["name"] == "Person1"

def test_endpoint_search(owner_store):
    endpoint = EndpointCluster(owner_store, Owner)
    app = FastAPI()
    app.include_router(endpoint.router)

    client = TestClient(app)

    # test '{"age":12}' with all_include = True
    res = client.get("/search?query=%27%7B%22age%22%3A12%7D%27&limit=10&all_include=true")
    assert res.status_code == 200
    assert len(res.json()) >= 1
    assert True in [d["name"] == "PersonAge12" for d in res.json()]

    # test '{"weight":150}' with projection '["name","age"]', which means weight should be missing in the return object
    res = client.get("search?query=%27%7B%22weight%22%3A150%7D%27&projection=%27%5B%22name%22%2C%22age%22%5D%27&limit"
                     "=10&all_include=false")
    assert res.status_code == 200
    assert len(res.json()) >= 1
    assert True in [d["name"] == "PersonWeight150" for d in res.json()]
    for d in res.json():
        assert d["weight"] is None

    # test '{"age":12}' with all_include = True and limit = 0, which i should
    res = client.get("/search?query=%27%7B%22age%22%3A12%7D%27&limit=0&all_include=true")
    assert res.status_code == 200
    assert res.json() == []

    # test '{"age":200}' with all_include = True, which i should get nothing
    res = client.get("/search?query=%27%7B%22age%22%3A200%7D%27&limit=10&all_include=true")
    assert res.status_code == 200
    assert res.json() == []



