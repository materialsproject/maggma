from enum import Enum
from random import choice, randint
from urllib.parse import urlencode

import pytest
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field
from requests import Response
from starlette.testclient import TestClient

from maggma.api.APIManager import APIManager
from maggma.api.resource import Resource
from maggma.stores import MemoryStore


class PetType(str, Enum):
    cat = "cat"
    dog = "dog"


class Owner(BaseModel):
    name: str = Field(..., title="Owner's name")
    age: int = Field(..., title="Owne'r Age")
    weight: int = Field(..., title="Owner's weight")


class Pet(BaseModel):
    name: str = Field(..., title="Pet's Name")
    pet_type: PetType = Field(..., title="Pet Type")
    owner_name: str = Field(..., title="Owner's name")


@pytest.fixture(scope="session")
def owners():
    return [
        Owner(name=f"Person{i}", age=randint(10, 100), weight=randint(100, 200))
        for i in list(range(10)[1:])
    ]


@pytest.fixture(scope="session")
def pets(owners):
    return [
        Pet(
            name=f"Pet{i}",
            pet_type=choice(list(PetType)),
            owner_name=choice(owners).name,
        )
        for i in list(range(40))[1:]
    ]


@pytest.fixture
def owner_store(owners):
    store = MemoryStore("owners", key="name")
    store.connect()
    owners = [jsonable_encoder(d) for d in owners]
    store.update(owners)
    return store


@pytest.fixture
def pet_store(pets):
    store = MemoryStore("pets", key="name")
    store.connect()
    pets = [jsonable_encoder(d) for d in pets]
    store.update(pets)
    return store


def test_cluster_dict_behavior(owner_store, pet_store):
    owner_endpoint = Resource(owner_store, Owner)
    pet_endpoint = Resource(pet_store, Pet)

    manager = APIManager({"owners": owner_endpoint})

    assert manager["owners"] == owner_endpoint  # test __getitem__
    assert len(manager) == 1  # test len
    assert "owners" in manager.keys()  # test keys
    assert "owners" in manager  # test __contains__
    assert "pets" not in manager.keys()  # test __keys__

    manager["pets"] = pet_endpoint  # test __setitem__
    assert len(manager) == 2
    assert manager["pets"] == pet_endpoint


def dynamic_model_search_response_helper(
    client, payload, base: str = "/search?"
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
    url = base + urlencode(payload)
    print(url)
    res = client.get(url)
    return res


def get_fields(res: Response):
    json = res.json()
    return json.get("data", []), json.get("meta", {}), json.get("error", [])


def test_cluster_run(owner_store, pet_store):
    owner_endpoint = Resource(owner_store, Owner)
    pet_endpoint = Resource(pet_store, Pet)

    manager = APIManager({"owners": owner_endpoint, "pets": pet_endpoint})

    client = TestClient(manager.app)

    assert client.get("/").status_code == 404
    payload = {"name_eq": "Person1", "limit": 10, "all_fields": True}

    res = dynamic_model_search_response_helper(
        client=client, payload=payload, base="/owners/?"
    )
    data, meta, err = get_fields(res)
    assert res.status_code == 200
    assert len(data) == 1
    assert data[0]["name"] == "Person1"

    payload = {"name_eq": "Pet1", "limit": 10, "all_fields": True}

    res = dynamic_model_search_response_helper(
        client=client, payload=payload, base="/pets/?"
    )
    data, meta, err = get_fields(res)
    assert res.status_code == 200
    assert len(data) == 1
    assert data[0]["name"] == "Pet1"


def test_cluster_pprint(owner_store, pet_store):
    endpoint_main = Resource(owner_store, Owner, description="main")
    endpoint_main_temp = Resource(pet_store, Pet, description="main_temp")
    endpoint_temp = Resource(owner_store, Owner, description="temp")

    manager = APIManager(
        {
            "/temp": endpoint_temp,
            "/main": endpoint_main,
            "/main/temp": endpoint_main_temp,
        }
    )

    res = manager.sort()

    assert res == ["/main", "/main/temp", "/temp"]
