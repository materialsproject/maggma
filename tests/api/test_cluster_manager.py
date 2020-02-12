import pytest
from pydantic import BaseModel, Schema
from enum import Enum
from random import randint, choice
from maggma.stores import MemoryStore
from maggma.api import EndpointCluster, ClusterManager
from starlette.testclient import TestClient
from fastapi.encoders import jsonable_encoder


class PetType(str, Enum):
    cat = "cat"
    dog = "dog"


class Owner(BaseModel):
    name: str = Schema(..., title="Owner's name")
    age: int = Schema(..., title="Owne'r Age")
    weight: int = Schema(..., title="Owner's weight")


class Pet(BaseModel):
    name: str = Schema(..., title="Pet's Name")
    pet_type: PetType = Schema(..., title="Pet Type")
    owner_name: str = Schema(..., title="Owner's name")


@pytest.fixture("session")
def owners():
    return [
        Owner(name=f"Person{i}", age=randint(10, 100), weight=randint(100, 200))
        for i in list(range(10)[1:])
    ]


@pytest.fixture("session")
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
    print(pets[0])
    store.update(pets)
    return store


def test_cluster_dict_behavior(owner_store, pet_store):
    pass
    # owner_endpoint = EndpointCluster(owner_store, Owner)
    # pet_endpoint = EndpointCluster(pet_store, Pet)
    #
    # manager = ClusterManager({"owners": owner_endpoint})
    #
    # assert manager["owners"] == owner_endpoint  # test __getitem__
    # assert len(manager) == 1  # test len
    # assert "owners" in manager.keys()  # test keys
    # assert "owners" in manager  # test __contains__
    # assert "pets" not in manager.keys()  # test __keys__
    #
    # manager["pets"] = pet_endpoint  # test __setitem__
    # assert len(manager) == 2
    # assert manager["pets"] == pet_endpoint


def test_cluster_run(owner_store, pet_store):
    pass
    # owner_endpoint = EndpointCluster(owner_store, Owner)
    # pet_endpoint = EndpointCluster(pet_store, Pet)
    #
    # manager = ClusterManager({"owners": owner_endpoint, "pets": pet_endpoint})
    #
    # client = TestClient(manager.app)
    #
    # assert client.get("/").status_code == 404
    # assert client.get("/owners/name/Person1").status_code == 200
    # assert client.get("/owners/name/Person1").json()["name"] == "Person1"
    #
    # assert client.get("/pets/name/Pet1").status_code == 200
    # assert client.get("/pets/name/Pet1").json()["name"] == "Pet1"


def test_cluster_pprint(owner_store, pet_store):
    pass
    # endpoint_main = EndpointCluster(owner_store, Owner, description="main")
    # endpoint_main_temp = EndpointCluster(pet_store, Pet, description="main_temp")
    # endpoint_temp = EndpointCluster(owner_store, Owner, description="temp")

    # manager = ClusterManager(
    #     {
    #         "/temp": endpoint_temp,
    #         "/main": endpoint_main,
    #         "/main/temp": endpoint_main_temp,
    #     }
    # )
    #
    # res = manager.sort()
    #
    # assert res == ["/main", "/main/temp", "/temp"]
