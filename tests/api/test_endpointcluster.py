import pytest
from pydantic import BaseModel, Schema, validator
from enum import Enum
from random import randint, choice
from maggma.stores import MemoryStore
from maggma.api import EndpointCluster


class PetType(str, Enum):
    cat = "cat"
    dog = "dog"
    types = [cat, dog]


class Owner(BaseModel):
    name: str = Schema(..., title="Owner's name")
    age: int = Schema(..., title="Owne'r Age")
    weight: int = Schema(..., title="Owner's weight")


class Pet(BaseModel):
    name: str = Schema(..., title="Pet's Name")
    pet_type: PetType = Schema(..., title="Pet Type")
    owner_name: str = Schema(..., title="Owner's name")

    @validator("pet_type")
    def check_pet_type(cls, pt):
        if pt in PetType.types:
            return pt
        else:
            raise ValueError(f"Unknown Pet Type")


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
    owners = [d.dict() for d in owners]
    store.update(owners)
    return store


def test_init_endpoint(owner_store):
    endpoint = EndpointCluster(owner_store, Owner)
    assert len(endpoint.router.routes) == 1
    assert endpoint.router.routes[0]

    endpoint = EndpointCluster(owner_store, "tests.api.test_endpointcluster.Owner")
    assert len(endpoint.router.routes) == 1
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
