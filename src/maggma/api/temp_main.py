import pytest
from random import randint
from pydantic import BaseModel, Field
from maggma.stores import MemoryStore
# from maggma.api import EndpointCluster
from endpoint_cluster import EndpointCluster
from starlette.testclient import TestClient
from fastapi import FastAPI
import uvicorn


class Owner(BaseModel):
    name: str = Field(..., title="Owner's name")
    age: int = Field(..., title="Owne'r Age")
    weight: int = Field(..., title="Owner's weight")

def owners():
    return [
        Owner(name=f"Person{i}", age=randint(10, 100), weight=randint(100, 200))
        for i in list(range(10)[1:])
    ]

def owner_store(owners):
    store = MemoryStore("owners", key="name")
    store.connect()
    owners = [d.dict() for d in owners]
    store.update(owners)
    return store

o_s = owner_store(owners())

endpoint = EndpointCluster(o_s, Owner)

endpoint.run()
