"""
Test for endpoint_cluster
"""
import pytest
from maggma.stores import JSONStore
from maggma.api.endpoint_cluster import EndpointCluster
from maggma.examples.materials.models import MaterialModel
from fastapi import FastAPI
from starlette.testclient import TestClient


@pytest.fixture()
def setup_store():
    store = JSONStore("../examples/materials/data/more_mats.json.gz")
    store.connect()
    return store


def test_route(setup_store):
    cluster = EndpointCluster(setup_store, MaterialModel)
    app = FastAPI()
    app.include_router(cluster.router)
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
