from datetime import datetime
from random import randint
from urllib.parse import urlencode

import pytest
from fastapi import FastAPI
from datetime import datetime
from requests import Response
from starlette.testclient import TestClient

from maggma.api.models import S3URLDoc
from maggma.api.resource import S3URLResource
from maggma.stores import MemoryStore, AliasingStore

import inspect

from maggma.api.resource.core import HintScheme


entries = [S3URLDoc(url="URL1", requested_datetime=datetime.utcnow(), expiry_datetime=datetime.utcnow())] + [
    S3URLDoc(url="URL2", requested_datetime=datetime.utcnow(), expiry_datetime=datetime.utcnow())
]

total_owners = len(entries)


@pytest.fixture
def entries_store():
    store = MemoryStore("entries", key="url")
    store.connect()
    store.update([d.dict() for d in entries])
    return store


def test_init(entries_store):
    resource = S3URLResource(store=entries_store, url_lifetime=500)
    assert len(resource.router.routes) == 2


def test_msonable(entries_store):
    resource = S3URLResource(store=entries_store, url_lifetime=500)
    endpoint_dict = resource.as_dict()

    for k in ["@class", "@module", "store", "model"]:
        assert k in endpoint_dict

    assert isinstance(endpoint_dict["model"], str)
    assert endpoint_dict["model"] == "maggma.api.models.S3URLDoc"
