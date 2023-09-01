import json
import os
from datetime import datetime

import numpy as np
import numpy.testing as nptu
import pytest
from maggma.core import StoreError
from maggma.stores import GridFSStore, MongoStore
from maggma.stores.gridfs import GridFSURIStore, files_collection_fields
from pymongo.errors import ConfigurationError


@pytest.fixture()
def mongostore():
    store = MongoStore("maggma_test", "test")
    store.connect()
    yield store
    store._collection.drop()


@pytest.fixture()
def gridfsstore():
    store = GridFSStore("maggma_test", "test", key="task_id")
    store.connect()
    yield store
    store._files_collection.drop()
    store._chunks_collection.drop()


def test_update(gridfsstore):
    data1 = np.random.rand(256)
    data2 = np.random.rand(256)
    tic = datetime(2018, 4, 12, 16)
    # Test metadata storage
    gridfsstore.update([{"task_id": "mp-1", "data": data1, gridfsstore.last_updated_field: tic}])
    assert gridfsstore._files_collection.find_one({"metadata.task_id": "mp-1"}) is not None

    # Test storing data
    gridfsstore.update([{"task_id": "mp-1", "data": data2, gridfsstore.last_updated_field: tic}])
    assert len(list(gridfsstore.query({"task_id": "mp-1"}))) == 1
    assert "task_id" in gridfsstore.query_one({"task_id": "mp-1"})
    nptu.assert_almost_equal(gridfsstore.query_one({"task_id": "mp-1"})["data"], data2, 7)

    # Test storing compressed data
    gridfsstore = GridFSStore("maggma_test", "test", key="task_id", compression=True)
    gridfsstore.connect()
    gridfsstore.update([{"task_id": "mp-1", "data": data1}])
    assert gridfsstore._files_collection.find_one({"metadata.compression": "zlib"}) is not None

    nptu.assert_almost_equal(gridfsstore.query_one({"task_id": "mp-1"})["data"], data1, 7)


def test_remove(gridfsstore):
    data1 = np.random.rand(256)
    data2 = np.random.rand(256)
    tic = datetime(2018, 4, 12, 16)
    gridfsstore.update([{"task_id": "mp-1", "data": data1, gridfsstore.last_updated_field: tic}])
    gridfsstore.update([{"task_id": "mp-2", "data": data2, gridfsstore.last_updated_field: tic}])

    assert gridfsstore.query_one(criteria={"task_id": "mp-1"})
    assert gridfsstore.query_one(criteria={"task_id": "mp-2"})
    gridfsstore.remove_docs({"task_id": "mp-1"})
    assert gridfsstore.query_one(criteria={"task_id": "mp-1"}) is None
    assert gridfsstore.query_one(criteria={"task_id": "mp-2"})


def test_count(gridfsstore):
    data1 = np.random.rand(256)
    data2 = np.random.rand(256)
    tic = datetime(2018, 4, 12, 16)
    gridfsstore.update([{"task_id": "mp-1", "data": data1, gridfsstore.last_updated_field: tic}])

    assert gridfsstore.count() == 1

    gridfsstore.update([{"task_id": "mp-2", "data": data2, gridfsstore.last_updated_field: tic}])

    assert gridfsstore.count() == 2
    assert gridfsstore.count({"task_id": "mp-2"}) == 1


def test_query(gridfsstore):
    data1 = np.random.rand(256)
    data2 = np.random.rand(256)
    tic = datetime(2018, 4, 12, 16)
    gridfsstore.update([{"task_id": "mp-1", "data": data1, gridfsstore.last_updated_field: tic}])
    gridfsstore.update([{"task_id": "mp-2", "data": data2, gridfsstore.last_updated_field: tic}])

    doc = gridfsstore.query_one(criteria={"task_id": "mp-1"})
    nptu.assert_almost_equal(doc["data"], data1, 7)

    doc = gridfsstore.query_one(criteria={"task_id": "mp-2"})
    nptu.assert_almost_equal(doc["data"], data2, 7)
    assert gridfsstore.last_updated_field in doc

    assert gridfsstore.query_one(criteria={"task_id": "mp-3"}) is None


def test_query_gridfs_file(gridfsstore):
    # put the data directly in gridfs, mimicking an existing gridfs collection
    # generated without the store
    gridfsstore._collection.put(b"hello world", task_id="mp-1")
    doc = gridfsstore.query_one()
    assert doc["data"].decode() == "hello world"
    assert doc[gridfsstore.key] == "mp-1"


def test_last_updated(gridfsstore):
    data1 = np.random.rand(256)
    data2 = np.random.rand(256)
    tic = datetime(2018, 4, 12, 16)

    gridfsstore.update([{"task_id": "mp-1", "data": data1, gridfsstore.last_updated_field: tic}])
    gridfsstore.update([{"task_id": "mp-2", "data": data2, gridfsstore.last_updated_field: tic}])

    assert gridfsstore.last_updated == tic

    toc = datetime(2019, 6, 12, 16)
    gridfsstore.update([{"task_id": "mp-3", "data": data2, gridfsstore.last_updated_field: toc}])

    assert gridfsstore.last_updated == toc

    tic = datetime(2017, 6, 12, 16)
    gridfsstore.update([{"task_id": "mp-4", "data": data2, gridfsstore.last_updated_field: tic}])

    assert gridfsstore.last_updated == toc


def test_groupby(gridfsstore):
    tic = datetime(2018, 4, 12, 16)

    for i in range(3):
        gridfsstore.update(
            [{"task_id": f"mp-{i}", "a": 1, gridfsstore.last_updated_field: tic}],
            key=["task_id", "a"],
        )

    for i in range(3, 7):
        gridfsstore.update(
            [{"task_id": f"mp-{i}", "a": 2, gridfsstore.last_updated_field: tic}],
            key=["task_id", "a"],
        )

    groups = list(gridfsstore.groupby("a"))
    assert len(groups) == 2
    assert {g[0]["a"] for g in groups} == {1, 2}

    by_group = {}
    for group, docs in groups:
        by_group[group["a"]] = {d["task_id"] for d in docs}
    assert by_group[1] == {"mp-0", "mp-1", "mp-2"}
    assert by_group[2] == {"mp-3", "mp-4", "mp-5", "mp-6"}


def test_distinct(gridfsstore):
    tic = datetime(2018, 4, 12, 16)

    for i in range(3):
        gridfsstore.update(
            [{"task_id": f"mp-{i}", "a": 1, gridfsstore.last_updated_field: tic}],
            key=["task_id", "a"],
        )

    for i in range(3, 7):
        gridfsstore.update(
            [{"task_id": f"mp-{i}", "a": 2, gridfsstore.last_updated_field: tic}],
            key=["task_id", "a"],
        )

    assert set(gridfsstore.distinct("a")) == {1, 2}


def test_eq(mongostore, gridfsstore):
    assert gridfsstore == gridfsstore

    assert mongostore != gridfsstore


def test_index(gridfsstore):
    assert gridfsstore.ensure_index("test_key")
    for field in files_collection_fields:
        assert gridfsstore.ensure_index(field)


def test_gfs_metadata(gridfsstore):
    """
    Ensure metadata is put back in the document
    """
    tic = datetime(2018, 4, 12, 16)

    gridfsstore.ensure_metadata = True
    for i in range(3):
        data = {
            "a": 1,
        }
        metadata = {"task_id": f"mp-{i}", "a": 1, gridfsstore.last_updated_field: tic}
        data = json.dumps(data).encode("UTF-8")

        gridfsstore._collection.put(data, metadata=metadata)

    for d in gridfsstore.query():
        assert "task_id" in d
        assert gridfsstore.last_updated_field in d


def test_gridfsstore_from_launchpad_file(lp_file):
    ms = GridFSStore.from_launchpad_file(lp_file, collection_name="tmp")
    ms.connect()
    assert ms.name == "gridfs://localhost/maggma_tests/tmp"


def test_searchable_fields(gridfsstore):
    tic = datetime(2018, 4, 12, 16)

    data = [{"task_id": f"mp-{i}", "a": i, gridfsstore.last_updated_field: tic} for i in range(3)]
    gridfsstore.searchable_fields = ["task_id"]
    gridfsstore.update(data, key="a")

    # This should only work if the searchable field was put into the index store
    assert set(gridfsstore.distinct("task_id")) == {"mp-0", "mp-1", "mp-2"}


def test_additional_metadata(gridfsstore):
    tic = datetime(2018, 4, 12, 16)

    data = [{"task_id": f"mp-{i}", "a": i, gridfsstore.last_updated_field: tic} for i in range(3)]

    gridfsstore.update(data, key="a", additional_metadata="task_id")

    # This should only work if the searchable field was put into the index store
    assert set(gridfsstore.distinct("task_id")) == {"mp-0", "mp-1", "mp-2"}


@pytest.mark.skipif(
    "mongodb+srv" not in os.environ.get("MONGODB_SRV_URI", ""),
    reason="requires special mongodb+srv URI",
)
def test_gridfs_uri():
    uri = os.environ["MONGODB_SRV_URI"]
    store = GridFSURIStore(uri, database="mp_core", collection_name="xas")
    store.connect()
    is_name = store.name is uri
    # This is try and keep the secret safe
    assert is_name


def test_gridfs_uri_dbname_parse():
    # test parsing dbname from uri
    uri_with_db = "mongodb://uuu:xxxx@host:27017/fake_db"
    store = GridFSURIStore(uri_with_db, "test")
    assert store.database == "fake_db"

    uri_with_db = "mongodb://uuu:xxxx@host:27017/fake_db"
    store = GridFSURIStore(uri_with_db, "test", database="fake_db2")
    assert store.database == "fake_db2"

    uri_with_db = "mongodb://uuu:xxxx@host:27017"
    with pytest.raises(ConfigurationError):
        GridFSURIStore(uri_with_db, "test")


def test_close(gridfsstore):
    assert gridfsstore.query_one() is None
    gridfsstore.close()
    with pytest.raises(StoreError):
        gridfsstore.query_one()
    # reconnect to allow the drop of the collection in the fixture
    gridfsstore.connect()
