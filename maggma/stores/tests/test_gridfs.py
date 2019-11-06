import pytest
import numpy as np
import numpy.testing.utils as nptu
from datetime import datetime
from maggma.stores import GridFSStore


@pytest.fixture
def gridfsstore():
    store = GridFSStore("maggma_test", "test", key="task_id")
    store.connect()
    yield store
    store._files_collection.drop()
    store._chunks_collection.drop()


def test_update(gridfsstore):
    data1 = np.random.rand(256)
    data2 = np.random.rand(256)
    # Test metadata storage
    gridfsstore.update([{"task_id": "mp-1", "data": data1}])
    assert (
        gridfsstore._files_collection.find_one({"metadata.task_id": "mp-1"}) is not None
    )

    # Test storing data
    gridfsstore.update([{"task_id": "mp-1", "data": data2}])
    assert len(list(gridfsstore.query({"task_id": "mp-1"}))) == 1
    assert "task_id" in gridfsstore.query_one({"task_id": "mp-1"})
    nptu.assert_almost_equal(
        gridfsstore.query_one({"task_id": "mp-1"})["data"], data2, 7
    )

    # Test storing compressed data
    gridfsstore = GridFSStore("maggma_test", "test", key="task_id", compression=True)
    gridfsstore.connect()
    gridfsstore.update([{"task_id": "mp-1", "data": data1}])
    assert (
        gridfsstore._files_collection.find_one({"metadata.compression": "zlib"})
        is not None
    )

    nptu.assert_almost_equal(
        gridfsstore.query_one({"task_id": "mp-1"})["data"], data1, 7
    )


def test_query(gridfsstore):
    data1 = np.random.rand(256)
    data2 = np.random.rand(256)
    tic = datetime(2018, 4, 12, 16)
    gridfsstore.update([{"task_id": "mp-1", "data": data1}])
    gridfsstore.update(
        [{"task_id": "mp-2", "data": data2, gridfsstore.last_updated_field: tic}], update_lu=False
    )

    doc = gridfsstore.query_one(criteria={"task_id": "mp-1"})
    nptu.assert_almost_equal(doc["data"], data1, 7)

    doc = gridfsstore.query_one(criteria={"task_id": "mp-2"})
    nptu.assert_almost_equal(doc["data"], data2, 7)
    assert gridfsstore.last_updated_field in doc

    assert gridfsstore.query_one(criteria={"task_id": "mp-3"}) is None


@pytest.mark.skip("Not Done")
def test_distinct(gridfsstore):
    # TODO
    pass
