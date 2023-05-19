import pytest

from maggma.stores import GridFSStore, MongoStore
from maggma.stores.shared_stores import MultiStore, StoreFacade

@pytest.fixture
def mongostore():
    store = MongoStore("maggma_test", "test")
    store.connect()
    yield store
    store._collection.drop()


@pytest.fixture
def gridfsstore():
    store = GridFSStore("maggma_test", "test", key="task_id")
    store.connect()
    yield store
    store._files_collection.drop()
    store._chunks_collection.drop()

@pytest.fixture
def multistore():
    store = MultiStore()
    yield store

def test_add_stores(multistore, mongostore, gridfsstore):
    # Should be empty at the start
    assert multistore.count_stores() == 0
    
    multistore.ensure_store(mongostore)
    assert multistore.count_stores() == 1
    assert multistore.get_store_index(mongostore) == 0

    # Attempting to reinsert this store should do nothing
    multistore.ensure_store(mongostore)
    assert multistore.count_stores() == 1

    # Make a copy of the mongostore and it should still do nothing
    temp_mongostore = MongoStore.from_dict(mongostore.as_dict())
    multistore.ensure_store(temp_mongostore)
    assert multistore.count_stores() == 1
    assert multistore.get_store_index(temp_mongostore) == 0

    # Add the GridFSStore to the MultiStore()
    multistore.ensure_store(gridfsstore)
    assert multistore.count_stores() == 2
    assert multistore.get_store_index(mongostore) == 1

def test_store_facade(multistore, mongostore, gridfsstore):
    StoreFacade(mongostore, multistore)
    assert multistore.count_stores() == 1
    assert multistore.get_store_index(mongostore) == 0

    StoreFacade(gridfsstore, multistore)
    assert multistore.count_stores() == 2
    assert multistore.get_store_index(gridfsstore) == 1





