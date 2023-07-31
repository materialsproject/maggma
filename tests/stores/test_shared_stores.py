import pymongo
import pytest
from maggma.stores import GridFSStore, MemoryStore, MongoStore
from maggma.stores.shared_stores import MultiStore, StoreFacade
from maggma.validators import JSONSchemaValidator
from pymongo.errors import DocumentTooLarge, OperationFailure


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


@pytest.fixture()
def multistore():
    return MultiStore()


@pytest.fixture()
def memorystore():
    store = MemoryStore()
    store.connect()
    return store


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
    # Add this copy again, but don't use ensure_store
    # This tests the case in which a prior thread added
    # the store, but this current process was already
    # waiting for the lock acquisition
    multistore.add_store(temp_mongostore)
    assert multistore.count_stores() == 1

    # Add the GridFSStore to the MultiStore()
    multistore.ensure_store(gridfsstore)
    assert multistore.count_stores() == 2
    assert multistore.get_store_index(gridfsstore) == 1

    # Add something that isn't a store
    class DummyObject:
        def __init__(self, a: int):
            self.a = a

    with pytest.raises(TypeError):
        multistore.ensure_store(DummyObject(1))


def test_store_facade(multistore, mongostore, gridfsstore):
    StoreFacade(mongostore, multistore)
    assert multistore.count_stores() == 1
    assert multistore.get_store_index(mongostore) == 0

    StoreFacade(gridfsstore, multistore)
    assert multistore.count_stores() == 2
    assert multistore.get_store_index(gridfsstore) == 1


def test_multistore_query(multistore, mongostore, memorystore):
    memorystore_facade = StoreFacade(memorystore, multistore)
    mongostore_facade = StoreFacade(mongostore, multistore)
    temp_mongostore_facade = StoreFacade(MongoStore.from_dict(mongostore.as_dict()), multistore)

    memorystore_facade._collection.insert_one({"a": 1, "b": 2, "c": 3})
    assert memorystore_facade.query_one(properties=["a"])["a"] == 1
    assert memorystore_facade.query_one(properties=["a"])["a"] == 1
    assert memorystore_facade.query_one(properties=["b"])["b"] == 2
    assert memorystore_facade.query_one(properties=["c"])["c"] == 3

    mongostore_facade._collection.insert_one({"a": 4, "b": 5, "c": 6})
    assert mongostore_facade.query_one(properties=["a"])["a"] == 4
    assert mongostore_facade.query_one(properties=["a"])["a"] == 4
    assert mongostore_facade.query_one(properties=["b"])["b"] == 5
    assert mongostore_facade.query_one(properties=["c"])["c"] == 6

    assert temp_mongostore_facade.query_one(properties=["a"])["a"] == 4
    assert temp_mongostore_facade.query_one(properties=["a"])["a"] == 4
    assert temp_mongostore_facade.query_one(properties=["b"])["b"] == 5
    assert temp_mongostore_facade.query_one(properties=["c"])["c"] == 6


def test_multistore_count(multistore, mongostore, memorystore):
    memorystore_facade = StoreFacade(memorystore, multistore)

    memorystore_facade._collection.insert_one({"a": 1, "b": 2, "c": 3})
    assert memorystore_facade.count() == 1
    memorystore_facade._collection.insert_one({"aa": 1, "b": 2, "c": 3})
    assert memorystore_facade.count() == 2
    assert memorystore_facade.count({"a": 1}) == 1


def test_multistore_distinct(multistore, mongostore):
    mongostore_facade = StoreFacade(mongostore, multistore)

    mongostore_facade._collection.insert_one({"a": 1, "b": 2, "c": 3})
    mongostore_facade._collection.insert_one({"a": 4, "d": 5, "e": 6, "g": {"h": 1}})
    assert set(mongostore_facade.distinct("a")) == {1, 4}

    # Test list distinct functionality
    mongostore_facade._collection.insert_one({"a": 4, "d": 6, "e": 7})
    mongostore_facade._collection.insert_one({"a": 4, "d": 6, "g": {"h": 2}})

    # Test distinct subdocument functionality
    ghs = mongostore_facade.distinct("g.h")
    assert set(ghs) == {1, 2}

    # Test when key doesn't exist
    assert mongostore_facade.distinct("blue") == []

    # Test when null is a value
    mongostore_facade._collection.insert_one({"i": None})
    assert mongostore_facade.distinct("i") == [None]

    # Test to make sure DocumentTooLarge errors get dealt with properly using built in distinct
    mongostore_facade._collection.insert_many([{"key": [f"mp-{i}"]} for i in range(1000000)])
    vals = mongostore_facade.distinct("key")
    # Test to make sure distinct on array field is unraveled when using manual distinct
    assert len(vals) == len(list(range(1000000)))
    assert all(isinstance(v, str) for v in vals)

    # Test to make sure manual distinct uses the criteria query
    mongostore_facade._collection.insert_many([{"key": f"mp-{i}", "a": 2} for i in range(1000001, 2000001)])
    vals = mongostore_facade.distinct("key", {"a": 2})
    assert len(vals) == len(list(range(1000001, 2000001)))


def test_multistore_update(multistore, mongostore):
    mongostore_facade = StoreFacade(mongostore, multistore)

    mongostore_facade.update({"e": 6, "d": 4}, key="e")
    assert mongostore_facade.query_one(criteria={"d": {"$exists": 1}}, properties=["d"])["d"] == 4

    mongostore_facade.update([{"e": 7, "d": 8, "f": 9}], key=["d", "f"])
    assert mongostore_facade.query_one(criteria={"d": 8, "f": 9}, properties=["e"])["e"] == 7

    mongostore_facade.update([{"e": 11, "d": 8, "f": 9}], key=["d", "f"])
    assert mongostore_facade.query_one(criteria={"d": 8, "f": 9}, properties=["e"])["e"] == 11

    test_schema = {
        "type": "object",
        "properties": {"e": {"type": "integer"}},
        "required": ["e"],
    }
    mongostore_facade.validator = JSONSchemaValidator(schema=test_schema)
    mongostore_facade.update({"e": 100, "d": 3}, key="e")

    # Continue to update doc when validator is not set to strict mode
    mongostore_facade.update({"e": "abc", "d": 3}, key="e")

    # ensure safe_update works to not throw DocumentTooLarge errors
    large_doc = {f"mp-{i}": f"mp-{i}" for i in range(1000000)}
    large_doc["e"] = 999
    with pytest.raises((OperationFailure, DocumentTooLarge)):
        mongostore_facade.update([large_doc, {"e": 1001}], key="e")

    mongostore_facade.safe_update = True
    assert mongostore_facade.safe_update is True
    mongostore_facade.update([large_doc, {"e": 1001}], key="e")
    assert mongostore_facade.query_one({"e": 1001}) is not None


def test_multistore_groupby(multistore, mongostore):
    mongostore_facade = StoreFacade(mongostore, multistore)

    mongostore_facade.update(
        [
            {"e": 7, "d": 9, "f": 9},
            {"e": 7, "d": 9, "f": 10},
            {"e": 8, "d": 9, "f": 11},
            {"e": 9, "d": 10, "f": 12},
        ],
        key="f",
    )
    data = list(mongostore_facade.groupby("d"))
    assert len(data) == 2
    grouped_by_9 = next(g[1] for g in data if g[0]["d"] == 9)
    assert len(grouped_by_9) == 3
    grouped_by_10 = next(g[1] for g in data if g[0]["d"] == 10)
    assert len(grouped_by_10) == 1

    data = list(mongostore_facade.groupby(["e", "d"]))
    assert len(data) == 3


def test_multistore_remove_docs(multistore, mongostore):
    mongostore_facade = StoreFacade(mongostore, multistore)

    mongostore_facade._collection.insert_one({"a": 1, "b": 2, "c": 3})
    mongostore_facade._collection.insert_one({"a": 4, "d": 5, "e": 6, "g": {"h": 1}})
    mongostore_facade.remove_docs({"a": 1})
    assert len(list(mongostore_facade.query({"a": 4}))) == 1
    assert len(list(mongostore_facade.query({"a": 1}))) == 0


def test_multistore_connect_reconnect(multistore, mongostore):
    mongostore_facade = StoreFacade(mongostore, multistore)

    assert isinstance(mongostore_facade._collection, pymongo.collection.Collection)
    mongostore_facade.close()
    assert mongostore_facade._coll is None
    mongostore_facade.connect()

    # Test using the multistore to close connections
    multistore.close_all()
    assert mongostore_facade._coll is None
    multistore.connect_all()
    assert isinstance(mongostore_facade._collection, pymongo.collection.Collection)


def test_multistore_name(multistore, mongostore):
    mongostore_facade = StoreFacade(mongostore, multistore)

    assert mongostore_facade.name == "mongo://localhost/maggma_test/test"


def test_multistore_ensure_index(multistore, mongostore):
    mongostore_facade = StoreFacade(mongostore, multistore)
    assert mongostore_facade.ensure_index("test_key")
    # TODO: How to check for exception?
