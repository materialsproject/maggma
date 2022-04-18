import os
import shutil
from datetime import datetime
from unittest import mock
from pathlib import Path

import mongomock.collection
from monty.tempfile import ScratchDir
import pymongo.collection
import pytest
from pymongo.errors import ConfigurationError, DocumentTooLarge, OperationFailure

import maggma.stores
from maggma.core import StoreError
from maggma.stores import JSONStore, MemoryStore, MongoStore, MongoURIStore
from maggma.stores.mongolike import MontyStore
from maggma.validators import JSONSchemaValidator


@pytest.fixture
def mongostore():
    store = MongoStore(
        database="maggma_test",
        collection_name="test",
    )
    store.connect()
    yield store
    store._collection.drop()


@pytest.fixture
def montystore(tmp_dir):
    store = MontyStore("maggma_test")
    store.connect()
    return store


@pytest.fixture
def memorystore():
    store = MemoryStore()
    store.connect()
    return store


@pytest.fixture
def jsonstore(test_dir):

    files = []
    for f in ["a.json", "b.json"]:
        files.append(test_dir / "test_set" / f)

    return JSONStore(files)


@pytest.mark.xfail(raises=StoreError)
def test_mongostore_connect_error():
    mongostore = MongoStore("maggma_test", "test")
    mongostore.count()


def test_mongostore_connect_reconnect():
    mongostore = MongoStore("maggma_test", "test")
    assert mongostore._coll is None
    mongostore.connect()
    assert isinstance(mongostore._collection, pymongo.collection.Collection)
    mongostore.close()
    assert mongostore._coll is None
    mongostore.connect()


def test_mongostore_query(mongostore):
    mongostore._collection.insert_one({"a": 1, "b": 2, "c": 3})
    assert mongostore.query_one(properties=["a"])["a"] == 1
    assert mongostore.query_one(properties=["a"])["a"] == 1
    assert mongostore.query_one(properties=["b"])["b"] == 2
    assert mongostore.query_one(properties=["c"])["c"] == 3


def test_mongostore_count(mongostore):
    mongostore._collection.insert_one({"a": 1, "b": 2, "c": 3})
    assert mongostore.count() == 1
    mongostore._collection.insert_one({"aa": 1, "b": 2, "c": 3})
    assert mongostore.count() == 2
    assert mongostore.count({"a": 1}) == 1


def test_mongostore_distinct(mongostore):
    mongostore._collection.insert_one({"a": 1, "b": 2, "c": 3})
    mongostore._collection.insert_one({"a": 4, "d": 5, "e": 6, "g": {"h": 1}})
    assert set(mongostore.distinct("a")) == {1, 4}

    # Test list distinct functionality
    mongostore._collection.insert_one({"a": 4, "d": 6, "e": 7})
    mongostore._collection.insert_one({"a": 4, "d": 6, "g": {"h": 2}})

    # Test distinct subdocument functionality
    ghs = mongostore.distinct("g.h")
    assert set(ghs) == {1, 2}

    # Test when key doesn't exist
    assert mongostore.distinct("blue") == []

    # Test when null is a value
    mongostore._collection.insert_one({"i": None})
    assert mongostore.distinct("i") == [None]

    # Test to make sure DocumentTooLarge errors get dealt with properly using built in distinct
    mongostore._collection.insert_many([{"key": [f"mp-{i}"]} for i in range(1000000)])
    vals = mongostore.distinct("key")
    # Test to make sure distinct on array field is unraveled when using manual distinct
    assert len(vals) == len(list(range(1000000)))
    assert all([isinstance(v, str) for v in vals])

    # Test to make sure manual distinct uses the criteria query
    mongostore._collection.insert_many(
        [{"key": f"mp-{i}", "a": 2} for i in range(1000001, 2000001)]
    )
    vals = mongostore.distinct("key", {"a": 2})
    assert len(vals) == len(list(range(1000001, 2000001)))


def test_mongostore_update(mongostore):
    mongostore.update({"e": 6, "d": 4}, key="e")
    assert (
        mongostore.query_one(criteria={"d": {"$exists": 1}}, properties=["d"])["d"] == 4
    )

    mongostore.update([{"e": 7, "d": 8, "f": 9}], key=["d", "f"])
    assert mongostore.query_one(criteria={"d": 8, "f": 9}, properties=["e"])["e"] == 7

    mongostore.update([{"e": 11, "d": 8, "f": 9}], key=["d", "f"])
    assert mongostore.query_one(criteria={"d": 8, "f": 9}, properties=["e"])["e"] == 11

    test_schema = {
        "type": "object",
        "properties": {"e": {"type": "integer"}},
        "required": ["e"],
    }
    mongostore.validator = JSONSchemaValidator(schema=test_schema)
    mongostore.update({"e": 100, "d": 3}, key="e")

    # Continue to update doc when validator is not set to strict mode
    mongostore.update({"e": "abc", "d": 3}, key="e")

    # ensure safe_update works to not throw DocumentTooLarge errors
    large_doc = {f"mp-{i}": f"mp-{i}" for i in range(1000000)}
    large_doc["e"] = 999
    with pytest.raises((OperationFailure, DocumentTooLarge)):
        mongostore.update([large_doc, {"e": 1001}], key="e")

    mongostore.safe_update = True
    mongostore.update([large_doc, {"e": 1001}], key="e")
    assert mongostore.query_one({"e": 1001}) is not None


def test_mongostore_groupby(mongostore):
    mongostore.update(
        [
            {"e": 7, "d": 9, "f": 9},
            {"e": 7, "d": 9, "f": 10},
            {"e": 8, "d": 9, "f": 11},
            {"e": 9, "d": 10, "f": 12},
        ],
        key="f",
    )
    data = list(mongostore.groupby("d"))
    assert len(data) == 2
    grouped_by_9 = [g[1] for g in data if g[0]["d"] == 9][0]
    assert len(grouped_by_9) == 3
    grouped_by_10 = [g[1] for g in data if g[0]["d"] == 10][0]
    assert len(grouped_by_10) == 1

    data = list(mongostore.groupby(["e", "d"]))
    assert len(data) == 3


def test_mongostore_remove_docs(mongostore):
    mongostore._collection.insert_one({"a": 1, "b": 2, "c": 3})
    mongostore._collection.insert_one({"a": 4, "d": 5, "e": 6, "g": {"h": 1}})
    mongostore.remove_docs({"a": 1})
    assert len(list(mongostore.query({"a": 4}))) == 1
    assert len(list(mongostore.query({"a": 1}))) == 0


def test_mongostore_from_db_file(mongostore, db_json):
    ms = MongoStore.from_db_file(db_json)
    ms.connect()
    assert ms._collection.full_name == "maggma_tests.tmp"


def test_mongostore_from_launchpad_file(lp_file):
    ms = MongoStore.from_launchpad_file(lp_file, collection_name="tmp")
    ms.connect()
    assert ms._collection.full_name == "maggma_tests.tmp"


def test_mongostore_from_collection(mongostore, db_json):
    ms = MongoStore.from_db_file(db_json)
    ms.connect()

    other_ms = MongoStore.from_collection(ms._collection)
    assert ms._coll.full_name == other_ms._collection.full_name
    assert ms.database == other_ms.database


def test_mongostore_name(mongostore):
    assert mongostore.name == "mongo://localhost/maggma_test/test"


def test_ensure_index(mongostore):
    assert mongostore.ensure_index("test_key")
    # TODO: How to check for exception?


def test_mongostore_last_updated(mongostore):
    assert mongostore.last_updated == datetime.min
    start_time = datetime.utcnow()
    mongostore._collection.insert_one({mongostore.key: 1, "a": 1})
    with pytest.raises(StoreError) as cm:
        mongostore.last_updated
    assert cm.match(mongostore.last_updated_field)
    mongostore.update(
        [{mongostore.key: 1, "a": 1, mongostore.last_updated_field: datetime.utcnow()}]
    )
    assert mongostore.last_updated > start_time


def test_mongostore_newer_in(mongostore):
    target = MongoStore("maggma_test", "test_target")
    target.connect()

    # make sure docs are newer in mongostore then target and check updated_keys

    target.update(
        [
            {mongostore.key: i, mongostore.last_updated_field: datetime.utcnow()}
            for i in range(10)
        ]
    )

    # Update docs in source
    mongostore.update(
        [
            {mongostore.key: i, mongostore.last_updated_field: datetime.utcnow()}
            for i in range(10)
        ]
    )

    assert len(target.newer_in(mongostore)) == 10
    assert len(target.newer_in(mongostore, exhaustive=True)) == 10
    assert len(mongostore.newer_in(target)) == 0

    target._collection.drop()


# Memory store tests
def test_memory_store_connect():
    memorystore = MemoryStore()
    assert memorystore._coll is None
    memorystore.connect()
    assert isinstance(memorystore._collection, mongomock.collection.Collection)


def test_groupby(memorystore):
    memorystore.update(
        [
            {"e": 7, "d": 9, "f": 9},
            {"e": 7, "d": 9, "f": 10},
            {"e": 8, "d": 9, "f": 11},
            {"e": 9, "d": 10, "f": 12},
        ],
        key="f",
    )
    data = list(memorystore.groupby("d", properties={"e": 1, "f": 1}))
    assert len(data) == 2
    grouped_by_9 = [g[1] for g in data if g[0]["d"] == 9][0]
    assert len(grouped_by_9) == 3
    assert all([d.get("f", False) for d in grouped_by_9])
    assert all([d.get("e", False) for d in grouped_by_9])
    grouped_by_10 = [g[1] for g in data if g[0]["d"] == 10][0]
    assert len(grouped_by_10) == 1

    data = list(memorystore.groupby(["e", "d"]))
    assert len(data) == 3

    memorystore.update(
        [
            {"e": {"d": 9}, "f": 9},
            {"e": {"d": 9}, "f": 10},
            {"e": {"d": 9}, "f": 11},
            {"e": {"d": 10}, "f": 12},
        ],
        key="f",
    )
    data = list(memorystore.groupby("e.d", properties=["f"]))
    assert len(data) == 2
    assert data[0][1][0].get("f", False)


# Monty store tests
def test_monty_store_connect(tmp_dir):
    montystore = MontyStore(collection_name="my_collection")
    assert montystore._coll is None
    montystore.connect()
    assert montystore._collection is not None


def test_monty_store_groupby(montystore):
    montystore.update(
        [
            {"e": 7, "d": 9, "f": 9},
            {"e": 7, "d": 9, "f": 10},
            {"e": 8, "d": 9, "f": 11},
            {"e": 9, "d": 10, "f": 12},
        ],
        key="f",
    )
    data = list(montystore.groupby("d"))
    assert len(data) == 2
    grouped_by_9 = [g[1] for g in data if g[0]["d"] == 9][0]
    assert len(grouped_by_9) == 3
    grouped_by_10 = [g[1] for g in data if g[0]["d"] == 10][0]
    assert len(grouped_by_10) == 1

    data = list(montystore.groupby(["e", "d"]))
    assert len(data) == 3

    montystore.update(
        [
            {"e": {"d": 9}, "f": 9},
            {"e": {"d": 9}, "f": 10},
            {"e": {"d": 9}, "f": 11},
            {"e": {"d": 10}, "f": 12},
        ],
        key="f",
    )
    data = list(montystore.groupby("e.d"))
    assert len(data) == 2


def test_montystore_query(montystore):
    montystore._collection.insert_one({"a": 1, "b": 2, "c": 3})
    assert montystore.query_one(properties=["a"])["a"] == 1
    assert montystore.query_one(properties=["a"])["a"] == 1
    assert montystore.query_one(properties=["b"])["b"] == 2
    assert montystore.query_one(properties=["c"])["c"] == 3


def test_montystore_count(montystore):
    montystore._collection.insert_one({"a": 1, "b": 2, "c": 3})
    assert montystore.count() == 1
    montystore._collection.insert_one({"aa": 1, "b": 2, "c": 3})
    assert montystore.count() == 2
    assert montystore.count({"a": 1}) == 1


def test_montystore_distinct(montystore):
    montystore._collection.insert_one({"a": 1, "b": 2, "c": 3})
    montystore._collection.insert_one({"a": 4, "d": 5, "e": 6, "g": {"h": 1}})
    assert set(montystore.distinct("a")) == {1, 4}

    # Test list distinct functionality
    montystore._collection.insert_one({"a": 4, "d": 6, "e": 7})
    montystore._collection.insert_one({"a": 4, "d": 6, "g": {"h": 2}})

    # Test distinct subdocument functionality
    ghs = montystore.distinct("g.h")
    assert set(ghs) == {1, 2}

    # Test when key doesn't exist
    assert montystore.distinct("blue") == []

    # Test when null is a value
    montystore._collection.insert_one({"i": None})
    assert montystore.distinct("i") == [None]


def test_montystore_update(montystore):
    montystore.update({"e": 6, "d": 4}, key="e")
    assert (
        montystore.query_one(criteria={"d": {"$exists": 1}}, properties=["d"])["d"] == 4
    )

    montystore.update([{"e": 7, "d": 8, "f": 9}], key=["d", "f"])
    assert montystore.query_one(criteria={"d": 8, "f": 9}, properties=["e"])["e"] == 7

    montystore.update([{"e": 11, "d": 8, "f": 9}], key=["d", "f"])
    assert montystore.query_one(criteria={"d": 8, "f": 9}, properties=["e"])["e"] == 11

    test_schema = {
        "type": "object",
        "properties": {"e": {"type": "integer"}},
        "required": ["e"],
    }
    montystore.validator = JSONSchemaValidator(schema=test_schema)
    montystore.update({"e": 100, "d": 3}, key="e")

    # Continue to update doc when validator is not set to strict mode
    montystore.update({"e": "abc", "d": 3}, key="e")


def test_montystore_remove_docs(montystore):
    montystore._collection.insert_one({"a": 1, "b": 2, "c": 3})
    montystore._collection.insert_one({"a": 4, "d": 5, "e": 6, "g": {"h": 1}})
    montystore.remove_docs({"a": 1})
    assert len(list(montystore.query({"a": 4}))) == 1
    assert len(list(montystore.query({"a": 1}))) == 0


def test_json_store_load(jsonstore, test_dir):
    jsonstore.connect()
    assert len(list(jsonstore.query())) == 20

    jsonstore = JSONStore(test_dir / "test_set" / "c.json.gz")
    jsonstore.connect()
    assert len(list(jsonstore.query())) == 20

    # confirm descriptive error raised if you get a KeyError
    with pytest.raises(KeyError, match="Key field 'random_key' not found"):
        jsonstore = JSONStore(test_dir / "test_set" / "c.json.gz", key="random_key")
        jsonstore.connect()

    # if the .json does not exist, it should be created
    with pytest.warns(DeprecationWarning, match="file_writable is deprecated"):
        jsonstore = JSONStore("a.json", file_writable=False)
        assert jsonstore.read_only is True


def test_json_store_writeable(test_dir):
    with ScratchDir("."):
        # if the .json does not exist, it should be created
        jsonstore = JSONStore("a.json", read_only=False)
        assert Path("a.json").exists()
        jsonstore.connect()
        # confirm RunTimeError with multiple paths
        with pytest.raises(RuntimeError, match="multiple JSON"):
            jsonstore = JSONStore(["a.json", "d.json"], read_only=False)
        shutil.copy(test_dir / "test_set" / "d.json", ".")
        jsonstore = JSONStore("d.json", read_only=False)
        jsonstore.connect()
        assert jsonstore.count() == 2
        jsonstore.update({"new": "hello", "task_id": 2})
        assert jsonstore.count() == 3
        jsonstore.close()

        # repeat the above with the deprecated file_writable kwarg
        # if the .json does not exist, it should be created
        with pytest.warns(UserWarning, match="Received conflicting keyword arguments"):
            jsonstore = JSONStore("a.json", file_writable=True)
            assert jsonstore.read_only is False
        assert Path("a.json").exists()
        jsonstore.connect()

        # confirm RunTimeError with multiple paths
        with pytest.raises(RuntimeError, match="multiple JSON"):
            jsonstore = JSONStore(["a.json", "d.json"], file_writable=True)
        shutil.copy(test_dir / "test_set" / "d.json", ".")
        jsonstore = JSONStore("d.json", file_writable=True)
        jsonstore.connect()
        assert jsonstore.count() == 2
        jsonstore.update({"new": "hello", "task_id": 2})
        assert jsonstore.count() == 3
        jsonstore.close()
        jsonstore = JSONStore("d.json", file_writable=True)
        jsonstore.connect()
        assert jsonstore.count() == 3
        jsonstore.remove_docs({"a": 5})
        assert jsonstore.count() == 2
        jsonstore.close()
        jsonstore = JSONStore("d.json", file_writable=True)
        jsonstore.connect()
        assert jsonstore.count() == 2
        jsonstore.close()
        with mock.patch(
            "maggma.stores.JSONStore.update_json_file"
        ) as update_json_file_mock:
            jsonstore = JSONStore("d.json", file_writable=False)
            jsonstore.connect()
            jsonstore.update({"new": "hello", "task_id": 5})
            assert jsonstore.count() == 3
            jsonstore.close()
            update_json_file_mock.assert_not_called()
        with mock.patch(
            "maggma.stores.JSONStore.update_json_file"
        ) as update_json_file_mock:
            jsonstore = JSONStore("d.json", file_writable=False)
            jsonstore.connect()
            jsonstore.remove_docs({"task_id": 5})
            assert jsonstore.count() == 2
            jsonstore.close()
            update_json_file_mock.assert_not_called()


def test_eq(mongostore, memorystore, jsonstore):
    assert mongostore == mongostore
    assert memorystore == memorystore
    assert jsonstore == jsonstore

    assert mongostore != memorystore
    assert mongostore != jsonstore
    assert memorystore != jsonstore


@pytest.mark.skipif(
    "mongodb+srv" not in os.environ.get("MONGODB_SRV_URI", ""),
    reason="requires special mongodb+srv URI",
)
def test_mongo_uri():
    uri = os.environ["MONGODB_SRV_URI"]
    store = MongoURIStore(uri, database="mp_core", collection_name="xas")
    store.connect()
    is_name = store.name is uri
    # This is try and keep the secret safe
    assert is_name


def test_mongo_uri_localhost():
    store = MongoURIStore("mongodb://localhost:27017/mp_core", collection_name="xas")
    store.connect()


def test_mongo_uri_dbname_parse():
    # test parsing dbname from uri
    uri_with_db = "mongodb://uuu:xxxx@host:27017/fake_db"
    store = MongoURIStore(uri_with_db, "test")
    assert store.database == "fake_db"

    uri_with_db = "mongodb://uuu:xxxx@host:27017/fake_db"
    store = MongoURIStore(uri_with_db, "test", database="fake_db2")
    assert store.database == "fake_db2"

    uri_with_db = "mongodb://uuu:xxxx@host:27017"
    with pytest.raises(ConfigurationError):
        MongoURIStore(uri_with_db, "test")
