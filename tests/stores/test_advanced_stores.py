"""
Tests for advanced stores
"""
import os
import shutil
import signal
import subprocess
import tempfile
import time
from unittest.mock import patch
from uuid import uuid4

import pytest
from maggma.core import StoreError
from maggma.stores import AliasingStore, MemoryStore, MongograntStore, MongoStore, SandboxStore, VaultStore
from maggma.stores.advanced_stores import substitute
from mongogrant import Client
from mongogrant.client import check, seed
from mongogrant.config import Config
from pymongo import MongoClient
from pymongo.collection import Collection


@pytest.fixture()
def mongostore():
    store = MongoStore("maggma_test", "test")
    store.connect()
    yield store
    store._collection.drop()


@pytest.fixture(scope="module")
def mgrant_server():
    # TODO: This is whacked code that starts a mongo server. How do we fix this?
    _, config_path = tempfile.mkstemp()
    _, mdlogpath = tempfile.mkstemp()
    mdpath = tempfile.mkdtemp()
    mdport = 27020
    if not os.getenv("CONTINUOUS_INTEGRATION"):
        basecmd = f"mongod --port {mdport} --dbpath {mdpath} --quiet --logpath {mdlogpath} --bind_ip_all --auth"
        mongod_process = subprocess.Popen(basecmd, shell=True, start_new_session=True)
        time.sleep(5)
        client = MongoClient(port=mdport)
        client.admin.command("createUser", "mongoadmin", pwd="mongoadminpass", roles=["root"])
        client.close()
    else:
        pytest.skip("Disabling mongogrant tests on CI for now")
    dbname = "test_" + uuid4().hex
    db = MongoClient(f"mongodb://mongoadmin:mongoadminpass@127.0.0.1:{mdport}/admin")[dbname]
    db.command("createUser", "reader", pwd="readerpass", roles=["read"])
    db.command("createUser", "writer", pwd="writerpass", roles=["readWrite"])
    db.client.close()

    # Yields the fixture to use
    yield config_path, mdport, dbname

    if not (os.getenv("CONTINUOUS_INTEGRATION") and os.getenv("TRAVIS")):
        os.killpg(os.getpgid(mongod_process.pid), signal.SIGTERM)
        os.waitpid(mongod_process.pid, 0)
    os.remove(config_path)
    shutil.rmtree(mdpath)
    os.remove(mdlogpath)


@pytest.fixture(scope="module")
def mgrant_user(mgrant_server):
    config_path, mdport, dbname = mgrant_server

    config = Config(check=check, path=config_path, seed=seed())
    client = Client(config)
    client.set_auth(
        host=f"localhost:{mdport}",
        db=dbname,
        role="read",
        username="reader",
        password="readerpass",
    )
    client.set_auth(
        host=f"localhost:{mdport}",
        db=dbname,
        role="readWrite",
        username="writer",
        password="writerpass",
    )
    client.set_alias("testhost", f"localhost:{mdport}", which="host")
    client.set_alias("testdb", dbname, which="db")

    return client


def connected_user(store):
    return store._collection.database.command("connectionStatus")["authInfo"]["authenticatedUsers"][0]["user"]


def test_mgrant_init():
    with pytest.raises(StoreError):
        store = MongograntStore("", "", username="")

    with pytest.raises(ValueError):  # noqa: PT012
        store = MongograntStore("", "")
        store.connect()


def test_mgrant_connect(mgrant_server, mgrant_user):
    config_path, mdport, dbname = mgrant_server
    assert mgrant_user is not None
    store = MongograntStore("ro:testhost/testdb", "tasks", mgclient_config_path=config_path)
    store.connect()
    assert isinstance(store._collection, Collection)
    assert connected_user(store) == "reader"
    store = MongograntStore("rw:testhost/testdb", "tasks", mgclient_config_path=config_path)
    store.connect()
    assert isinstance(store._collection, Collection)
    assert connected_user(store) == "writer"


def test_mgrant_differences():
    with pytest.raises(ValueError):
        MongograntStore.from_db_file("")

    with pytest.raises(ValueError):
        MongograntStore.from_collection("")


def test_mgrant_equal(mgrant_server, mgrant_user):
    config_path, mdport, dbname = mgrant_server
    assert mgrant_user is not None
    store1 = MongograntStore("ro:testhost/testdb", "tasks", mgclient_config_path=config_path)
    store1.connect()
    store2 = MongograntStore("ro:testhost/testdb", "tasks", mgclient_config_path=config_path)
    store3 = MongograntStore("ro:testhost/testdb", "test", mgclient_config_path=config_path)
    store2.connect()
    assert store1 == store2
    assert store1 != store3


def vault_store():
    with patch("hvac.Client") as mock:
        instance = mock.return_value
        instance.auth_github.return_value = True
        instance.is_authenticated.return_value = True
        instance.read.return_value = {
            "wrap_info": None,
            "request_id": "2c72c063-2452-d1cd-19a2-91163c7395f7",
            "data": {
                "value": '{"db": "mg_core_prod", "host": "matgen2.lbl.gov", "username": "test", "password": "pass"}'
            },
            "auth": None,
            "warnings": None,
            "renewable": False,
            "lease_duration": 2764800,
            "lease_id": "",
        }
        return VaultStore("test_coll", "secret/matgen/maggma")


def test_vault_init():
    """
    Test initing a vault store using a mock hvac client
    """
    os.environ["VAULT_ADDR"] = "https://fake:8200/"
    os.environ["VAULT_TOKEN"] = "dummy"

    # Just test that we successfully instantiated
    v = vault_store()
    assert isinstance(v, MongoStore)


def test_vault_github_token():
    """
    Test using VaultStore with GITHUB_TOKEN and mock hvac
    """
    # Save token in env
    os.environ["VAULT_ADDR"] = "https://fake:8200/"
    os.environ["GITHUB_TOKEN"] = "dummy"

    v = vault_store()
    # Just test that we successfully instantiated
    assert isinstance(v, MongoStore)


def test_vault_missing_env():
    """
    Test VaultStore should raise an error if environment is not set
    """
    del os.environ["VAULT_TOKEN"]
    del os.environ["VAULT_ADDR"]
    del os.environ["GITHUB_TOKEN"]

    # Create should raise an error
    with pytest.raises(RuntimeError):
        vault_store()


@pytest.fixture()
def alias_store():
    memorystore = MemoryStore("test")
    memorystore.connect()
    return AliasingStore(memorystore, {"a": "b", "c.d": "e", "f": "g.h"})


def test_alias_count(alias_store):
    d = [{"b": 1}, {"e": 2}, {"g": {"h": 3}}]
    alias_store.store._collection.insert_many(d)
    assert alias_store.count({"a": 1}) == 1


def test_aliasing_query(alias_store):
    d = [{"b": 1}, {"e": 2}, {"g": {"h": 3}}]
    alias_store.store._collection.insert_many(d)

    assert "a" in next(iter(alias_store.query(criteria={"a": {"$exists": 1}})))
    assert "c" in next(iter(alias_store.query(criteria={"c.d": {"$exists": 1}})))
    assert "d" in next(iter(alias_store.query(criteria={"c.d": {"$exists": 1}}))).get("c", {})
    assert "f" in next(iter(alias_store.query(criteria={"f": {"$exists": 1}})))


def test_aliasing_update(alias_store):
    alias_store.update(
        [
            {"task_id": "mp-3", "a": 4},
            {"task_id": "mp-4", "c": {"d": 5}},
            {"task_id": "mp-5", "f": 6},
        ]
    )
    assert next(iter(alias_store.query(criteria={"task_id": "mp-3"})))["a"] == 4
    assert next(iter(alias_store.query(criteria={"task_id": "mp-4"})))["c"]["d"] == 5
    assert next(iter(alias_store.query(criteria={"task_id": "mp-5"})))["f"] == 6

    assert next(iter(alias_store.store.query(criteria={"task_id": "mp-3"})))["b"] == 4
    assert next(iter(alias_store.store.query(criteria={"task_id": "mp-4"})))["e"] == 5

    assert next(iter(alias_store.store.query(criteria={"task_id": "mp-5"})))["g"]["h"] == 6


def test_aliasing_remove_docs(alias_store):
    alias_store.update(
        [
            {"task_id": "mp-3", "a": 4},
            {"task_id": "mp-4", "c": {"d": 5}},
            {"task_id": "mp-5", "f": 6},
        ]
    )
    assert alias_store.query_one(criteria={"task_id": "mp-3"})
    assert alias_store.query_one(criteria={"task_id": "mp-4"})
    assert alias_store.query_one(criteria={"task_id": "mp-5"})

    alias_store.remove_docs({"a": 4})
    assert alias_store.query_one(criteria={"task_id": "mp-3"}) is None


def test_aliasing_substitute(alias_store):
    aliases = {"a": "b", "c.d": "e", "f": "g.h"}

    d = {"b": 1}
    substitute(d, aliases)
    assert "a" in d

    d = {"e": 1}
    substitute(d, aliases)
    assert "c" in d
    assert "d" in d.get("c", {})

    d = {"g": {"h": 4}}
    substitute(d, aliases)
    assert "f" in d

    d = None
    substitute(d, aliases)
    assert d is None


def test_aliasing_distinct(alias_store):
    d = [{"b": 1}, {"e": 2}, {"g": {"h": 3}}]
    alias_store.store._collection.insert_many(d)

    assert alias_store.distinct("a") == [1]
    assert alias_store.distinct("c.d") == [2]
    assert alias_store.distinct("f") == [3]


@pytest.fixture()
def sandbox_store():
    memstore = MemoryStore()
    store = SandboxStore(memstore, sandbox="test")
    store.connect()
    return store


def test_sandbox_count(sandbox_store):
    sandbox_store._collection.insert_one({"a": 1, "b": 2, "c": 3})
    assert sandbox_store.count({"a": 1}) == 1

    sandbox_store._collection.insert_one({"a": 1, "b": 3, "sbxn": ["test"]})
    assert sandbox_store.count({"a": 1}) == 2


def test_sandbox_query(sandbox_store):
    sandbox_store._collection.insert_one({"a": 1, "b": 2, "c": 3})
    assert sandbox_store.query_one(properties=["a"])["a"] == 1

    sandbox_store._collection.insert_one({"a": 2, "b": 2, "sbxn": ["test"]})
    assert sandbox_store.query_one(properties=["b"], criteria={"a": 2})["b"] == 2

    sandbox_store._collection.insert_one({"a": 3, "b": 2, "sbxn": ["not_test"]})
    assert sandbox_store.query_one(properties=["c"], criteria={"a": 3}) is None


def test_sandbox_distinct(sandbox_store):
    sandbox_store.connect()
    sandbox_store._collection.insert_one({"a": 1, "b": 2, "c": 3})
    assert sandbox_store.distinct("a") == [1]

    sandbox_store._collection.insert_one({"a": 4, "d": 5, "e": 6, "sbxn": ["test"]})
    assert sandbox_store.distinct("a")[1] == 4

    sandbox_store._collection.insert_one({"a": 7, "d": 8, "e": 9, "sbxn": ["not_test"]})
    assert sandbox_store.distinct("a")[1] == 4


def test_sandbox_update(sandbox_store):
    sandbox_store.connect()
    sandbox_store.update([{"e": 6, "d": 4}], key="e")
    assert next(sandbox_store.query(criteria={"d": {"$exists": 1}}, properties=["d"]))["d"] == 4
    assert sandbox_store._collection.find_one({"e": 6})["sbxn"] == ["test"]
    sandbox_store.update([{"e": 7, "sbxn": ["core"]}], key="e")
    assert set(sandbox_store.query_one(criteria={"e": 7})["sbxn"]) == {"test", "core"}


def test_sandbox_remove_docs(sandbox_store):
    sandbox_store.connect()
    sandbox_store.update([{"e": 6, "d": 4}], key="e")
    sandbox_store.update([{"e": 7, "sbxn": ["core"]}], key="e")

    assert sandbox_store.query_one(criteria={"d": {"$exists": 1}}, properties=["d"])
    assert sandbox_store.query_one(criteria={"e": 7})
    sandbox_store.remove_docs(criteria={"d": 4})

    assert sandbox_store.query_one(criteria={"d": {"$exists": 1}}, properties=["d"]) is None
    assert sandbox_store.query_one(criteria={"e": 7})


@pytest.fixture()
def mgrantstore(mgrant_server, mgrant_user):
    config_path, mdport, dbname = mgrant_server
    assert mgrant_user is not None
    store = MongograntStore("ro:testhost/testdb", "tasks", mgclient_config_path=config_path)
    store.connect()

    return store


@pytest.fixture()
def vaultstore():
    os.environ["VAULT_ADDR"] = "https://fake:8200/"
    os.environ["VAULT_TOKEN"] = "dummy"

    # Just test that we successfully instantiated
    return vault_store()


def test_eq_mgrant(mgrantstore, mongostore):
    assert mgrantstore == mgrantstore
    assert mgrantstore != mongostore


def test_eq(vaultstore, alias_store, sandbox_store):
    assert alias_store == alias_store
    assert sandbox_store == sandbox_store
    assert vaultstore == vaultstore

    assert sandbox_store != alias_store
    assert alias_store != vaultstore
    assert vaultstore != sandbox_store
