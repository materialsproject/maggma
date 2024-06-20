"""
Tests for MapBuilder
"""

from datetime import datetime, timedelta

import pytest

from maggma.builders import CopyBuilder
from maggma.stores import MemoryStore


@pytest.fixture()
def source():
    store = MemoryStore("source", key="k", last_updated_field="lu")
    store.connect()
    store.ensure_index("k")
    store.ensure_index("lu")
    return store


@pytest.fixture()
def target():
    store = MemoryStore("target", key="k", last_updated_field="lu")
    store.connect()
    store.ensure_index("k")
    store.ensure_index("lu")
    return store


@pytest.fixture(scope="module")
def now():
    return datetime.utcnow()


@pytest.fixture()
def old_docs(now):
    return [{"lu": now, "k": k, "v": "old"} for k in range(20)]


@pytest.fixture()
def new_docs(now):
    toc = now + timedelta(seconds=1)
    return [{"lu": toc, "k": k, "v": "new"} for k in range(10)]


@pytest.fixture()
def some_failed_old_docs(now):
    docs = [{"lu": now, "k": k, "v": "old", "state": "failed"} for k in range(3)]
    docs.extend([{"lu": now, "k": k, "v": "old", "state": "failed"} for k in range(18, 20)])
    return docs


def test_get_items(source, target, old_docs, some_failed_old_docs):
    builder = CopyBuilder(source, target)
    source.update(old_docs)
    assert len(list(builder.get_items())) == len(old_docs)

    target.update(old_docs)
    assert len(list(builder.get_items())) == 0

    builder = CopyBuilder(source, target, projection=["k"])
    target.remove_docs({})
    assert len(list(builder.get_items())) == len(old_docs)
    assert all("v" not in d for d in builder.get_items())

    source.update(some_failed_old_docs)
    target.update(old_docs)
    target.update(some_failed_old_docs)
    builder = CopyBuilder(source, target)

    assert len(list(builder.get_items())) == 0

    builder = CopyBuilder(source, target, retry_failed=True)
    assert len(list(builder.get_items())) == len(some_failed_old_docs)

    builder = CopyBuilder(source, target, query={"k": {"$lt": 11}})
    assert len(list(builder.get_items())) == 0

    builder = CopyBuilder(source, target, retry_failed=True, query={"k": {"$lt": 11}})
    assert len(list(builder.get_items())) == 3


def test_process_item(source, target, old_docs):
    builder = CopyBuilder(source, target)
    source.update(old_docs)
    items = list(builder.get_items())
    assert len(items) == len(list(map(builder.process_item, items)))


def test_update_targets(source, target, old_docs, new_docs):
    builder = CopyBuilder(source, target)
    builder.update_targets(old_docs)
    builder.update_targets(new_docs)
    assert target.query_one(criteria={"k": 0})["v"] == "new"
    assert target.query_one(criteria={"k": 10})["v"] == "old"


def test_run(source, target, old_docs, new_docs):
    source.update(old_docs)
    source.update(new_docs)
    target.update(old_docs)

    builder = CopyBuilder(source, target)
    builder.run()
    builder.target.connect()
    assert builder.target.query_one(criteria={"k": 0})["v"] == "new"
    assert builder.target.query_one(criteria={"k": 10})["v"] == "old"


def test_query(source, target, old_docs, new_docs):
    builder = CopyBuilder(source, target)
    builder.query = {"k": {"$gt": 5}}
    source.update(old_docs)
    source.update(new_docs)
    builder.run()
    all_docs = list(target.query(criteria={}))
    assert len(all_docs) == 14
    assert min([d["k"] for d in all_docs]) == 6


def test_delete_orphans(source, target, old_docs, new_docs):
    builder = CopyBuilder(source, target, delete_orphans=True)
    source.update(old_docs)
    source.update(new_docs)
    target.update(old_docs)

    deletion_criteria = {"k": {"$in": list(range(5))}}
    source._collection.delete_many(deletion_criteria)
    builder.run()

    assert target._collection.count_documents(deletion_criteria) == 0
    assert target.query_one(criteria={"k": 5})["v"] == "new"
    assert target.query_one(criteria={"k": 10})["v"] == "old"


def test_prechunk(source, target, old_docs, new_docs):
    builder = CopyBuilder(source, target, delete_orphans=True)
    source.update(old_docs)
    source.update(new_docs)

    chunk_queries = list(builder.prechunk(2))
    assert len(chunk_queries) == 2
    assert chunk_queries[0] == {"query": {"k": {"$in": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}}}
