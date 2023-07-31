from datetime import datetime
from itertools import chain

import pytest
from maggma.stores import ConcatStore, JointStore, MemoryStore, MongoStore
from pydash import get


@pytest.fixture()
def mongostore():
    store = MongoStore("magmma_test", "test")
    store.connect()
    yield store
    store._collection.drop()


@pytest.fixture(scope="module")
def jointstore_test1():
    store = MongoStore("maggma_test", "test1")
    store.connect()
    yield store
    store._collection.drop()


@pytest.fixture(scope="module")
def jointstore_test2():
    store = MongoStore("maggma_test", "test2")
    store.connect()
    yield store
    store._collection.drop()


@pytest.fixture(scope="module")
def jointstore(jointstore_test1, jointstore_test2):
    jointstore_test1.update(
        [
            {
                "task_id": k,
                "my_prop": k + 1,
                "last_updated": datetime.utcnow(),
                "category": k // 5,
            }
            for k in range(10)
        ]
    )

    jointstore_test2.update(
        [
            {
                "task_id": 2 * k,
                "your_prop": k + 3,
                "last_updated": datetime.utcnow(),
                "category2": k // 3,
            }
            for k in range(5)
        ]
    )

    store = JointStore("maggma_test", ["test1", "test2"])
    store.connect()

    return store


def test_joint_store_count(jointstore):
    assert jointstore.count() == 10
    assert jointstore.count({"test2.category2": {"$exists": 1}}) == 5


def test_joint_store_query(jointstore):
    # Test query all
    docs = list(jointstore.query())
    assert len(docs) == 10
    docs_w_field = [d for d in docs if "test2" in d]
    assert len(docs_w_field) == 5
    docs_w_field = sorted(docs_w_field, key=lambda x: x["task_id"])
    assert docs_w_field[0]["test2"]["your_prop"] == 3
    assert docs_w_field[0]["task_id"] == 0
    assert docs_w_field[0]["my_prop"] == 1


def test_joint_store_query_one(jointstore):
    doc = jointstore.query_one()
    assert doc["my_prop"] == doc["task_id"] + 1
    # Test limit properties
    doc = jointstore.query_one(properties=["test2", "task_id"])
    assert doc["test2"]["your_prop"] == doc["task_id"] + 3
    assert doc.get("my_prop") is None
    # Test criteria
    doc = jointstore.query_one(criteria={"task_id": {"$gte": 10}})
    assert doc is None
    doc = jointstore.query_one(criteria={"test2.your_prop": {"$gt": 6}})
    assert doc["task_id"] == 8

    # Test merge_at_root
    jointstore.merge_at_root = True

    # Test merging is working properly
    doc = jointstore.query_one(criteria={"task_id": 2})
    assert doc["my_prop"] == 3
    assert doc["your_prop"] == 4

    # Test merging is allowing for subsequent match
    doc = jointstore.query_one(criteria={"your_prop": {"$gt": 6}})
    assert doc["task_id"] == 8


@pytest.mark.xfail(reason="key grouping appears to make lists")
def test_joint_store_distinct(jointstore):
    your_prop = jointstore.distinct("test2.your_prop")
    assert set(your_prop) == {k + 3 for k in range(5)}
    my_prop = jointstore.distinct("my_prop")
    assert set(my_prop) == {k + 1 for k in range(10)}
    my_prop_cond = jointstore.distinct("my_prop", {"test2.your_prop": {"$gte": 5}})
    assert set(my_prop_cond), {5, 7 == 9}


def test_joint_store_last_updated(jointstore, jointstore_test1, jointstore_test2):
    test1 = jointstore_test1
    test2 = jointstore_test2
    doc = jointstore.query_one({"task_id": 0})
    test1doc = test1.query_one({"task_id": 0})
    test2doc = test2.query_one({"task_id": 0})
    assert test1doc["last_updated"] == doc["last_updated"]
    assert test2doc["last_updated"] != doc["last_updated"]
    # Swap the two
    test2date = test2doc["last_updated"]
    test2doc["last_updated"] = test1doc["last_updated"]
    test1doc["last_updated"] = test2date
    test1.update([test1doc])
    test2.update([test2doc])
    doc = jointstore.query_one({"task_id": 0})
    test1doc = test1.query_one({"task_id": 0})
    test2doc = test2.query_one({"task_id": 0})
    assert test1doc["last_updated"] == doc["last_updated"]
    assert test2doc["last_updated"] != doc["last_updated"]
    # Check also that still has a field if no task2 doc
    doc = jointstore.query_one({"task_id": 1})
    assert doc["last_updated"] is not None


def test_joint_store_groupby(jointstore):
    docs = list(jointstore.groupby("category"))
    assert len(docs[0][1]) == 5
    assert len(docs[1][1]) == 5
    docs = list(jointstore.groupby("test2.category2"))

    none_docs = next(d for d in docs if get(d[0], "test2.category2") == [])
    one_docs = next(d for d in docs if get(d[0], "test2.category2") == [1])
    zero_docs = next(d for d in docs if get(d[0], "test2.category2") == [0])
    assert len(none_docs[1]) == 5
    assert len(one_docs[1]) == 2
    assert len(zero_docs[1]) == 3


def test_joint_update(jointstore):
    with pytest.raises(NotImplementedError):
        jointstore.update({})


def test_joint_remove_docs(jointstore):
    with pytest.raises(NotImplementedError):
        jointstore.remove_docs({})


@pytest.fixture()
def concat_store():
    mem_stores = [MemoryStore(str(i)) for i in range(4)]
    store = ConcatStore(mem_stores)
    store.connect()

    index = 0

    props = {i: str(i) for i in range(10)}
    for mem_store in mem_stores:
        docs = [{"task_id": i, "prop": props[i - index], "index": index} for i in range(index, index + 10)]
        index = index + 10
        mem_store.update(docs)
    return store


def test_concat_store_distinct(concat_store):
    docs = list(concat_store.distinct("task_id"))
    actual_docs = list(chain.from_iterable([store.distinct("task_id") for store in concat_store.stores]))
    assert len(docs) == len(actual_docs)
    assert set(docs) == set(actual_docs)


def test_concat_store_groupby(concat_store):
    assert len(list(concat_store.groupby("index"))) == 4
    assert len(list(concat_store.groupby("task_id"))) == 40


def test_concat_store_count(concat_store):
    assert concat_store.count() == 40
    assert concat_store.count({"prop": "3"}) == 4


def test_concat_store_query(concat_store):
    docs = list(concat_store.query(properties=["task_id"]))
    t_ids = [d["task_id"] for d in docs]
    assert len(t_ids) == len(set(t_ids))
    assert len(t_ids) == 40


def test_eq(mongostore, jointstore, concat_store):
    assert jointstore == jointstore
    assert concat_store == concat_store

    assert mongostore != jointstore
    assert mongostore != concat_store
    assert jointstore != concat_store


def test_serialize(concat_store):
    d = concat_store.as_dict()
    new_concat_store = ConcatStore.from_dict(d)

    assert len(new_concat_store.stores) == len(concat_store.stores)
