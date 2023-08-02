"""
Tests for Projection_Builder
"""
import pytest
from maggma.builders.projection_builder import Projection_Builder
from maggma.stores import MemoryStore


@pytest.fixture()
def source1():
    store = MemoryStore("source1", key="k", last_updated_field="lu")
    store.connect()
    store.ensure_index("k")
    store.ensure_index("lu")
    store.update([{"k": k, "a": "a", "b": "b"} for k in range(10)])
    return store


@pytest.fixture()
def source2():
    store = MemoryStore("source2", key="k", last_updated_field="lu")
    store.connect()
    store.ensure_index("k")
    store.ensure_index("lu")
    store.update([{"k": k, "c": "c", "d": "d"} for k in range(15)])
    return store


@pytest.fixture()
def target():
    store = MemoryStore("target", key="k", last_updated_field="lu")
    store.connect()
    store.ensure_index("k")
    store.ensure_index("lu")
    return store


def test_get_items(source1, source2, target):
    builder = Projection_Builder(source_stores=[source1, source2], target_store=target)
    items = next(iter(builder.get_items()))
    assert len(items) == 25


def test_process_item(source1, source2, target):
    # test fields_to_project = empty dict and list
    builder = Projection_Builder(
        source_stores=[source1, source2],
        target_store=target,
        fields_to_project=[[], {}],
    )
    items = next(iter(builder.get_items()))
    outputs = builder.process_item(items)
    assert len(outputs) == 15
    output = next(o for o in outputs if o["k"] < 10)
    assert all(k in ["k", "a", "b", "c", "d"] for k in output)
    output = next(o for o in outputs if o["k"] > 9)
    assert all(k in ["k", "c", "d"] for k in output)
    assert all(k not in ["a", "b"] for k in output)

    # test fields_to_project = lists
    builder = Projection_Builder(
        source_stores=[source1, source2],
        target_store=target,
        fields_to_project=[["a", "b"], ["d"]],
    )
    items = next(iter(builder.get_items()))
    outputs = builder.process_item(items)
    output = next(o for o in outputs if o["k"] < 10)
    assert all(k in ["k", "a", "b", "d"] for k in output)
    assert all(k not in ["c"] for k in output)

    # test fields_to_project = dict and list
    builder = Projection_Builder(
        source_stores=[source1, source2],
        target_store=target,
        fields_to_project=[{"newa": "a", "b": "b"}, ["d"]],
    )
    items = next(iter(builder.get_items()))
    outputs = builder.process_item(items)
    output = next(o for o in outputs if o["k"] < 10)
    assert all(k in ["k", "newa", "b", "d"] for k in output)
    assert all(k not in ["a", "c"] for k in output)


def test_update_targets(source1, source2, target):
    builder = Projection_Builder(
        source_stores=[source1, source2],
        target_store=target,
        fields_to_project=[{"newa": "a", "b": "b"}, ["d"]],
    )
    items = list(builder.get_items())
    processed_chunk = [builder.process_item(item) for item in items]
    processed_items = [item for item in processed_chunk if item is not None]
    builder.update_targets(processed_items)
    assert len(list(target.query())) == 15
    assert target.query_one(criteria={"k": 0})["newa"] == "a"
    assert target.query_one(criteria={"k": 0})["d"] == "d"
    assert target.query_one(criteria={"k": 10})["d"] == "d"
    assert "a" not in target.query_one(criteria={"k": 10}).keys()


def test_run(source1, source2, target):
    builder = Projection_Builder(source_stores=[source1, source2], target_store=target)
    builder.run()
    assert len(list(target.query())) == 15
    assert target.query_one(criteria={"k": 0})["a"] == "a"
    assert target.query_one(criteria={"k": 0})["d"] == "d"
    assert target.query_one(criteria={"k": 10})["d"] == "d"
    assert "a" not in target.query_one(criteria={"k": 10}).keys()


def test_query(source1, source2, target):
    target.remove_docs({})
    builder = Projection_Builder(
        source_stores=[source1, source2],
        target_store=target,
        query_by_key=[0, 1, 2, 3, 4],
    )
    builder.run()
    assert len(list(target.query())) == 5
