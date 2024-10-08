"""
Tests for group builder
"""

from datetime import datetime, timezone
from random import randint

import pytest

from maggma.builders import GroupBuilder
from maggma.stores import MemoryStore


@pytest.fixture(scope="module")
def now():
    return datetime.now(timezone.utc)


@pytest.fixture()
def docs(now):
    return [{"k": i, "a": i % 3, "b": randint(0, i), "lu": now} for i in range(20)]


@pytest.fixture()
def source(docs):
    store = MemoryStore("source", key="k", last_updated_field="lu")
    store.connect()
    store.ensure_index("k")
    store.ensure_index("lu")
    store.update(docs)
    return store


@pytest.fixture()
def target():
    store = MemoryStore("target", key="ks", last_updated_field="lu")
    store.connect()
    store.ensure_index("ks")
    store.ensure_index("lu")
    return store


class DummyGrouper(GroupBuilder):
    def unary_function(self, items: list[dict]) -> dict:
        """
        Processing function for GroupBuilder

        Args:
            items: list of documents that are already grouped by the grouping_keys

        Returns:
            Dictionary mapping:
                tuple of source document keys that are in the grouped document
                to
                the grouped and processed document
        """
        new_doc = {}
        for k in self.grouping_keys:
            new_doc[k] = {d[k] for d in items}
        new_doc["b"] = [d["b"] for d in items]
        return new_doc


def test_grouping(source, target, docs):
    builder = DummyGrouper(source, target, query={"k": {"$ne": 3}}, grouping_keys=["a"])

    assert len(docs) - 1 == len(builder.get_ids_to_process()), f"{len(docs) -1} != {len(builder.get_ids_to_process())}"
    assert len(builder.get_groups_from_keys([d["k"] for d in docs])) == 3

    to_process = list(builder.get_items())
    assert len(to_process) == 3

    processed = [builder.process_item(d) for d in to_process]
    assert len(processed) == 3

    builder.update_targets(processed)

    assert len(builder.get_ids_to_process()) == 0, f"{len(builder.get_ids_to_process())} != 0"
