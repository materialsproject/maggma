# coding: utf-8
"""
Tests for builders
"""
from datetime import datetime
from time import sleep

import pytest

from maggma.utils import Timeout  # dt_to_isoformat_ceil_ms,; isostr_to_dt,
from maggma.utils import (
    dynamic_import,
    grouper,
    primed,
    recursive_update,
    to_dt,
    to_isoformat_ceil_ms,
)


def test_recursiveupdate():
    d = {"a": {"b": 3}, "c": [4]}

    recursive_update(d, {"c": [5]})
    assert d["c"] == [5]

    recursive_update(d, {"a": {"b": 5}})
    assert d["a"]["b"] == 5

    recursive_update(d, {"a": {"b": [6]}})
    assert d["a"]["b"] == [6]

    recursive_update(d, {"a": {"b": [7]}})
    assert d["a"]["b"] == [7]


def test_timeout():
    def takes_too_long():
        with Timeout(seconds=1):
            sleep(2)

    with pytest.raises(TimeoutError):
        takes_too_long()


def test_primed():
    global is_primed
    is_primed = False

    def unprimed_iter():
        global is_primed
        is_primed = True
        for i in range(10):
            yield i

    iterator = unprimed_iter()

    # iterator is still unprimed
    assert is_primed is False

    iterator = primed(iterator)
    assert is_primed is True
    assert list(iterator) == list(range(10))

    # test stop itteration
    with pytest.raises(StopIteration):
        next(primed(iterator))


def test_datetime_utils():
    assert (
        to_isoformat_ceil_ms(datetime(2019, 12, 13, 0, 23, 11, 9515))
        == "2019-12-13T00:23:11.010"
    )
    assert to_isoformat_ceil_ms("2019-12-13T00:23:11.010") == "2019-12-13T00:23:11.010"

    assert to_dt("2019-12-13T00:23:11.010") == datetime(2019, 12, 13, 0, 23, 11, 10000)
    assert to_dt(datetime(2019, 12, 13, 0, 23, 11, 10000)) == datetime(
        2019, 12, 13, 0, 23, 11, 10000
    )


def test_dynamic_import():
    assert dynamic_import("maggma.stores", "MongoStore").__name__ == "MongoStore"


def test_grouper():
    my_iterable = list(range(100))

    assert len(list(grouper(my_iterable, 10))) == 10

    my_iterable = list(range(100)) + [None]
    my_groups = list(grouper(my_iterable, 10))
    assert len(my_groups) == 11
    assert len(my_groups[10]) == 1
