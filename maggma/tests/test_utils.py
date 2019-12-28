# coding: utf-8
"""
Tests for builders
"""
import pytest

from maggma.utils import (
    recursive_update,
    Timeout,
    primed,
    dt_to_isoformat_ceil_ms,
    isostr_to_dt,
)
from time import sleep
from datetime import datetime


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


def test_datetime_utils():

    assert (
        dt_to_isoformat_ceil_ms(datetime(2019, 12, 13, 0, 23, 11, 9515))
        == "2019-12-13T00:23:11.010"
    )

    assert isostr_to_dt("2019-12-13T00:23:11.010") == datetime(
        2019, 12, 13, 0, 23, 11, 10000
    )
