# coding: utf-8
"""
Tests for builders
"""
import pytest

from maggma.utils import recursive_update, Timeout
from time import sleep


def test_recursiveupdate():
    d = {"a": {"b": 3}, "c": [4]}

    recursive_update(d, {"c": [5]})
    assert d["c"] ==  [5]

    recursive_update(d, {"a": {"b": 5}})
    assert d["a"]["b"] ==  5

    recursive_update(d, {"a": {"b": [6]}})
    assert d["a"]["b"] ==  [6]

    recursive_update(d, {"a": {"b": [7]}})
    assert d["a"]["b"] ==  [7]

def test_timeout():

    def takes_too_long():
        with Timeout(seconds=1):
            sleep(2)
    with pytest.raises(TimeoutError):
        takes_too_long()

