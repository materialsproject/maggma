# coding: utf-8
"""
Tests utilities
"""
import unittest

from maggma.utils import recursive_update, Timeout
from time import sleep

class UtilsTests(unittest.TestCase):

    def test_recursiveupdate(self):
        d = {"a": {"b": 3}, "c": [4]}

        recursive_update(d, {"c": [5]})
        self.assertEqual(d["c"], [5])

        recursive_update(d, {"a": {"b": 5}})
        self.assertEqual(d["a"]["b"], 5)

        recursive_update(d, {"a": {"b": [6]}})
        self.assertEqual(d["a"]["b"], [6])

        recursive_update(d, {"a": {"b": [7]}})
        self.assertEqual(d["a"]["b"], [7])

    def test_timeout(self):

        def takes_too_long():
            with Timeout(seconds=1):
                sleep(2)

        self.assertRaises(TimeoutError, takes_too_long)
