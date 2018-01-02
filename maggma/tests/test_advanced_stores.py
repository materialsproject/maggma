import os
import glob
import unittest

import mongomock.collection
import pymongo.collection

from maggma.stores import MemoryStore
from maggma.advanced_stores import *

module_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))


class TestAliasingStore(unittest.TestCase):

    def setUp(self):
        self.memorystore = MemoryStore("test")
        self.memorystore.connect()
        self.aliasingstore = AliasingStore(
            self.memorystore, {"a": "b", "c.d": "e", "f": "g.h"})

    def test_query(self):

        d = [{"b": 1}, {"e": 2}, {"g": {"h": 3}}]
        self.memorystore.collection.insert_many(d)

        self.assertTrue("a" in list(self.aliasingstore.query(
            criteria={"a": {"$exists": 1}}))[0])
        self.assertTrue("c" in list(self.aliasingstore.query(
            criteria={"c.d": {"$exists": 1}}))[0])
        self.assertTrue("d" in list(self.aliasingstore.query(
            criteria={"c.d": {"$exists": 1}}))[0].get("c", {}))
        self.assertTrue("f" in list(self.aliasingstore.query(
            criteria={"f": {"$exists": 1}}))[0])


    def test_update(self):

        self.aliasingstore.update([{"task_id": "mp-3", "a": 4},{"task_id": "mp-4", "c": {"d": 5}},{"task_id": "mp-5", "f": 6}])
        self.assertEqual(list(self.aliasingstore.query(criteria={"task_id": "mp-3" }))[0]["a"],4)
        self.assertEqual(list(self.aliasingstore.query(criteria={"task_id": "mp-4" }))[0]["c"]["d"],5)
        self.assertEqual(list(self.aliasingstore.query(criteria={"task_id": "mp-5" }))[0]["f"],6)

        self.assertEqual(list(self.aliasingstore.store.query(criteria={"task_id": "mp-3" }))[0]["b"],4)
        self.assertEqual(list(self.aliasingstore.store.query(criteria={"task_id": "mp-4" }))[0]["e"],5)
        self.assertEqual(list(self.aliasingstore.store.query(criteria={"task_id": "mp-5" }))[0]["g"]["h"],6)

    def test_substitute(self):
        aliases = {"a": "b", "c.d": "e", "f": "g.h"}

        d = {"b": 1}
        substitute(d, aliases)
        self.assertTrue("a" in d)

        d = {"e": 1}
        substitute(d, aliases)
        self.assertTrue("c" in d)
        self.assertTrue("d" in d.get("c", {}))

        d = {"g": {"h": 4}}
        substitute(d, aliases)
        self.assertTrue("f" in d)

        d = None
        substitute(d,aliases)
        self.assertTrue(d is None)

if __name__ == "__main__":
    unittest.main()
