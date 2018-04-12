# coding: utf-8
"""
Tests for advanced stores
"""
import os
import unittest
from unittest.mock import patch, MagicMock
import mongomock.collection

from maggma.stores import MemoryStore, MongoStore
from maggma.advanced_stores import *
import zlib

module_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))


class TestVaultStore(unittest.TestCase):
    """
    Test VaultStore class
    """

    def _create_vault_store(self):
        with patch('hvac.Client') as mock:

            instance = mock.return_value
            instance.auth_github.return_value = True
            instance.is_authenticated.return_value = True
            instance.read.return_value = {
                'wrap_info': None,
                'request_id': '2c72c063-2452-d1cd-19a2-91163c7395f7',
                'data': {'value': '{"db": "mg_core_prod", "host": "matgen2.lbl.gov", "username": "test", "password": "pass"}'},
                'auth': None,
                'warnings': None,
                'renewable': False,
                'lease_duration': 2764800, 'lease_id': ''
            }
            v = VaultStore("test_coll", "secret/matgen/maggma")

        return v

    def test_vault_init(self):
        """
        Test initing a vault store using a mock hvac client
        """
        os.environ['VAULT_ADDR'] = "https://fake:8200/"
        os.environ['VAULT_TOKEN'] = "dummy"

        v = self._create_vault_store()
        # Just test that we successfully instantiated
        assert isinstance(v, MongoStore)

    def test_vault_github_token(self):
        """
        Test using VaultStore with GITHUB_TOKEN and mock hvac
        """
        # Save token in env
        os.environ['VAULT_ADDR'] = "https://fake:8200/"
        os.environ['GITHUB_TOKEN'] = "dummy"

        v = self._create_vault_store()
        # Just test that we successfully instantiated
        assert isinstance(v, MongoStore)

    def test_vault_missing_env(self):
        """
        Test VaultStore should raise an error if environment is not set
        """
        del os.environ['VAULT_TOKEN']
        del os.environ['VAULT_ADDR']
        del os.environ['GITHUB_TOKEN']

        # Create should raise an error
        with self.assertRaises(RuntimeError):
            self._create_vault_store()


class TestS3Store(unittest.TestCase):

    def setUp(self):
        self.index = MemoryStore("index'")
        with patch("boto3.resource") as mock_resource:
            mock_resource.return_value = MagicMock()
            mock_resource("s3").list_buckets.return_value = ["bucket1", "bucket2"]
            self.s3store = AmazonS3Store(self.index, "bucket1")
            self.s3store.connect()

    def test_qeuery_one(self):
        self.s3store.s3_bucket.Object.return_value = MagicMock()
        self.s3store.s3_bucket.Object().get.return_value = '{"task_id": "mp-1", "data": "asd"}'
        self.index.update([{"task_id": "mp-1"}])
        self.assertEqual(self.s3store.query_one(criteria={"task_id": "mp-2"}), None)
        self.assertEqual(self.s3store.query_one(criteria={"task_id": "mp-1"})["data"], "asd")

        self.s3store.s3_bucket.Object().get.return_value = zlib.compress('{"task_id": "mp-3", "data": "sdf"}'.encode())
        self.index.update([{"task_id": "mp-3", "compression": "zlib"}])
        self.assertEqual(self.s3store.query_one(criteria={"task_id": "mp-3"})["data"], "sdf")

    def test_update(self):

        self.s3store.update([{"task_id": "mp-1", "data": "asd"}])
        self.assertEqual(self.s3store.s3_bucket.put_object.call_count, 1)
        called_kwargs = self.s3store.s3_bucket.put_object.call_args[1]
        self.assertEqual(self.s3store.s3_bucket.put_object.call_count, 1)
        self.assertEqual(called_kwargs["Key"], "mp-1")
        self.assertTrue(len(called_kwargs["Body"]) > 0)
        self.assertEqual(called_kwargs["Metadata"]["task_id"], "mp-1")

    def test_update_compression(self):
        self.s3store.update([{"task_id": "mp-1", "data": "asd"}], compress=True)
        self.assertEqual(self.s3store.s3_bucket.put_object.call_count, 1)
        called_kwargs = self.s3store.s3_bucket.put_object.call_args[1]
        self.assertEqual(self.s3store.s3_bucket.put_object.call_count, 1)
        self.assertEqual(called_kwargs["Key"], "mp-1")
        self.assertTrue(len(called_kwargs["Body"]) > 0)
        self.assertEqual(called_kwargs["Metadata"]["task_id"], "mp-1")
        self.assertEqual(called_kwargs["Metadata"]["compression"], "zlib")


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

        self.aliasingstore.update([{"task_id": "mp-3", "a": 4}, {"task_id": "mp-4",
                                                                 "c": {"d": 5}}, {"task_id": "mp-5", "f": 6}])
        self.assertEqual(list(self.aliasingstore.query(criteria={"task_id": "mp-3"}))[0]["a"], 4)
        self.assertEqual(list(self.aliasingstore.query(criteria={"task_id": "mp-4"}))[0]["c"]["d"], 5)
        self.assertEqual(list(self.aliasingstore.query(criteria={"task_id": "mp-5"}))[0]["f"], 6)

        self.assertEqual(list(self.aliasingstore.store.query(criteria={"task_id": "mp-3"}))[0]["b"], 4)
        self.assertEqual(list(self.aliasingstore.store.query(criteria={"task_id": "mp-4"}))[0]["e"], 5)
        self.assertEqual(list(self.aliasingstore.store.query(criteria={"task_id": "mp-5"}))[0]["g"]["h"], 6)

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
        substitute(d, aliases)
        self.assertTrue(d is None)


class TestSandboxStore(unittest.TestCase):

    def setUp(self):
        self.store = MemoryStore()
        self.sandboxstore = SandboxStore(self.store, sandbox="test")

    def test_connect(self):
        self.assertEqual(self.sandboxstore.collection, None)
        self.sandboxstore.connect()
        self.assertIsInstance(self.sandboxstore.collection, mongomock.collection.Collection)

    def test_query(self):
        self.sandboxstore.connect()
        self.sandboxstore.collection.insert_one({"a": 1, "b": 2, "c": 3})
        self.assertEqual(self.sandboxstore.query_one(properties=["a"])['a'], 1)

        self.sandboxstore.collection.insert_one({"a": 2, "b": 2, "sbxn": ["test"]})
        self.assertEqual(self.sandboxstore.query_one(properties=["b"],
                                                     criteria={"a": 2})['b'], 2)

        self.sandboxstore.collection.insert_one({"a": 3, "b": 2, "sbxn": ["not_test"]})
        self.assertEqual(self.sandboxstore.query_one(properties=["c"],
                                                   criteria={"a": 3}), None)

    def test_distinct(self):
        self.sandboxstore.connect()
        self.sandboxstore.collection.insert_one({"a": 1, "b": 2, "c": 3})
        self.assertEqual(self.sandboxstore.distinct("a"), [1])

        self.sandboxstore.collection.insert_one({"a": 4, "d": 5, "e": 6, "sbxn": ["test"]})
        self.assertEqual(self.sandboxstore.distinct("a"), [1, 4])

        self.sandboxstore.collection.insert_one({"a": 7, "d": 8, "e": 9, "sbxn": ["not_test"]})
        self.assertEqual(self.sandboxstore.distinct("a"), [1, 4])

    def test_update(self):
        self.sandboxstore.connect()
        self.sandboxstore.update([{"e": 6, "d": 4}], key="e")
        self.assertEqual(self.sandboxstore.query(criteria={"d": {"$exists": 1}}, properties=["d"])[0]["d"], 4)
        self.assertEqual(self.sandboxstore.collection.find_one({"e": 6})["sbxn"], ["test"])
        self.sandboxstore.update([{"e": 7, "sbxn": ["core"]}], key="e")
        self.assertEqual(set(self.sandboxstore.query_one(
            criteria={"e": 7})["sbxn"]), {"test", "core"})

    def tearDown(self):
        if self.sandboxstore.collection:
            self.sandboxstore.collection.drop()


if __name__ == "__main__":
    unittest.main()
