# coding: utf-8
"""
Tests for advanced stores
"""
import time

import os
import shutil
import signal
import subprocess
import tempfile
import unittest

from itertools import chain
from mongogrant.client import seed
from pymongo import MongoClient
from pymongo.collection import Collection
from unittest.mock import patch, MagicMock
import mongomock.collection
from uuid import uuid4

from maggma.stores import MemoryStore, MongoStore
from maggma.advanced_stores import *
import zlib

module_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))


class TestMongograntStore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _, cls.config_path = tempfile.mkstemp()
        _, cls.mdlogpath = tempfile.mkstemp()
        cls.mdpath = tempfile.mkdtemp()
        cls.mdport = 27020
        if not (os.getenv("CONTINUOUS_INTEGRATION") and os.getenv("TRAVIS")):
            basecmd = ("mongod --port {} --dbpath {} --quiet --logpath {} "
                       "--bind_ip_all --auth".format(cls.mdport, cls.mdpath, cls.mdlogpath))
            cls.mongod_process = subprocess.Popen(basecmd, shell=True, start_new_session=True)
            time.sleep(5)
            client = MongoClient(port=cls.mdport)
            client.admin.command("createUser", "mongoadmin", pwd="mongoadminpass", roles=["root"])
            client.close()
        cls.dbname = "test_" + uuid4().hex
        cls.db = MongoClient("mongodb://mongoadmin:mongoadminpass@127.0.0.1:{}/admin".format(cls.mdport))[cls.dbname]
        cls.db.command("createUser", "reader", pwd="readerpass", roles=["read"])
        cls.db.command("createUser", "writer", pwd="writerpass", roles=["readWrite"])
        cls.db.client.close()

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.config_path)
        if not (os.getenv("CONTINUOUS_INTEGRATION") and os.getenv("TRAVIS")):
            os.killpg(os.getpgid(cls.mongod_process.pid), signal.SIGTERM)
            os.waitpid(cls.mongod_process.pid, 0)
        shutil.rmtree(cls.mdpath)
        os.remove(cls.mdlogpath)

    def setUp(self):
        config = Config(check=check, path=self.config_path, seed=seed())
        self.client = Client(config)
        self.client.set_auth(
            host="localhost:{}".format(self.mdport),
            db=self.dbname,
            role="read",
            username="reader",
            password="readerpass",
        )
        self.client.set_auth(
            host="localhost:{}".format(self.mdport),
            db=self.dbname,
            role="readWrite",
            username="writer",
            password="writerpass",
        )
        self.client.set_alias("testhost", "localhost:{}".format(self.mdport), which="host")
        self.client.set_alias("testdb", self.dbname, which="db")

    @staticmethod
    def connected_user(store):
        return store.collection.database.command("connectionStatus")['authInfo']['authenticatedUsers'][0]['user']

    def test_connect(self):
        store = MongograntStore("ro:testhost/testdb", "tasks", mgclient_config_path=self.config_path)
        store.connect()
        self.assertIsInstance(store.collection, Collection)
        self.assertEqual(self.connected_user(store), "reader")
        store = MongograntStore("rw:testhost/testdb", "tasks", mgclient_config_path=self.config_path)
        store.connect()
        self.assertIsInstance(store.collection, Collection)
        self.assertEqual(self.connected_user(store), "writer")


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
                'data': {
                    'value':
                    '{"db": "mg_core_prod", "host": "matgen2.lbl.gov", "username": "test", "password": "pass"}'
                },
                'auth': None,
                'warnings': None,
                'renewable': False,
                'lease_duration': 2764800,
                'lease_id': ''
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
        self.aliasingstore = AliasingStore(self.memorystore, {"a": "b", "c.d": "e", "f": "g.h"})

    def test_query(self):

        d = [{"b": 1}, {"e": 2}, {"g": {"h": 3}}]
        self.memorystore.collection.insert_many(d)

        self.assertTrue("a" in list(self.aliasingstore.query(criteria={"a": {"$exists": 1}}))[0])
        self.assertTrue("c" in list(self.aliasingstore.query(criteria={"c.d": {"$exists": 1}}))[0])
        self.assertTrue("d" in list(self.aliasingstore.query(criteria={"c.d": {"$exists": 1}}))[0].get("c", {}))
        self.assertTrue("f" in list(self.aliasingstore.query(criteria={"f": {"$exists": 1}}))[0])

    def test_update(self):

        self.aliasingstore.update([{
            "task_id": "mp-3",
            "a": 4
        }, {
            "task_id": "mp-4",
            "c": {
                "d": 5
            }
        }, {
            "task_id": "mp-5",
            "f": 6
        }])
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
        with self.assertRaises(Exception):
            self.sandboxstore.collection

        self.sandboxstore.connect()
        self.assertIsInstance(self.sandboxstore.collection, mongomock.collection.Collection)

    def test_query(self):
        self.sandboxstore.connect()
        self.sandboxstore.collection.insert_one({"a": 1, "b": 2, "c": 3})
        self.assertEqual(self.sandboxstore.query_one(properties=["a"])['a'], 1)

        self.sandboxstore.collection.insert_one({"a": 2, "b": 2, "sbxn": ["test"]})
        self.assertEqual(self.sandboxstore.query_one(properties=["b"], criteria={"a": 2})['b'], 2)

        self.sandboxstore.collection.insert_one({"a": 3, "b": 2, "sbxn": ["not_test"]})
        self.assertEqual(self.sandboxstore.query_one(properties=["c"], criteria={"a": 3}), None)

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
        self.assertEqual(set(self.sandboxstore.query_one(criteria={"e": 7})["sbxn"]), {"test", "core"})

    def tearDown(self):
        try:
            self.sandboxstore.collection.drop()
        except:
            pass


class JointStoreTest(unittest.TestCase):
    def setUp(self):
        self.jointstore = JointStore("maggma_test", ["test1", "test2"])
        self.jointstore.connect()
        self.jointstore.collection.drop()
        self.jointstore.collection.insert_many([{
            "task_id": k,
            "my_prop": k + 1,
            "last_updated": datetime.utcnow(),
            "category": k // 5
        } for k in range(10)])
        self.jointstore.collection.database["test2"].drop()
        self.jointstore.collection.database["test2"].insert_many([{
            "task_id": 2 * k,
            "your_prop": k + 3,
            "last_updated": datetime.utcnow(),
            "category2": k // 3
        } for k in range(5)])
        self.test1 = MongoStore("maggma_test", "test1")
        self.test1.connect()
        self.test2 = MongoStore("maggma_test", "test2")
        self.test2.connect()

    def test_query(self):
        # Test query all
        docs = list(self.jointstore.query())
        self.assertEqual(len(docs), 10)
        docs_w_field = [d for d in docs if "test2" in d]
        self.assertEqual(len(docs_w_field), 5)
        docs_w_field = sorted(docs_w_field, key=lambda x: x['task_id'])
        self.assertEqual(docs_w_field[0]['test2']['your_prop'], 3)
        self.assertEqual(docs_w_field[0]['task_id'], 0)
        self.assertEqual(docs_w_field[0]['my_prop'], 1)

    def test_query_one(self):
        doc = self.jointstore.query_one()
        self.assertEqual(doc['my_prop'], doc['task_id'] + 1)
        # Test limit properties
        doc = self.jointstore.query_one(properties=['test2', 'task_id'])
        self.assertEqual(doc['test2']['your_prop'], doc['task_id'] + 3)
        self.assertIsNone(doc.get("my_prop"))
        # Test criteria
        doc = self.jointstore.query_one(criteria={"task_id": {"$gte": 10}})
        self.assertIsNone(doc)
        doc = self.jointstore.query_one(criteria={"test2.your_prop": {"$gt": 6}})
        self.assertEqual(doc['task_id'], 8)

        # Test merge_at_root
        self.jointstore.merge_at_root = True

        # Test merging is working properly
        doc = self.jointstore.query_one(criteria={"task_id": 2})
        self.assertEqual(doc['my_prop'], 3)
        self.assertEqual(doc['your_prop'], 4)

        # Test merging is allowing for subsequent match
        doc = self.jointstore.query_one(criteria={"your_prop": {"$gt": 6}})
        self.assertEqual(doc['task_id'], 8)

    def test_distinct(self):
        dyour_prop = self.jointstore.distinct("test2.your_prop")
        self.assertEqual(set(dyour_prop), {k + 3 for k in range(5)})
        dmy_prop = self.jointstore.distinct("my_prop")
        self.assertEqual(set(dmy_prop), {k + 1 for k in range(10)})
        dmy_prop_cond = self.jointstore.distinct("my_prop", {"test2.your_prop": {"$gte": 5}})
        self.assertEqual(set(dmy_prop_cond), {5, 7, 9})

    def test_last_updated(self):
        doc = self.jointstore.query_one({"task_id": 0})
        test1doc = self.test1.query_one({"task_id": 0})
        test2doc = self.test2.query_one({"task_id": 0})
        self.assertEqual(test2doc['last_updated'], doc['last_updated'])
        self.assertNotEqual(test1doc['last_updated'], doc['last_updated'])
        # Swap the two
        test2date = test2doc['last_updated']
        test2doc['last_updated'] = test1doc['last_updated']
        test1doc['last_updated'] = test2date
        self.test1.update([test1doc], update_lu=False)
        self.test2.update([test2doc], update_lu=False)
        doc = self.jointstore.query_one({"task_id": 0})
        test1doc = self.test1.query_one({"task_id": 0})
        test2doc = self.test2.query_one({"task_id": 0})
        self.assertEqual(test1doc['last_updated'], doc['last_updated'])
        self.assertNotEqual(test2doc['last_updated'], doc['last_updated'])
        # Check also that still has a field if no task2 doc
        doc = self.jointstore.query_one({"task_id": 1})
        self.assertIsNotNone(doc['last_updated'])

    def test_groupby(self):
        docs = list(self.jointstore.groupby("category"))
        self.assertEqual(len(docs[0]['docs']), 5)
        self.assertEqual(len(docs[1]['docs']), 5)
        docs = list(self.jointstore.groupby("test2.category2"))
        docs_by_id = {get(d, '_id.test2.category2'): d['docs'] for d in docs}
        self.assertEqual(len(docs_by_id[None]), 5)
        self.assertEqual(len(docs_by_id[0]), 3)
        self.assertEqual(len(docs_by_id[1]), 2)


class ConcatStoreTest(unittest.TestCase):
    def setUp(self):
        self.mem_stores = [MemoryStore(str(i)) for i in range(4)]
        self.store = ConcatStore(*self.mem_stores)
        self.store.connect()

        index = 0

        props = {i: str(i) for i in range(10)}
        for store in self.mem_stores:
            docs = [{"task_id": i, "prop": props[i - index], "index": index} for i in range(index, index + 10)]
            index = index + 10
            store.update(docs)

    def test_distinct(self):
        docs = list(self.store.distinct("task_id"))
        actual_docs = list(chain.from_iterable([store.distinct("task_id") for store in self.mem_stores]))
        self.assertEqual(len(docs), len(actual_docs))
        self.assertEqual(set(docs), set(actual_docs))

    def test_not_implemented(self):
        # Ensure collection property and update throw errors
        with self.assertRaises(NotImplementedError):
            self.store.collection
            self.store.update([])

    def test_groupby(self):
        self.assertEqual(len(list(self.store.groupby("index"))), 4)
        self.assertEqual(len(list(self.store.groupby("task_id"))), 40)

    def test_query(self):

        docs = list(self.store.query(properties=["task_id"]))
        t_ids = [d["task_id"] for d in docs]
        self.assertEqual(len(t_ids), len(set(t_ids)))
        self.assertEqual(len(t_ids), 40)


if __name__ == "__main__":
    unittest.main()
