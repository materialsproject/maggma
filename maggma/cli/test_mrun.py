import os

import subprocess
from maggma.runner import Runner
from monty.serialization import dumpfn
import unittest
from unittest import TestCase
from uuid import uuid4

from maggma.builders import CopyBuilder
from maggma.stores import MongoStore


class TestMRun(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dbname = "test_" + uuid4().hex
        cls.source = MongoStore(cls.dbname, "source")
        cls.target = MongoStore(cls.dbname, "target")
        cls.stores = [cls.source, cls.target]
        for store in cls.stores:
            store.connect()
            store.ensure_index(store.key)
            store.ensure_index([(store.lu_field, -1), (store.key, 1)])
        cls.client = cls.stores[0].collection.database.client

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(cls.dbname)

    def setUp(self):
        self.runner_filename = "runner_" + uuid4().hex + ".json"

    def tearDown(self):
        os.remove(self.runner_filename)

    def test_simple_runner(self):
        builder = CopyBuilder(self.source, self.target)
        runner = Runner([builder])
        dumpfn(runner, self.runner_filename)
        p = subprocess.run("python -m maggma.cli.mrun {}".format(
            self.runner_filename).split(), timeout=15)
        self.assertEqual(p.returncode, 0)


if __name__ == "__main__":
    unittest.main()