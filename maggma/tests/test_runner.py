import os
import unittest
import json

from maggma.helpers import get_database
from maggma.stores import MongoStore
from maggma.builder import Builder
from maggma.runner import Runner

__author__ = 'Kiran Mathew'
__email__ = 'kmathew@lbl.gov'

module_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
db_dir = os.path.abspath(os.path.join(module_dir, "..", "..", "test_files", "settings_files"))


class Bldr(Builder):

    def get_items(self):
        pass

    def process_item(self, item):
        pass

    def update_targets(self, items):
        pass

    def finalize(self):
        pass


class TestRunner(unittest.TestCase):

    def setUp(self):
        with open(os.path.join(db_dir, "db.json"), "r") as f:
            creds_dict = json.load(f)
        self.db = get_database(creds_dict)
        colls = [self.db[str(i)] for i in range(7)]
        stores = [MongoStore(c) for c in colls]
        builder1 = Bldr([stores[0], stores[1], stores[2]], [stores[3], stores[4], stores[5]])
        builder2 = Bldr([stores[0], stores[1], stores[3]], [stores[3], stores[6]])
        self.builders = [builder1, builder2]

    def test_1(self):
        rnr = Runner(self.builders)
        ans = {1: [0]}
        self.assertDictEqual(rnr.dependency_graph, ans)

    def tearDown(self):
        for coll in self.db.collection_names():
            if not coll.startswith("system."):
                self.db[coll].drop()
