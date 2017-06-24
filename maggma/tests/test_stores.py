import os
import unittest

import mongomock.collection

from maggma.stores import *

__author__ = 'Kiran Mathew'
__email__ = 'kmathew@lbl.gov'

module_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
db_dir = os.path.abspath(os.path.join(module_dir, "..", "..", "test_files", "settings_files"))



class TestMemoryStore(unittest.TestCase):

    def setUp(self):
        self.memstore = MemoryStore("collection")

    def test(self):
        self.assertEqual(self.memstore.collection,None)
        self.memstore.connect()
        self.assertIsInstance(self.memstore.collection,mongomock.collection.Collection)
        self.assertEqual(self.memstore(),self.memstore.collection)
        self.assertEqual(self.memstore.meta.name,"collection.db.collection.meta")



