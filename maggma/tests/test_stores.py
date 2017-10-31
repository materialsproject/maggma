import os
import unittest

import mongomock.collection
import pymongo.collection

from maggma.stores import *

module_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
db_dir = os.path.abspath(os.path.join(module_dir, "..", "..", "test_files", "settings_files"))



class TestMongoStore(unittest.TestCase):

	def setUp(self):
		self.mongostore = MongoStore("maggma_test","test")

	def test(self):
		self.assertEqual(self.mongostore.collection,None)
		self.mongostore.connect()
		self.assertIsInstance(self.mongostore.collection, pymongo.collection.Collection)
		
		self.mongostore.collection.insert({"a":1,"b":2,"c":3})
		self.assertEqual(self.mongostore.query(properties=["a"])[0]['a'],1)
		self.assertEqual(self.mongostore.query(properties=["b"])[0]['b'],2)
		self.assertEqual(self.mongostore.query(properties=["c"])[0]['c'],3)

		self.mongostore.collection.insert({"a":4,"d":5,"e":6})
		self.assertEqual(self.mongostore.distinct("a"),[1,4])

		self.mongostore.update("e",[{"e":6,"d": 4}])
		self.assertEqual(self.mongostore.query(criteria={"d":{"$exists":1}},properties=["d"])[0]["d"],4)

	def tearDown(self):
		self.mongostore.collection.drop()


class TestMemoryStore(unittest.TestCase):

    def setUp(self):
        self.memstore = MemoryStore("collection")

    def test(self):
        self.assertEqual(self.memstore.collection, None)
        self.memstore.connect()
        self.assertIsInstance(self.memstore.collection, mongomock.collection.Collection)


