import os
import unittest
import json

from maggma.helpers import get_database
from maggma.stores import JSONStore

__author__ = 'Kiran Mathew'
__email__ = 'kmathew@lbl.gov'

module_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
db_dir = os.path.abspath(os.path.join(module_dir, "..", "..", "test_files", "settings_files"))


class TestRuner(unittest.TestCase):

    def setUp(self):
        creds_dict = json.load(open(os.path.join(db_dir, "db.json"), "r"))
        db = get_database(creds_dict)
        
        #stores =
        #self.builders =
