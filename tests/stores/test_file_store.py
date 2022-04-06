"""
Future home of unit tests for FileStore
"""

import os

import json
from datetime import datetime

import numpy as np
import numpy.testing.utils as nptu
import pytest

from maggma.stores import MongoStore
from maggma.stores.file_store import FileStore
from pymongo.errors import ConfigurationError


@pytest.fixture
def mongostore():
    store = MongoStore("maggma_test", "test")
    store.connect()
    yield store
    store._collection.drop()
