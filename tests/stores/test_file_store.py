"""
Future home of unit tests for FileStore
"""

import os

import json
from datetime import datetime
from pathlib import Path
import numpy as np
import numpy.testing.utils as nptu
import pytest

from maggma.stores import MemoryStore
from maggma.stores.file_store import FileStore
from pymongo.errors import ConfigurationError


@pytest.fixture
def test_dir():
    module_dir = Path(__file__).resolve().parent
    test_dir = module_dir / ".." / "test_files" / "file_store_test"
    return test_dir.resolve()


# @pytest.fixture
# def mongostore():
#     store = MemoryStore("memory_compare")
#     store.connect()
#     yield store
#     store._collection.drop()


def test_newer_in_on_local_update(test_dir):
    """
    Init a FileStore
    modify one of the files on disk
    Init another FileStore on the same directory
    confirm that one record shows up in newer_in
    """
    fs = FileStore(test_dir)
    fs.connect()
    with open(test_dir / "calculation1" / "input.in", "w") as f:
        f.write("Ryan was here")
    fs2 = FileStore(test_dir)
    fs2.connect()

    assert fs2.last_updated > fs.last_updated
    assert (
        fs2.query_one({"record_key": "calculation1"})["last_updated"]
        > fs.query_one({"record_key": "calculation1"})["last_updated"]
    )

    # TODO - I can't figure out why this one fails!
    assert len(fs.newer_in(fs2)) == 1
