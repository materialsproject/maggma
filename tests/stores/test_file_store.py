"""
Future home of unit tests for FileStore
"""

from datetime import datetime, timezone
from distutils.dir_util import copy_tree
from pathlib import Path
import numpy as np
import numpy.testing as nptu
import pytest

from maggma.stores import MemoryStore
from maggma.stores.file_store import FileStore, File


@pytest.fixture
def test_dir(tmp_path):
    module_dir = Path(__file__).resolve().parent
    test_dir = module_dir / ".." / "test_files" / "file_store_test"
    copy_tree(str(test_dir), str(tmp_path))
    return tmp_path.resolve()


# @pytest.fixture
# def mongostore():
#     store = MemoryStore("memory_compare")
#     store.connect()
#     yield store
#     store._collection.drop()


def test_file_class(test_dir):
    """
    Test functionality of the file class
    """
    f = File.from_file(test_dir / "calculation1" / "input.in")
    assert f.name == "input.in"
    assert f.parent == "calculation1"
    assert f.path == test_dir / "calculation1" / "input.in"
    assert f.size == 0
    assert f.hash == f.compute_hash()
    assert f.file_id == f.get_file_id()
    assert f.last_updated == datetime.fromtimestamp(
        f.path.stat().st_mtime, tz=timezone.utc
    )


def test_newer_in_on_local_update(test_dir):
    """
    Init a FileStore
    modify one of the files on disk
    Init another FileStore on the same directory
    confirm that one record shows up in newer_in
    """
    fs = FileStore(test_dir, read_only=False)
    fs.connect()
    with open(test_dir / "calculation1" / "input.in", "w") as f:
        f.write("Ryan was here")
    fs2 = FileStore(test_dir, read_only=False)
    fs2.connect()

    assert fs2.last_updated > fs.last_updated
    assert (
        fs2.query_one({"path": {"$regex": "calculation1/input.in"}})["last_updated"]
        > fs.query_one({"path": {"$regex": "calculation1/input.in"}})["last_updated"]
    )
    assert len(fs.newer_in(fs2)) == 1


def test_max_depth(test_dir):
    """
    test max_depth parameter
    """
    # default (None) should parse all 6 files
    fs = FileStore(test_dir, read_only=False)
    fs.connect()
    assert len(list(fs.query())) == 6
    # TODO - fix later
    Path(test_dir / "FileStore.json").unlink()

    # 0 depth should parse 1 file
    fs = FileStore(test_dir, read_only=False, max_depth=0)
    fs.connect()
    assert len(list(fs.query())) == 1

    # 1 depth should parse 5 files
    fs = FileStore(test_dir, read_only=False, max_depth=1)
    fs.connect()
    assert len(list(fs.query())) == 5

    # 2 depth should parse 6 files
    fs = FileStore(test_dir, read_only=False, max_depth=2)
    fs.connect()
    assert len(list(fs.query())) == 6


def test_track_files(test_dir):
    """
    Make sure multiple patterns work correctly
    """
    # here, we should get 2 input.in files and the file_2_levels_deep.json
    # the store's FileStore.json should be skipped even though .json is
    # in the file patterns
    fs = FileStore(test_dir, read_only=False, track_files=["*.in", "*.json"])
    fs.connect()
    assert len(list(fs.query())) == 3


def test_read_only(test_dir):
    pass


def test_json_name(test_dir):
    """
    Make sure custom .json name works
    """
    fs = FileStore(test_dir, read_only=False, json_name="random.json")
    fs.connect()
    assert Path(test_dir / "random.json").exists()
