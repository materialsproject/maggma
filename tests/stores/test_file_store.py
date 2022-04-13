"""
Tests for FileStore

Desired behavior
----------------
- A FileStore is initialized on a directory containing files
- The FileStore reads the files and populates itself with file metadata
- If there is a FileStore.json present, its contents are read and merged with
  the file metadata
- If there are records (file_id) in the JSON metadata that are not associated
  with a file on disk anymore, they are marked as orphans with 'orphan: True'
  and added to the store.
- If there is no FileStore.json present
    - if read_only=False, the file is created
    - if read_only=True, no metadata is read in
- if read_only=False, the update() method is enabled
"""

from datetime import datetime, timezone
from distutils.dir_util import copy_tree
from pathlib import Path
import pytest

from maggma.core import StoreError
from maggma.stores.file_store import FileStore, FileRecord
from monty.io import zopen


@pytest.fixture
def test_dir(tmp_path):
    module_dir = Path(__file__).resolve().parent
    test_dir = module_dir / ".." / "test_files" / "file_store_test"
    copy_tree(str(test_dir), str(tmp_path))
    return tmp_path.resolve()


def test_filerecord(test_dir):
    """
    Test functionality of the FileRecord class
    """
    f = FileRecord.from_file(test_dir / "calculation1" / "input.in")
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

    # 0 depth should parse 1 file
    # exclude orphaned metadata in the query
    fs = FileStore(test_dir, read_only=False, max_depth=0)
    fs.connect()
    assert len(list(fs.query({"name": {"$exists": True}}))) == 1

    # 1 depth should parse 5 files
    # exclude orphaned metadata in the query
    fs = FileStore(test_dir, read_only=False, max_depth=1)
    fs.connect()
    assert len(list(fs.query({"name": {"$exists": True}}))) == 5

    # 2 depth should parse 6 files
    # exclude orphaned metadata in the query
    fs = FileStore(test_dir, read_only=False, max_depth=2)
    fs.connect()
    assert len(list(fs.query({"name": {"$exists": True}}))) == 6


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
    """
    Make sure nothing is written to a read-only FileStore and that
    documents cannot be deleted
    """
    with pytest.warns(UserWarning, match="JSON file 'random.json' not found"):
        fs = FileStore(test_dir, read_only=True, json_name="random.json")
        fs.connect()
    assert not Path(test_dir / "random.json").exists()
    with pytest.raises(StoreError, match="read-only"):
        file_id = fs.query_one()["file_id"]
        fs.update({"file_id": file_id, "tags": "something"})
    with pytest.raises(StoreError, match="read-only"):
        fs.remove_docs({})


def test_remove(test_dir):
    """
    Test behavior of remove_docs()
    """
    fs = FileStore(test_dir, read_only=False)
    fs.connect()
    with pytest.raises(NotImplementedError, match="deleting"):
        fs.remove_docs({})


def test_metadata(test_dir):
    """
    1. init a FileStore
    2. add some metadata to both 'input.in' files
    3. confirm metadata written to .json
    4. close the store, init a new one
    5. confirm metadata correctly associated with the files
    """
    fs = FileStore(test_dir, read_only=False)
    fs.connect()
    k1 = list(fs.query({"name": "input.in", "parent": "calculation1"}))[0][fs.key]
    fs.update([{"file_id": k1, "metadata": {"experiment date": "2022-01-18"}}], fs.key)
    fs.close()
    data = fs.metadata_store.read_json_file(fs.path / fs.json_name)
    record = [d for d in data if d["file_id"] == k1][0]
    assert record["metadata"] == {"experiment date": "2022-01-18"}

    # make sure metadata is preserved after reconnecting
    fs2 = FileStore(test_dir, read_only=True)
    fs2.connect()
    data = fs2.metadata_store.read_json_file(fs2.path / fs.json_name)
    record = [d for d in data if d["file_id"] == k1][0]
    assert record["metadata"] == {"experiment date": "2022-01-18"}
    docs = [d for d in fs2.query({"file_id": k1})]
    print(docs)
    assert docs[0].get("metadata") == {"experiment date": "2022-01-18"}

    # make sure reconnecting with read_only=False doesn't remove metadata from the JSON
    fs2 = FileStore(test_dir, read_only=False)
    fs2.connect()
    data = fs2.metadata_store.read_json_file(fs2.path / fs.json_name)
    record = [d for d in data if d["file_id"] == k1][0]
    assert record["metadata"] == {"experiment date": "2022-01-18"}
    docs = [d for d in fs2.query({"file_id": k1})]
    assert docs[0].get("metadata") == {"experiment date": "2022-01-18"}


def test_json_name(test_dir):
    """
    Make sure custom .json name works
    """
    fs = FileStore(test_dir, read_only=False, json_name="random.json")
    fs.connect()
    assert Path(test_dir / "random.json").exists()
