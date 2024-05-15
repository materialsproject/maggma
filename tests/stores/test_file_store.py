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
- if a FileStore is moved to a different location on disk (but all contents of the
  main directory are preserved), file_ids should not change and metadata should
  remain intact.
"""

import hashlib
from datetime import datetime, timezone
from distutils.dir_util import copy_tree
from pathlib import Path

import pytest

from maggma.core import StoreError
from maggma.stores.file_store import FileStore


@pytest.fixture()
def test_dir(tmp_path):
    module_dir = Path(__file__).resolve().parent
    test_dir = module_dir / ".." / "test_files" / "file_store_test"
    copy_tree(str(test_dir), str(tmp_path))
    return tmp_path.resolve()


def test_record_from_file(test_dir):
    """
    Test functionality of _create_record_from_file
    """
    fs = FileStore(test_dir, read_only=True)
    fs.connect()
    f = Path(test_dir / "calculation1" / "input.in")

    relative_path = f.relative_to(test_dir)
    digest = hashlib.md5()
    digest.update(str(relative_path).encode())
    file_id = str(digest.hexdigest())

    d = fs._create_record_from_file(f)
    assert d["name"] == "input.in"
    assert d["parent"] == "calculation1"
    assert d["path"] == test_dir / "calculation1" / "input.in"
    assert d["size"] == pytest.approx(90, abs=1)
    assert isinstance(d["hash"], str)
    assert d["file_id"] == file_id
    assert d["last_updated"] == datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)


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

    NOTE this test only creates a single temporary directory, meaning that
    the JSON file created by the first FileStore.init() persists for the other
    tests. This creates the possibility of orphaned metadata.
    """
    # default (None) should parse all 6 files
    fs = FileStore(test_dir, read_only=False)
    fs.connect()
    assert len(list(fs.query())) == 6

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


def test_orphaned_metadata(test_dir):
    """
    test behavior when orphaned metadata is found

    NOTE the design of this test exploits the fact that the test only creates
    a single temporary directory, meaning that the JSON file created by the
    first FileStore.init() persists for the other tests.
    """
    # make a FileStore of all files and add metadata to all of them
    fs = FileStore(test_dir, read_only=False)
    fs.connect()
    data = list(fs.query())
    for d in data:
        d.update({"tags": "Ryan was here"})
    fs.update(data)

    assert len(list(fs.query())) == 6
    assert len(list(fs.query({"tags": {"$exists": True}}))) == 6
    # the orphan field should be populated for all documents
    assert len(list(fs.query({"orphan": {"$exists": True}}))) == 6
    fs.close()

    # re-init the store with a different max_depth parameter
    # this will result in orphaned metadata
    # with include_orphans=True, this should be returned in queries
    fs = FileStore(test_dir, read_only=True, max_depth=1, include_orphans=True)
    with pytest.warns(UserWarning, match="Orphaned metadata was found in FileStore.json"):
        fs.connect()
    assert len(list(fs.query())) == 6
    assert len(list(fs.query({"tags": {"$exists": True}}))) == 6
    # all items, including orphans, should have a file_id and path_relative
    assert len(list(fs.query({"file_id": {"$exists": True}}))) == 6
    assert len(list(fs.query({"path_relative": {"$exists": True}}))) == 6
    assert len(list(fs.query({"orphan": True}))) == 1
    fs.close()

    # re-init the store after renaming one of the files on disk
    # this will result in orphaned metadata
    # with include_orphans=False (default), that metadata should be
    # excluded from query results
    Path(test_dir / "calculation1" / "input.in").rename(test_dir / "calculation1" / "input_renamed.in")
    fs = FileStore(test_dir, read_only=True, include_orphans=False)
    with pytest.warns(UserWarning, match="Orphaned metadata was found in FileStore.json"):
        fs.connect()
    assert len(list(fs.query())) == 6
    assert len(list(fs.query({"tags": {"$exists": True}}))) == 5
    assert len(list(fs.query({"path": {"$exists": True}}))) == 6
    # manually specifying orphan: True should still work
    assert len(list(fs.query({"orphan": True}))) == 1
    fs.close()


def test_store_files_moved(test_dir):
    """
    test behavior when the directory that constitutes the FileStore is
    moved to a new location on disk
    """
    # make a FileStore of all files and add metadata to all of them
    fs = FileStore(test_dir, read_only=False)
    fs.connect()
    data = list(fs.query())
    for d in data:
        d.update({"tags": "Ryan was here"})
    fs.update(data)

    # the orphan field should be populated for all documents, and False
    assert len(list(fs.query({"orphan": False}))) == 6
    original_file_ids = {f["file_id"] for f in fs.query()}
    original_paths = {f["path"] for f in fs.query()}
    fs.close()

    # now copy the entire FileStore to a new directory and re-initialize
    copy_tree(test_dir, str(test_dir / "new_store_location"))
    fs = FileStore(test_dir / "new_store_location", read_only=False)
    fs.connect()
    assert len(list(fs.query({"orphan": False}))) == 6
    assert {f["file_id"] for f in fs.query()} == original_file_ids
    # absolute paths should change to follow the FileStore
    assert {f["path"] for f in fs.query()} != original_paths
    for d in fs.query(properties=["path"]):
        assert str(d["path"]).startswith(str(fs.path))


def test_file_filters(test_dir):
    """
    Make sure multiple patterns work correctly
    """
    # here, we should get 2 input.in files and the file_2_levels_deep.json
    # the store's FileStore.json should be skipped even though .json is
    # in the file patterns
    fs = FileStore(test_dir, read_only=False, file_filters=["*.in", "*.json"])
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
    file_id = fs.query_one()["file_id"]
    with pytest.raises(StoreError, match="read-only"):
        fs.update({"file_id": file_id, "tags": "something"})
    with pytest.raises(StoreError, match="read-only"):
        fs.remove_docs({})


def test_query(test_dir):
    """
    File contents should be read unless file is too large
    size and path keys should not be returned unless explicitly requested
    querying on 'contents' should raise a warning
    contents should be empty if a file is too large
    empty properties kwarg should return contents, size, and path (along with everything else)
    """
    fs = FileStore(test_dir, read_only=True)
    fs.connect()
    d = fs.query_one(
        {"name": "input.in", "parent": "calculation1"},
        properties=["file_id", "contents"],
    )
    assert not d.get("size")
    assert not d.get("path")
    assert d.get("file_id")
    assert d.get("contents")
    assert "This is the file named input.in" in d["contents"]

    d = fs.query_one(
        {"name": "input.in", "parent": "calculation1"},
        properties=None,
    )
    assert d.get("size")
    assert d.get("path")
    assert d.get("file_id")
    assert d.get("contents")

    with pytest.warns(UserWarning, match="'contents' is not a queryable field!"):
        fs.query_one({"contents": {"$regex": "input.in"}})

    d = fs.query_one(
        {"name": "input.in", "parent": "calculation1"},
        properties=["name", "contents"],
        contents_size_limit=50,
    )
    assert d["contents"] == "File exceeds size limit of 50 bytes"
    assert d.get("name")


def test_remove(test_dir):
    """
    Test behavior of remove_docs()
    """
    fs = FileStore(test_dir, read_only=False)
    fs.connect()
    paths = [d["path"] for d in fs.query()]
    with pytest.raises(StoreError, match="about to delete 6 items"):
        fs.remove_docs({})
    fs.remove_docs({"name": "input.in"}, confirm=True)
    assert len(list(fs.query())) == 4
    assert not Path.exists(test_dir / "calculation1" / "input.in")
    assert not Path.exists(test_dir / "calculation2" / "input.in")
    fs.remove_docs({}, confirm=True)
    assert not any(Path(p).exists() for p in paths)


def test_metadata(test_dir):
    """
    1. init a FileStore
    2. add some metadata to both 'input.in' files
    3. confirm metadata written to .json
    4. close the store, init a new one
    5. confirm metadata correctly associated with the files
    """
    fs = FileStore(test_dir, read_only=False, last_updated_field="last_change")
    fs.connect()
    query = {"name": "input.in", "parent": "calculation1"}
    key = next(iter(fs.query(query)))[fs.key]
    fs.add_metadata(
        {
            "metadata": {"experiment date": "2022-01-18"},
            fs.last_updated_field: "this should not be here",
        },
        query,
    )

    # make sure metadata has been added to the item without removing other contents
    item_from_store = next(iter(fs.query({"file_id": key})))
    assert item_from_store.get("name", False)
    assert item_from_store.get("metadata", False)
    fs.close()

    # only the updated item should have been written to the JSON,
    # and it should not contain any of the protected keys
    data = fs.metadata_store.read_json_file(fs.path / fs.json_name)
    assert len(data) == 1
    item_from_file = next(d for d in data if d["file_id"] == key)
    assert item_from_file["metadata"] == {"experiment date": "2022-01-18"}
    assert not item_from_file.get("name")
    assert not item_from_file.get("path")
    assert not item_from_file.get(fs.last_updated_field)
    assert item_from_file.get("path_relative")

    # make sure metadata is preserved after reconnecting
    fs2 = FileStore(test_dir, read_only=True)
    fs2.connect()
    data = fs2.metadata_store.read_json_file(fs2.path / fs2.json_name)
    item_from_file = next(d for d in data if d["file_id"] == key)
    assert item_from_file["metadata"] == {"experiment date": "2022-01-18"}

    # make sure reconnected store properly merges in the metadata
    item_from_store = next(iter(fs2.query({"file_id": key})))
    assert item_from_store["name"] == "input.in"
    assert item_from_store["parent"] == "calculation1"
    assert item_from_store.get("metadata") == {"experiment date": "2022-01-18"}
    fs2.close()

    # make sure reconnecting with read_only=False doesn't remove metadata from the JSON
    fs3 = FileStore(test_dir, read_only=False)
    fs3.connect()
    data = fs3.metadata_store.read_json_file(fs3.path / fs3.json_name)
    item_from_file = next(d for d in data if d["file_id"] == key)
    assert item_from_file["metadata"] == {"experiment date": "2022-01-18"}
    item_from_store = next(iter(fs3.query({"file_id": key})))
    assert item_from_store["name"] == "input.in"
    assert item_from_store["parent"] == "calculation1"
    assert item_from_store.get("metadata") == {"experiment date": "2022-01-18"}
    fs3.close()

    # test automatic metadata assignment
    def add_data_from_name(d):
        return {"calc_name": d["name"][0:5]}

    fs4 = FileStore(test_dir, read_only=False)
    fs4.connect()
    # apply the auto function to all records
    fs4.add_metadata(auto_data=add_data_from_name)
    for d in fs4.query():
        print(d)
        assert d.get("calc_name", False) == d["name"][0:5]


def test_json_name(test_dir):
    """
    Make sure custom .json name works
    """
    fs = FileStore(test_dir, read_only=False, json_name="random.json")
    fs.connect()
    assert Path(test_dir / "random.json").exists()


def test_this_dir():
    """
    Make sure connect() works when path is "."
    """
    fs = FileStore(".")
    fs.connect()
    assert not fs.name.endswith(".")


def test_encoding():
    """
    Make sure custom encoding works
    """
    fs = FileStore(".", read_only=False, encoding="utf8")
    fs.connect()
    assert Path("FileStore.json").exists()
