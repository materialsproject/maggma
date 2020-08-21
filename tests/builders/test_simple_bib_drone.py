from datetime import datetime
from pathlib import Path

import pytest

from maggma.stores import MongoStore

from .simple_bib_drone import SimpleBibDrone


@pytest.fixture
def init_drone(test_dir):
    """
    Initialize the drone, do not initialize the connection with the database

    :return:
        initialized drone
    """
    mongo_store = MongoStore(
        database="drone_test", collection_name="drone_test", key="record_key"
    )
    simple_path = test_dir / "simple_bib_example_data"
    assert simple_path.exists(), f"{simple_path} not found"
    simple_bib_drone = SimpleBibDrone(store=mongo_store, path=simple_path)
    return simple_bib_drone


def test_read(init_drone: SimpleBibDrone):
    """
    Test whether read function is correct
    :param init_drone: un-connected simpleBibDrone instance
    :return:
        None
    """
    list_record_id = init_drone.read(init_drone.path)
    assert len(list_record_id) == 7
    state_hashes = [r.state_hash for r in list_record_id]
    assert len(state_hashes) == len(list_record_id)  # all record_id has hash
    assert len((set(state_hashes))) == len(state_hashes)  # all unique hashes

    num_docs = sum([len(r.documents) for r in list_record_id])
    assert num_docs == 12


def test_record_id(init_drone: SimpleBibDrone):
    """
    Test validity of RecordIdentifier

    :param init_drone: un-connected simpleBibDrone instance
    :return:
        None
    """
    list_record_id = init_drone.read(init_drone.path)
    record0 = list_record_id[0]
    assert record0.parent_directory == init_drone.path
    assert record0.last_updated < datetime.now()
    assert len(record0.documents) > 0
    # state hash does not change when the file is not changed
    assert record0.compute_state_hash() == record0.state_hash


def test_process_item(init_drone: SimpleBibDrone):
    """
    Test whether data is expaneded correctly and whether meta data is added
    :param init_drone: un-connected simpleBibDrone instance
    :return:
        None
    """
    list_record_id = init_drone.read(init_drone.path)
    data = init_drone.process_item(list_record_id[0])
    assert "citations" in data
    assert "text" in data
    assert "record_key" in data
    assert "last_updated" in data
    assert "documents" in data
    assert "state_hash" in data


def test_compute_record_identifier_key(init_drone: SimpleBibDrone):
    list_record_id = init_drone.read(init_drone.path)
    record0 = list_record_id[0]
    doc0 = record0.documents[0]
    assert record0.record_key == init_drone.compute_record_identifier_key(doc0)


def test_get_items(init_drone: SimpleBibDrone):
    """
    This test might take a while
    test whether get_items work correctly.
    It should fetch from database all the files that needs to be updated

    :param init_drone: un-connected simpleBibDrone instance
    :return:
        None
    """

    init_drone.connect()
    init_drone.run()  # make sure the database is up-to-date
    init_drone.connect()
    assert sum([1 for _ in init_drone.get_items()]) == 0
    init_drone.finalize()

    init_drone.connect()
    init_drone.store.remove_docs(criteria={})  # clears the database
    assert sum([1 for _ in init_drone.get_items()]) == 7
    init_drone.finalize()


def test_assimilate(init_drone: SimpleBibDrone):
    """
    Test whether assimilate file is correct
    :param init_drone: un-connected simpleBibDrone instance
    :return:
    None
    """
    record_ids = init_drone.assimilate(init_drone.path)
    assert len(record_ids) == 7


def test_compute_data(init_drone: SimpleBibDrone):
    """
    test whether data is extracted as expected

    :param init_drone: un-connected simpleBibDrone instance
    :return:
        None
    """
    list_record_id = init_drone.read(init_drone.path)
    data = init_drone.process_item(list_record_id[0])
    assert "citations" in data
    assert "text" in data
