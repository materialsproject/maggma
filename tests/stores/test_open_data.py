import time
from datetime import datetime

import boto3
import pytest
import orjson
from bson import json_util

from botocore.exceptions import ClientError
from maggma.stores import MemoryStore
from maggma.stores.open_data import OpenDataStore, S3IndexStore
from moto import mock_s3


@pytest.fixture()
def memstore():
    store = MemoryStore("maggma_test", key="task_id")
    store.connect()
    yield store
    store._collection.drop()


@pytest.fixture()
def s3store():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        index = S3IndexStore(collection_name="index", bucket="bucket1", key="task_id")
        store = OpenDataStore(index=index, bucket="bucket1", key="task_id")
        store.connect()

        store.update(
            [
                {
                    "task_id": "mp-1",
                    "data": "asd",
                    store.last_updated_field: datetime.utcnow(),
                }
            ]
        )
        store.update(
            [
                {
                    "task_id": "mp-3",
                    "data": "sdf",
                    store.last_updated_field: datetime.utcnow(),
                }
            ]
        )

        yield store


@pytest.fixture()
def s3store_w_subdir():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        index = MemoryStore("index")
        store = OpenDataStore(index=index, bucket="bucket1", sub_dir="subdir1", s3_workers=1)
        store.connect()

        yield store


@pytest.fixture()
def s3store_multi():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        index = MemoryStore("index")
        store = OpenDataStore(index=index, bucket="bucket1", s3_workers=4)
        store.connect()

        yield store


@pytest.fixture()
def s3indexstore():
    data = [{"task_id": "mp-1", "last_updated": datetime.utcnow()}]
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")
        client = boto3.client("s3", region_name="us-east-1")
        client.put_object(
            Bucket="bucket1",
            Body=orjson.dumps(data, default=json_util.default),
            Key="manifest.json",
        )

        store = S3IndexStore(collection_name="index", bucket="bucket1", key="task_id")
        store.connect()

        yield store


def test_missing_manifest():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket2")
        store = S3IndexStore(collection_name="index", bucket="bucket2", key="task_id")
        store.connect()
        assert store.count() == 0


def test_boto_error_index():
    with mock_s3():
        store = S3IndexStore(collection_name="index", bucket="bucket2", key="task_id")
        with pytest.raises(RuntimeError):
            store.connect()


def test_index_eq(memstore, s3indexstore):
    assert s3indexstore == s3indexstore
    assert memstore != s3indexstore


def test_index_load_manifest(s3indexstore):
    assert s3indexstore.count() == 1
    assert s3indexstore.query_one({"task_id": "mp-1"}) is not None


def test_index_store_manifest(s3indexstore):
    data = [{"task_id": "mp-2", "last_updated": datetime.utcnow()}]
    s3indexstore.store_manifest(data)
    assert s3indexstore.count() == 1
    assert s3indexstore.query_one({"task_id": "mp-1"}) is None
    assert s3indexstore.query_one({"task_id": "mp-2"}) is not None


def test_keys():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")
        index = MemoryStore("index", key=1)
        with pytest.raises(AssertionError, match=r"Since we are.*"):
            store = OpenDataStore(index=index, bucket="bucket1", s3_workers=4, key=1)
        index = MemoryStore("index", key="key1")
        with pytest.warns(UserWarning, match=r"The desired S3Store.*$"):
            store = OpenDataStore(index=index, bucket="bucket1", s3_workers=4, key="key2")
        store.connect()
        store.update({"key1": "mp-1", "data": "1234"})
        with pytest.raises(KeyError):
            store.update({"key2": "mp-2", "data": "1234"})
        assert store.key == store.index.key == "key1"


def test_multi_update(s3store, s3store_multi):
    data = [
        {
            "task_id": str(j),
            "data": "DATA",
            s3store_multi.last_updated_field: datetime.utcnow(),
        }
        for j in range(32)
    ]

    def fake_writing(doc, search_keys):
        time.sleep(0.20)
        return {k: doc[k] for k in search_keys}

    s3store.write_doc_to_s3 = fake_writing
    s3store_multi.write_doc_to_s3 = fake_writing

    start = time.time()
    s3store_multi.update(data, key=["task_id"])
    end = time.time()
    time_multi = end - start

    start = time.time()
    s3store.update(data, key=["task_id"])
    end = time.time()
    time_single = end - start
    assert time_single > time_multi * (s3store_multi.s3_workers - 1) / (s3store.s3_workers)


def test_count(s3store):
    assert s3store.count() == 2
    assert s3store.count({"task_id": "mp-3"}) == 1


def test_qeuery(s3store):
    assert s3store.query_one(criteria={"task_id": "mp-2"}) is None
    assert s3store.query_one(criteria={"task_id": "mp-1"})["data"] == "asd"
    assert s3store.query_one(criteria={"task_id": "mp-3"})["data"] == "sdf"

    assert len(list(s3store.query())) == 2


def test_update(s3store):
    s3store.update(
        [
            {
                "task_id": "mp-199999",
                "data": "asd",
                s3store.last_updated_field: datetime.utcnow(),
            }
        ]
    )
    assert s3store.query_one({"task_id": "mp-199999"}) is not None

    s3store.update([{"task_id": "mp-4", "data": "asd"}])
    assert s3store.query_one({"task_id": "mp-4"})["data"] == "asd"
    assert s3store.s3_bucket.Object(s3store._get_full_key_path("mp-4")).key == "mp-4.json.gz"


def test_rebuild_index_from_s3_data(s3store):
    s3store.update([{"task_id": "mp-2", "data": "asd"}])
    index_docs = s3store.rebuild_index_from_s3_data()
    assert len(index_docs) == 3
    for doc in index_docs:
        for key in doc.keys():
            assert key == "task_id" or key == "last_updated"


def test_rebuild_index_from_data(s3store):
    data = [{"task_id": "mp-2", "data": "asd", "last_updated": datetime.utcnow()}]
    index_docs = s3store.rebuild_index_from_data(data)
    assert len(index_docs) == 1
    for doc in index_docs:
        for key in doc.keys():
            assert key == "task_id" or key == "last_updated"


def tests_msonable_read_write(s3store):
    dd = s3store.as_dict()
    s3store.update([{"task_id": "mp-2", "data": dd}])
    res = s3store.query_one({"task_id": "mp-2"})
    assert res["data"]["@module"] == "maggma.stores.open_data"


def test_remove(s3store):
    def objects_in_bucket(key):
        objs = list(s3store.s3_bucket.objects.filter(Prefix=key))
        return key in [o.key for o in objs]

    s3store.update([{"task_id": "mp-2", "data": "asd"}])
    s3store.update([{"task_id": "mp-4", "data": "asd"}])
    s3store.update({"task_id": "mp-5", "data": "aaa"})
    assert s3store.query_one({"task_id": "mp-2"}) is not None
    assert s3store.query_one({"task_id": "mp-4"}) is not None
    assert objects_in_bucket("mp-2.json.gz")
    assert objects_in_bucket("mp-4.json.gz")

    s3store.remove_docs({"task_id": "mp-2"})
    s3store.remove_docs({"task_id": "mp-4"}, remove_s3_object=True)

    assert objects_in_bucket("mp-2.json.gz")
    assert not objects_in_bucket("mp-4.json.gz")

    assert s3store.query_one({"task_id": "mp-5"}) is not None


def test_close(s3store):
    list(s3store.query())
    s3store.close()
    with pytest.raises(AttributeError):
        list(s3store.query())


def test_bad_import(mocker):
    mocker.patch("maggma.stores.aws.boto3", None)
    index = MemoryStore("index")
    with pytest.raises(RuntimeError):
        OpenDataStore(index=index, bucket="bucket1")


def test_aws_error(s3store):
    def raise_exception_NoSuchKey(data):
        error_response = {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}}
        raise ClientError(error_response, "raise_exception")

    def raise_exception_other(data):
        error_response = {"Error": {"Code": 405}}
        raise ClientError(error_response, "raise_exception")

    s3store.s3_bucket.Object = raise_exception_other
    with pytest.raises(ClientError):
        s3store.query_one()

    # Should just pass
    s3store.s3_bucket.Object = raise_exception_NoSuchKey
    s3store.query_one()


def test_eq(memstore, s3store):
    assert s3store == s3store
    assert memstore != s3store


def test_count_subdir(s3store_w_subdir):
    s3store_w_subdir.update([{"task_id": "mp-1", "data": "asd"}])
    s3store_w_subdir.update([{"task_id": "mp-2", "data": "asd"}])

    assert s3store_w_subdir.count() == 2
    assert s3store_w_subdir.count({"task_id": "mp-2"}) == 1


def test_subdir_storage(s3store_w_subdir):
    def objects_in_bucket(key):
        objs = list(s3store_w_subdir.s3_bucket.objects.filter(Prefix=key))
        return key in [o.key for o in objs]

    s3store_w_subdir.update([{"task_id": "mp-1", "data": "asd"}])
    s3store_w_subdir.update([{"task_id": "mp-2", "data": "asd"}])

    assert objects_in_bucket("subdir1/mp-1.json.gz")
    assert objects_in_bucket("subdir1/mp-2.json.gz")


def test_remove_subdir(s3store_w_subdir):
    s3store_w_subdir.update([{"task_id": "mp-2", "data": "asd"}])
    s3store_w_subdir.update([{"task_id": "mp-4", "data": "asd"}])

    assert s3store_w_subdir.query_one({"task_id": "mp-2"}) is not None
    assert s3store_w_subdir.query_one({"task_id": "mp-4"}) is not None

    s3store_w_subdir.remove_docs({"task_id": "mp-2"})

    assert s3store_w_subdir.query_one({"task_id": "mp-2"}) is None
    assert s3store_w_subdir.query_one({"task_id": "mp-4"}) is not None


def test_searchable_fields(s3store):
    tic = datetime(2018, 4, 12, 16)

    data = [{"task_id": f"mp-{i}", "a": i, s3store.last_updated_field: tic} for i in range(4)]

    s3store.searchable_fields = ["task_id"]
    s3store.update(data, key="a")

    # This should only work if the searchable field was put into the index store
    assert set(s3store.distinct("task_id")) == {"mp-0", "mp-1", "mp-2", "mp-3"}


def test_newer_in(s3store):
    with mock_s3():
        tic = datetime(2018, 4, 12, 16)
        tic2 = datetime.utcnow()
        conn = boto3.client("s3")
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket")

        index_old = S3IndexStore(collection_name="index_old", bucket="bucket")
        old_store = OpenDataStore(index=index_old, bucket="bucket")
        old_store.connect()
        old_store.update([{"task_id": "mp-1", "last_updated": tic}])
        old_store.update([{"task_id": "mp-2", "last_updated": tic}])

        index_new = S3IndexStore(collection_name="index_new", bucket="bucket", prefix="new")
        new_store = OpenDataStore(index=index_new, bucket="bucket", sub_dir="new")
        new_store.connect()
        new_store.update([{"task_id": "mp-1", "last_updated": tic2}])
        new_store.update([{"task_id": "mp-2", "last_updated": tic2}])

        assert len(old_store.newer_in(new_store)) == 2
        assert len(new_store.newer_in(old_store)) == 0

        assert len(old_store.newer_in(new_store.index)) == 2
        assert len(new_store.newer_in(old_store.index)) == 0


def test_additional_metadata(s3store):
    tic = datetime(2018, 4, 12, 16)

    data = [{"task_id": f"mp-{i}", "a": i, s3store.last_updated_field: tic} for i in range(4)]

    s3store.update(data, key="a", additional_metadata="task_id")

    # This should only work if the searchable field was put into the index store
    assert set(s3store.distinct("task_id")) == {"mp-0", "mp-1", "mp-2", "mp-3"}


def test_get_session(s3store):
    index = MemoryStore("index")
    store = OpenDataStore(
        index=index,
        bucket="bucket1",
        s3_profile={
            "aws_access_key_id": "ACCESS_KEY",
            "aws_secret_access_key": "SECRET_KEY",
        },
    )
    assert store._get_session().get_credentials().access_key == "ACCESS_KEY"
    assert store._get_session().get_credentials().secret_key == "SECRET_KEY"


def test_no_bucket():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        index = MemoryStore("index")
        store = OpenDataStore(index=index, bucket="bucket2")
        with pytest.raises(RuntimeError, match=r".*Bucket not present.*"):
            store.connect()
