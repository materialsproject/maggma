import time
import zlib

import boto3
import msgpack
import pytest
from botocore.exceptions import ClientError
from monty.msgpack import default, object_hook
from moto import mock_s3

from maggma.stores import MemoryStore, MongoStore, S3Store


@pytest.fixture
def mongostore():
    store = MongoStore("maggma_test", "test")
    store.connect()
    yield store
    store._collection.drop()


@pytest.fixture
def s3store():
    with mock_s3():
        conn = boto3.client("s3")
        conn.create_bucket(Bucket="bucket1")

        index = MemoryStore("index'")
        store = S3Store(index, "bucket1")
        store.connect()

        check_doc = {"task_id": "mp-1", "data": "asd"}
        store.index.update([{"task_id": "mp-1"}])
        store.s3_bucket.put_object(
            Key="mp-1", Body=msgpack.packb(check_doc, default=default)
        )

        check_doc2 = {"task_id": "mp-3", "data": "sdf"}
        store.index.update([{"task_id": "mp-3", "compression": "zlib"}])
        store.s3_bucket.put_object(
            Key="mp-3", Body=zlib.compress(msgpack.packb(check_doc2, default=default))
        )

        yield store


@pytest.fixture
def s3store_w_subdir():
    with mock_s3():
        conn = boto3.client("s3")
        conn.create_bucket(Bucket="bucket1")

        index = MemoryStore("index'")
        store = S3Store(index, "bucket1", sub_dir="subdir1", s3_workers=1)
        store.connect()

        yield store


@pytest.fixture
def s3store_multi():
    with mock_s3():
        conn = boto3.client("s3")
        conn.create_bucket(Bucket="bucket1")

        index = MemoryStore("index'")
        store = S3Store(index, "bucket1", s3_workers=4)
        store.connect()

        yield store


def test_multi_update(s3store, s3store_multi):
    data = [{"task_id": str(j), "data": "DATA"} for j in range(32)]

    def fake_writing(doc, search_keys):
        time.sleep(0.20)
        search_doc = {k: doc[k] for k in search_keys}
        return search_doc

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
    assert time_single > time_multi * (s3store_multi.s3_workers - 1) / (
        s3store.s3_workers
    )


def test_count(s3store):
    assert s3store.count() == 2
    assert s3store.count({"task_id": "mp-3"}) == 1


def test_qeuery(s3store):
    assert s3store.query_one(criteria={"task_id": "mp-2"}) is None
    assert s3store.query_one(criteria={"task_id": "mp-1"})["data"] == "asd"
    assert s3store.query_one(criteria={"task_id": "mp-3"})["data"] == "sdf"

    assert len(list(s3store.query())) == 2


def test_update(s3store):
    s3store.update([{"task_id": "mp-2", "data": "asd"}])
    assert s3store.query_one({"task_id": "mp-2"}) is not None

    s3store.compress = True
    s3store.update([{"task_id": "mp-4", "data": "asd"}])
    assert s3store.index.query_one({"task_id": "mp-4"})["compression"] == "zlib"
    assert s3store.query_one({"task_id": "mp-4"}) is not None
    assert s3store.query_one({"task_id": "mp-4"})["data"] == "asd"


def test_rebuild_meta_from_index(s3store):
    s3store.update([{"task_id": "mp-2", "data": "asd"}])
    s3store.index.update({"task_id": "mp-2", "add_meta": "hello"})
    s3store.rebuild_metadata_from_index()
    s3_object = s3store.s3_bucket.Object("mp-2")
    assert s3_object.metadata["add_meta"] == "hello"


def tests_msonable_read_write(s3store):
    dd = s3store.as_dict()
    s3store.update([{"task_id": "mp-2", "data": dd}])
    res = s3store.query_one({"task_id": "mp-2"})
    assert isinstance(res["data"], S3Store)


def test_remove(s3store):
    s3store.update([{"task_id": "mp-2", "data": "asd"}])
    s3store.update([{"task_id": "mp-4", "data": "asd"}])

    assert s3store.query_one({"task_id": "mp-2"}) is not None
    assert s3store.query_one({"task_id": "mp-4"}) is not None

    s3store.remove_docs({"task_id": "mp-2"})

    assert s3store.query_one({"task_id": "mp-2"}) is None
    assert s3store.query_one({"task_id": "mp-4"}) is not None


def test_close(s3store):
    list(s3store.query())
    s3store.close()
    with pytest.raises(AttributeError):
        list(s3store.query())


def test_bad_import(mocker):
    mocker.patch("maggma.stores.aws.boto3", None)
    with pytest.raises(RuntimeError):
        index = MemoryStore("index'")
        S3Store(index, "bucket1")


def test_aws_error(s3store):
    def raise_exception_404(data):
        error_response = {"Error": {"Code": 404}}
        raise ClientError(error_response, "raise_exception")

    def raise_exception_other(data):
        error_response = {"Error": {"Code": 405}}
        raise ClientError(error_response, "raise_exception")

    s3store.s3_bucket.Object = raise_exception_other
    with pytest.raises(ClientError):
        s3store.query_one()

    # Should just pass
    s3store.s3_bucket.Object = raise_exception_404
    s3store.query_one()


def test_eq(mongostore, s3store):
    assert s3store == s3store
    assert mongostore != s3store


def test_count_subdir(s3store_w_subdir):
    s3store_w_subdir.update([{"task_id": "mp-1", "data": "asd"}])
    s3store_w_subdir.update([{"task_id": "mp-2", "data": "asd"}])

    assert s3store_w_subdir.count() == 2
    assert s3store_w_subdir.count({"task_id": "mp-2"}) == 1


def test_subdir_field(s3store_w_subdir):
    s3store_w_subdir.update([{"task_id": "mp-1", "data": "asd"}])
    s3store_w_subdir.update([{"task_id": "mp-2", "data": "asd"}])

    for cc in s3store_w_subdir.index.query():
        assert len(cc["sub_dir"]) > 0
        assert cc["sub_dir"] == s3store_w_subdir.sub_dir


def test_remove_subdir(s3store_w_subdir):
    s3store_w_subdir.update([{"task_id": "mp-2", "data": "asd"}])
    s3store_w_subdir.update([{"task_id": "mp-4", "data": "asd"}])

    assert s3store_w_subdir.query_one({"task_id": "mp-2"}) is not None
    assert s3store_w_subdir.query_one({"task_id": "mp-4"}) is not None

    s3store_w_subdir.remove_docs({"task_id": "mp-2"})

    assert s3store_w_subdir.query_one({"task_id": "mp-2"}) is None
    assert s3store_w_subdir.query_one({"task_id": "mp-4"}) is not None
