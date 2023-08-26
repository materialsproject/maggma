import time
from datetime import datetime

import boto3
import pytest
from botocore.exceptions import ClientError
from maggma.stores import MemoryStore, MongoStore, S3Store
from moto import mock_s3


@pytest.fixture()
def mongostore():
    store = MongoStore("maggma_test", "test")
    store.connect()
    yield store
    store._collection.drop()


@pytest.fixture()
def s3store():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        index = MemoryStore("index", key="task_id")
        store = S3Store(index, "bucket1", key="task_id")
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
        store = S3Store(index, "bucket1", sub_dir="subdir1", s3_workers=1)
        store.connect()

        yield store


@pytest.fixture()
def s3store_multi():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        index = MemoryStore("index")
        store = S3Store(index, "bucket1", s3_workers=4)
        store.connect()

        yield store


def test_keys():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")
        index = MemoryStore("index", key=1)
        with pytest.raises(AssertionError, match=r"Since we are.*"):
            store = S3Store(index, "bucket1", s3_workers=4, key=1)
        index = MemoryStore("index", key="key1")
        with pytest.warns(UserWarning, match=r"The desired S3Store.*$"):
            store = S3Store(index, "bucket1", s3_workers=4, key="key2")
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

    s3store.compress = True
    s3store.update([{"task_id": "mp-4", "data": "asd"}])
    obj = s3store.index.query_one({"task_id": "mp-4"})
    assert obj["compression"] == "zlib"
    assert obj["obj_hash"] == "be74de5ac71f00ec9e96441a3c325b0592c07f4c"
    assert s3store.query_one({"task_id": "mp-4"})["data"] == "asd"


def test_rebuild_meta_from_index(s3store):
    s3store.update([{"task_id": "mp-2", "data": "asd"}])
    s3store.index.update({"task_id": "mp-2", "add_meta": "hello"})
    s3store.rebuild_metadata_from_index()
    s3_object = s3store.s3_bucket.Object("mp-2")
    assert s3_object.metadata["add_meta"] == "hello"


def test_rebuild_index(s3store):
    s3store.update([{"task_id": "mp-2", "data": "asd"}])
    assert s3store.index.query_one({"task_id": "mp-2"})["obj_hash"] == "a69fe0c2cca3a3384c2b1d2f476972704f179741"
    s3store.index.remove_docs({})
    assert s3store.index.query_one({"task_id": "mp-2"}) is None
    s3store.rebuild_index_from_s3_data()
    assert s3store.index.query_one({"task_id": "mp-2"})["obj_hash"] == "a69fe0c2cca3a3384c2b1d2f476972704f179741"


def tests_msonable_read_write(s3store):
    dd = s3store.as_dict()
    s3store.update([{"task_id": "mp-2", "data": dd}])
    res = s3store.query_one({"task_id": "mp-2"})
    assert res["data"]["@module"] == "maggma.stores.aws"


def test_remove(s3store):
    def objects_in_bucket(key):
        objs = list(s3store.s3_bucket.objects.filter(Prefix=key))
        return key in [o.key for o in objs]

    s3store.update([{"task_id": "mp-2", "data": "asd"}])
    s3store.update([{"task_id": "mp-4", "data": "asd"}])
    s3store.update({"task_id": "mp-5", "data": "aaa"})
    assert s3store.query_one({"task_id": "mp-2"}) is not None
    assert s3store.query_one({"task_id": "mp-4"}) is not None
    assert objects_in_bucket("mp-2")
    assert objects_in_bucket("mp-4")

    s3store.remove_docs({"task_id": "mp-2"})
    s3store.remove_docs({"task_id": "mp-4"}, remove_s3_object=True)

    assert objects_in_bucket("mp-2")
    assert not objects_in_bucket("mp-4")

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
        S3Store(index, "bucket1")


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

        index_old = MemoryStore("index_old")
        old_store = S3Store(index_old, "bucket")
        old_store.connect()
        old_store.update([{"task_id": "mp-1", "last_updated": tic}])
        old_store.update([{"task_id": "mp-2", "last_updated": tic}])

        index_new = MemoryStore("index_new")
        new_store = S3Store(index_new, "bucket")
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
    store = S3Store(
        index,
        "bucket1",
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
        store = S3Store(index, "bucket2")
        with pytest.raises(RuntimeError, match=r".*Bucket not present.*"):
            store.connect()
