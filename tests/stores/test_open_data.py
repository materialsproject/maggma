import pickle
from datetime import datetime
from io import BytesIO, StringIO

import boto3
import jsonlines
import pandas as pd
import pytest
from botocore.exceptions import ClientError
from bson import json_util
from moto import mock_s3

from maggma.stores.open_data import OpenDataStore, PandasMemoryStore, S3IndexStore


@pytest.fixture()
def memstore():
    store = PandasMemoryStore(key="task_id")
    store.connect()
    return store


@pytest.fixture()
def s3store():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        index = S3IndexStore(collection_name="index", bucket="bucket1", key="task_id")
        store = OpenDataStore(index=index, bucket="bucket1", key="task_id", object_grouping=["task_id"])
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

        index = S3IndexStore(collection_name="index", bucket="bucket1", key="task_id")
        store = OpenDataStore(
            index=index, bucket="bucket1", key="task_id", sub_dir="subdir1", s3_workers=1, object_grouping=["task_id"]
        )
        store.connect()

        yield store


@pytest.fixture()
def s3indexstore():
    data = [{"task_id": "mp-1", "last_updated": datetime.utcnow()}]
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")
        client = boto3.client("s3", region_name="us-east-1")
        string_io = StringIO()
        with jsonlines.Writer(string_io, dumps=json_util.dumps) as writer:
            for _, row in pd.DataFrame(data).iterrows():
                writer.write(row.to_dict())
        client.put_object(
            Bucket="bucket1",
            Body=BytesIO(string_io.getvalue().encode("utf-8")),
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
    assert s3indexstore.query_one({"query": "task_id == 'mp-1'"}) is not None


def test_index_store_manifest(s3indexstore):
    data = pd.DataFrame([{"task_id": "mp-2", "last_updated": datetime.utcnow()}])
    s3indexstore.store_manifest(data)
    assert s3indexstore.count() == 1
    assert s3indexstore.query_one({"query": "task_id == 'mp-1'"}) is None
    assert s3indexstore.query_one({"query": "task_id == 'mp-2'"}) is not None


def test_keys():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")
        index = PandasMemoryStore(key=1)
        with pytest.raises(AssertionError, match=r"Since we are.*"):
            store = OpenDataStore(index=index, bucket="bucket1", s3_workers=4, key=1)
        index = S3IndexStore(collection_name="test", bucket="bucket1", key="key1")
        with pytest.warns(UserWarning, match=r"The desired S3Store.*$"):
            store = OpenDataStore(index=index, bucket="bucket1", s3_workers=4, key="key2", object_grouping=["key1"])
        store.connect()
        store.update({"key1": "mp-1", "data": "1234", store.last_updated_field: datetime.utcnow()})
        with pytest.raises(KeyError):
            store.update({"key2": "mp-2", "data": "1234"})
        assert store.key == store.index.key == "key1"


def test_count(s3store):
    assert s3store.count() == 2
    assert s3store.count({"query": "task_id == 'mp-3'"}) == 1


def test_query(s3store):
    assert s3store.query_one(criteria={"query": "task_id == 'mp-2'"}) is None
    assert s3store.query_one(criteria={"query": "task_id == 'mp-1'"})["data"] == "asd"
    assert s3store.query_one(criteria={"query": "task_id == 'mp-3'"})["data"] == "sdf"
    assert s3store.query_one(criteria={"query": "task_id == 'mp-1'"}, properties=["task_id"])["task_id"] == "mp-1"
    assert s3store.query_one(criteria={"query": "task_id == 'mp-1'"}, properties=["task_id", "data"])["data"] == "asd"

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
    assert s3store.query_one({"query": "task_id == 'mp-199999'"}) is not None

    mp4 = [{"task_id": "mp-4", "data": "asd", s3store.last_updated_field: datetime.utcnow()}]
    s3store.update(mp4)
    assert s3store.query_one({"query": "task_id == 'mp-4'"})["data"] == "asd"
    assert s3store.s3_bucket.Object(s3store._get_full_key_path(pd.DataFrame(mp4))).key == "task_id=mp-4.jsonl.gz"


def test_rebuild_index_from_s3_data(s3store):
    s3store.update([{"task_id": "mp-2", "data": "asd", s3store.last_updated_field: datetime.utcnow()}])
    index_docs = s3store.rebuild_index_from_s3_data()
    assert len(index_docs) == 3
    for key in index_docs.columns:
        assert key == "task_id" or key == "last_updated"


def test_rebuild_index_from_data(s3store):
    data = [{"task_id": "mp-2", "data": "asd", s3store.last_updated_field: datetime.utcnow()}]
    index_docs = s3store.rebuild_index_from_data(pd.DataFrame(data))
    assert len(index_docs) == 1
    for key in index_docs.columns:
        assert key == "task_id" or key == "last_updated"


def tests_msonable_read_write(s3store, memstore):
    dd = memstore.as_dict()
    s3store.update([{"task_id": "mp-2", "data": dd, s3store.last_updated_field: datetime.utcnow()}])
    res = s3store.query_one({"query": "task_id == 'mp-2'"})
    assert res["data"]["@module"] == "maggma.stores.open_data"


def test_remove(s3store):
    with pytest.raises(NotImplementedError):
        s3store.remove_docs({"query": "task_id == 'mp-2'"})
    with pytest.raises(NotImplementedError):
        s3store.remove_docs({"query": "task_id == 'mp-4'"}, remove_s3_object=True)


def test_bad_import(mocker):
    mocker.patch("maggma.stores.aws.boto3", None)
    index = PandasMemoryStore()
    with pytest.raises(RuntimeError):
        OpenDataStore(index=index, bucket="bucket1", key="task_id")


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
    assert s3store != memstore


def test_count_subdir(s3store_w_subdir):
    s3store_w_subdir.update(
        [{"task_id": "mp-1", "data": "asd", s3store_w_subdir.last_updated_field: datetime.utcnow()}]
    )
    s3store_w_subdir.update(
        [{"task_id": "mp-2", "data": "asd", s3store_w_subdir.last_updated_field: datetime.utcnow()}]
    )

    assert s3store_w_subdir.count() == 2
    assert s3store_w_subdir.count({"query": "task_id == 'mp-2'"}) == 1


def test_subdir_storage(s3store_w_subdir):
    def objects_in_bucket(key):
        objs = list(s3store_w_subdir.s3_bucket.objects.filter(Prefix=key))
        return key in [o.key for o in objs]

    s3store_w_subdir.update(
        [{"task_id": "mp-1", "data": "asd", s3store_w_subdir.last_updated_field: datetime.utcnow()}]
    )
    s3store_w_subdir.update(
        [{"task_id": "mp-2", "data": "asd", s3store_w_subdir.last_updated_field: datetime.utcnow()}]
    )

    assert objects_in_bucket("subdir1/task_id=mp-1.jsonl.gz")
    assert objects_in_bucket("subdir1/task_id=mp-2.jsonl.gz")


def test_newer_in(s3store):
    with mock_s3():
        tic = datetime(2018, 4, 12, 16)
        tic2 = datetime.utcnow()
        conn = boto3.client("s3")
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket")

        index_old = S3IndexStore(collection_name="index_old", bucket="bucket", key="task_id")
        old_store = OpenDataStore(index=index_old, bucket="bucket", key="task_id", object_grouping=["task_id"])
        old_store.connect()
        old_store.update([{"task_id": "mp-1", "last_updated": tic}])
        old_store.update([{"task_id": "mp-2", "last_updated": tic}])

        index_new = S3IndexStore(collection_name="index_new", bucket="bucket", prefix="new", key="task_id")
        new_store = OpenDataStore(
            index=index_new, bucket="bucket", sub_dir="new", key="task_id", object_grouping=["task_id"]
        )
        new_store.connect()
        new_store.update([{"task_id": "mp-1", "last_updated": tic2}])
        new_store.update([{"task_id": "mp-2", "last_updated": tic2}])

        assert len(old_store.newer_in(new_store)) == 2
        assert len(new_store.newer_in(old_store)) == 0

        assert len(old_store.newer_in(new_store.index)) == 2
        assert len(new_store.newer_in(old_store.index)) == 0

        assert len(old_store.newer_in(new_store, exhaustive=True)) == 2
        assert len(new_store.newer_in(old_store, exhaustive=True)) == 0

        with pytest.raises(AttributeError):
            old_store.newer_in(new_store, criteria={"query": "task_id == 'mp-1'"})


def test_additional_metadata(s3store):
    tic = datetime(2018, 4, 12, 16)

    data = [{"task_id": f"mp-{i}", "a": i, s3store.last_updated_field: tic} for i in range(4)]

    with pytest.raises(NotImplementedError):
        s3store.update(data, key="a", additional_metadata="task_id")


def test_get_session(s3store):
    index = PandasMemoryStore(key="task_id")
    store = OpenDataStore(
        index=index,
        bucket="bucket1",
        key="task_id",
        s3_profile={
            "aws_access_key_id": "ACCESS_KEY",
            "aws_secret_access_key": "SECRET_KEY",
        },
        object_grouping=["task_id"],
    )
    assert store._get_session().get_credentials().access_key == "ACCESS_KEY"
    assert store._get_session().get_credentials().secret_key == "SECRET_KEY"


def test_no_bucket():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        index = PandasMemoryStore(key="task_id")
        store = OpenDataStore(index=index, bucket="bucket2", key="task_id", object_grouping=["task_id"])
        with pytest.raises(RuntimeError, match=r".*Bucket not present.*"):
            store.connect()


def test_pickle(s3store_w_subdir):
    sobj = pickle.dumps(s3store_w_subdir.index)
    dobj = pickle.loads(sobj)
    assert hash(dobj) == hash(s3store_w_subdir.index)
    assert dobj == s3store_w_subdir.index
    sobj = pickle.dumps(s3store_w_subdir)
    dobj = pickle.loads(sobj)
    assert hash(dobj) == hash(s3store_w_subdir)
    assert dobj == s3store_w_subdir
