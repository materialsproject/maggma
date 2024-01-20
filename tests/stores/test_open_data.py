import pickle
import time
from datetime import datetime

import boto3
import orjson
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

        index = S3IndexStore(collection_name="index", bucket="bucket1", key="task_id")
        store = OpenDataStore(index=index, bucket="bucket1", key="task_id", sub_dir="subdir1", s3_workers=1)
        store.connect()

        yield store


@pytest.fixture()
def s3store_multi():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        index = S3IndexStore(collection_name="index", bucket="bucket1", key="task_id")
        store = OpenDataStore(index=index, bucket="bucket1", key="task_id", s3_workers=4)
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
    assert s3indexstore.query_one({"query": "task_id == 'mp-1'"}) is not None


def test_index_store_manifest(s3indexstore):
    data = [{"task_id": "mp-2", "last_updated": datetime.utcnow()}]
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
    assert s3store.count({"query": "task_id == 'mp-3'"}) == 1


def test_qeuery(s3store):
    assert s3store.query_one(criteria={"query": "task_id == 'mp-2'"}) is None
    assert s3store.query_one(criteria={"query": "task_id == 'mp-1'"})["data"] == "asd"
    assert s3store.query_one(criteria={"query": "task_id == 'mp-3'"})["data"] == "sdf"

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

    s3store.update([{"task_id": "mp-4", "data": "asd"}])
    assert s3store.query_one({"query": "task_id == 'mp-4'"})["data"] == "asd"
    assert s3store.s3_bucket.Object(s3store._get_full_key_path("mp-4")).key == "mp-4.json.gz"


def test_rebuild_index_from_s3_data(s3store):
    s3store.update([{"task_id": "mp-2", "data": "asd"}])
    index_docs = s3store.rebuild_index_from_s3_data()
    assert len(index_docs) == 3
    for doc in index_docs:
        for key in doc:
            assert key == "task_id" or key == "last_updated"


def test_rebuild_index_from_data(s3store):
    data = [{"task_id": "mp-2", "data": "asd", "last_updated": datetime.utcnow()}]
    index_docs = s3store.rebuild_index_from_data(data)
    assert len(index_docs) == 1
    for doc in index_docs:
        for key in doc:
            assert key == "task_id" or key == "last_updated"


def tests_msonable_read_write(s3store, memstore):
    dd = memstore.as_dict()
    s3store.update([{"task_id": "mp-2", "data": dd}])
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
    s3store_w_subdir.update([{"task_id": "mp-1", "data": "asd"}])
    s3store_w_subdir.update([{"task_id": "mp-2", "data": "asd"}])

    assert s3store_w_subdir.count() == 2
    assert s3store_w_subdir.count({"query": "task_id == 'mp-2'"}) == 1


def test_subdir_storage(s3store_w_subdir):
    def objects_in_bucket(key):
        objs = list(s3store_w_subdir.s3_bucket.objects.filter(Prefix=key))
        return key in [o.key for o in objs]

    s3store_w_subdir.update([{"task_id": "mp-1", "data": "asd"}])
    s3store_w_subdir.update([{"task_id": "mp-2", "data": "asd"}])

    assert objects_in_bucket("subdir1/mp-1.json.gz")
    assert objects_in_bucket("subdir1/mp-2.json.gz")


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

        index_old = S3IndexStore(collection_name="index_old", bucket="bucket", key="task_id")
        old_store = OpenDataStore(index=index_old, bucket="bucket", key="task_id")
        old_store.connect()
        old_store.update([{"task_id": "mp-1", "last_updated": tic}])
        old_store.update([{"task_id": "mp-2", "last_updated": tic}])

        index_new = S3IndexStore(collection_name="index_new", bucket="bucket", prefix="new", key="task_id")
        new_store = OpenDataStore(index=index_new, bucket="bucket", sub_dir="new", key="task_id")
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

    s3store.update(data, key="a", additional_metadata="task_id")

    # This should only work if the searchable field was put into the index store
    assert set(s3store.distinct("task_id")) == {"mp-0", "mp-1", "mp-2", "mp-3"}


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
    )
    assert store._get_session().get_credentials().access_key == "ACCESS_KEY"
    assert store._get_session().get_credentials().secret_key == "SECRET_KEY"


def test_no_bucket():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        index = PandasMemoryStore(key="task_id")
        store = OpenDataStore(index=index, bucket="bucket2", key="task_id")
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


@pytest.fixture()
def thermo_store():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        index = S3IndexStore(collection_name="thermo", bucket="bucket1", key="thermo_id")
        store = OpenDataStore(index=index, bucket="bucket1", key="thermo_id")
        store.connect()

        store.update(
            [
                {
                    "thermo_id": "mp-1_R2SCAN",
                    "data": "asd",
                    store.last_updated_field: datetime.utcnow(),
                }
            ]
        )

        yield store


def test_thermo_collection_special_handling(thermo_store):
    assert thermo_store.s3_bucket.Object(thermo_store._get_full_key_path("mp-1_R2SCAN")).key == "R2SCAN/mp-1.json.gz"
    thermo_store.update([{"thermo_id": "mp-2_RSCAN", "data": "asd"}])
    index_docs = thermo_store.rebuild_index_from_s3_data()
    assert len(index_docs) == 2
    for doc in index_docs:
        for key in doc:
            assert key == "thermo_id" or key == "last_updated"


@pytest.fixture()
def xas_store():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        index = S3IndexStore(collection_name="xas", bucket="bucket1", key="spectrum_id")
        store = OpenDataStore(index=index, bucket="bucket1", key="spectrum_id")
        store.connect()

        store.update(
            [
                {
                    "spectrum_id": "mp-1-XAFS-Cr-K",
                    "data": "asd",
                    store.last_updated_field: datetime.utcnow(),
                }
            ]
        )

        yield store


def test_xas_collection_special_handling(xas_store):
    assert xas_store.s3_bucket.Object(xas_store._get_full_key_path("mp-1-XAFS-Cr-K")).key == "K/XAFS/Cr/mp-1.json.gz"
    xas_store.update([{"spectrum_id": "mp-2-XAFS-Li-K", "data": "asd"}])
    index_docs = xas_store.rebuild_index_from_s3_data()
    assert len(index_docs) == 2
    for doc in index_docs:
        for key in doc:
            assert key == "spectrum_id" or key == "last_updated"


@pytest.fixture()
def synth_descriptions_store():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        index = S3IndexStore(collection_name="synth_descriptions", bucket="bucket1", key="doi")
        store = OpenDataStore(index=index, bucket="bucket1", key="doi")
        store.connect()

        store.update(
            [
                {
                    "doi": "10.1149/2.051201jes",
                    "data": "asd",
                    store.last_updated_field: datetime.utcnow(),
                }
            ]
        )

        yield store


def test_synth_descriptions_collection_special_handling(synth_descriptions_store):
    assert (
        synth_descriptions_store.s3_bucket.Object(
            synth_descriptions_store._get_full_key_path("10.1149/2.051201jes")
        ).key
        == "10.1149_2.051201jes.json.gz"
    )
    synth_descriptions_store.update([{"doi": "10.1039/C5CP01095K", "data": "asd"}])
    index_docs = synth_descriptions_store.rebuild_index_from_s3_data()
    assert len(index_docs) == 2
    for doc in index_docs:
        for key in doc:
            assert key == "doi" or key == "last_updated"
