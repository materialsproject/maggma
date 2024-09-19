import gzip
import pickle
from datetime import datetime, timedelta
from io import BytesIO, StringIO

import boto3
import jsonlines
import pandas as pd
import pytest
from bson import json_util
from moto import mock_aws

from maggma.stores.open_data import OpenDataStore, PandasMemoryStore, S3IndexStore, TasksOpenDataStore

pd.set_option("future.no_silent_downcasting", True)


# PandasMemoryStore tests
@pytest.fixture()
def memstore():
    store = PandasMemoryStore(key="task_id")
    store.update(
        pd.DataFrame(
            [
                {
                    store.key: "mp-1",
                    store.last_updated_field: datetime.utcnow(),
                    "data": "asd",
                    "int_val": 1,
                },
                {
                    store.key: "mp-3",
                    store.last_updated_field: datetime.utcnow(),
                    "data": "sdf",
                    "int_val": 3,
                },
            ]
        )
    )
    return store


def test_pdmems_pickle(memstore):
    sobj = pickle.dumps(memstore)
    dobj = pickle.loads(sobj)
    assert hash(dobj) == hash(memstore)
    assert dobj == memstore


def test_pdmems_query(memstore):
    # bad criteria
    with pytest.raises(AttributeError, match=r".*only support query or is_in"):
        memstore.query(criteria={"boo": "hoo"})
    with pytest.raises(AttributeError, match=r".*please just use one or the other"):
        memstore.query(
            criteria={
                "query": f"{memstore.key} == 'mp-1'",
                "is_in": ("data", ["sdf", "fdr"]),
                "boo": "hoo",
            }
        )
    # all
    pd = memstore.query()
    assert len(pd) == 2
    # query criteria
    pd = memstore.query(criteria={"query": f"{memstore.key} == 'mp-1'"})
    assert len(pd) == 1
    assert pd[memstore.key].iloc[0] == "mp-1"
    assert pd["data"].iloc[0] == "asd"
    # is_in criteria
    pd = memstore.query(criteria={"is_in": ("data", ["sdf", "fdr"]), "boo": "hoo"})
    assert len(pd) == 1
    assert pd[memstore.key].iloc[0] == "mp-3"
    assert pd["data"].iloc[0] == "sdf"
    # properties
    pd = memstore.query(properties=[memstore.key, memstore.last_updated_field])
    assert len(pd) == 2
    assert len(pd[memstore.key]) == 2
    with pytest.raises(KeyError):
        assert pd["data"]
    with pytest.raises(KeyError):
        memstore.query(properties=["fake"])
    # sort
    pd = memstore.query(sort={memstore.key: -1})
    assert len(pd) == 2
    assert pd[memstore.key].iloc[0] == "mp-3"
    # skip
    pd = memstore.query(sort={memstore.key: -1}, skip=1)
    assert len(pd) == 1
    assert pd[memstore.key].iloc[0] == "mp-1"
    # limit
    pd = memstore.query(sort={memstore.key: -1}, limit=1)
    assert len(pd) == 1
    assert pd[memstore.key].iloc[0] == "mp-3"
    # all
    pd = memstore.query(
        criteria={
            "is_in": ("data", ["sdf", "fdr", "asd"]),
            "boo": "hoo",
        },
        properties=[memstore.key],
        sort={"data": -1},
        skip=1,
        limit=1,
    )
    assert len(pd) == 1
    assert pd[memstore.key].iloc[0] == "mp-1"
    with pytest.raises(KeyError):
        assert pd["data"]


def test_pdmems_count(memstore):
    assert memstore.count() == 2
    assert memstore.count(criteria={"query": f"{memstore.key} == 'mp-1'"}) == 1
    assert memstore.count(criteria={"is_in": ("data", ["sdf", "fdr"]), "boo": "hoo"}) == 1
    assert PandasMemoryStore(key="task_id").count() == 0


@pytest.fixture()
def memstore2():
    store = PandasMemoryStore(key="task_id")
    store.update(
        pd.DataFrame(
            [
                {
                    store.key: "mp-1",
                    store.last_updated_field: datetime.utcnow() - timedelta(hours=1),
                    "data": "asd",
                },
                {
                    store.key: "mp-2",
                    store.last_updated_field: datetime.utcnow() + timedelta(hours=1),
                    "data": "asd",
                },
                {
                    store.key: "mp-3",
                    store.last_updated_field: datetime.utcnow() + timedelta(hours=1),
                    "data": "sdf",
                },
            ]
        )
    )
    return store


def test_pdmems_distinct(memstore2):
    assert len(memstore2.distinct(field=memstore2.key)) == len(memstore2._data)
    assert len(memstore2.distinct(field="data")) == 2
    assert len(memstore2.distinct(field=memstore2.key, criteria={"query": "data == 'asd'"})) == 2


def test_pdmems_last_updated(memstore2):
    assert memstore2._data[memstore2.last_updated_field].iloc[2] == memstore2.last_updated
    store = PandasMemoryStore(key="task_id")
    assert store.last_updated == datetime.min


def test_pdmems_newer_in(memstore, memstore2):
    s = memstore.newer_in(memstore2)
    assert len(s) == 2
    assert "mp-2" in s.unique()
    assert "mp-3" in s.unique()
    s = memstore.newer_in(target=memstore2, criteria={"query": "data == 'asd'"}, exhaustive=True)
    assert len(s) == 1
    assert "mp-2" in s.unique()
    with pytest.raises(AttributeError):
        memstore.newer_in(target=memstore2, criteria={"query": "data == 'asd'"}, exhaustive=False)


def test_pdmems_update(memstore):
    df = pd.DataFrame(
        [
            {
                memstore.key: "mp-1",
                memstore.last_updated_field: datetime.utcnow(),
                "data": "boo",
                "int_val": 1,
            }
        ]
    )
    df2 = memstore.update(df)
    assert len(memstore._data) == 2
    assert memstore.query(criteria={"query": f"{memstore.key} == 'mp-1'"})["data"].iloc[0] == "boo"
    assert memstore.query(criteria={"query": f"{memstore.key} == 'mp-1'"})["int_val"].iloc[0] == 1
    assert df2.equals(df)
    df = pd.DataFrame(
        [
            {
                memstore.key: "mp-2",
                memstore.last_updated_field: datetime.utcnow(),
                "data": "boo",
                "int_val": 2,
            }
        ]
    )
    df2 = memstore.update(df)
    assert len(memstore._data) == 3
    assert memstore.query(criteria={"query": f"{memstore.key} == 'mp-2'"})["data"].iloc[0] == "boo"
    assert memstore.query(criteria={"query": f"{memstore.key} == 'mp-2'"})["int_val"].iloc[0] == 2
    assert df2.equals(df)


@pytest.fixture()
def s3indexstore():
    data = [{"task_id": "mp-1", "last_updated": datetime.utcnow()}]
    with mock_aws():
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
            Key="manifest.jsonl",
        )

        store = S3IndexStore(collection_name="index", bucket="bucket1", key="task_id")
        store.connect()

        yield store


def test_s3is_pickle(s3indexstore):
    sobj = pickle.dumps(s3indexstore)
    dobj = pickle.loads(sobj)
    assert hash(dobj) == hash(s3indexstore)
    assert dobj == s3indexstore


def test_s3is_connect_retrieve_manifest(s3indexstore):
    assert s3indexstore.retrieve_manifest().equals(s3indexstore._data)
    with mock_aws():
        with pytest.raises(s3indexstore.s3_client.exceptions.NoSuchBucket):
            S3IndexStore(collection_name="foo", bucket="bucket2", key="task_id").retrieve_manifest()
        with pytest.raises(RuntimeError):
            S3IndexStore(collection_name="foo", bucket="bucket2", key="task_id").connect()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket2")
        s3is = S3IndexStore(collection_name="foo", bucket="bucket2", key="task_id")
        s3is.connect()
        assert s3is._data is None
        assert s3is.retrieve_manifest() is None
        assert s3is.count() == 0


def test_s3is_store_manifest():
    with mock_aws():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket2")
        s3is = S3IndexStore(collection_name="foo", bucket="bucket2", key="task_id")
        s3is.connect()
        s3is.update(pd.DataFrame([{"task_id": "mp-2", "last_updated": "now"}]))
        s3is.store_manifest()
        df = s3is.retrieve_manifest()
        assert len(df) == 1
        assert df.equals(s3is._data)
        s3is.update(pd.DataFrame([{"task_id": "mp-3", "last_updated": "later"}]))
        df = s3is.retrieve_manifest()
        assert not df.equals(s3is._data)


def test_s3is_close(s3indexstore):
    s3indexstore.close()
    assert len(s3indexstore.query()) == 1  # actions auto-reconnect
    s3indexstore.update(pd.DataFrame([{"task_id": "mp-2", "last_updated": "now"}]))
    assert len(s3indexstore.query()) == 2
    s3indexstore.close()
    assert len(s3indexstore.query()) == 2  # actions auto-reconnect
    s3indexstore.close()
    s3indexstore.connect()
    assert len(s3indexstore.query()) == 1  # explicit connect reloads manifest


@pytest.fixture()
def s3store():
    with mock_aws():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        store = OpenDataStore(
            collection_name="index", bucket="bucket1", key="task_id", object_grouping=["group_level_two", "task_id"]
        )
        store.connect()

        store.update(
            pd.DataFrame(
                [
                    {
                        "task_id": "mp-1",
                        "data": "asd",
                        store.last_updated_field: datetime.utcnow(),
                        "group": {"level_two": 4},
                    },
                    {
                        "task_id": "mp-3",
                        "data": "sdf",
                        store.last_updated_field: datetime.utcnow(),
                        "group": {"level_two": 4},
                    },
                ]
            )
        )

        yield store


@pytest.fixture()
def s3store_w_subdir():
    with mock_aws():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")
        conn.create_bucket(Bucket="bucket2")

        index = S3IndexStore(collection_name="index", bucket="bucket1", key="task_id")
        store = OpenDataStore(
            index=index,
            collection_name=index.collection_name,
            bucket="bucket2",
            key=index.key,
            prefix="subdir1",
            object_grouping=["data_foo"],
        )
        store.connect()

        store.update(
            pd.DataFrame(
                [
                    {
                        "task_id": "mp-1",
                        "data": {"foo": "asd"},
                        store.last_updated_field: datetime.utcnow(),
                    },
                    {
                        "task_id": "mp-3",
                        "data": {"foo": "sdf"},
                        store.last_updated_field: datetime.utcnow(),
                    },
                ]
            )
        )

        yield store


def test_pickle(s3store):
    sobj = pickle.dumps(s3store)
    dobj = pickle.loads(sobj)
    assert hash(dobj) == hash(s3store)
    assert dobj == s3store
    assert len(dobj.query(criteria={"query": "task_id == 'mp-2'"})) == 0
    assert dobj.query(criteria={"query": "task_id == 'mp-1'"})["data"].iloc[0] == "asd"


def test_index_property(s3store, s3store_w_subdir):
    assert s3store.index != s3store
    assert s3store_w_subdir.index != s3store_w_subdir


def test_read_doc_from_s3():
    data = [
        {"task_id": "mp-1", "last_updated": datetime.utcnow(), "group": "foo", "also_group": "boo"},
        {"task_id": "mp-2", "last_updated": datetime.utcnow(), "group": "foo", "also_group": "boo"},
    ]
    with mock_aws():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")
        client = boto3.client("s3", region_name="us-east-1")
        string_io = StringIO()
        with jsonlines.Writer(string_io, dumps=json_util.dumps) as writer:
            for _, row in pd.DataFrame(data).iterrows():
                writer.write(row.to_dict())

        client.put_object(
            Bucket="bucket1",
            Body=BytesIO(gzip.compress(string_io.getvalue().encode("utf-8"))),
            Key="group=foo/also_group=boo.jsonl.gz",
        )

        store = OpenDataStore(
            collection_name="index", bucket="bucket1", key="task_id", object_grouping=["group", "also_group"]
        )
        store.connect()
        df = store._read_doc_from_s3(file_id="group=foo/also_group=boo.jsonl.gz")
        assert len(df) == 2
        assert store._get_full_key_path(index=df) == "group=foo/also_group=boo.jsonl.gz"
        assert (df["task_id"] == "mp-1").any()
        assert (df["task_id"] == "mp-2").any()


@pytest.mark.xfail(
    reason="Known issue, the store is in a deprecated state, and in particular may be incompatible with numpy 2.0+"
)
def test_update(s3store):
    assert len(s3store.index_data) == 2
    s3store.update(
        pd.DataFrame(
            [
                {
                    "task_id": "mp-199999",
                    "data": "asd",
                    "group": {"level_two": 4},
                    s3store.last_updated_field: datetime.utcnow(),
                }
            ]
        )
    )
    assert len(s3store.index_data) == 3
    with pytest.raises(KeyError):
        assert s3store.index_data.query("task_id == 'mp-199999'")["data"].iloc[0] == "asd"

    s3store.update(
        pd.DataFrame(
            [
                {
                    "task_id": "mp-199999",
                    "data": "foo",
                    "group": {"level_two": 4},
                    s3store.last_updated_field: datetime.utcnow(),
                }
            ]
        )
    )
    assert len(s3store.index_data) == 3
    assert len(s3store.index_data.query("task_id == 'mp-199999'")) == 1

    mp4 = [{"task_id": "mp-4", "data": "asd", "group": {"level_two": 4}, s3store.last_updated_field: datetime.utcnow()}]
    s3store.update(pd.DataFrame(mp4))
    assert len(s3store.index_data) == 4
    mp4_index = [{"task_id": "mp-4", "group_level_two": 4, s3store.last_updated_field: datetime.utcnow()}]
    assert s3store._get_full_key_path(pd.DataFrame(mp4_index)) == "group_level_two=4/task_id=mp-4.jsonl.gz"
    s3store.s3_client.head_object(Bucket=s3store.bucket, Key=s3store._get_full_key_path(pd.DataFrame(mp4_index)))


def test_query(s3store):
    assert len(s3store.query(criteria={"query": "task_id == 'mp-2'"})) == 0
    assert s3store.query(criteria={"query": "task_id == 'mp-1'"})["data"].iloc[0] == "asd"
    assert s3store.query(criteria={"query": "task_id == 'mp-3'"})["data"].iloc[0] == "sdf"
    assert s3store.query(criteria={"query": "task_id == 'mp-1'"}, properties=["task_id"])["task_id"].iloc[0] == "mp-1"
    assert (
        s3store.query(criteria={"query": "task_id == 'mp-1'"}, properties=["task_id", "data"])["data"].iloc[0] == "asd"
    )

    assert len(s3store.query()) == 2

    # will use optimized search
    df = s3store.query(criteria={"query": "task_id == 'mp-1'"}, properties=["task_id"], criteria_fields=["task_id"])
    assert len(df) == 1
    assert df["task_id"].iloc[0] == "mp-1"
    with pytest.raises(KeyError):
        assert df["data"].iloc[0] == "asd"

    # will use optimized search
    df = s3store.query(properties=["task_id"], criteria_fields=[])
    assert len(df) == 2

    # will not use optimized search even with hints since data is not in the searchable_fields
    df = s3store.query(criteria={"query": "data == 'asd'"}, properties=["task_id"], criteria_fields=["data"])
    assert len(df) == 1
    assert df["task_id"].iloc[0] == "mp-1"
    with pytest.raises(KeyError):
        assert df["data"].iloc[0] == "asd"


def test_rebuild_index_from_s3_data(s3store):
    data = [
        {"task_id": "mp-2", "data": "asd", s3store.last_updated_field: datetime.utcnow(), "group": {"level_two": 4}}
    ]
    client = boto3.client("s3", region_name="us-east-1")
    string_io = StringIO()
    with jsonlines.Writer(string_io, dumps=json_util.dumps) as writer:
        for _, row in pd.DataFrame(data).iterrows():
            writer.write(row.to_dict())

    data = [
        {"task_id": "mp-99", "data": "asd", s3store.last_updated_field: datetime.utcnow(), "group": {"level_two": 4}}
    ]
    string_io2 = StringIO()
    with jsonlines.Writer(string_io2, dumps=json_util.dumps) as writer:
        for _, row in pd.DataFrame(data).iterrows():
            writer.write(row.to_dict())

    client.put_object(
        Bucket="bucket1",
        Body=BytesIO(gzip.compress(string_io.getvalue().encode("utf-8"))),
        Key="group_level_two=4/task_id=mp-2.jsonl.gz",
    )

    # creating file that should not be indexed to test that it gets skipped
    client.put_object(
        Bucket="bucket1",
        Body=BytesIO(gzip.compress(string_io2.getvalue().encode("utf-8"))),
        Key="task_id=mp-99.gz",
    )

    assert len(s3store.index.index_data) == 2
    index_docs = s3store.rebuild_index_from_s3_data()
    assert len(index_docs) == 3
    assert len(s3store.index.index_data) == 3
    for key in index_docs.columns:
        assert key == "task_id" or key == "last_updated" or key == "group_level_two"


def test_rebuild_index_from_data(s3store):
    data = [
        {"task_id": "mp-2", "data": "asd", s3store.last_updated_field: datetime.utcnow(), "group": {"level_two": 4}}
    ]
    index_docs = s3store.rebuild_index_from_data(pd.DataFrame(data))
    assert len(index_docs) == 1
    assert len(s3store.index.index_data) == 1
    for key in index_docs.columns:
        assert key == "task_id" or key == "last_updated" or key == "group_level_two"


def test_count_subdir(s3store_w_subdir):
    s3store_w_subdir.update(
        pd.DataFrame(
            [{"task_id": "mp-1", "data": {"foo": "asd"}, s3store_w_subdir.last_updated_field: datetime.utcnow()}]
        )
    )
    s3store_w_subdir.update(
        pd.DataFrame(
            [{"task_id": "mp-2", "data": {"foo": "asd"}, s3store_w_subdir.last_updated_field: datetime.utcnow()}]
        )
    )

    assert len(s3store_w_subdir.query()) == 3
    assert len(s3store_w_subdir.query(criteria=None)) == 3
    assert s3store_w_subdir.count() == 3
    assert s3store_w_subdir.count({"query": "task_id == 'mp-2'"}) == 1


def test_subdir_storage(s3store_w_subdir):
    def objects_in_bucket(key):
        objs = s3store_w_subdir.s3_client.list_objects_v2(Bucket=s3store_w_subdir.bucket, Prefix=key)
        return key in [o["Key"] for o in objs["Contents"]]

    s3store_w_subdir.update(
        pd.DataFrame(
            [{"task_id": "mp-1", "data": {"foo": "asd"}, s3store_w_subdir.last_updated_field: datetime.utcnow()}]
        )
    )
    s3store_w_subdir.update(
        pd.DataFrame(
            [{"task_id": "mp-2", "data": {"foo": "asd"}, s3store_w_subdir.last_updated_field: datetime.utcnow()}]
        )
    )

    assert objects_in_bucket("subdir1/data_foo=asd.jsonl.gz")
    assert objects_in_bucket("subdir1/data_foo=sdf.jsonl.gz")


def test_additional_metadata(s3store):
    tic = datetime(2018, 4, 12, 16)

    data = [{"task_id": f"mp-{i}", "a": i, s3store.last_updated_field: tic} for i in range(4)]

    with pytest.raises(TypeError):
        s3store.update(data, key="a", additional_metadata="task_id")


def test_rebuild_index_from_s3_for_tasks_store():
    data = [{"task_id": "mp-2", "data": "asd", "last_updated": datetime.utcnow(), "group": {"level_two": 4}}]
    with mock_aws():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")

        string_io = StringIO()
        with jsonlines.Writer(string_io, dumps=json_util.dumps) as writer:
            for _, row in pd.DataFrame(data).iterrows():
                writer.write(row.to_dict())

        client = boto3.client("s3", region_name="us-east-1")
        client.put_object(
            Bucket="bucket1",
            Body=BytesIO(gzip.compress(string_io.getvalue().encode("utf-8"))),
            Key="group_level_two=4/dt=some_random_data.jsonl.gz",
        )

        store = TasksOpenDataStore(
            collection_name="index", bucket="bucket1", key="task_id", object_grouping=["group_level_two", "dt"]
        )
        store.connect()

        index_docs = store.rebuild_index_from_s3_data()
        assert len(index_docs) == 1
        assert len(store.index.index_data) == 1
        for key in index_docs.columns:
            assert key == "task_id" or key == "last_updated" or key == "group_level_two" or key == "dt"
        assert index_docs["dt"].iloc[0] == "some_random_data"


def test_no_update_for_tasks_store():
    with mock_aws():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")
        store = TasksOpenDataStore(
            collection_name="index", bucket="bucket1", key="task_id", object_grouping=["group_level_two", "dt"]
        )
        store.connect()

        with pytest.raises(NotImplementedError):
            store.update(
                pd.DataFrame(
                    [
                        {
                            "task_id": "mp-199999",
                            "data": "foo",
                            "group": {"level_two": 4},
                            store.last_updated_field: datetime.utcnow(),
                        }
                    ]
                )
            )
