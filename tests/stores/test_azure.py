"""
Azure testing requires Azurite.
It can be set up according to the instructions:
https://github.com/Azure/Azurite
With docker can be started by running:
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
"""

import time
from contextlib import contextmanager
from datetime import datetime

import pytest

from maggma.stores import AzureBlobStore, MemoryStore, MongoStore

try:
    import azure.storage.blob as azure_blob
    from azure.storage.blob import BlobServiceClient
except (ImportError, ModuleNotFoundError):
    azure_blob = None  # type: ignore


AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey="
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq"
    "/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)

AZURITE_CONTAINER_NAME = "maggma-test-container"


@pytest.fixture()
def mongostore():
    store = MongoStore("maggma_test", "test")
    store.connect()
    yield store
    store._collection.drop()


# a context manager and not a fixture to handle multiple containers in the
# same test
@contextmanager
def azurite_container(container_name=AZURITE_CONTAINER_NAME, create_container=True):
    if azure_blob is None:
        pytest.skip("azure-storage-blob is required to test AzureBlobStore")

    blob_service_client = BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)

    container_client = blob_service_client.get_container_client(container_name)
    if container_client.exists():
        container_client.delete_container()

    if create_container:
        container_client.create_container()

    try:
        yield
    finally:
        if container_client.exists():
            container_client.delete_container()


@pytest.fixture()
def blobstore():
    with azurite_container():
        index = MemoryStore("index", key="task_id")
        store = AzureBlobStore(
            index,
            container_name=AZURITE_CONTAINER_NAME,
            azure_client_info={"connection_string": AZURITE_CONNECTION_STRING},
        )
        store.connect()

        yield store


@pytest.fixture()
def blobstore_two_docs(blobstore):
    blobstore.update(
        [
            {
                "task_id": "mp-1",
                "data": "asd",
                blobstore.last_updated_field: datetime.utcnow(),
            }
        ]
    )
    blobstore.update(
        [
            {
                "task_id": "mp-3",
                "data": "sdf",
                blobstore.last_updated_field: datetime.utcnow(),
            }
        ]
    )

    return blobstore


@pytest.fixture()
def blobstore_w_subdir():
    with azurite_container():
        index = MemoryStore("index")
        store = AzureBlobStore(
            index,
            container_name=AZURITE_CONTAINER_NAME,
            azure_client_info={"connection_string": AZURITE_CONNECTION_STRING},
            sub_dir="subdir1",
        )
        store.connect()

        yield store


@pytest.fixture()
def blobstore_multi(blobstore):
    blobstore.workers = 4

    return blobstore


def test_keys():
    with azurite_container():
        index = MemoryStore("index", key=1)
        with pytest.raises(AssertionError, match=r"Since we are.*"):
            store = AzureBlobStore(index, AZURITE_CONTAINER_NAME, workers=4, key=1)
        index = MemoryStore("index", key="key1")
        with pytest.warns(UserWarning, match=r"The desired .*key.*$"):
            store = AzureBlobStore(
                index,
                AZURITE_CONTAINER_NAME,
                workers=4,
                key="key2",
                azure_client_info={"connection_string": AZURITE_CONNECTION_STRING},
            )
        store.connect()
        store.update({"key1": "mp-1", "data": "1234"})
        with pytest.raises(KeyError):
            store.update({"key2": "mp-2", "data": "1234"})
        assert store.key == store.index.key == "key1"


def test_multi_update(blobstore_two_docs, blobstore_multi):
    data = [
        {
            "task_id": str(j),
            "data": "DATA",
            blobstore_multi.last_updated_field: datetime.utcnow(),
        }
        for j in range(32)
    ]

    def fake_writing(doc, search_keys):
        time.sleep(0.20)
        return {k: doc[k] for k in search_keys}

    blobstore_two_docs.write_doc_to_blob = fake_writing
    blobstore_multi.write_doc_to_blob = fake_writing

    start = time.time()
    blobstore_multi.update(data, key=["task_id"])
    end = time.time()
    time_multi = end - start

    start = time.time()
    blobstore_two_docs.update(data, key=["task_id"])
    end = time.time()
    time_single = end - start
    assert time_single > time_multi * (blobstore_multi.workers - 1) / (blobstore_two_docs.workers)


def test_count(blobstore_two_docs):
    assert blobstore_two_docs.count() == 2
    assert blobstore_two_docs.count({"task_id": "mp-3"}) == 1


def test_query(blobstore_two_docs):
    assert blobstore_two_docs.query_one(criteria={"task_id": "mp-2"}) is None
    assert blobstore_two_docs.query_one(criteria={"task_id": "mp-1"})["data"] == "asd"
    assert blobstore_two_docs.query_one(criteria={"task_id": "mp-3"})["data"] == "sdf"

    assert len(list(blobstore_two_docs.query())) == 2


def test_update(blobstore_two_docs):
    blobstore_two_docs.update(
        [
            {
                "task_id": "mp-199999",
                "data": "asd",
                blobstore_two_docs.last_updated_field: datetime.utcnow(),
            }
        ]
    )
    assert blobstore_two_docs.query_one({"task_id": "mp-199999"}) is not None

    blobstore_two_docs.compress = True
    blobstore_two_docs.update([{"task_id": "mp-4", "data": "asd"}])
    obj = blobstore_two_docs.index.query_one({"task_id": "mp-4"})
    assert obj["compression"] == "zlib"
    assert obj["obj_hash"] == "be74de5ac71f00ec9e96441a3c325b0592c07f4c"
    assert blobstore_two_docs.query_one({"task_id": "mp-4"})["data"] == "asd"


def test_rebuild_meta_from_index(blobstore_two_docs):
    blobstore_two_docs.update([{"task_id": "mp-2", "data": "asd"}])
    blobstore_two_docs.index.update({"task_id": "mp-2", "add_meta": "hello"})
    blobstore_two_docs.rebuild_metadata_from_index()
    print(list(blobstore_two_docs.container.list_blobs()))
    blob_client = blobstore_two_docs.container.get_blob_client("mp-2")
    metadata = blob_client.get_blob_properties()["metadata"]
    assert metadata["add_meta"] == "hello"


def test_rebuild_index(blobstore_two_docs):
    blobstore_two_docs.update([{"task_id": "mp-2", "data": "asd"}])
    assert (
        blobstore_two_docs.index.query_one({"task_id": "mp-2"})["obj_hash"]
        == "a69fe0c2cca3a3384c2b1d2f476972704f179741"
    )
    blobstore_two_docs.index.remove_docs({})
    assert blobstore_two_docs.index.query_one({"task_id": "mp-2"}) is None
    blobstore_two_docs.rebuild_index_from_blob_data()
    assert (
        blobstore_two_docs.index.query_one({"task_id": "mp-2"})["obj_hash"]
        == "a69fe0c2cca3a3384c2b1d2f476972704f179741"
    )


def tests_msonable_read_write(blobstore_two_docs):
    dd = blobstore_two_docs.as_dict()
    blobstore_two_docs.update([{"task_id": "mp-2", "data": dd}])
    res = blobstore_two_docs.query_one({"task_id": "mp-2"})
    assert res["data"]["@module"] == "maggma.stores.azure"


def test_remove(blobstore_two_docs):
    # At time of writing, Azurite does not support the delete_blobs operation
    # of the ContainerClient. See https://github.com/Azure/Azurite/issues/1809
    pytest.skip("Azurite currently does not support delete_blobs")

    def objects_in_bucket(key):
        objs = list(blobstore_two_docs.container.list_blobs())
        return key in [o["name"] for o in objs]

    blobstore_two_docs.update([{"task_id": "mp-2", "data": "asd"}])
    blobstore_two_docs.update([{"task_id": "mp-4", "data": "asd"}])
    blobstore_two_docs.update({"task_id": "mp-5", "data": "aaa"})
    assert blobstore_two_docs.query_one({"task_id": "mp-2"}) is not None
    assert blobstore_two_docs.query_one({"task_id": "mp-4"}) is not None
    assert objects_in_bucket("mp-2")
    assert objects_in_bucket("mp-4")

    blobstore_two_docs.remove_docs({"task_id": "mp-2"})
    blobstore_two_docs.remove_docs({"task_id": "mp-4"}, remove_blob_object=True)

    assert objects_in_bucket("mp-2")
    assert not objects_in_bucket("mp-4")

    assert blobstore_two_docs.query_one({"task_id": "mp-5"}) is not None


def test_close(blobstore_two_docs):
    list(blobstore_two_docs.query())
    blobstore_two_docs.close()
    with pytest.raises(RuntimeError):
        list(blobstore_two_docs.query())


def test_bad_import(mocker):
    mocker.patch("maggma.stores.azure.azure_blob", None)
    index = MemoryStore("index")
    with pytest.raises(RuntimeError):
        AzureBlobStore(index, "bucket1")


def test_eq(mongostore, blobstore_two_docs):
    assert blobstore_two_docs == blobstore_two_docs
    assert blobstore_two_docs != mongostore


def test_count_subdir(blobstore_w_subdir):
    blobstore_w_subdir.update([{"task_id": "mp-1", "data": "asd"}])
    blobstore_w_subdir.update([{"task_id": "mp-2", "data": "asd"}])

    assert blobstore_w_subdir.count() == 2
    assert blobstore_w_subdir.count({"task_id": "mp-2"}) == 1


def test_subdir_field(blobstore_w_subdir):
    blobstore_w_subdir.update([{"task_id": "mp-1", "data": "asd"}])
    blobstore_w_subdir.update([{"task_id": "mp-2", "data": "asd"}])

    for cc in blobstore_w_subdir.index.query():
        assert len(cc["sub_dir"]) > 0
        assert cc["sub_dir"] == blobstore_w_subdir.sub_dir


def test_remove_subdir(blobstore_w_subdir):
    blobstore_w_subdir.update([{"task_id": "mp-2", "data": "asd"}])
    blobstore_w_subdir.update([{"task_id": "mp-4", "data": "asd"}])

    assert blobstore_w_subdir.query_one({"task_id": "mp-2"}) is not None
    assert blobstore_w_subdir.query_one({"task_id": "mp-4"}) is not None

    blobstore_w_subdir.remove_docs({"task_id": "mp-2"})

    assert blobstore_w_subdir.query_one({"task_id": "mp-2"}) is None
    assert blobstore_w_subdir.query_one({"task_id": "mp-4"}) is not None


def test_searchable_fields(blobstore_two_docs):
    tic = datetime(2018, 4, 12, 16)

    data = [{"task_id": f"mp-{i}", "a": i, blobstore_two_docs.last_updated_field: tic} for i in range(4)]

    blobstore_two_docs.searchable_fields = ["task_id"]
    blobstore_two_docs.update(data, key="a")

    # This should only work if the searchable field was put into the index store
    assert set(blobstore_two_docs.distinct("task_id")) == {
        "mp-0",
        "mp-1",
        "mp-2",
        "mp-3",
    }


def test_newer_in(blobstore):
    tic = datetime(2018, 4, 12, 16)
    tic2 = datetime.utcnow()

    name_old = AZURITE_CONTAINER_NAME
    name_new = AZURITE_CONTAINER_NAME + "-2"
    with azurite_container(name_old), azurite_container(name_new):
        index_old = MemoryStore("index_old")
        old_store = AzureBlobStore(
            index_old,
            name_old,
            azure_client_info={"connection_string": AZURITE_CONNECTION_STRING},
        )
        old_store.connect()
        old_store.update([{"task_id": "mp-1", "last_updated": tic}])
        old_store.update([{"task_id": "mp-2", "last_updated": tic}])

        index_new = MemoryStore("index_new")
        new_store = AzureBlobStore(
            index_new,
            name_new,
            azure_client_info={"connection_string": AZURITE_CONNECTION_STRING},
        )
        new_store.connect()
        new_store.update([{"task_id": "mp-1", "last_updated": tic2}])
        new_store.update([{"task_id": "mp-2", "last_updated": tic2}])

    assert len(old_store.newer_in(new_store)) == 2
    assert len(new_store.newer_in(old_store)) == 0

    assert len(old_store.newer_in(new_store.index)) == 2
    assert len(new_store.newer_in(old_store.index)) == 0


def test_additional_metadata(blobstore_two_docs):
    tic = datetime(2018, 4, 12, 16)

    data = [{"task_id": f"mp-{i}", "a": i, blobstore_two_docs.last_updated_field: tic} for i in range(4)]

    blobstore_two_docs.update(data, key="a", additional_metadata="task_id")

    # This should only work if the searchable field was put into the index store
    assert set(blobstore_two_docs.distinct("task_id")) == {
        "mp-0",
        "mp-1",
        "mp-2",
        "mp-3",
    }


def test_no_container():
    with azurite_container(create_container=False):
        index = MemoryStore("index")
        store = AzureBlobStore(
            index,
            AZURITE_CONTAINER_NAME,
            azure_client_info={"connection_string": AZURITE_CONNECTION_STRING},
        )
        with pytest.raises(RuntimeError, match=r".*Container not present.*"):
            store.connect()

        # check that the store can create
        store = AzureBlobStore(
            index,
            AZURITE_CONTAINER_NAME,
            azure_client_info={"connection_string": AZURITE_CONNECTION_STRING},
            create_container=True,
        )
        store.connect()


def test_name(blobstore):
    assert blobstore.name == f"container://{AZURITE_CONTAINER_NAME}"


def test_ensure_index(blobstore_two_docs):
    assert blobstore_two_docs.ensure_index("task-id")


def test_no_login():
    with azurite_container():
        index = MemoryStore("index")
        store = AzureBlobStore(
            index,
            AZURITE_CONTAINER_NAME,
            azure_client_info={},
        )

    with pytest.raises(RuntimeError, match=r".*Could not instantiate BlobServiceClient.*"):
        store.connect()


def test_credential_type_valid():
    credential_type = "DefaultAzureCredential"
    index = MemoryStore("index")
    store = AzureBlobStore(
        index,
        AZURITE_CONTAINER_NAME,
        azure_client_info="client_url",
        credential_type=credential_type,
    )
    assert store.credential_type == credential_type

    # tricks the store into thinking you already
    # provided the blob service client so it skips
    # the connection checks. We are only testing that
    # the credential import works properly
    store.service = True
    store.connect()

    from azure.identity import DefaultAzureCredential

    credential_type = DefaultAzureCredential
    index = MemoryStore("index")
    store = AzureBlobStore(
        index,
        AZURITE_CONTAINER_NAME,
        azure_client_info="client_url",
        credential_type=credential_type,
    )
    assert not isinstance(store.credential_type, str)
