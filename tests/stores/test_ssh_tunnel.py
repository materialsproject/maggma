import paramiko
import pymongo
import pytest
from monty.serialization import dumpfn, loadfn
from paramiko.ssh_exception import (
    AuthenticationException,
    NoValidConnectionsError,
    SSHException,
)

from maggma.stores.mongolike import MongoStore, SSHTunnel


@pytest.fixture
def ssh_server_available():
    """
    Fixture to determine if an SSH server is available
    to test the SSH tunnel
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect("127.0.0.1", 22)
        client.close()
    except (AuthenticationException, NoValidConnectionsError, SSHException):
        pytest.skip("No SSH server to test tunnel against")


def test_mongostore_connect_via_ssh(ssh_server_available):
    server = SSHTunnel("127.0.0.1:22", "127.0.0.1:27017")

    mongostore = MongoStore("maggma_test", "test", ssh_tunnel=server)
    mongostore.connect()
    assert isinstance(mongostore._collection, pymongo.collection.Collection)
    mongostore.remove_docs({})
    assert mongostore.count() == 0

    mongostore.update([{"task_id": 0}])
    assert mongostore.count() == 1
    mongostore.remove_docs({})
    mongostore.close()


def test_serialization(tmpdir, ssh_server_available):
    tunnel = SSHTunnel("127.0.0.1:22", "127.0.0.1:27017")
    dumpfn(tunnel, tmpdir / "tunnel.json")
    new_tunnel = loadfn(tmpdir / "tunnel.json")

    assert isinstance(new_tunnel, SSHTunnel)
