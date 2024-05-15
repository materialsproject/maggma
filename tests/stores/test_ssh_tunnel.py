import paramiko
import pymongo
import pytest
from monty.serialization import dumpfn, loadfn
from paramiko.ssh_exception import AuthenticationException, NoValidConnectionsError, SSHException

from maggma.stores.mongolike import MongoStore
from maggma.stores.ssh_tunnel import SSHTunnel


@pytest.fixture()
def ssh_server_available():  # noqa: PT004
    """Fixture to determine if an SSH server is available to test the SSH tunnel."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect("127.0.0.1", 22)
        client.close()
    except (AuthenticationException, NoValidConnectionsError, SSHException):
        pytest.skip("No SSH server to test tunnel against")


def local_port_available(local_port):
    """Fixture to determine if a local port is available to test the SSH tunnel."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect("127.0.0.1", local_port)
        client.close()
    except (AuthenticationException, NoValidConnectionsError, SSHException):
        pytest.skip("Local port unavailable to test tunnel against")


@pytest.mark.parametrize("local_port", [None, 9000])
def test_mongostore_connect_via_ssh(ssh_server_available, local_port):
    if local_port is not None:
        local_port_available(local_port)

    tunnel = SSHTunnel("127.0.0.1:22", "127.0.0.1:27017", local_port=local_port)
    mongostore = MongoStore("maggma_test", "test", ssh_tunnel=tunnel)
    mongostore.connect()
    assert isinstance(mongostore._collection, pymongo.collection.Collection)
    mongostore.remove_docs({})
    assert mongostore.count() == 0

    mongostore.update([{"task_id": 0}])
    assert mongostore.count() == 1
    mongostore.remove_docs({})
    mongostore.close()


@pytest.mark.parametrize("local_port", [None, 9000])
def test_serialization(tmpdir, ssh_server_available, local_port):
    if local_port is not None:
        local_port_available(local_port)

    tunnel = SSHTunnel("127.0.0.1:22", "127.0.0.1:27017", local_port=local_port)
    dumpfn(tunnel, tmpdir / "tunnel.json")
    new_tunnel = loadfn(tmpdir / "tunnel.json")

    assert isinstance(new_tunnel, SSHTunnel)
