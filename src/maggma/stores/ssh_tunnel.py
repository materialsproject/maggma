from socket import socket
from typing import Optional

from monty.json import MSONable
from sshtunnel import SSHTunnelForwarder


class SSHTunnel(MSONable):
    """SSH tunnel to remote server."""

    __TUNNELS: dict[str, SSHTunnelForwarder] = {}

    def __init__(
        self,
        tunnel_server_address: str,
        remote_server_address: str,
        local_port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        private_key: Optional[str] = None,
        **kwargs,
    ):
        """
        Args:
            tunnel_server_address: string address with port for the SSH tunnel server
            remote_server_address: string address with port for the server to connect to
            local_port: optional port to use for the local address (127.0.0.1);
                if `None`, a random open port will be automatically selected
            username: optional username for the ssh tunnel server
            password: optional password for the ssh tunnel server; If a private_key is
                supplied this password is assumed to be the private key password
            private_key: ssh private key to authenticate to the tunnel server
            kwargs: any extra args passed to the SSHTunnelForwarder.
        """
        self.tunnel_server_address = tunnel_server_address
        self.remote_server_address = remote_server_address
        self.local_port = local_port
        self.username = username
        self.password = password
        self.private_key = private_key
        self.kwargs = kwargs

        if remote_server_address in SSHTunnel.__TUNNELS:
            self.tunnel = SSHTunnel.__TUNNELS[remote_server_address]
        else:
            if local_port is None:
                local_port = _find_free_port("127.0.0.1")
            local_bind_address = ("127.0.0.1", local_port)

            ssh_address, ssh_port = tunnel_server_address.split(":")
            ssh_port = int(ssh_port)  # type: ignore

            remote_bind_address, remote_bind_port = remote_server_address.split(":")
            remote_bind_port = int(remote_bind_port)  # type: ignore

            if private_key is not None:
                ssh_password = None
                ssh_private_key_password = password
            else:
                ssh_password = password
                ssh_private_key_password = None

            self.tunnel = SSHTunnelForwarder(
                ssh_address_or_host=(ssh_address, ssh_port),
                local_bind_address=local_bind_address,
                remote_bind_address=(remote_bind_address, remote_bind_port),
                ssh_username=username,
                ssh_password=ssh_password,
                ssh_private_key_password=ssh_private_key_password,
                ssh_pkey=private_key,
                **kwargs,
            )

    def start(self):
        if not self.tunnel.is_active:
            self.tunnel.start()

    def stop(self):
        if self.tunnel.tunnel_is_up:
            self.tunnel.stop()

    @property
    def local_address(self) -> tuple[str, int]:
        return self.tunnel.local_bind_address


def _find_free_port(address="0.0.0.0"):
    s = socket()
    s.bind((address, 0))  # Bind to a free port provided by the host.
    return s.getsockname()[1]  # Return the port number assigned.
