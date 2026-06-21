import select
import socketserver
import threading
from socket import socket
from typing import Optional

import paramiko
from monty.json import MSONable


class _ForwardHandler(socketserver.BaseRequestHandler):
    """Handle a single forwarded TCP connection through an SSH channel."""

    def handle(self):
        try:
            chan = self.server.ssh_transport.open_channel(
                "direct-tcpip",
                self.server.remote_address,
                self.request.getpeername(),
            )
        except Exception:
            return

        if chan is None:
            return

        try:
            while True:
                r, _, _ = select.select([self.request, chan], [], [])
                if self.request in r:
                    data = self.request.recv(1024)
                    if not data:
                        break
                    chan.send(data)
                if chan in r:
                    data = chan.recv(1024)
                    if not data:
                        break
                    self.request.send(data)
        finally:
            chan.close()


class _ForwardServer(socketserver.ThreadingTCPServer):
    """Local TCP server that forwards connections through an SSH tunnel."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, local_address, remote_address, ssh_transport):
        self.remote_address = remote_address
        self.ssh_transport = ssh_transport
        super().__init__(local_address, _ForwardHandler)


class SSHTunnel(MSONable):
    """SSH tunnel to remote server."""

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
            private_key: path to an SSH private key file to authenticate to the tunnel server
            kwargs: unused, kept for backward compatibility
        """
        self.tunnel_server_address = tunnel_server_address
        self.remote_server_address = remote_server_address
        self.local_port = local_port
        self.username = username
        self.password = password
        self.private_key = private_key
        self.kwargs = kwargs

        if local_port is None:
            local_port = _find_free_port("127.0.0.1")

        self._local_port = local_port

        ssh_address, ssh_port_str = tunnel_server_address.split(":")
        self._ssh_address = ssh_address
        self._ssh_port = int(ssh_port_str)

        remote_host, remote_port_str = remote_server_address.split(":")
        self._remote_host = remote_host
        self._remote_port = int(remote_port_str)

        self._transport: Optional[paramiko.Transport] = None
        self._server: Optional[_ForwardServer] = None
        self._is_active = False

    def start(self):
        if self._is_active:
            return

        transport = paramiko.Transport((self._ssh_address, self._ssh_port))

        if self.private_key is not None:
            pkey = _load_private_key(self.private_key, password=self.password)
            transport.connect(hostkey=None, username=self.username or "", pkey=pkey)
        else:
            transport.connect(hostkey=None, username=self.username or "", password=self.password)

        if not transport.is_authenticated():
            transport.close()
            raise paramiko.AuthenticationException(
                "SSH authentication failed: no credentials were accepted. "
                "Provide a username with a password or private key."
            )

        server = _ForwardServer(
            ("127.0.0.1", self._local_port),
            (self._remote_host, self._remote_port),
            transport,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        self._transport = transport
        self._server = server
        self._is_active = True

    def stop(self):
        if not self._is_active:
            return

        if self._server is not None:
            self._server.shutdown()
            self._server = None

        if self._transport is not None:
            self._transport.close()
            self._transport = None

        self._is_active = False

    @property
    def local_address(self) -> tuple[str, int]:
        return ("127.0.0.1", self._local_port)


def _load_private_key(path: str, password: Optional[str] = None) -> paramiko.PKey:
    """Load a private key of any supported type from a file."""
    try:
        return paramiko.PKey.from_private_key_file(path, password=password)
    except paramiko.SSHException as e:
        raise ValueError(f"Could not load private key from {path}: {e}") from e


def _find_free_port(address="127.0.0.1"):
    with socket() as s:
        s.bind((address, 0))
        return s.getsockname()[1]
