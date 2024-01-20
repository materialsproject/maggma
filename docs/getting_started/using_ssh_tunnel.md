# Using `SSHTunnel` to connect to remote database

One of the typical scenarios to use `maggma` is to connect to a remote database that is behind a firewall and thus cannot be accessed directly from your local computer (as shown below, [image credits](https://github.com/pahaz/sshtunnel/)).

In this case, you can use `SSHTunnel` to first connect to the remote server, and then connect to the database from the server.

```
----------------------------------------------------------------------

                            |
-------------+              |    +----------+               +---------
    LOCAL    |              |    |  REMOTE  |               | PRIVATE
  COMPUTER   | <== SSH ========> |  SERVER  | <== local ==> | SERVER
-------------+              |    +----------+               +---------
                            |
                         FIREWALL (only port 22 is open)

----------------------------------------------------------------------

Note, the `local` indicates that the connection to the PRIVATE SERVER can only be made from the REMOTE SERVER.
```

## Example usage with `S3Store`

Below is an example of how to use `SSHTunnel` to connect to an AWS `S3Store` hosted on a private server.

Let's assume that, from you local computer, you can ssh to the remote server using the following command with your credentials (e.g. <USER_CREDENTIAL>):

```bash
ssh <USERNAME>@<REMOTE_SERVER_ADDRESS>
```

and then from the remote server, you can access your database using, e.g., the following information:
```
private_server_address: COMPUTE_NODE_1
private_server_port: 9000
```

You can create an `SSHTunnel` object as follows:

```python
from maggma.stores.ssh_tunnel import SSHTunnel

tunnel = SSHTunnel(
    tunnel_server_address = "<REMOTE_SERVER_ADDRESS>:22",
    username = "<USERNAME>",
    password= "<USER_CREDENTIAL>",
    remote_server_address = "COMPUTE_NODE_1:9000",
    local_port = 9000,
)
```
and then pass it to the `S3Store` to connect to the database. The arguments of the `SSHTunnel` are self-explanatory, but `local_port` needs more explanation. We assume that on the local computer, we want to connect to the localhost address `http://127.0.0.1`, so we do not need to provide the address, but only the port number (`9000` in this case.)

 In essence, `SSHTunnel` allows the connection to the database at `COMPUTE_NODE_1:9000` on the private server from the localhost address `http://127.0.0.1:9000` on the local computer as if the database is hosted on the local computer.

## Other use cases

Alternative to using `username` and `password` for authentication with the remote server, `SSHTunnel` also supports authentication using SSH keys. In this case, you will need to provide your SSH credentials using the `private_key` argument. Read the docs of the `SSHTunnel` for more information.


`SSHTunnel` can also be used with other stores such as `MongoStore`, `MongoURIStore`, and `GridFSStore`. The usage is similar to the example above, but you might need to adjust the arguments to the `SSHTunnel` to match the use case.
