# Using `SSHTunnel` to conect to remote database 

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

Note, the `local` indicates that the connction to the PRIVATE SERVER can only be made from the REMOTE SERVER.
```


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
and then pass it to the `S3Store` to connect to the database. 
By doing so, you can access the database at the localhost address `http://127.0.0.1:9000` from your local computer as if it is hosted on your local computer. 

The arguments of the `SSHTunnel` are self-explanatory, but `local_port` needs more explanation. We assume that on the local computer, we want t o connect to the localhost address `http://127.0.0.1`, so we do not need to provide the address, but only the port number (`9000` in this case.)

In essence, `SSHTunnel` allows you to connect to `COMPUTE_NODE_1:9000` on the private server from `http://127.0.0.1:9000` on your local computer.