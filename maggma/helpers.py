import json

from pymongo import MongoClient


def get_database(cred, **mongo_client_kwargs):
    """Connect to a database given a credential dict.

    Args:
        cred (dict): {database, [host, port, username, password]}

    Returns:
        pymongo.database.Database: The database object.
    """
    # respect potential multiprocessing fork
    mc_kwargs = dict(connect=False)
    mc_kwargs.update(mongo_client_kwargs)
    conn = MongoClient(
        cred.get('host', 'localhost'),
        cred.get('port', 27017),
        **mc_kwargs)
    db = conn[cred['database']]
    if cred.get('username'):
        db.authenticate(cred['username'], cred['password'])
    return db


class CredentialManager:

    roles = ['read', 'write', 'admin']

    def __init__(self, filepath):
        """
        Args:
            filepath (str): path to the file
        """
        with open(filepath) as f:
            self.creds = json.load(f)
            self.filepath = filepath

    def get_cred(self, spec):
        """Get DB credential dict.

        Args:
            spec (str): "<role>:<host[:port]>/<database>", where <role> is
                "read", "write", or "admin".

        Returns:
            dict: {host,port,database,username,password}

        """
        pass

    def add_cred(self, cred, role):
        """
        Add DB credential dict to `self.filepath`.

        Args:
            cred
            role
        """
        assert role in self.roles
        pass

    def ensure_cred(self, spec):
        """
        Attempt to ensure credentials as per spec.

        Generates user/pass if no existing spec match.
        Fails if host requires user/pass and cred file has neither
        an admin cred for the spec database nor a cred for the
        spec host admin db.

        Args:
            spec
        """
        pass
