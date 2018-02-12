# coding: utf-8
"""
Advanced Stores for behavior outside normal access patterns
"""
from maggma.stores import Store, MongoStore
from pydash.objects import set_, get, has
from pydash.utilities import to_path
import pydash.objects
import hvac
import json
import os


class VaultStore(MongoStore):
    """
    Extends MongoStore to read credentials out of Vault server
    and uses these values to initialize MongoStore instance
    """
    def __init__(self, collection_name, vault_secret_path):
        """
        collection (string): name of mongo collection
        vault_secret_path (string): path on vault server with mongo creds object

        Environment (must be set prior to invocation):
        VAULT_ADDR - URL of vault server (eg. https://matgen8.lbl.gov:8200)
        VAULT_TOKEN or GITHUB_TOKEN - token used to authenticate to vault
        """
        vault_addr = os.getenv("VAULT_ADDR")

        if not vault_addr:
            raise RuntimeError("VAULT_ADDR not set")

        client = hvac.Client(vault_addr)

        # If we have a vault token use this
        token = os.getenv("VAULT_TOKEN")

        # Look for a github token instead
        if not token:
            github_token = os.getenv("GITHUB_TOKEN")

            if github_token:
                client.auth_github(github_token)
            else:
                raise RuntimeError("VAULT_TOKEN or GITHUB_TOKEN not set")
        else:
            client.token = token
            if not client.is_authenticated():
                raise RuntimeError("Bad token")

        # Read the vault secret
        json_db_creds = client.read(vault_secret_path)
        db_creds = json.loads(json_db_creds['data']['value'])

        database = db_creds.get("db")
        host = db_creds.get("host", "localhost")
        port = db_creds.get("port", 27017)
        username = db_creds.get("username", "")
        password = db_creds.get("password", "")

        super(VaultStore, self).__init__(database,
                                         collection_name,
                                         host,
                                         port,
                                         username,
                                         password)


class AliasingStore(Store):
    """
    Special Store that aliases for the primary accessors
    """

    def __init__(self, store, aliases, **kwargs):
        """
        store (Store): the store to wrap around
        aliases (dict): dict of aliases of the form external key: internal key
        """
        self.store = store
        self.aliases = aliases
        self.reverse_aliases = {v: k for k, v in aliases.items()}
        self.kwargs = kwargs

        kwargs.update({"lu_field": store.lu_field, "lu_type": store.lu_type})
        super(AliasingStore, self).__init__(**kwargs)

    def query(self, properties=None, criteria=None, **kwargs):

        if isinstance(properties, list):
            properties = {p: 1 for p in properties}

        criteria = criteria if criteria else {}
        substitute(properties, self.reverse_aliases)
        lazy_substitute(criteria, self.reverse_aliases)
        for d in self.store.query(properties, criteria, **kwargs):
            substitute(d, self.aliases)
            yield d

    def query_one(self, properties=None, criteria=None, **kwargs):

        if isinstance(properties, list):
            properties = {p: 1 for p in properties}

        criteria = criteria if criteria else {}
        substitute(properties, self.reverse_aliases)
        lazy_substitute(criteria, self.reverse_aliases)
        d = self.store.query_one(properties, criteria, **kwargs)
        substitute(d, self.aliases)
        return d

    def distinct(self, key, criteria=None, **kwargs):
        if key in self.aliases:
            key = self.aliases[key]
        criteria = criteria if criteria else {}
        lazy_substitute(criteria, self.aliases)
        return self.store.distinct(key, criteria, **kwargs)

    def update(self, docs, update_lu=True, key=None):
        key = key if key else self.key

        for d in docs:
            substitute(d, self.reverse_aliases)

        if key in self.aliases:
            key = self.aliases[key]

        self.store.update(docs, update_lu=update_lu, key=key)

    def ensure_index(self, key, unique=False):
        if key in self.aliases:
            key = self.aliases
        return self.store.ensure_index(key, unique)

    def close(self):
        self.store.close()

    @property
    def collection(self):
        return self.store.collection

    def connect(self):
        self.store.connect()


def lazy_substitute(d, aliases):
    """
    Simple top level substitute that doesn't dive into mongo like strings
    """
    for alias, key in aliases.items():
        if key in d:
            d[alias] = d[key]
            del d[key]


def substitute(d, aliases):
    """
    Substitutes keys in dictionary
    Accepts multilevel mongo like keys
    """
    for alias, key in aliases.items():
        if has(d, key):
            set_(d, alias, get(d, key))
            unset(d, key)


def unset(d, key):
    """
    Unsets a key
    """
    pydash.objects.unset(d, key)
    path = to_path(key)
    for i in reversed(range(1, len(path))):
        if len(get(d, path[:i])) == 0:
            unset(d, path[:i])
