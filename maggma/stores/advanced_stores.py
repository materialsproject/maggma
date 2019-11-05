# coding: utf-8
"""
Advanced Stores for behavior outside normal access patterns
"""
import os
import hvac
import json
from typing import Union, Optional, Dict, List, Iterator

from maggma.core import Store, StoreError, Sort
from maggma.stores.mongolike import MongoStore
from maggma.utils import lazy_substitute, substitute
from mongogrant import Client
from mongogrant.client import check
from mongogrant.config import Config


class MongograntStore(MongoStore):
    """Initialize a Store with a mongogrant "<role>:<host>/<db>." spec.

    This class does not subclass MongoStore, though it aims to reproduce
    relevant functionality through method delegation, e.g. groupby.

    It does not subclass MongoStore because some class methods of
    MongoStore, e.g. from_db_file and from_collection, are not supported.

    mongogrant documentation: https://github.com/materialsproject/mongogrant
    """

    def __init__(
        self,
        mongogrant_spec: str,
        collection_name: str,
        mgclient_config_path: Optional[str] = None,
        **kwargs
    ):
        """

        Args:
            mongogrant_spec (str): of the form <role>:<host>/<db>, where
                role is one of {"read", "readWrite"} or aliases {"ro", "rw"};
                host is a db host (w/ optional port) or alias; and db is a db
                on that host, or alias. See mongogrant documentation.
            collection_name (str): name of mongo collection
            mgclient_config_path (str): Path to mongogrant client config file,
               or None if default path (`mongogrant.client.path`).
        """
        self.mongogrant_spec = mongogrant_spec
        self.collection_name = collection_name
        self.mgclient_config_path = mgclient_config_path
        self._collection = None
        if set(("username", "password", "database", "host")) & set(kwargs):
            raise StoreError(
                "MongograntStore does not accept "
                "username, password, database, or host "
                "arguments. Use `mongogrant_spec`."
            )
        self.kwargs = kwargs
        super().__init__(**kwargs)

    def connect(self, force_reset: bool = False):
        """
        Connect to the mongogrant source
        Args:
            force_reset: forces the connection to reset rather than just
                         ensuring the connection is present
        """
        if not self._collection or force_reset:
            if self.mgclient_config_path:
                config = Config(check=check, path=self.mgclient_config_path)
                client = Client(config)
            else:
                client = Client()
            db = client.db(self.mongogrant_spec)
            self._collection = db[self.collection_name]

    def __hash__(self):
        return hash((self.mongogrant_spec, self.collection_name, self.lu_field))


class VaultStore(MongoStore):
    """
    Extends MongoStore to read credentials out of Vault server
    and uses these values to initialize MongoStore instance
    """

    def __init__(self, collection_name: str, vault_secret_path: str):
        """
        collection (string): name of mongo collection
        vault_secret_path (string): path on vault server with mongo creds object

        Environment (must be set prior to invocation):
        VAULT_ADDR - URL of vault server (eg. https://matgen8.lbl.gov:8200)
        VAULT_TOKEN or GITHUB_TOKEN - token used to authenticate to vault
        """
        # TODO: Switch this over to Pydantic ConfigSettings
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
        db_creds = json.loads(json_db_creds["data"]["value"])

        database = db_creds.get("db")
        host = db_creds.get("host", "localhost")
        port = db_creds.get("port", 27017)
        username = db_creds.get("username", "")
        password = db_creds.get("password", "")

        super(VaultStore, self).__init__(
            database, collection_name, host, port, username, password
        )


class AliasingStore(Store):
    """
    Special Store that aliases for the primary accessors
    """

    def __init__(self, store: Store, aliases: Dict, **kwargs):
        """
        Args:
            store: the store to wrap around
            aliases: dict of aliases of the form external key: internal key
        """
        self.store = store
        # Given an external key tells what the internal key is
        self.aliases = aliases
        # Given the internal key tells us what the external key is
        self.reverse_aliases = {v: k for k, v in aliases.items()}
        self.kwargs = kwargs

        kwargs.update({"lu_field": store.lu_field, "lu_type": store.lu_type})
        super(AliasingStore, self).__init__(**kwargs)

    def query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Sort]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Dict]:
        """
        Queries the Store for a set of documents

        Args:
            criteria : PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields
            skip: number documents to skip
            limit: limit on total number of documents returned
        """

        if isinstance(properties, list):
            properties = {p: 1 for p in properties}

        criteria = criteria if criteria else {}
        substitute(properties, self.reverse_aliases)
        lazy_substitute(criteria, self.reverse_aliases)
        for d in self.store.query(
            properties=properties, criteria=criteria, sort=sort, limit=limit, skip=skip
        ):
            substitute(d, self.aliases)
            yield d

    def distinct(
        self, field: Union[List[str], str], criteria: Optional[Dict] = None, all_exist: bool = False
    ) -> List:
        """
        Get all distinct values for a key

        Args:
            field: the field(s) to get distinct values for
            criteria : PyMongo filter for documents to search in
            all_exist : ensure all fields exist for the distinct set
        """
        criteria = criteria if criteria else {}
        lazy_substitute(criteria, self.reverse_aliases)
        field = field if isinstance(field, list) else [field]
        # substitute forward
        field = [self.aliases[f] for f in field]
        return self.store.distinct(field, criteria=criteria)

    def groupby(self, keys, criteria=None, properties=None, **kwargs):
        # Convert to a list
        keys = keys if isinstance(keys, list) else [keys]

        # Make the aliasing transformations on keys
        keys = [self.aliases[k] if k in self.aliases else k for k in keys]

        # Update criteria and properties based on aliases
        criteria = criteria if criteria else {}
        substitute(properties, self.reverse_aliases)
        lazy_substitute(criteria, self.reverse_aliases)

        return self.store.groupby(
            keys=keys, properties=properties, criteria=criteria, **kwargs
        )

    def update(self, docs, update_lu=True, key=None):
        key = key if key else self.key

        for d in docs:
            substitute(d, self.reverse_aliases)

        if key in self.aliases:
            key = self.aliases[key]

        self.store.update(docs, update_lu=update_lu, key=key)

    def ensure_index(self, key, unique=False, **kwargs):
        if key in self.aliases:
            key = self.aliases
        return self.store.ensure_index(key, unique, **kwargs)

    def close(self):
        self.store.close()

    @property
    def collection(self):
        return self.store.collection

    def connect(self, force_reset=False):
        self.store.connect(force_reset=force_reset)


class SandboxStore(Store):
    """
    Provides a sandboxed view to another store
    """

    def __init__(self, store, sandbox, exclusive=False):
        """
        store (Store): store to wrap sandboxing around
        sandbox (string): the corresponding sandbox
        exclusive (bool): whether to be exclusively in this sandbox or include global items
        """
        self.store = store
        self.sandbox = sandbox
        self.exclusive = exclusive
        super().__init__(
            key=self.store.key,
            lu_field=self.store.lu_field,
            lu_type=self.store.lu_type,
            validator=self.store.validator,
        )

    @property
    def sbx_criteria(self):
        if self.exclusive:
            return {"sbxn": self.sandbox}
        else:
            return {
                "$or": [{"sbxn": {"$in": [self.sandbox]}}, {"sbxn": {"$exists": False}}]
            }

    def query(self, criteria=None, properties=None, **kwargs):
        criteria = (
            dict(**criteria, **self.sbx_criteria) if criteria else self.sbx_criteria
        )
        return self.store.query(properties=properties, criteria=criteria, **kwargs)

    def query_one(self, criteria=None, properties=None, **kwargs):
        criteria = (
            dict(**criteria, **self.sbx_criteria) if criteria else self.sbx_criteria
        )
        return self.store.query_one(properties=properties, criteria=criteria, **kwargs)

    def distinct(self, key, criteria=None, **kwargs):
        criteria = (
            dict(**criteria, **self.sbx_criteria) if criteria else self.sbx_criteria
        )
        return self.store.distinct(key=key, criteria=criteria, **kwargs)

    def groupby(self, keys, criteria=None, properties=None, **kwargs):
        criteria = (
            dict(**criteria, **self.sbx_criteria) if criteria else self.sbx_criteria
        )

        return self.store.groupby(
            keys=keys, properties=properties, criteria=criteria, **kwargs
        )

    def update(self, docs, update_lu=True, key=None):
        for d in docs:
            if "sbxn" in d:
                d["sbxn"] = list(set(d["sbxn"] + [self.sandbox]))
            else:
                d["sbxn"] = [self.sandbox]

        self.store.update(docs, update_lu=update_lu, key=key)

    def ensure_index(self, key, unique=False, **kwargs):
        return self.store.ensure_index(key, unique, **kwargs)

    def close(self):
        self.store.close()

    @property
    def collection(self):
        return self.store.collection

    def connect(self, force_reset=False):
        self.store.connect(force_reset=force_reset)
