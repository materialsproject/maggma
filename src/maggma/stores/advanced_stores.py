# coding: utf-8
"""
Advanced Stores for behavior outside normal access patterns
"""
import json
import os
from typing import Dict, Iterator, List, Optional, Tuple, Union

from mongogrant import Client
from mongogrant.client import check
from mongogrant.config import Config
from monty.dev import requires

from maggma.core import Sort, Store, StoreError
from maggma.stores.mongolike import MongoStore
from maggma.utils import lazy_substitute, substitute

try:
    import hvac
except ImportError:
    hvac = None


class MongograntStore(MongoStore):
    """Initialize a Store with a mongogrant "`<role>`:`<host>`/`<db>`." spec.

    Some class methods of MongoStore, e.g. from_db_file and from_collection,
    are not supported.

    mongogrant documentation: https://github.com/materialsproject/mongogrant
    """

    def __init__(
        self,
        mongogrant_spec: str,
        collection_name: str,
        mgclient_config_path: Optional[str] = None,
        **kwargs,
    ):
        """
        Args:
            mongogrant_spec: of the form `<role>`:`<host>`/`<db>`, where
                role is one of {"read", "readWrite"} or aliases {"ro", "rw"};
                host is a db host (w/ optional port) or alias; and db is a db
                on that host, or alias. See mongogrant documentation.
            collection_name: name of mongo collection
            mgclient_config_path: Path to mongogrant client config file,
               or None if default path (`mongogrant.client.path`).
        """
        self.mongogrant_spec = mongogrant_spec
        self.collection_name = collection_name
        self.mgclient_config_path = mgclient_config_path
        self._collection = None

        if self.mgclient_config_path:
            config = Config(check=check, path=self.mgclient_config_path)
            client = Client(config)
        else:
            client = Client()

        if set(("username", "password", "database", "host")) & set(kwargs):
            raise StoreError(
                "MongograntStore does not accept "
                "username, password, database, or host "
                "arguments. Use `mongogrant_spec`."
            )

        self.kwargs = kwargs
        _auth_info = client.get_db_auth_from_spec(self.mongogrant_spec)
        super(MongograntStore, self).__init__(
            host=_auth_info["host"],
            database=_auth_info["authSource"],
            username=_auth_info["username"],
            password=_auth_info["password"],
            collection_name=self.collection_name,
            **kwargs,
        )

    @property
    def name(self):
        return f"mgrant://{self.mongogrant_spec}/{self.collection_name}"

    def __hash__(self):
        return hash(
            (self.mongogrant_spec, self.collection_name, self.last_updated_field)
        )

    @classmethod
    def from_db_file(cls, file):
        """
        Raises ValueError since MongograntStores can't be initialized from a file
        """
        raise ValueError("MongograntStore doesn't implement from_db_file")

    @classmethod
    def from_collection(cls, collection):
        """
        Raises ValueError since MongograntStores can't be initialized from a PyMongo collection
        """
        raise ValueError("MongograntStore doesn't implement from_collection")

    def __eq__(self, other: object) -> bool:
        """
        Check equality for MongograntStore

        Args:
            other: other MongograntStore to compare with
        """
        if not isinstance(other, MongograntStore):
            return False

        fields = [
            "mongogrant_spec",
            "collection_name",
            "mgclient_config_path",
            "last_updated_field",
        ]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


class VaultStore(MongoStore):
    """
    Extends MongoStore to read credentials out of Vault server
    and uses these values to initialize MongoStore instance
    """

    @requires(hvac is not None, "hvac is required to use VaultStore")
    def __init__(self, collection_name: str, vault_secret_path: str):
        """
        Args:
            collection_name: name of mongo collection
            vault_secret_path: path on vault server with mongo creds object

        Important:
            Environment variables that must be set prior to invocation
            VAULT_ADDR - URL of vault server (eg. https://matgen8.lbl.gov:8200)
            VAULT_TOKEN or GITHUB_TOKEN - token used to authenticate to vault
        """
        self.collection_name = collection_name
        self.vault_secret_path = vault_secret_path

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

    def __eq__(self, other: object) -> bool:
        """
        Check equality for VaultStore

        Args:
            other: other VaultStore to compare with
        """
        if not isinstance(other, VaultStore):
            return False

        fields = ["vault_secret_path", "collection_name", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


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

        kwargs.update(
            {
                "last_updated_field": store.last_updated_field,
                "last_updated_type": store.last_updated_type,
            }
        )
        super(AliasingStore, self).__init__(**kwargs)

    def name(self) -> str:
        """
        Return a string representing this data source
        """
        return self.store.name

    def count(self, criteria: Optional[Dict] = None) -> int:
        """
        Counts the number of documents matching the query criteria

        Args:
            criteria: PyMongo filter for documents to count in
        """
        criteria = criteria if criteria else {}
        lazy_substitute(criteria, self.reverse_aliases)
        return self.store.count(criteria)

    def query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Dict]:
        """
        Queries the Store for a set of documents

        Args:
            criteria: PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip
            limit: limit on total number of documents returned
        """

        criteria = criteria if criteria else {}

        if properties is not None:
            if isinstance(properties, list):
                properties = {p: 1 for p in properties}
            substitute(properties, self.reverse_aliases)

        lazy_substitute(criteria, self.reverse_aliases)
        for d in self.store.query(
            properties=properties, criteria=criteria, sort=sort, limit=limit, skip=skip
        ):
            substitute(d, self.aliases)
            yield d

    def distinct(
        self, field: str, criteria: Optional[Dict] = None, all_exist: bool = False
    ) -> List:
        """
        Get all distinct values for a field

        Args:
            field: the field(s) to get distinct values for
            criteria: PyMongo filter for documents to search in
        """
        criteria = criteria if criteria else {}
        lazy_substitute(criteria, self.reverse_aliases)

        # substitute forward
        return self.store.distinct(self.aliases[field], criteria=criteria)

    def groupby(
        self,
        keys: Union[List[str], str],
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Tuple[Dict, List[Dict]]]:
        """
        Simple grouping function that will group documents
        by keys.

        Args:
            keys: fields to group documents
            criteria: PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip
            limit: limit on total number of documents returned

        Returns:
            generator returning tuples of (dict, list of docs)
        """
        # Convert to a list
        keys = keys if isinstance(keys, list) else [keys]

        # Make the aliasing transformations on keys
        keys = [self.aliases[k] if k in self.aliases else k for k in keys]

        # Update criteria and properties based on aliases
        criteria = criteria if criteria else {}

        if properties is not None:
            if isinstance(properties, list):
                properties = {p: 1 for p in properties}
            substitute(properties, self.reverse_aliases)

        lazy_substitute(criteria, self.reverse_aliases)

        return self.store.groupby(
            keys=keys, properties=properties, criteria=criteria, skip=skip, limit=limit
        )

    def update(self, docs: Union[List[Dict], Dict], key: Union[List, str, None] = None):
        """
        Update documents into the Store

        Args:
            docs: the document or list of documents to update
            key: field name(s) to determine uniqueness for a
                 document, can be a list of multiple fields,
                 a single field, or None if the Store's key
                 field is to be used
        """
        key = key if key else self.key

        for d in docs:
            substitute(d, self.reverse_aliases)

        if key in self.aliases:
            key = self.aliases[key]

        self.store.update(docs, key=key)

    def remove_docs(self, criteria: Dict):
        """
        Remove docs matching the query dictionary

        Args:
            criteria: query dictionary to match
        """
        # Update criteria and properties based on aliases
        lazy_substitute(criteria, self.reverse_aliases)
        self.store.remove_docs(criteria)

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

    def __eq__(self, other: object) -> bool:
        """
        Check equality for AliasingStore

        Args:
            other: other AliasingStore to compare with
        """
        if not isinstance(other, AliasingStore):
            return False

        fields = ["store", "aliases", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


class SandboxStore(Store):
    """
    Provides a sandboxed view to another store
    """

    def __init__(self, store: Store, sandbox: str, exclusive: bool = False):
        """
        Args:
            store: store to wrap sandboxing around
            sandbox: the corresponding sandbox
            exclusive: whether to be exclusively in this sandbox or include global items
        """
        self.store = store
        self.sandbox = sandbox
        self.exclusive = exclusive
        super().__init__(
            key=self.store.key,
            last_updated_field=self.store.last_updated_field,
            last_updated_type=self.store.last_updated_type,
            validator=self.store.validator,
        )

    def name(self) -> str:
        """
        Returns:
            a string representing this data source
        """
        return f"Sandbox[{self.store.name}][{self.sandbox}]"

    @property
    def sbx_criteria(self) -> Dict:
        """
        Returns:
            the sandbox criteria dict used to filter the source store
        """
        if self.exclusive:
            return {"sbxn": self.sandbox}
        else:
            return {
                "$or": [{"sbxn": {"$in": [self.sandbox]}}, {"sbxn": {"$exists": False}}]
            }

    def count(self, criteria: Optional[Dict] = None) -> int:
        """
        Counts the number of documents matching the query criteria

        Args:
            criteria: PyMongo filter for documents to count in
        """
        criteria = (
            dict(**criteria, **self.sbx_criteria) if criteria else self.sbx_criteria
        )
        return self.store.count(criteria=criteria)

    def query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Dict]:
        """
        Queries the Store for a set of documents

        Args:
            criteria: PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip
            limit: limit on total number of documents returned
        """
        criteria = (
            dict(**criteria, **self.sbx_criteria) if criteria else self.sbx_criteria
        )
        return self.store.query(
            properties=properties, criteria=criteria, sort=sort, limit=limit, skip=skip
        )

    def groupby(
        self,
        keys: Union[List[str], str],
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[Tuple[Dict, List[Dict]]]:
        """
        Simple grouping function that will group documents
        by keys.

        Args:
            keys: fields to group documents
            criteria: PyMongo filter for documents to search in
            properties: properties to return in grouped documents
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            skip: number documents to skip
            limit: limit on total number of documents returned

        Returns:
            generator returning tuples of (dict, list of docs)
        """
        criteria = (
            dict(**criteria, **self.sbx_criteria) if criteria else self.sbx_criteria
        )

        return self.store.groupby(
            keys=keys, properties=properties, criteria=criteria, skip=skip, limit=limit
        )

    def update(self, docs: Union[List[Dict], Dict], key: Union[List, str, None] = None):
        """
        Update documents into the Store

        Args:
            docs: the document or list of documents to update
            key: field name(s) to determine uniqueness for a
                 document, can be a list of multiple fields,
                 a single field, or None if the Store's key
                 field is to be used
        """
        for d in docs:
            if "sbxn" in d:
                d["sbxn"] = list(set(d["sbxn"] + [self.sandbox]))
            else:
                d["sbxn"] = [self.sandbox]

        self.store.update(docs, key=key)

    def remove_docs(self, criteria: Dict):
        """
        Remove docs matching the query dictionary

        Args:
            criteria: query dictionary to match
        """
        # Update criteria and properties based on aliases
        criteria = (
            dict(**criteria, **self.sbx_criteria) if criteria else self.sbx_criteria
        )
        self.store.remove_docs(criteria)

    def ensure_index(self, key, unique=False, **kwargs):
        return self.store.ensure_index(key, unique, **kwargs)

    def close(self):
        self.store.close()

    @property
    def collection(self):
        return self.store.collection

    def connect(self, force_reset=False):
        self.store.connect(force_reset=force_reset)

    def __eq__(self, other: object) -> bool:
        """
        Check equality for SandboxStore

        Args:
            other: other SandboxStore to compare with
        """
        if not isinstance(other, SandboxStore):
            return False

        fields = ["store", "sandbox", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)
