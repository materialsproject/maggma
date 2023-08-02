""" Root store module with easy imports for implemented Stores """
from maggma.core import Store
from maggma.stores.advanced_stores import AliasingStore, MongograntStore, SandboxStore, VaultStore
from maggma.stores.aws import S3Store
from maggma.stores.azure import AzureBlobStore
from maggma.stores.compound_stores import ConcatStore, JointStore
from maggma.stores.file_store import FileStore
from maggma.stores.gridfs import GridFSStore
from maggma.stores.mongolike import JSONStore, MemoryStore, MongoStore, MongoURIStore, MontyStore

__all__ = [
    "Store",
    "AliasingStore",
    "MongograntStore",
    "SandboxStore",
    "VaultStore",
    "S3Store",
    "AzureBlobStore",
    "ConcatStore",
    "JointStore",
    "GridFSStore",
    "FileStore",
    "JSONStore",
    "MemoryStore",
    "MongoStore",
    "MongoURIStore",
    "MontyStore",
]
