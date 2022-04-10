# coding: utf-8
"""
Module defining a FileStore that enables accessing files in a local directory
using typical maggma access patterns.
"""

import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field

from monty.io import zopen
from maggma.core import StoreError
from maggma.stores.mongolike import MemoryStore, JSONStore, json_serial


class FileRecord(BaseModel):
    """
    Represent a file on disk. Records of this type will populate the
    'documents' key of each Item (directory) in the FileStore.

    TODO: perhaps add convenience methods for reading/writing?
    TODO: is there a pre-existing Python equivalent of this?
    """

    name: str = Field(..., title="File name")
    path: Path = Field(..., title="Path of this file")
    parent: str = Field(None, title="Name of the parent directory")
    size: int = Field(None, title="Size of this file in bytes")
    file_id: str = Field(None, title="Unique identifier for this file")
    last_updated: datetime = Field(None, title="Time this file was last modified")
    hash: str = Field(None, title="Hash of the file contents")

    def __init__(self, *args, **kwargs):
        """
        Overriding __init__ allows class methods to function like a default_factory
        argument to various fields. Class methods cannot be used as default_factory
        methods because they have not been defined on init.

        See https://stackoverflow.com/questions/63051253/using-class-or-static-method-as-default-factory-in-dataclasses, except
        post_init is not supported in BaseModel at this time
        """
        super().__init__(*args, **kwargs)
        if not self.last_updated:
            self.last_updated = self.get_mtime()

        if not self.hash:
            self.hash = self.compute_hash()

        if not self.file_id:
            self.file_id = self.get_file_id()

        if not self.parent:
            self.parent = self.path.parent.name

        if not self.size:
            self.size = self.path.stat().st_size

    def compute_hash(self) -> str:
        """
        Hash of the state of the documents in this Directory
        """
        digest = hashlib.md5()
        block_size = 128 * digest.block_size
        digest.update(self.name.encode())
        with open(self.path.as_posix(), "rb") as file:
            buf = file.read(block_size)
            digest.update(buf)
        return str(digest.hexdigest())

    def get_mtime(self) -> datetime:
        """
        Get the timestamp when the file was last modified, using pathlib's
        stat.st_mtime() function. The timestamp is returned in UTC time.
        """
        return datetime.fromtimestamp(self.path.stat().st_mtime, tz=timezone.utc)

    def get_file_id(self) -> str:
        """
        Compute a unique identifier for this file. Currently this is just the hash of the full path
        """
        digest = hashlib.md5()
        digest.update(str(self.path).encode())
        return str(digest.hexdigest())

    @classmethod
    def from_file(cls, path):
        return FileRecord(path=path, name=path.name)


class FileStore(MemoryStore):
    """
    A Store for files on disk. Provides a common access method consistent with
    other stores. Each Item in the Store represents one file. Files can be organized
    into any type of directory structure.

    A hash of the full path to each file is used to define a file_id that uniquely
    identifies each item.

    Any metadata added to the items is written to a .json file in the root directory
    of the FileStore.
    """

    def __init__(
        self,
        path: Union[str, Path],
        track_files: Optional[List] = None,
        max_depth: Optional[int] = None,
        read_only: bool = True,
        json_name: str = "FileStore.json",
        **kwargs,
    ):
        """
        Initializes a FileStore
        Args:
            path: parent directory containing all files and subdirectories to process
            track_files: List of fnmatch patterns defining the files to be tracked by
                the FileStore. Only files that match one of the patterns  provided will
                be included in the Store If None (default), all files are included.

                Examples: "*.txt", "test-[abcd].txt", etc.
                See https://docs.python.org/3/library/fnmatch.html for full syntax
            max_depth: The maximum depth to look into subdirectories. 0 = no recursion,
                1 = include files 1 directory below the FileStore, etc.
                None (default) will scan all files below
                the FileStore root directory, regardless of depth.
            read_only: If True (default), the .update() and .remove_docs()
                methods are disabled, preventing any changes to the files on
                disk. In addition, metadata cannot be written to disk.
            json_name: Name of the .json file to which metadata is saved. If read_only
                is False, this file will be created in the root directory of the
                FileStore.
            kwargs: kwargs passed to MemoryStore.__init__()
        """

        self.path = Path(path) if isinstance(path, str) else path
        self.json_name = json_name
        self.track_files = track_files if track_files else ["*"]
        self.collection_name = "file_store"
        self.key = "file_id"
        self.read_only = read_only
        self.max_depth = max_depth

        if not self.read_only:
            self.metadata_store = JSONStore(
                paths=[str(self.path / self.json_name)],
                file_writable=(not self.read_only),
                collection_name=self.collection_name,
                key=self.key,
            )
        else:
            self.metadata_store = None
        self.kwargs = kwargs

        super().__init__(
            collection_name=self.collection_name,
            key=self.key,
            **self.kwargs,
        )

    @property
    def name(self) -> str:
        """
        Return a string representing this data source
        """
        return f"file://{self.path}"

    def read(self) -> List[FileRecord]:
        """
        Iterate through all files in the Store folder and populate
        the Store with FileRecord objects.
        """
        file_list = []
        # generate a list of files in subdirectories
        for pattern in self.track_files:
            # list every file that matches the pattern
            for f in self.path.rglob(pattern):
                if f.is_file():
                    # ignore the .json file created by the Store
                    if f.name == self.json_name:
                        continue
                    # filter based on depth
                    depth = len(f.relative_to(self.path).parts) - 1
                    if self.max_depth is None or depth <= self.max_depth:
                        file_list.append(FileRecord.from_file(f))

        return file_list

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data

        Read all the files in the directory, create corresponding File
        items in the internal MemoryStore.

        If there is a metadata .json file in the directory, read its
        contents into the MemoryStore

        Args:
            force_reset: whether to reset the connection or not
        """
        super().connect()
        super().update([k.dict() for k in self.read()], key=self.key)

        if self.metadata_store:
            self.metadata_store.connect()
            metadata = [d for d in self.metadata_store.query()]
            for d in metadata:
                d.pop("_id")
            super().update(metadata)

    def update(self, docs: Union[List[Dict], Dict], key: Union[List, str, None] = None):
        """
        Update items (directories) in the Store

        Args:
            docs: the document or list of documents to update
            key: field name(s) to determine uniqueness for a
                 document, can be a list of multiple fields,
                 a single field, or None if the Store's key
                 field is to be used
        """
        if self.read_only:
            raise StoreError(
                "This Store is read-only. To enable file I/O, re-initialize the store with read_only=False."
            )

        super().update(docs, key)
        self.metadata_store.update([{self.key: d[self.key], "metadata": d.get("metadata", {})} for d in docs], key)

    def remove_docs(self, criteria: Dict):
        """
        Remove Items (directories) matching the query dictionary.

        TODO: This method should delete the corresponding files on disk

        Args:
            criteria: query dictionary to match
        """
        if self.read_only:
            raise StoreError(
                "This Store is read-only. To enable file I/O, re-initialize the "
                "store with read_only=False."
            )

        # ids = [cursor._id for cursor in self._collection.find(criteria)]

        raise NotImplementedError("FileStore does not yet support deleting files.")
