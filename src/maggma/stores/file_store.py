# coding: utf-8
"""
Module defining a FileStore that enables accessing files in a local directory
using typical maggma access patterns.
"""

import hashlib

import warnings
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field
from pymongo import UpdateOne

from maggma.core import StoreError
from maggma.stores.mongolike import MemoryStore, JSONStore


class FileRecord(BaseModel):
    """
    Represent a file on disk. Records of this type will populate the
    'documents' key of each Item (directory) in the FileStore.
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

        See https://stackoverflow.com/questions/63051253/using-class-or-static-method-as-default-factory-in-dataclasses,
        except post_init is not supported in BaseModel at this time
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
        file_filters: Optional[List] = None,
        max_depth: Optional[int] = None,
        read_only: bool = True,
        json_name: str = "FileStore.json",
        **kwargs,
    ):
        """
        Initializes a FileStore
        Args:
            path: parent directory containing all files and subdirectories to process
            file_filters: List of fnmatch patterns defining the files to be tracked by
                the FileStore. Only files that match one of the patterns  provided will
                be included in the Store If None (default), all files are included.

                Examples: ["*.txt", "test-[abcd].txt"], etc.
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
        self.file_filters = file_filters if file_filters else ["*"]
        self.collection_name = "file_store"
        self.key = "file_id"
        self.read_only = read_only
        self.max_depth = max_depth

        self.metadata_store = JSONStore(
            paths=[str(self.path / self.json_name)],
            read_only=self.read_only,
            collection_name=self.collection_name,
            key=self.key,
        )

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
        for pattern in self.file_filters:
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
        # read all files and place them in the MemoryStore
        # use super.update to bypass the read_only guard statement
        # because we want the file data to be populated in memory
        super().connect()
        super().update([d.dict() for d in self.read()])

        # now read any metadata from the .json file
        try:
            self.metadata_store.connect()
            metadata = [d for d in self.metadata_store.query()]
        except FileNotFoundError:
            metadata = []
            warnings.warn(
                f"""
                JSON file '{self.json_name}' not found. To create this file automatically, re-initialize
                the FileStore with read_only=False.
                """
            )

        # merge metadata with file data and check for orphaned metadata
        requests = []
        found_orphans = False
        key = self.key
        file_ids = self.distinct(self.key)
        for d in metadata:
            if isinstance(key, list):
                search_doc = {k: d[k] for k in key}
            else:
                search_doc = {key: d[key]}

            if d[key] not in file_ids:
                found_orphans = True
                d.update({"orphan": True})

            del d["_id"]

            requests.append(UpdateOne(search_doc, {"$set": d}, upsert=True))

        if found_orphans:
            warnings.warn(
                f"Orphaned metadata was found in {self.json_name}. This metadata"
                "will be added to the store with {'orphan': True}"
            )
        if len(requests) > 0:
            self._collection.bulk_write(requests, ordered=False)

    def update(self, docs: Union[List[Dict], Dict], key: Union[List, str, None] = None):
        """
        Update items in the Store. Only possible if the store is not read only. Any new
        fields that are added will be written to the JSON file in the root directory
        of the FileStore.

        Note that certain fields that come from file metadata on disk are protected and
        cannot be updated with this method. This prevents the contents of the FileStore
        from becoming out of sync with the files on which it is based. The protected fields
        are all attributes of the FileRecord class, e.g. 'name', 'parent', 'last_updated',
        'hash', 'size', and 'orphan'. The 'path' and key fields are retained to
        make each document in the JSON file identifiable by manual inspection.

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
        data = [d for d in self.query()]
        filtered_data = []
        # remove fields that are populated by .read()
        protected_keys = {
            "_id",
            "name",
            "last_updated",
            "hash",
            "size",
            "parent",
            "orphan",
        }
        for d in data:
            filtered_d = {k: v for k, v in d.items() if k not in protected_keys}
            # don't write records that contain only file_id and path
            if len(set(filtered_d.keys()).difference(set(["path", self.key]))) != 0:
                filtered_data.append(filtered_d)
        self.metadata_store.update(filtered_data, self.key)

    def remove_docs(self, criteria: Dict):
        """
        Remove Items (FileRecord) matching the query dictionary.

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
