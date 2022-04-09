# coding: utf-8
"""
Module defining a FileStore that enables accessing files in a local directory
using typical maggma access patterns.
"""

import warnings
import os
import json
import hashlib
import fnmatch
from pathlib import Path, PosixPath
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field

from monty.io import zopen
from maggma.core import Sort, StoreError
from maggma.stores.mongolike import MemoryStore, JSONStore, json_serial


class File(BaseModel):
    """
    Represent a file on disk. Records of this type will populate the
    'documents' key of each Item (directory) in the FileStore.

    TODO: perhaps add convenience methods for reading/writing?
    TODO: is there a pre-existing Python equivalent of this?
    """

    name: str = Field(..., title="File name")
    path: Path = Field(..., title="Path of this file")
    file_id: str = Field(None, title="Unique identifier for this file")
    last_updated: datetime = Field(None, title="Time this file was last modified")
    hash: str = Field(None, title="Hash of the file contents")

    def __init__(self, *args, **kwargs):
        """
        Overriding __init__ allows class methods
        to function like a default_factory argument to the last_updated and hash
        fields. Class methods cannot be used as default_factory methods because
        they have not been defined on init.

        See https://stackoverflow.com/questions/63051253/using-class-or-static-method-as-default-factory-in-dataclasses, except post_init is not
        supported in BaseModel at this time
        """
        super().__init__(*args, **kwargs)
        if not self.last_updated:
            self.last_updated = self.get_mtime()

        if not self.hash:
            self.hash = self.compute_hash()

        if not self.file_id:
            self.file_id = self.get_file_id()

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
        return Document(path=path, name=path.name)


class Directory(BaseModel):
    """
    The metadata for an item in the FileStore (a directory)
    """

    last_updated: datetime = Field(
        ..., title="The time in which this record is last updated"
    )
    documents: List[Document] = Field([], title="List of documents in this Directory")
    dir_name: str = Field(
        ...,
        title="Hash that uniquely define this record, can be inferred from each document inside",
    )

    @property
    def parent_directory(self) -> Path:
        """
        root most directory that documnents in this record share
        :return:
        """
        paths = [doc.path.as_posix() for doc in self.documents]
        parent_path = Path(os.path.commonprefix(paths))
        if not parent_path.is_dir():
            return parent_path.parent

        return parent_path

    @property
    def state_hash(self) -> str:
        """
        Hash of the state of the documents in this Directory
        """
        digest = hashlib.md5()
        block_size = 128 * digest.block_size
        for doc in self.documents:
            digest.update(doc.name.encode())
            with open(doc.path.as_posix(), "rb") as file:
                buf = file.read(block_size)
                digest.update(buf)
        return str(digest.hexdigest())


class FileStore(JSONStore):
    """
    A Store for files on disk. Provides a common access method consistent with other stores.

    Each Item is a subdirectory of the Path used to instantiate the Store
    that contains one or more files. For example,

    <path passed to FileStore.__init__()>
        calculation1/
            input.in
            output.out
            logfile.log
        calculation2/
            input.in
            output.out
            logfile.log
        calculation3/
            input.in
            output.out
            logfile.log

    The name of the subdirectory serves as the identifier for
    each item, and each item contains a list of File objects which each
    corresponds to a single file contained in the subdirectory. So the example
    data above would result in 3 unique items with keys 'calculation1',
    'calculation2', and 'calculation3'.
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
        track_files: List of files or fnmatch patterns to be    tracked by the FileStore.
                Only files that match the pattern provided will be included in the
                Directory for each directory or monitored for changes. If None
                (default), all files are included.
        max_depth: The maximum depth to look into subdirectories. 0 = no recursion, 1 = include files 1 directory below the FileStore, etc. None (default) will scan all files below
        the FileStore root directory, regardless of depth.
        read_only: If True (default), the .update() and .remove_docs
                () methods are disabled, preventing any changes to the files on disk. In addition, metadata cannot be written to disk.
        json_name: Name of the .json file to which metadata is saved. If read_only
                is False, this file will be created in the root directory of the
                FileStore.
        """

        self.path = Path(path) if isinstance(path, str) else path
        self.json_name = json_name
        self.paths = [str(self.path / self.json_name)]
        self.track_files = track_files if track_files else ["*"]
        self.kwargs = kwargs
        self.collection_name = "file_store"
        self.key = "file_id"
        self.read_only = read_only

        super().__init__(
            paths=self.paths,
            file_writable=(not self.read_only),
            collection_name=self.collection_name,
            key=self.key,
            **kwargs,
        )

    @property
    def name(self) -> str:
        """
        Return a string representing this data source
        """
        return f"file://{self.path}"

    def read(self) -> List[File]:
        """
        Iterate through all files in the Store folder and populate
        the Store with File objects.
        """
        file_list = []
        # generate a list of subdirectories
        for f in [f for f in self.path.rglob("*")]:
            if f.is_file():
                if f.name == self.json_name:
                    continue
                elif any([fnmatch.fnmatch(f.name, fn) for fn in self.track_files]):
                    file_list.append(File.from_file(f))

        return file_list

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data

        Read all the files in the directory, create corresponding File
        items in the internal MemoryStore

        Args:
            force_reset: whether to reset the connection or not
        """
        super().connect()
        super().update([k.dict() for k in self.read()], key=self.key)

    def close(self):
        """
        Closes any connections
        """
        # write out metadata and close the file handles
        super().close()

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

        # warnings.warn(
        #     "FileStore does not yet support file I/O. Therefore, adding a document to the store only affects the underlying MemoryStore and not any files on disk.", UserWarning
        # )
        super().update(docs, key)

    def update_json_file(self):
        """
        Updates the json file when a write-like operation is performed.
        """
        with zopen(self.paths[0], "w") as f:
            data = [d for d in self.query({}, properties=["file_id", "metadata"])]
            for d in data:
                d.pop("_id")
            json.dump(data, f, default=json_serial)

    def remove_docs(self, criteria: Dict):
        """
        Remove Items (directories) matching the query dictionary.

        TODO: should it be possible to make changes at the individual file (Document) level?
        TODO: This method should delete the corresponding files on disk

        Args:
            criteria: query dictionary to match
        """
        if self.read_only:
            raise StoreError(
                "This Store is read-only. To enable file I/O, re-initialize the store with read_only=False."
            )

        # ids = [cursor._id for cursor in self._collection.find(criteria)]

        raise NotImplementedError(
            "FileStore does not yet support file I/O. Therefore, documents cannot be removed from the FileStore."
        )
