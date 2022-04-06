# coding: utf-8
"""
Module defining a FileStore that enables accessing files in a local directory
using typical maggma access patterns.
"""

import copy
import json
import zlib

# import yaml
import warnings
import os
import hashlib
import fnmatch
from pathlib import Path, PosixPath
from datetime import datetime

# from pymongo.errors import ConfigurationError
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from pydantic import BaseModel, Field

from monty.json import jsanitize

from maggma.core import Sort, Store, StoreError
from maggma.stores.mongolike import MemoryStore


class Document(BaseModel):
    """
    Represent a file on disk. Records of this type will populate the
    'documents' key of each Item (directory) in the FileStore.

    TODO: perhaps add convenience methods for reading/writing?
    TODO: is there a pre-existing Python equivalent of this?
    """

    path: PosixPath = Field(..., title="Path of this file")
    name: str = Field(..., title="File name")
    last_updated: datetime = Field(
        ..., title="The time in which this record is last updated"
    )


class RecordIdentifier(BaseModel):
    """
    The metadata for an item in the FileStore (a directory)
    """

    last_updated: datetime = Field(
        ..., title="The time in which this record is last updated"
    )
    documents: List[Document] = Field(
        [], title="List of documents this RecordIdentifier indicate"
    )
    record_key: str = Field(
        ...,
        title="Hash that uniquely define this record, can be inferred from each document inside",
    )
    state_hash: Optional[str] = Field(
        None, title="Hash of the state of the documents in this Record"
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

    def compute_state_hash(self) -> str:
        """
        compute the hash of the state of the documents in this record
        :param doc_list: list of documents
        :return:
            hash of the list of documents passed in
        """
        digest = hashlib.md5()
        block_size = 128 * digest.block_size
        for doc in self.documents:
            digest.update(doc.name.encode())
            with open(doc.path.as_posix(), "rb") as file:
                buf = file.read(block_size)
                digest.update(buf)
        return str(digest.hexdigest())


class FileStore(MemoryStore):
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
    each item, and each item contains a list of Document objects which each
    corresponds to a single file contained in the subdirectory. So the example
    data above would result in 3 unique items with keys 'calculation1',
    'calculation2', and 'calculation3'.
    """

    def __init__(
        self,
        path: Union[str, Path],
        track_files: Optional[List],
        read_only: bool = True,
        **kwargs,
    ):
        """
        Initializes a FileStore
        Args:
        path: parent directory containing all files and subdirectories to process
        track_files: List of files or fnmatch patterns to be tracked by the FileStore.
                Only files that match the pattern provided will be included in the
                RecordIdentifier for each directory or monitored for changes. If None
                (default), all files are included.
        read_only: If True (default), the .update() and .remove_docs
                () methods are disabled, preventing any changes to the files on disk.
        """

        self.path = Path(path) if isinstance(path, str) else path
        self.track_files = track_files if track_files else ["*"]
        self.kwargs = kwargs
        self.collection_name = "file_store"
        self.read_only = read_only

        super().__init__(collection_name=self.collection_name, **kwargs)

    @property
    def name(self) -> str:
        """
        Return a string representing this data source
        """
        return f"file://{self.path}"

    def read(self) -> List[RecordIdentifier]:
        """
        Given a folder path to a data folder, read all the files, and return a
        list of RecordIdentifier objects containing metadata about the contents
        of each directory.
        """
        record_id_list = []
        # generate a list of subdirectories
        for d in [d for d in self.path.iterdir() if d.is_dir()]:
            doc_list = [
                Document(path=f, name=f.name, last_updated=f.stat().st_mtime)
                for f in d.iterdir()
                if f.is_file()
                and any([fnmatch.fnmatch(f.name, fn) for fn in self.track_files])
            ]
            record_id = RecordIdentifier(
                last_updated=datetime.now(), documents=doc_list, record_key=d.name
            )
            record_id.state_hash = record_id.compute_state_hash()
            record_id_list.append(record_id)
        return record_id_list

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data

        Read all the files in the directory, create corresponding Document
        items in the internal MemoryStore

        Args:
            force_reset: whether to reset the connection or not
        """
        super().connect()
        super().update([k.dict() for k in self.read()], key="record_key")

    def close(self):
        """
        Closes any connections
        """
        # write out metadata and close the file handles
        super().close()

    def update(self, docs: Union[List[Dict], Dict], key: Union[List, str, None] = None):
        """
        Update items (directories) in the Store

        TODO: should it be possible to make changes at the individual file (Document) level?
        TODO: this method should create the new directory on disk.

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

        warnings.warn(
            UserWarning,
            "FileStore does not yet support file I/O. Therefore, adding a document to the store only affects the underlying MemoryStore and not any files on disk.",
        )

        super.update()

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

        ids = [cursor._id for cursor in self._collection.find(criteria)]

        for _id in ids:
            self._collection.delete(_id)

        warnings.warn(
            UserWarning,
            "FileStore does not yet support file I/O. Therefore, removing a document from the store only affects the underlying MemoryStore and not any files on disk.",
        )
