# coding: utf-8
"""
Module defining a FileStore that enables accessing files in a local directory
using typical maggma access patterns.
"""

import hashlib

import warnings
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union, Iterator, Callable

from pymongo import UpdateOne
from monty.io import zopen
from maggma.core import StoreError, Sort
from maggma.stores.mongolike import MemoryStore, JSONStore

# These keys are automatically populated by the FileStore.read() method and
# hence are not allowed to be manually overwritten
PROTECTED_KEYS = {
    "_id",
    "name",
    "last_updated",
    "hash",
    "size",
    "parent",
    "orphan",
    "contents",
}


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
        include_orphans: bool = False,
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
            include_orphans: Whether to include orphaned metadata records in query results.
                Orphaned metadata records are records found in the local JSON file that can
                no longer be associated to a file on disk. This can happen if a file is renamed
                or deleted, or if the FileStore is re-initialized with a more restrictive
                file_filters or max_depth argument. By default (False), these records
                do not appear in query results. Nevertheless, the metadata records are
                retained in the JSON file and the FileStore to prevent accidental data loss.
            json_name: Name of the .json file to which metadata is saved. If read_only
                is False, this file will be created in the root directory of the
                FileStore.
            kwargs: kwargs passed to MemoryStore.__init__()
        """
        # this conditional block is needed in order to guarantee that the 'name'
        # property, which is passed to `MemoryStore`, works correctly
        # collection names passed to MemoryStore cannot end with '.'
        if path == ".":
            path = Path.cwd()
        self.path = Path(path) if isinstance(path, str) else path

        self.json_name = json_name
        self.file_filters = file_filters if file_filters else ["*"]
        self.collection_name = "file_store"
        self.key = "file_id"
        self.include_orphans = include_orphans
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

    def add_metadata(
        self,
        metadata: Dict = {},
        query: Optional[Dict] = None,
        auto_data: Callable[[Dict], Dict] = None,
        **kwargs,
    ):
        """
        Add metadata to a record in the FileStore, either manually or by computing it automatically
        from another field, such as name or path (see auto_data).

        Args:
            metadata: dict of additional data to add to the records returned by query.
                      Note that any protected keys (such as 'name', 'path', etc.)
                      will be ignored.
            query: Query passed to FileStore.query()
            auto_data: A function that automatically computes metadata based on a field in
                    the record itself. The function must take in the item as a dict and
                    return a dict containing the desired metadata. A typical use case is
                    to assign metadata based on the name of a file. For example, for
                    data files named like `2022-04-01_april_fool_experiment.txt`, the
                    auto_data function could be:

                    def get_metadata_from_filename(d):
                        return {"date": d["name"].split("_")[0],
                                "test_name": d["name"].split("_")[1]
                                }

                    Note that in the case of conflict between manual and automatically
                    computed metadata (for example, if metadata={"name": "another_name"} was
                    supplied alongside the auto_data function above), the manually-supplied
                    metadata is used.
            kwargs: kwargs passed to FileStore.query()
        """
        # sanitize the metadata
        filtered_metadata = self._filter_data(metadata)
        updated_docs = []

        for doc in self.query(query, **kwargs):
            if auto_data:
                extra_data = self._filter_data(auto_data(doc))
                doc.update(extra_data)
            doc.update(filtered_metadata)
            updated_docs.append(doc)

        self.update(updated_docs, key=self.key)

    def read(self) -> List[Dict]:
        """
        Iterate through all files in the Store folder and populate
        the Store with dictionaries containing basic information about each file.

        The keys of the documents added to the Store are

            name: str = File name
            path: Path = Absolute path of this file
            parent: str = Name of the parent directory (if any)
            file_id: str = Unique identifier for this file, computed from the hash
                        of its path relative to the base FileStore directory and
                        the file creation time. The key of this field is 'file_id'
                        by default but can be changed via the 'key' kwarg to
                        FileStore.__init__().
            size: int = Size of this file in bytes
            last_updated: datetime = Time this file was last modified
            hash: str = Hash of the file contents
            orphan: bool = Whether this record is an orphan
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
                        file_list.append(self._create_record_from_file(f))

        return file_list

    def _create_record_from_file(self, f: Union[str, Path]) -> Dict:
        """
        Given the path to a file, return a Dict that constitues a record of
        basic information about that file. The keys in the returned dict
        are:

            name: str = File name
            path: Path = Absolute path of this file
            parent: str = Name of the parent directory (if any)
            file_id: str = Unique identifier for this file, computed from the hash
                        of its path relative to the base FileStore directory and
                        the file creation time. The key of this field is 'file_id'
                        by default but can be changed via the 'key' kwarg to
                        FileStore.__init__().
            size: int = Size of this file in bytes
            last_updated: datetime = Time this file was last modified
            hash: str = Hash of the file contents
            orphan: bool = Whether this record is an orphan
        """
        # make sure f is a Path object
        if not isinstance(f, Path):
            f = Path(f)

        # compute the file_id from the relative path
        relative_path = f.relative_to(self.path)
        digest = hashlib.md5()
        digest.update(str(relative_path).encode())
        file_id = str(digest.hexdigest())

        # hash the file contents
        digest2 = hashlib.md5()
        block_size = 128 * digest2.block_size
        digest2.update(self.name.encode())
        with open(f.as_posix(), "rb") as file:
            buf = file.read(block_size)
            digest2.update(buf)
        content_hash = str(digest2.hexdigest())

        d = {
            "name": f.name,
            "path": f,
            "parent": f.parent.name,
            "size": f.stat().st_size,
            "last_updated": datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc),
            "orphan": False,
            "hash": content_hash,
            self.key: file_id,
        }

        return d

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
        super().update(self.read())

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
        are keys in the dict returned by _create_record_from_file, e.g. 'name', 'parent',
        'last_updated', 'hash', 'size', 'contents', and 'orphan'. The 'path' and key fields
        are retained to make each document in the JSON file identifiable by manual inspection.

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
        for d in data:
            filtered_d = self._filter_data(d)
            # don't write records that contain only file_id and path
            if len(set(filtered_d.keys()).difference(set(["path", self.key]))) != 0:
                filtered_data.append(filtered_d)
        self.metadata_store.update(filtered_data, self.key)

    def _filter_data(self, d):
        """
        Remove any protected keys from a dictionary

        Args:
            d: Dictionary whose keys are to be filtered
        """
        filtered_d = {k: v for k, v in d.items() if k not in PROTECTED_KEYS}
        return filtered_d

    def query(  # type: ignore
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        hint: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
        contents_size_limit: Optional[int] = None,
    ) -> Iterator[Dict]:
        """
        Queries the Store for a set of documents

        Args:
            criteria: PyMongo filter for documents to search in
            properties: fields to include in returned documents. By default, all fields are returned.
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            hint: Dictionary of indexes to use as hints for query optimizer.
                Keys are field names and values are 1 for ascending or -1 for descending.
            skip: number documents to skip
            limit: limit on total number of documents returned
            contents_size_limit: Maximum file size in bytes for which to return contents.
                The FileStore will attempt to read the file and populate the 'contents' key
                with its content at query time, unless the file size is larger than this value.
        """
        return_contents = False
        criteria = criteria if criteria else {}
        if criteria.get("orphan", None) is None:
            if not self.include_orphans:
                criteria.update({"orphan": False})

        if criteria.get("contents"):
            warnings.warn("'contents' is not a queryable field! Ignoring.")

        if isinstance(properties, list):
            properties = {p: 1 for p in properties}

        orig_properties = properties.copy() if properties else None

        if properties is None or properties.get("contents"):
            return_contents = True

        if properties is not None and return_contents:
            # remove contents b/c it isn't stored in the MemoryStore
            properties.pop("contents")
            # add size and path to query so that file can be read
            properties.update({"size": 1})
            properties.update({"path": 1})

        for d in super().query(
            criteria=criteria,
            properties=properties,
            sort=sort,
            hint=hint,
            skip=skip,
            limit=limit,
        ):
            # add file contents to the returned documents, if appropriate
            if return_contents:
                if contents_size_limit is None or d["size"] <= contents_size_limit:
                    # attempt to read the file contents and inject into the document
                    # TODO - could add more logic for detecting different file types
                    # and more nuanced exception handling
                    try:
                        with zopen(d["path"], "r") as f:
                            data = f.read()
                    except Exception as e:
                        data = f"Unable to read: {e}"

                elif d["size"] > contents_size_limit:
                    data = "Unable to read: file too large"
                else:
                    data = "Unable to read: Unknown error"

                d.update({"contents": data})

                # remove size and path if not explicitly requested
                if orig_properties is not None and "size" not in orig_properties:
                    d.pop("size")
                if orig_properties is not None and "path" not in orig_properties:
                    d.pop("path")

            yield d

    def query_one(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        contents_size_limit: Optional[int] = None,
    ):
        """
        Queries the Store for a single document

        Args:
            criteria: PyMongo filter for documents to search
            properties: properties to return in the document
            sort: Dictionary of sort order for fields. Keys are field names and
                values are 1 for ascending or -1 for descending.
            contents_size_limit: Maximum file size in bytes for which to return contents.
                The FileStore will attempt to read the file and populate the 'contents' key
                with its content at query time, unless the file size is larger than this value.
        """
        return next(
            self.query(
                criteria=criteria,
                properties=properties,
                sort=sort,
                contents_size_limit=contents_size_limit,
            ),
            None,
        )

    def remove_docs(self, criteria: Dict, confirm: bool = False):
        """
        Remove items matching the query dictionary.

        Args:
            criteria: query dictionary to match
            confirm: Boolean flag to confirm that remove_docs should delete
                     files on disk. Default: False.
        """
        if self.read_only:
            raise StoreError(
                "This Store is read-only. To enable file I/O, re-initialize the "
                "store with read_only=False."
            )

        docs = [d for d in self.query(criteria)]
        # this ensures that any modifications to criteria made by self.query
        # (e.g., related to orphans or contents) are propagated through to the superclass
        new_criteria = {"file_id": {"$in": [d["file_id"] for d in docs]}}

        if len(docs) > 0 and not confirm:
            raise StoreError(
                f"Warning! This command is about to delete {len(docs)} items from disk! "
                "If this is what you want, reissue this command with confirm=True."
            )

        for d in docs:
            Path(d["path"]).unlink()
            super().remove_docs(criteria=new_criteria)
