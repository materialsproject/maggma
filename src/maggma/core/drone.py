import hashlib
import os
from abc import abstractmethod
from datetime import datetime
from pathlib import Path, PosixPath
from typing import Dict, Iterable, List, Optional, Any

from pydantic import BaseModel, Field

from maggma.core import Builder, Store


class Document(BaseModel):
    """
    Represent a file
    """

    path: PosixPath = Field(..., title="Path of this file")
    name: str = Field(..., title="File name")


class RecordIdentifier(BaseModel):
    """
    The meta data for a record
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


class Drone(Builder):
    """
    An abstract drone that handles operations with database, using Builder to support multi-threading
    User have to implement all abstract methods to specify the data that they want to use to interact with this class

    Note: All embarrassingly parallel function should be in process_items
    Note: The steps of a Drone can be divided into roughly 5 steps

     For sample usage, please see /docs/getting_started/core_drone.md
     and example implementation is available in tests/builders/test_simple_bibdrone.py
    """

    def __init__(self, path: Path, target: Store, chunk_size=1):
        if not isinstance(path, Path):
            path = Path(path)
        self.path = path
        self.target = target
        super().__init__(sources=[], targets=target, chunk_size=chunk_size)

    @abstractmethod
    def read(self, path: Path) -> List[RecordIdentifier]:
        """
        Given a folder path to a data folder, read all the files, and return list
        of RecordIdentifier

        Args:
            path: Path object that indicates a path to a data folder

        Returns:
            List of Record Identifiers
        """
        pass

    def should_update_records(
        self, record_identifiers: List[RecordIdentifier]
    ) -> List[RecordIdentifier]:
        """
        Batch query database by computing all the keys and send them at once

        Args:
            record_identifiers: all the record_identifiers that need to fetch from database

        Returns:
            List of recordIdentifiers representing data that needs to be updated
        """
        cursor = self.target.query(
            criteria={
                self.target.key: {"$in": [r.record_key for r in record_identifiers]}
            },
            properties=[self.target.key, "state_hash", "last_updated"],
        )

        not_exists = object()
        db_record_log = {doc[self.target.key]: doc["state_hash"] for doc in cursor}
        to_update_list = [
            recordID.state_hash != db_record_log.get(recordID.record_key, not_exists)
            for recordID in record_identifiers
        ]
        return [
            recordID
            for recordID, to_update in zip(record_identifiers, to_update_list)
            if to_update
        ]

    def get_items(self) -> Iterable:
        """
        Read from the path that was given, compare against database files to find recordIDs that needs to be updated

        Returns:
            RecordIdentifiers that needs to be updated
        """
        self.logger.debug(
            "Starting get_items in {} Drone".format(self.__class__.__name__)
        )
        record_identifiers: List[RecordIdentifier] = self.read(path=self.path)
        records_to_update = self.should_update_records(record_identifiers)
        return records_to_update

    def update_targets(self, items: List):
        """
        Receive a list of items to update, update the items

        Assume that each item are in the correct format
        Args:
            items: List of items to update

        Returns:
            None
        """
        if len(items) > 0:
            self.logger.debug("Updating {} items".format(len(items)))
            self.target.update(items)
        else:
            self.logger.debug("There are no items to update")

    def process_item(self, item: RecordIdentifier) -> Any:  # type: ignore
        """
        Process an item (i.e., a RecordIdentifier).

        Default behavior is to return the item.

        Arguments:
            item:

        Returns:
           A processed item.
        """
        return item
