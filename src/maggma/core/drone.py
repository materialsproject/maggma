import hashlib
import os
from abc import abstractmethod
from datetime import datetime
from pathlib import Path, PosixPath
from typing import Dict, Iterable, List, Optional

from pydantic import BaseModel, Field

from maggma.core import Builder


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
        return Path(os.path.commonprefix(paths))

    def compute_state_hash(self) -> str:
        """
        compute the hash of the state of the documents in this record
        :param doc_list: list of documents
        :return:
            hash of the list of documents passed in
        """
        digest = hashlib.md5()
        for doc in self.documents:
            digest.update(doc.name.encode())
            with open(doc.path.as_posix(), "rb") as file:
                buf = file.read()
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

    def __init__(self, store, path: Path):
        self.store = store
        self.path = path
        super().__init__(sources=[], targets=store)

    @abstractmethod
    def compute_record_identifier_key(self, doc: Document) -> str:
        """
        Compute the RecordIdentifier key that this document correspond to

        Args:
            doc: document which the record identifier key will be inferred from

        Returns:
            RecordIdentifiierKey
        """
        raise NotImplementedError

    @abstractmethod
    def read(self, path: Path) -> List[RecordIdentifier]:
        """
        Given a folder path to a data folder, read all the files, and return a dictionary
        that maps each RecordKey -> [RecordIdentifier]

        ** Note: require user to implement the function computeRecordIdentifierKey

        Args:
            path: Path object that indicate a path to a data folder

        Returns:
            List of Record Identifiers
        """
        pass

    @abstractmethod
    def compute_data(self, recordID: RecordIdentifier) -> Dict:
        """
        User can specify what raw data they want to save from the Documents that this recordID refers to

        Args:
            recordID: recordID that needs to be re-saved

        Returns:
            Dictionary of NAME_OF_DATA -> DATA
                        ex:
                            for a recordID refering to 1,
                            {
                                "citation": cite.bibtex ,
                                "text": data.txt
                            }
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
        cursor = self.store.query(
            criteria={
                "record_key": {"$in": [r.record_key for r in record_identifiers]}
            },
            properties=["record_key", "state_hash", "last_updated"],
        )

        not_exists = object()
        db_record_log = {doc["record_key"]: doc["state_hash"] for doc in cursor}
        to_update_list = [
            recordID.state_hash != db_record_log.get(recordID.record_key, not_exists)
            for recordID in record_identifiers
        ]
        return [
            recordID
            for recordID, to_update in zip(record_identifiers, to_update_list)
            if to_update
        ]

    def assimilate(self, path: Path) -> List[RecordIdentifier]:
        """
        Function mainly for debugging purpose. It will
            1. read file in the path specified
            2. convert them into recordIdentifier
            3. return the converted recordIdentifiers
        Args:
            path: path in which files are read

        Returns:
            list of record Identifiers
        """
        record_identifiers: List[RecordIdentifier] = self.read(path=path)
        return record_identifiers

    def get_items(self) -> Iterable:
        """
        Read from the path that was given, compare against database files to find recordIDs that needs to be updated

        Returns:
            RecordIdentifiers that needs to be updated
        """
        self.logger.debug(
            "Starting get_items in {} Builder".format(self.__class__.__name__)
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
            self.store.update(items)
        else:
            self.logger.debug("There are no items to update")

    def process_item(self, item: RecordIdentifier) -> Dict:  # type: ignore
        """
        compute the item to update

        Args:
            item: item to update

        Returns:
            result from expanding the item
        """

        return {**self.compute_data(recordID=item), **item.dict()}
