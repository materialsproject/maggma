import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from maggma.core.drone import Document, Drone, RecordIdentifier


class SimpleBibDrone(Drone):
    """
    Example implementation of Drone assuming that all data files are located in one folder and that their
    file names indicate association
    Ex:
        - data
            - example
                - citation-1.bibtex
                - citation-2.bibtex
                - text-1.bibtex
                ...
    """

    def __init__(self, store, path):
        super().__init__(store=store, path=path)

    def compute_record_identifier(
        self, record_key: str, doc_list: List[Document]
    ) -> RecordIdentifier:
        """
        Compute meta data for this list of documents, and generate a RecordIdentifier object
        :param record_key: record keys that indicate a record
        :param doc_list: document on disk that this record include
        :return:
            RecordIdentifier that represent this doc_list
        """
        recordIdentifier = RecordIdentifier(
            last_updated=datetime.now(), documents=doc_list, record_key=record_key
        )
        recordIdentifier.state_hash = recordIdentifier.compute_state_hash()
        return recordIdentifier

    def generate_documents(self, folder_path: Path) -> List[Document]:
        """
        Generate documents by going over the current directory:
        Note: Assumes that there's no folder in the current directory
        :param folder_path:
        :return:
        """
        files_paths = [folder_path / f for f in os.listdir(folder_path.as_posix())]
        return [Document(path=fp, name=fp.name) for fp in files_paths]

    def read(self, path: Path) -> List[RecordIdentifier]:
        """
        Given a folder path to a data folder, read all the files, and return a dictionary
        that maps each RecordKey -> [File Paths]

        ** Note: require user to implement the function computeRecordIdentifierKey

        :param path: Path object that indicate a path to a data folder
        :return:

        """
        documents: List[Document] = self.generate_documents(folder_path=path)
        log = self.organize_documents(documents=documents)
        record_identifiers = [
            self.compute_record_identifier(record_key, doc_list)
            for record_key, doc_list in log.items()
        ]
        return record_identifiers

    def compute_data(self, recordID: RecordIdentifier) -> Dict:
        """
        return the mapping of NAME_OF_DATA -> DATA

        :param recordID: recordID that needs to be re-saved
        :return:
            Dictionary of NAME_OF_DATA -> DATA
            ex:
                for a recordID refering to 1,
                {
                    "citation": cite.bibtex ,
                    "text": data.txt
                }
        """
        record = dict()

        for document in recordID.documents:
            if "citations" in document.name:
                with open(document.path.as_posix(), "r") as file:
                    s = file.read()
                    record["citations"] = s

            if "text" in document.name:
                with open(document.path.as_posix(), "r") as file:
                    s = file.read()
                    record["text"] = s
        return record

    def compute_record_identifier_key(self, doc: Document) -> str:
        """
        Compute the recordIdentifier key by interpreting the name
        :param doc:
        :return:
        """
        prefix, postfix = doc.name.split(sep="-", maxsplit=1)
        ID, ftype = postfix.split(sep=".", maxsplit=1)
        return ID

    def organize_documents(
        self, documents: List[Document]
    ) -> Dict[str, List[Document]]:
        """
        a dictionary that maps RecordIdentifierKey -> [File Paths]
            ex:
            1 -> [cite.bibtex(Represented in Document), data.txt(Represented in Document)]
            2 -> [cite2.bibtex(Represented in Document), text-2.txt(Represented in Document)]
            3 -> [citations-3.bibtex(Represented in Document), ]
            ...
        :param documents:
        :return:
        """
        log: Dict = dict()
        for doc in documents:
            key = self.compute_record_identifier_key(doc)
            log[key] = log.get(key, []) + [doc]
        return log
