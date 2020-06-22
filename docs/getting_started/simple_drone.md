# Simple Drone
Let's implement a Simple Drone example.

The simple drone will sync database with a local file structure like below. You may find sample files [here](https://github.com/materialsproject/maggma/tree/master/tests/test_files)
```
- data
    - citation-1.bibtex
    - citation-2.bibtex
    - citation-3.bibtex
    - citation-4.bibtex
    - citation-5.bibtex
    - citation-6.bibtex
    - text-1.txt
    - text-2.txt
    - text-3.txt
    - text-4.txt
    - text-5.txt
```

Notice that the pattern here for computing the key is the number between `-` and its file extension. So we can go ahead and define the `compute_record_identifier_key` function that does exactly that

    def compute_record_identifier_key(self, doc: Document) -> str:
        prefix, postfix = doc.name.split(sep="-", maxsplit=1)
        ID, ftype = postfix.split(sep=".", maxsplit=1)
        return ID

Notice that these files are all in one single directory, we can simply read all these files and generate a list of `Document`.
A `Document` represent a FILE, a file that contains data, not a directory.

    def generate_documents(self, folder_path: Path) -> List[Document]:
        files_paths = [folder_path / f for f in os.listdir(folder_path.as_posix())]
        return [Document(path=fp, name=fp.name) for fp in files_paths]

Now we need to organize these documents, or aka to build an association. So let's define a helper function called `organize_documents`

    def organize_documents(self, documents: List[Document]) -> Dict[str, List[Document]]:
        log: Dict = dict()
        for doc in documents:
            key = self.compute_record_identifier_key(doc)
            log[key] = log.get(key, []) + [doc]
        return log

We also want to have a way to compute `RecordIdentifier` when given a list of documents, so we overwrite the `compute_record_identifier`
Please note that `RecordIdentifier` comes with a `state_hash` field. This field is used to compare against the `state_hash` in the database
so that we can efficiently know which file has changed without compare byte by byte. `RecordIdentifier` comes with a default method of
computing `state_hash` using md5sum. You may modify it or simply use it by calling `recordIdentifier.compute_state_hash()`

    def compute_record_identifier(self, record_key: str, doc_list: List[Document]) -> RecordIdentifier:
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

At this point, we have all the necessary components to overwrite the `read` function from the base `Drone` class.
We basically generate a list of documents, organize them, and then generate a list of `RecordIdentifier`

    def read(self, path: Path) -> List[RecordIdentifier]:
        documents: List[Document] = self.generate_documents(folder_path=path)
        log = self.organize_documents(documents=documents)
        record_identifiers = [
            self.compute_record_identifier(record_key, doc_list)
            for record_key, doc_list in log.items()
        ]
        return record_identifiers

Lastly, if there's a file that needs to be updated, we want to extract the data and append some meta data. In our case, this is very simple.

    def compute_data(self, recordID: RecordIdentifier) -> Dict:
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
Now, you have a working SimpleBibDrone! You can use it like this:

    mongo_store = MongoStore(
        database="drone_test", collection_name="drone_test", key="record_key"
    )
    simple_path = Path.cwd() / "data"
    simple_bib_drone = SimpleBibDrone(store=mongo_store, path=simple_path)

    simple_bib_drone.run()
For complete code, please visit [here](https://github.com/materialsproject/maggma/tree/master/tests/builders)
