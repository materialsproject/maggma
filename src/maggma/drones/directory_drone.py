from pathlib import Path
import traceback
from datetime import datetime
from time import time
import fnmatch
from typing import List, Dict
from abc import ABCMeta, abstractmethod

from maggma.core import Store
from maggma.core.drone import Drone, RecordIdentifier, Document
from maggma.utils import Timeout


class DirectoryDrone(Drone, metaclass=ABCMeta):
    """
    Base Drone class for parsing data on disk in which each Record is a subdirectory
    of the Path used to instantiate the drone that contains one or more files.
    For example,

    <path passed to Drone.__init__()>
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

    In this drone, the name of the subdirectory serves as the 'record_key' for
    each item, and each item contains a list of Document objects which each
    correspondi to a single file contained in the subdirectory. So the example
    data above would result in 3 unique RecordIdentifier with keys 'calculation1',
    'calculation2', and 'calculation3'.
    """

    def __init__(
        self,
        path: Path,
        target: Store,
        track_files: List,
        timeout: int = 0,
        delete_orphans: bool = False,
        store_process_time: bool = True,
        retry_failed: bool = False,
        **kwargs,
    ):
        """
        Apply a unary function to each source document.

        Args:
            path: parent directory containing all files and subdirectories to process
            target: target Store
            track_files: List of files or fnmatch patterns to be tracked when computing
                the state_hash.
            delete_orphans: Whether to delete documents on target store
                with key values not present in source store. Deletion happens
                after all updates, during Builder.finalize.
            timeout: maximum running time per item in seconds
            store_process_time: If True, add "_process_time" key to
                document for profiling purposes
            retry_failed: If True, will retry building documents that
                previously failed
        """
        self.path = path
        self.target = target
        self.track_files = track_files
        self.delete_orphans = delete_orphans
        self.kwargs = kwargs
        self.timeout = timeout
        self.store_process_time = store_process_time
        self.retry_failed = retry_failed
        super().__init__(path, target=target, **kwargs)

    def read(self, path: Path = None) -> List[RecordIdentifier]:
        """
        Given a folder path to a data folder, read all the files, and return a dictionary
        that maps each RecordKey -> [RecordIdentifier]
        ** Note: require user to implement the function computeRecordIdentifierKey
        Args:
            path: Path object that indicate a path to a data folder
        Returns:
            List of Record Identifiers
        """
        if not path:
            path = self.path
        else:
            path = Path(path)

        record_id_list = []
        # generate a list of subdirectories
        for d in [d for d in self.path.iterdir() if d.is_dir()]:
            doc_list = [
                Document(path=f, name=f.name) for f in d.iterdir() if f.is_file() and any([fnmatch.fnmatch(f.name, fn) for fn in self.track_files])
            ]
            record_id = RecordIdentifier(
                last_updated=datetime.now(), documents=doc_list, record_key=d.name
            )
            record_id.state_hash = record_id.compute_state_hash()
            record_id_list.append(record_id)
        return record_id_list

    @abstractmethod
    def unary_function(self, item: RecordIdentifier) -> Dict:
        """
        ufn: Unary function to process item
                You do not need to provide values for
                source.key and source.last_updated_field in the output.
                Any uncaught exceptions will be caught by
                process_item and logged to the "error" field
                in the target document.
        """
        pass

    def process_item(self, item: RecordIdentifier):
        """
        Generic process items to process a RecordIdentifier using
        unary_function
        """

        self.logger.debug("Processing: {}".format(item.record_key))

        time_start = time()

        try:
            with Timeout(seconds=self.timeout):
                processed = dict(self.unary_function(item))
                processed.update({"state": "successful"})

        except Exception as e:
            self.logger.error(traceback.format_exc())
            processed = {"error": str(e), "state": "failed"}

        time_end = time()

        out = {
            self.target.key: item.record_key,
            self.target.last_updated_field: item.last_updated,
            "state_hash": item.state_hash,
        }

        if self.store_process_time:
            out["_process_time"] = time_end - time_start

        out.update(processed)
        return out
