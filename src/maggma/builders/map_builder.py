"""
One-to-One Map Builder and a simple CopyBuilder implementation
"""
import traceback
from abc import ABCMeta, abstractmethod
from datetime import datetime
from math import ceil
from time import time
from typing import Dict, Iterator, List, Optional

from maggma.core import Builder, Store
from maggma.utils import Timeout, grouper


class MapBuilder(Builder, metaclass=ABCMeta):
    """
    Apply a unary function to yield a target document for each source document.

    Supports incremental building, where a source document gets built only if it
    has newer (by last_updated_field) data than the corresponding (by key) target
    document.

    """

    def __init__(
        self,
        source: Store,
        target: Store,
        query: Optional[Dict] = None,
        projection: Optional[List] = None,
        delete_orphans: bool = False,
        timeout: int = 0,
        store_process_time: bool = True,
        retry_failed: bool = False,
        **kwargs,
    ):
        """
        Apply a unary function to each source document.

        Args:
            source: source store
            target: target store
            query: optional query to filter source store
            projection: list of keys to project from the source for
                processing. Limits data transfer to improve efficiency.
            delete_orphans: Whether to delete documents on target store
                with key values not present in source store. Deletion happens
                after all updates, during Builder.finalize.
            timeout: maximum running time per item in seconds
            store_process_time: If True, add "_process_time" key to
                document for profiling purposes
            retry_failed: If True, will retry building documents that
                previously failed
        """
        self.source = source
        self.target = target
        self.query = query
        self.projection = projection
        self.delete_orphans = delete_orphans
        self.kwargs = kwargs
        self.timeout = timeout
        self.store_process_time = store_process_time
        self.retry_failed = retry_failed
        super().__init__(sources=[source], targets=[target], **kwargs)

    def ensure_indexes(self):
        """
        Ensures indices on critical fields for MapBuilder
        """
        index_checks = [
            self.source.ensure_index(self.source.key),
            self.source.ensure_index(self.source.last_updated_field),
            self.target.ensure_index(self.target.key),
            self.target.ensure_index(self.target.last_updated_field),
            self.target.ensure_index("state"),
        ]

        if not all(index_checks):
            self.logger.warning(
                "Missing one or more important indices on stores. "
                "Performance for large stores may be severely degraded. "
                "Ensure indices on target.key and "
                "[(store.last_updated_field, -1), (store.key, 1)] "
                "for each of source and target."
            )

    def prechunk(self, number_splits: int) -> Iterator[Dict]:
        """
        Generic prechunk for map builder to perform domain-decompostion
        by the key field
        """
        self.ensure_indexes()
        keys = self.target.newer_in(self.source, criteria=self.query, exhaustive=True)

        N = ceil(len(keys) / number_splits)
        for split in grouper(keys, N):
            yield {"query": {self.source.key: {"$in": list(split)}}}

    def get_items(self):
        """
        Generic get items for Map Builder designed to perform
        incremental building
        """

        self.logger.info(f"Starting {self.__class__.__name__} Builder")

        self.ensure_indexes()

        keys = self.target.newer_in(self.source, criteria=self.query, exhaustive=True)
        if self.retry_failed:
            if isinstance(self.query, (dict)):
                failed_query = {"$and": [self.query, {"state": "failed"}]}
            else:
                failed_query = {"state": "failed"}
            failed_keys = self.target.distinct(self.target.key, criteria=failed_query)
            keys = list(set(keys + failed_keys))

        self.logger.info(f"Processing {len(keys)} items")

        if self.projection:
            projection = list({*self.projection, self.source.key, self.source.last_updated_field})
        else:
            projection = None

        self.total = len(keys)
        for chunked_keys in grouper(keys, self.chunk_size):
            chunked_keys = list(chunked_keys)
            for doc in list(
                self.source.query(
                    criteria={self.source.key: {"$in": chunked_keys}},
                    properties=projection,
                )
            ):
                yield doc

    def process_item(self, item: Dict):
        """
        Generic process items to process a dictionary using
        a map function
        """

        self.logger.debug(f"Processing: {item[self.source.key]}")

        time_start = time()

        try:
            with Timeout(seconds=self.timeout):
                processed = dict(self.unary_function(item))
                processed.update({"state": "successful"})

            for k in [self.source.key, self.source.last_updated_field]:
                if k in processed:
                    del processed[k]

        except Exception as e:
            self.logger.error(traceback.format_exc())
            processed = {"error": str(e), "state": "failed"}

        time_end = time()

        key, last_updated_field = self.source.key, self.source.last_updated_field

        out = {
            self.target.key: item[key],
            self.target.last_updated_field: self.source._lu_func[0](item.get(last_updated_field, datetime.utcnow())),
        }

        if self.store_process_time:
            out["_process_time"] = time_end - time_start

        out.update(processed)
        return out

    def update_targets(self, items: List[Dict]):
        """
        Generic update targets for Map Builder
        """
        target = self.target
        for item in items:
            item["_bt"] = datetime.utcnow()
            if "_id" in item:
                del item["_id"]

        if len(items) > 0:
            target.update(items)

    def finalize(self):
        """
        Finalize MapBuilder operations including removing orphaned documents
        """
        if self.delete_orphans:
            source_keyvals = set(self.source.distinct(self.source.key))
            target_keyvals = set(self.target.distinct(self.target.key))
            to_delete = list(target_keyvals - source_keyvals)
            if len(to_delete):
                self.logger.info(f"Finalize: Deleting {len(to_delete)} orphans.")
            self.target.remove_docs({self.target.key: {"$in": to_delete}})
        super().finalize()

    @abstractmethod
    def unary_function(self, item):
        """
        ufn: Unary function to process item
                You do not need to provide values for
                source.key and source.last_updated_field in the output.
                Any uncaught exceptions will be caught by
                process_item and logged to the "error" field
                in the target document.
        """


class CopyBuilder(MapBuilder):
    """Sync a source store with a target store."""

    def unary_function(self, item):
        """
        Identity function for copy builder map operation
        """
        if "_id" in item:
            del item["_id"]
        return item
