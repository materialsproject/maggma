# coding: utf-8
"""
Base Builder class to define how builders need to be defined
"""
import traceback
from abc import ABCMeta, abstractmethod, abstractproperty
from time import time
from math import ceil
from datetime import datetime
from maggma.utils import grouper, Timeout
from maggma.core import Builder, Store
from typing import Optional, Dict, List, Iterator, Iterable, Set, Tuple
from pydash import get
from itertools import groupby, chain


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
        Ensures indicies on critical fields for MapBuilder
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

        self.logger.info("Starting {} Builder".format(self.__class__.__name__))

        self.ensure_indexes()

        keys = self.target.newer_in(self.source, criteria=self.query, exhaustive=True)
        if self.retry_failed:
            failed_keys = self.target.distinct(
                self.target.key, criteria={"state": {"$ne": "failed"}}
            )
            keys = list(set(keys + failed_keys))

        self.logger.info("Processing {} items".format(len(keys)))

        if self.projection:
            projection = list(
                set(self.projection + [self.source.key, self.source.last_updated_field])
            )
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

        self.logger.debug("Processing: {}".format(item[self.source.key]))

        time_start = time()

        try:
            with Timeout(seconds=self.timeout):
                processed = self.unary_function(item)
                processed.update({"state": "successful"})
        except Exception as e:
            self.logger.error(traceback.format_exc())
            processed = {"error": str(e), "state": "failed"}

        time_end = time()

        key, last_updated_field = self.source.key, self.source.last_updated_field

        out = {
            self.target.key: item[key],
            self.target.last_updated_field: self.source._lu_func[0](
                item[last_updated_field]
            ),
        }
        if self.store_process_time:
            out["_process_time"] = time_end - time_start

        out.update(processed)
        return out

    def update_targets(self, items: List[Dict]):
        """
        Generic update targets for Map Builder
        """
        source, target = self.source, self.target
        for item in items:
            # Use source last-updated value, ensuring `datetime` type.
            item[target.last_updated_field] = source._lu_func[0](
                item[source.last_updated_field]
            )
            if source.last_updated_field != target.last_updated_field:
                del item[source.last_updated_field]
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
                self.logger.info(
                    "Finalize: Deleting {} orphans.".format(len(to_delete))
                )
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
        pass


class GroupBuilder(MapBuilder, metaclass=ABCMeta):
    """
    Group source docs and produces merged documents for each group
    Supports incremental building, where a source group gets (re)built only if
    it has a newer (by last_updated_field) doc than the corresponding (by key) target doc.
    """

    def ensure_indexes(self):
        """
        Ensures indicies on critical fields for MapBuilder
        """

        if not self.target.ensure_index(f"{self.target.key}s"):
            self.logger.warning()
        super().ensure_indexes()

    def prechunk(self, number_splits: int) -> Iterator[Dict]:
        """
        Generic prechunk for map builder to perform domain-decompostion
        by the key field
        """
        self.ensure_indexes()
        keys = self.get_ids_to_process()
        groups = self.get_groups_from_keys(keys)

        for split in grouper(groups, number_splits):
            yield {"query": dict(zip(self.grouping_keys, split))}

    def get_ids_to_process(self) -> Iterable:
        """
        Gets the IDs that need to be processed
        """

        processed_ids = set(self.target.distinct(f"{self.target.key}s"))
        all_ids = set(self.source.distinct(self.source.key))
        new_ids = all_ids - processed_ids
        self.logger.info(f"Found {len(new_ids)} to process")
        # TODO Updated keys and Failed keys

        return list(new_ids)

    def get_groups_from_keys(self, keys) -> Iterable:
        """
        Get the groups by grouping_keys for these documents
        """
        grouping_keys = self.grouping_keys

        groups: Set[Tuple] = set()
        for chunked_keys in grouper(keys, self.chunk_size, None):
            chunked_keys = list(filter(None.__ne__, chunked_keys))

            docs = list(
                self.source.query(
                    criteria={self.source.key: {"$in": chunked_keys}},
                    properties=grouping_keys,
                )
            )
            groups |= {(get(prop, d, None) for prop in grouping_keys) for d in docs}

        groups = [g[0] for g in groupby(sorted(groups))]
        self.logger.info(f"Found {len(groups)} to process")
        return groups

    def get_items(self):

        self.logger.info("Starting {} Builder".format(self.__class__.__name__))

        self.ensure_indexes()
        keys = self.get_ids_to_process()
        groups = self.get_groups_from_keys(keys)

        if self.projection:
            projection = list(
                set(self.projection + [self.source.key, self.source.last_updated_field])
            )
        else:
            projection = None

        self.total = len(groups)
        for group in groups:
            docs = list(
                self.source.query(
                    criteria=dict(zip(self.grouping_keys, group)), projection=projection
                )
            )
            yield docs

    def process_item(self, item: List[Dict]) -> Dict[Tuple, Dict]:

        keys = (d[self.source.key] for d in item)
        self.logger.debug("Processing: {}".format(keys))

        time_start = time()

        try:
            with Timeout(seconds=self.timeout):
                processed = self.grouped_unary_function(item)
                for _, d in processed:
                    d.update({"state": "successful"})
        except Exception as e:
            self.logger.error(traceback.format_exc())
            processed = {keys: {"error": str(e), "state": "failed"}}

        time_end = time()

        for doc_keys, doc in processed.items():
            last_updated = [item[k][self.source.last_updated_field] for k in doc_keys]
            last_updated = [self.source._lu_func[0](lu) for lu in last_updated]

            doc.update(
                {
                    f"{self.target.key}s": doc_keys,
                    self.target.last_updated_field: max(last_updated),
                    "_bt": datetime.utcnow(),
                    self.target.key: doc_keys[0],
                }
            )

        if self.store_process_time:
            for doc in processed.values():
                doc["_process_time"] = time_end - time_start

        return list(processed.values())

    def update_targets(self, items: List[List[Dict]]):
        """
        Generic update targets for Map Builder
        """
        items = list(chain.from_iterable(items))

        for item in items:
            # Add the built time
            if "_id" in item:
                del item["_id"]

        if len(items) > 0:
            self.target.update(items)

    def grouped_unary_function(self, items: List[Dict]) -> Dict[Tuple, Dict]:
        """
        Processing function for GroupBuilder


        Returns:
            Dictionary mapping:
                tuple of source document keys that are in the grouped document
                to
                the grouped and processed document
        """
        pass

    @abstractproperty
    def grouping_keys(self) -> List[str]:
        pass


class CopyBuilder(MapBuilder):
    """Sync a source store with a target store."""

    def unary_function(self, item):
        """
        Identity function for copy builder map operation
        """
        if "_id" in item:
            del item["_id"]
        return item
