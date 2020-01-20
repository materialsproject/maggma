# coding: utf-8
"""
Many-to-Many GroupBuilder
"""
import traceback
from abc import ABCMeta, abstractproperty
from time import time
from datetime import datetime
from maggma.utils import grouper, Timeout
from maggma.builders import MapBuilder
from typing import Dict, List, Iterator, Iterable, Set, Tuple
from pydash import get
from itertools import chain


class GroupBuilder(MapBuilder, metaclass=ABCMeta):
    """
    Group source docs and produces merged documents for each group
    Supports incremental building, where a source group gets (re)built only if
    it has a newer (by last_updated_field) doc than the corresponding (by key) target doc.
    """

    def ensure_indexes(self):
        """
        Ensures indicies on critical fields for GroupBuilder
        which include the plural version of the target's key field
        """

        if not self.target.ensure_index(f"{self.target.key}s"):
            self.logger.warning()
        super().ensure_indexes()

    def prechunk(self, number_splits: int) -> Iterator[Dict]:
        """
        Generic prechunk for group builder to perform domain-decompostion
        by the grouping keys
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

        query = self.query or {}

        processed_ids = set(self.target.distinct(f"{self.target.key}s", criteria=query))
        all_ids = set(self.source.distinct(self.source.key, criteria=query))
        unprocessed_ids = all_ids - processed_ids
        self.logger.info(f"Found {len(unprocessed_ids)} to process")

        new_ids = self.source.newer_in(self.target, criteria=query, exhaustive=False)

        return list(new_ids | unprocessed_ids)

    def get_groups_from_keys(self, keys) -> Set[Tuple]:
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
            groups |= set((get(prop, d, None) for prop in grouping_keys) for d in docs)

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
