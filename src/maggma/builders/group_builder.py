# coding: utf-8
"""
Many-to-Many GroupBuilder
"""
import traceback
from abc import ABCMeta, abstractmethod
from time import time
from datetime import datetime
from maggma.core import Store
from maggma.utils import grouper, Timeout
from maggma.builders import MapBuilder
from typing import Dict, List, Iterator, Iterable, Set, Tuple
from pydash import get


class GroupBuilder(MapBuilder, metaclass=ABCMeta):
    """
    Group source docs and produces merged documents for each group
    Supports incremental building, where a source group gets (re)built only if
    it has a newer (by last_updated_field) doc than the corresponding (by key) target doc.

    This is a Many-to-One Builder. As a result, this builder can't determined when a source document
    is orphaned.
    """

    def __init__(
        self, source: Store, target: Store, grouping_keys: List[str], **kwargs
    ):
        self.grouping_keys = grouping_keys
        self.kwargs = kwargs
        kwargs["delete_orphans"] = False
        super().__init__(source=source, target=target, **kwargs)

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

        distinct_from_target = list(
            self.target.distinct(f"{self.source.key}s", criteria=query)
        )
        processed_ids = []
        # Not always gauranteed that MongoDB will unpack the list so we
        # have to make sure we do that
        for d in distinct_from_target:
            if isinstance(d, list):
                processed_ids.extend(d)
            else:
                processed_ids.append(d)

        all_ids = set(self.source.distinct(self.source.key, criteria=query))
        unprocessed_ids = all_ids - set(processed_ids)
        self.logger.debug(f"Found {len(all_ids)} total docs in source")
        self.logger.info(f"Found {len(unprocessed_ids)} IDs to process")

        new_ids = set(
            self.source.newer_in(self.target, criteria=query, exhaustive=False)
        )

        return list(new_ids | unprocessed_ids)

    def get_groups_from_keys(self, keys) -> Set[Tuple]:
        """
        Get the groups by grouping_keys for these documents
        """

        grouping_keys = self.grouping_keys

        groups: Set[Tuple] = set()

        for chunked_keys in grouper(keys, self.chunk_size):
            docs = list(
                self.source.query(
                    criteria={self.source.key: {"$in": chunked_keys}},
                    properties=grouping_keys,
                )
            )

            sub_groups = set(
                tuple(get(d, prop, None) for prop in grouping_keys) for d in docs
            )
            self.logger.debug(f"Found {len(sub_groups)} subgroups to process")

            groups |= sub_groups

        self.logger.info(f"Found {len(groups)} groups to process")
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
                    criteria=dict(zip(self.grouping_keys, group)), properties=projection
                )
            )
            yield docs

    def process_item(self, item: List[Dict]) -> Dict[Tuple, Dict]:  # type: ignore

        keys = list(d[self.source.key] for d in item)

        self.logger.debug("Processing: {}".format(keys))

        time_start = time()

        try:
            with Timeout(seconds=self.timeout):
                processed = self.unary_function(item)
                processed.update({"state": "successful"})
        except Exception as e:
            self.logger.error(traceback.format_exc())
            processed = {"error": str(e), "state": "failed"}

        time_end = time()

        last_updated = [
            self.source._lu_func[0](d[self.source.last_updated_field]) for d in item
        ]

        processed.update(
            {
                self.target.key: keys[0],
                f"{self.source.key}s": keys,
                self.target.last_updated_field: max(last_updated),
                "_bt": datetime.utcnow(),
            }
        )

        if self.store_process_time:
            processed["_process_time"] = time_end - time_start

        return processed

    @abstractmethod
    def unary_function(self, items: List[Dict]) -> Dict:
        """
        Processing function for GroupBuilder

        Returns:
            Dictionary mapping:
                tuple of source document keys that are in the grouped document
                to
                the grouped and processed document
        """
        pass
