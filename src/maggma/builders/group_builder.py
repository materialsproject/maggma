"""
Many-to-Many GroupBuilder.
"""

import traceback
from abc import ABCMeta, abstractmethod
from collections.abc import Iterable, Iterator
from datetime import datetime
from math import ceil
from time import time
from typing import Optional

from pydash import get

from maggma.core import Builder, Store
from maggma.utils import Timeout, grouper


class GroupBuilder(Builder, metaclass=ABCMeta):
    """
    Group source docs and produces merged documents for each group
    Supports incremental building, where a source group gets (re)built only if
    it has a newer (by last_updated_field) doc than the corresponding (by key) target doc.

    This is a Many-to-One or Many-to-Many Builder. As a result, this builder can't determine when a source document
    is orphaned.
    """

    def __init__(
        self,
        source: Store,
        target: Store,
        grouping_keys: list[str],
        query: Optional[dict] = None,
        projection: Optional[list] = None,
        timeout: int = 0,
        store_process_time: bool = True,
        retry_failed: bool = False,
        **kwargs,
    ):
        """
        Args:
            source: source store
            target: target store
            query: optional query to filter items from the source store.
            projection: list of keys to project from the source for
                processing. Limits data transfer to improve efficiency.
            delete_orphans: Whether to delete documents on target store
                with key values not present in source store. Deletion happens
                after all updates, during Builder.finalize.
            timeout: maximum running time per item in seconds
            store_process_time: If True, add "_process_time" key to
                document for profiling purposes
            retry_failed: If True, will retry building documents that
                previously failed.
        """
        self.source = source
        self.target = target
        self.grouping_keys = grouping_keys
        self.query = query if query else {}
        self.projection = projection
        self.kwargs = kwargs
        self.timeout = timeout
        self.store_process_time = store_process_time
        self.retry_failed = retry_failed

        self._target_keys_field = f"{self.source.key}s"

        super().__init__(sources=[source], targets=[target], **kwargs)

    def ensure_indexes(self):
        """
        Ensures indices on critical fields for GroupBuilder
        which include the plural version of the target's key field.
        """
        index_checks = [
            self.source.ensure_index(self.source.key),
            self.source.ensure_index(self.source.last_updated_field),
            self.target.ensure_index(self.target.key),
            self.target.ensure_index(self.target.last_updated_field),
            self.target.ensure_index("state"),
            self.target.ensure_index(self._target_keys_field),
        ]

        if not all(index_checks):
            self.logger.warning(
                "Missing one or more important indices on stores. "
                "Performance for large stores may be severely degraded. "
                "Ensure indices on target.key and "
                "[(store.last_updated_field, -1), (store.key, 1)] "
                "for each of source and target."
            )

    def prechunk(self, number_splits: int) -> Iterator[dict]:
        """
        Generic prechunk for group builder to perform domain-decomposition
        by the grouping keys.
        """
        self.ensure_indexes()

        keys = self.get_ids_to_process()
        groups = self.get_groups_from_keys(keys)

        N = ceil(len(groups) / number_splits)
        for split in grouper(keys, N):
            yield {"query": dict(zip(self.grouping_keys, split))}

    def get_items(self):
        self.logger.info(f"Starting {self.__class__.__name__} Builder")

        self.ensure_indexes()
        keys = self.get_ids_to_process()
        groups = self.get_groups_from_keys(keys)

        if self.projection:
            projection = list({*self.projection, self.source.key, self.source.last_updated_field})
        else:
            projection = None

        self.total = len(groups)
        for group in groups:
            group_criteria = dict(zip(self.grouping_keys, group))
            group_criteria.update(self.query)
            yield list(self.source.query(criteria=group_criteria, properties=projection))

    def process_item(self, item: list[dict]) -> dict[tuple, dict]:  # type: ignore
        keys = [d[self.source.key] for d in item]

        self.logger.debug(f"Processing: {keys}")

        time_start = time()

        try:
            with Timeout(seconds=self.timeout):
                processed = self.unary_function(item)
                processed.update({"state": "successful"})
        except Exception as e:
            self.logger.error(traceback.format_exc())
            processed = {"error": str(e), "state": "failed"}

        time_end = time()

        last_updated = [self.source._lu_func[0](d[self.source.last_updated_field]) for d in item]

        update_doc = {
            self.target.key: keys[0],
            f"{self.source.key}s": keys,
            self.target.last_updated_field: max(last_updated),
            "_bt": datetime.utcnow(),
        }
        processed.update({k: v for k, v in update_doc.items() if k not in processed})

        if self.store_process_time:
            processed["_process_time"] = time_end - time_start

        return processed

    def update_targets(self, items: list[dict]):
        """
        Generic update targets for Group Builder.
        """
        target = self.target
        for item in items:
            if "_id" in item:
                del item["_id"]

        if len(items) > 0:
            target.update(items)

    @abstractmethod
    def unary_function(self, items: list[dict]) -> dict:
        """
        Processing function for GroupBuilder.

        Arguments:
            items: list of of documents with matching grouping keys

        Returns:
            Dictionary mapping:
                tuple of source document keys that are in the grouped document
                to the grouped and processed document
        """

    def get_ids_to_process(self) -> Iterable:
        """
        Gets the IDs that need to be processed.
        """
        distinct_from_target = list(self.target.distinct(self._target_keys_field, criteria=self.query))
        processed_ids = []
        # Not always guaranteed that MongoDB will unpack the list so we
        # have to make sure we do that
        for d in distinct_from_target:
            if isinstance(d, list):
                processed_ids.extend(d)
            else:
                processed_ids.append(d)

        all_ids = set(self.source.distinct(self.source.key, criteria=self.query))
        self.logger.debug(f"Found {len(all_ids)} total docs in source")

        if self.retry_failed:
            failed_keys = self.target.distinct(self._target_keys_field, criteria={"state": "failed", **self.query})
            unprocessed_ids = all_ids - (set(processed_ids) - set(failed_keys))
            self.logger.debug(f"Found {len(failed_keys)} failed IDs in target")
        else:
            unprocessed_ids = all_ids - set(processed_ids)

        self.logger.info(f"Found {len(unprocessed_ids)} IDs to process")

        new_ids = set(self.source.newer_in(self.target, criteria=self.query, exhaustive=False))

        self.logger.info(f"Found {len(new_ids)} updated IDs to process")
        return list(new_ids | unprocessed_ids)

    def get_groups_from_keys(self, keys) -> set[tuple]:
        """
        Get the groups by grouping_keys for these documents.
        """
        grouping_keys = self.grouping_keys

        groups: set[tuple] = set()

        for chunked_keys in grouper(keys, self.chunk_size):
            docs = list(
                self.source.query(
                    criteria={self.source.key: {"$in": chunked_keys}},
                    properties=grouping_keys,
                )
            )

            sub_groups = {tuple(get(d, prop, None) for prop in grouping_keys) for d in docs}
            self.logger.debug(f"Found {len(sub_groups)} subgroups to process")

            groups |= sub_groups

        self.logger.info(f"Found {len(groups)} groups to process")
        return groups
