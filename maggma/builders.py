# coding: utf-8
"""
Base Builder class to define how builders need to be defined
"""
from abc import ABCMeta, abstractmethod
import traceback
from datetime import datetime
from maggma.utils import source_keys_updated, grouper, Timeout
from time import time
from maggma.core import Builder, Store
from typing import Optional, Dict, List, Callable


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
        ufn: Callable,
        query: Optional[Dict] = None,
        incremental: bool = True,
        projection: Optional[List] = None,
        delete_orphans: bool = False,
        timeout: int = 0,
        store_process_time: bool = True,
        **kwargs
    ):
        """
        Apply a unary function to each source document.

        Args:
            source: source store
            target: target store
            ufn: Unary function to process item
                You do not need to provide values for
                source.key and source.last_updated_field in the output.
                Any uncaught exceptions will be caught by
                process_item and logged to the "error" field
                in the target document.
            query: optional query to filter source store
            incremental: Whether to limit query to filter for only updated source documents.
            projection: list of keys to project from the source for
                processing. Limits data transfer to improve efficiency.
            delete_orphans: Whether to delete documents on target store
                with key values not present in source store. Deletion happens
                after all updates, during Builder.finalize.
            timeout: maximum running time per item in seconds
            store_process_time: If True, add "_process_time" key to
            document for profiling purposes
        """
        self.source = source
        self.target = target
        self.query = query
        self.incremental = incremental
        self.ufn = ufn
        self.projection = projection
        self.delete_orphans = delete_orphans
        self.kwargs = kwargs
        self.total = None
        self.timeout = timeout
        self.store_process_time = store_process_time
        super().__init__(sources=[source], targets=[target], **kwargs)

    def ensure_indexes(self):

        index_checks = [
            self.source.ensure_index(self.source.key),
            self.source.ensure_index(self.source.last_updated_field),
            self.target.ensure_index(self.target.key),
            self.target.ensure_index(self.target.last_updated_field),
        ]

        if not all(index_checks):
            self.logger.warning(
                "Missing one or more important indices on stores. "
                "Performance for large stores may be severely degraded. "
                "Ensure indices on target.key and "
                "[(store.last_updated_field, -1), (store.key, 1)] "
                "for each of source and target."
            )

    def get_items(self):
        """
        Generic get items for Map Builder designed to perform
        incremental building
        """

        self.logger.info("Starting {} Builder".format(self.__class__.__name__))

        self.ensure_indexes()

        if self.incremental:
            keys = self.target.newer_in(
                self.source, criteria=self.query, exhaustive=True
            )
        else:
            keys = self.source.distinct(self.source.key, criteria=self.query)

        self.logger.info("Processing {} items".format(len(keys)))

        if self.projection:
            projection = list(
                set(self.projection + [self.source.key, self.source.last_updated_field])
            )
        else:
            projection = None

        self.total = len(keys)
        for chunked_keys in grouper(keys, self.chunk_size, None):
            chunked_keys = list(filter(None.__ne__, chunked_keys))
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
                processed = self.ufn.__call__(item)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            processed = {"error": str(e)}

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

    def finalize(self, cursor=None):
        if self.delete_orphans:
            source_keyvals = set(self.source.distinct(self.source.key))
            target_keyvals = set(self.target.distinct(self.target.key))
            to_delete = list(target_keyvals - source_keyvals)
            if len(to_delete):
                self.logger.info(
                    "Finalize: Deleting {} orphans.".format(len(to_delete))
                )
            self.target.remove_docs({self.target.key: {"$in": to_delete}})
        super().finalize(cursor)


class GroupBuilder(MapBuilder, metaclass=ABCMeta):
    """
    Group source docs and produce one target doc from each group.

    Supports incremental building, where a source group gets (re)built only if
    it has a newer (by last_updated_field) doc than the corresponding (by key) target doc.
    """

    def get_items(self):
        criteria = source_keys_updated(self.source, self.target, query=self.query)
        if all(isinstance(entry, str) for entry in self.grouping_properties()):
            properties = {entry: 1 for entry in self.grouping_properties()}
            if "_id" not in properties:
                properties.update({"_id": 0})
        else:
            properties = {
                entry: include for entry, include in self.grouping_properties()
            }
        groups = self.docs_to_groups(
            self.source.query(criteria=criteria, properties=properties)
        )
        self.total = len(groups)
        if hasattr(self, "n_items_per_group"):
            n = self.n_items_per_group
            if isinstance(n, int) and n >= 1:
                self.total *= n
        for group in groups:
            for item in self.group_to_items(group):
                yield item

    @staticmethod
    @abstractmethod
    def grouping_properties():
        """
        Needed projection for docs_to_groups (passed to source.query).

        Returns:
            list or dict: of the same form as projection param passed to
                pymongo.collection.Collection.find. If a list, it is converted
                to dict form with {"_id": 0} unless "_id" is explicitly
                included in the list. This is to ease use of index-covered
                queries in docs_to_groups.
        """

    @staticmethod
    @abstractmethod
    def docs_to_groups(docs):
        """
        Yield groups from (minimally-projected) documents.

        This could be as simple as returning a set of unique document keys.

        Args:
            docs (pymongo.cursor.Cursor): documents with minimal projections
                needed to determine groups.

        Returns:
            iterable: one group at a time
        """

    @abstractmethod
    def group_to_items(self, group):
        """
        Given a group, yield items for this builder's process_item method.

        This method does the work of fetching data needed for processing.

        Args:
            group (dict): sufficient as or to produce a source filter

        Returns:
            iterable: one or more items per group for process_item.
        """


class CopyBuilder(MapBuilder):
    """Sync a source store with a target store."""

    def __init__(self, source: Store, target: Store, **kwargs):
        super().__init__(
            source=source,
            target=target,
            ufn=lambda x: x,
            store_process_time=False,
            **kwargs
        )
