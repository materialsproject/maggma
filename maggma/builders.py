# coding: utf-8
"""
Base Builder class to define how builders need to be defined
"""
from abc import ABCMeta, abstractmethod
import logging
import traceback
from datetime import datetime
from monty.json import MSONable, MontyDecoder
from maggma.utils import source_keys_updated, grouper, Timeout
from time import time


class Builder(MSONable, metaclass=ABCMeta):
    """
    Base Builder class
    At minimum this class should implement:
    get_items - Get items from the sources
    update_targets - Updates the sources with results

    Multiprocessing and MPI processing can be used if all
    the data processing is  limited to process_items
    """

    def __init__(self, sources, targets, chunk_size=1000):
        """
        Initialize the builder the framework.

        Args:
            sources([Store]): list of source stores
            targets([Store]): list of target stores
            chunk_size(int): chunk size for processing
        """
        self.sources = sources
        self.targets = targets
        self.chunk_size = chunk_size

        self.logger = logging.getLogger(type(self).__name__)
        self.logger.addHandler(logging.NullHandler())

    def connect(self):
        """
        Connect to the builder sources and targets.
        """
        stores = self.sources + self.targets
        for s in stores:
            s.connect()

    @abstractmethod
    def get_items(self):
        """
        Returns all the items to process.

        Returns:
            generator or list of items to process
        """
        pass

    def process_item(self, item):
        """
        Process an item. Should not expect DB access as this can be run MPI
        Default behavior is to return the item.
        Args:
            item:

        Returns:
           item: an item to update
        """
        return item

    @abstractmethod
    def update_targets(self, items):
        """
        Takes a dictionary of targets and items from process item and updates them
        Can also perform other book keeping in the process such as storing gridfs oids, etc.

        Args:
            items:

        Returns:

        """
        pass

    def finalize(self, cursor=None):
        """
        Perform any final clean up.
        """
        # Close any Mongo connections.
        for store in self.sources + self.targets:
            try:
                store.collection.database.client.close()
            except AttributeError:
                continue
        # Runner will pass iterable yielded by `self.get_items` as `cursor`. If
        # this is a Mongo cursor with `no_cursor_timeout=True` (not the
        # default), we must be explicitly kill it.
        try:
            cursor and cursor.close()
        except AttributeError:
            pass

    def __getstate__(self):
        """
        Double underscore method used by pickle to serialize this object
        This uses MSONable serialization instead
        """
        return self.as_dict()

    def __setstate__(self, d):
        """
        Double underscore method used by pickle to deserialize this object
        This uses MSONable deerialization instead
        """
        del d["@class"]
        del d["@module"]
        md = MontyDecoder()
        d = md.process_decoded(d)
        self.__init__(**d)

    def run(self):
        """
        Run the builder serially

        Args:
            builder_id (int): the index of the builder in the builders list
        """
        self.connect()

        cursor = self.get_items()

        for chunk in grouper(cursor, self.chunk_size):
            self.logger.info("Processing batch of {} items".format(self.chunk_size))
            processed_items = [self.process_item(item) for item in chunk if item is not None]
            self.update_targets(processed_items)

        self.finalize(cursor)


class MapBuilder(Builder, metaclass=ABCMeta):
    """
    Apply a unary function to yield a target document for each source document.

    Supports incremental building, where a source document gets built only if it
    has newer (by lu_field) data than the corresponding (by key) target
    document.

    """

    def __init__(self,
                 source,
                 target,
                 ufn,
                 query=None,
                 incremental=True,
                 projection=None,
                 delete_orphans=False,
                 timeout=None,
                 store_process_time=True,
                 **kwargs):
        """
        Apply a unary function to each source document.

        Args:
            source (Store): source store
            target (Store): target store
            ufn (function): Unary function to process item
                            You do not need to provide values for
                            source.key and source.lu_field in the output.
                            Any uncaught exceptions will be caught by
                            process_item and logged to the "error" field
                            in the target document.
            query (dict): optional query to filter source store
            incremental (bool): Whether to limit query to filter for only updated source documents.
            projection (list): list of keys to project from the source for
                processing. Limits data transfer to improve efficiency.
            delete_orphans (bool): Whether to delete documents on target store
                with key values not present in source store. Deletion happens
                after all updates, during Builder.finalize.
            timeout (int): maximum running time per item in seconds
            store_process_time (bool): If True, add "_process_time" key to
            document for profiling purposes
        """
        self.source = source
        self.target = target
        self.query = query
        self.incremental = incremental
        self.ufn = ufn
        self.projection = projection if projection else []
        self.delete_orphans = delete_orphans
        self.kwargs = kwargs
        self.total = None
        self.timeout = timeout
        self.store_process_time = store_process_time
        super().__init__(sources=[source], targets=[target], **kwargs)

    def ensure_indexes(self):

        index_checks = [
            self.source.ensure_index(self.source.key),
            self.source.ensure_index(self.source.lu_field),
            self.target.ensure_index(self.target.key),
            self.target.ensure_index(self.target.lu_field),
        ]

        if not all(index_checks):
            self.logger.warning("Missing one or more important indices on stores. "
                                "Performance for large stores may be severely degraded. "
                                "Ensure indices on target.key and "
                                "[(store.lu_field, -1), (store.key, 1)] "
                                "for each of source and target.")

    def get_items(self):

        self.logger.info("Starting {} Builder".format(self.__class__.__name__))

        self.ensure_indexes()

        if self.incremental:
            keys = source_keys_updated(source=self.source, target=self.target, query=self.query)
        else:
            keys = self.source.distinct(self.source.key, self.query)

        self.logger.info("Processing {} items".format(len(keys)))

        if self.projection:
            projection = list(set(self.projection + [self.source.key, self.source.lu_field]))
        else:
            projection = None

        self.total = len(keys)
        for chunked_keys in grouper(keys, self.chunk_size, None):
            chunked_keys = list(filter(None.__ne__, chunked_keys))
            for doc in list(
                    self.source.query(
                        criteria={self.source.key: {
                            "$in": chunked_keys
                        }},
                        properties=projection,
                    )):
                yield doc

    def process_item(self, item):

        self.logger.debug("Processing: {}".format(item[self.source.key]))

        time_start = time()

        try:
            with Timeout(seconds=self.timeout):
                processed = self.ufn.__call__(item)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            processed = {"error": str(e)}

        time_end = time()

        key, lu_field = self.source.key, self.source.lu_field

        out = {
            self.target.key: item[key],
            self.target.lu_field: self.source.lu_func[0](item[self.source.lu_field]),
        }
        if self.store_process_time:
            out["_process_time"] = time_end - time_start

        out.update(processed)
        return out

    def update_targets(self, items):
        source, target = self.source, self.target
        for item in items:
            # Use source last-updated value, ensuring `datetime` type.
            item[target.lu_field] = source.lu_func[0](item[source.lu_field])
            if source.lu_field != target.lu_field:
                del item[source.lu_field]
            item["_bt"] = datetime.utcnow()
            if "_id" in item:
                del item["_id"]

        if len(items) > 0:
            target.update(items, update_lu=False)

    def finalize(self, cursor=None):
        if self.delete_orphans:
            if not hasattr(self.target, "collection"):
                self.logger.warning("delete_orphans parameter is only supported for "
                                    "Mongolike target stores at this time.")
            else:
                source_keyvals = set(self.source.distinct(self.source.key))
                target_keyvals = set(self.target.distinct(self.target.key))
                to_delete = list(target_keyvals - source_keyvals)
                if len(to_delete):
                    self.logger.info("Finalize: Deleting {} orphans.".format(len(to_delete)))
                self.target.collection.delete_many({self.target.key: {"$in": to_delete}})
        super().finalize(cursor)


class GroupBuilder(MapBuilder, metaclass=ABCMeta):
    """
    Group source docs and produce one target doc from each group.

    Supports incremental building, where a source group gets (re)built only if
    it has a newer (by lu_field) doc than the corresponding (by key) target doc.
    """

    def __init__(self, source, target, query=None, **kwargs):
        """

        Given criteria, get docs with needed grouping properties. With these
        minimal docs, yield groups. For each group, fetch all needed data for
        item processing, and yield one or more items (i.e. subgroups as
        appropriate).

        Args:
            source (Store): source store
            target (Store): target store
            query (dict): optional query to filter source store
        """
        super().__init__(source, target, query=query, **kwargs)
        self.total = None

    def get_items(self):
        criteria = source_keys_updated(self.source, self.target, query=self.query)
        if all(isinstance(entry, str) for entry in self.grouping_properties()):
            properties = {entry: 1 for entry in self.grouping_properties()}
            if "_id" not in properties:
                properties.update({"_id": 0})
        else:
            properties = {entry: include for entry, include in self.grouping_properties()}
        groups = self.docs_to_groups(self.source.query(criteria=criteria, properties=properties))
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

    def __init__(self, source, target, **kwargs):
        super().__init__(source=source, target=target, ufn=lambda x: x, store_process_time=False, **kwargs)
