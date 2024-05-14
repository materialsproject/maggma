from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime
from itertools import chain
from typing import Optional, Union

from pydash import get

from maggma.core import Builder, Store
from maggma.utils import grouper


class Projection_Builder(Builder):
    """
    This builder creates new documents that combine
    information from multiple input stores. These summary
    documents are then added to the specified target store.

    Key values are used for matching such that multiple docs
    from the input stores with the same key value will be combined
    into a single doc for that key value in the target store.

    Built in functionalities include user specification of which
    fields to project into the target store from each input store,
    renaming projected fields, and limiting the builder to only
    consider certain key values.
    """

    def __init__(
        self,
        source_stores: list[Store],
        target_store: Store,
        fields_to_project: Union[list[Union[list, dict]], None] = None,
        query_by_key: Optional[list] = None,
        **kwargs,
    ):
        """
        Args:
            source_stores ([MongoStore]): List of stores. Fields from
                these input stores will be projected into target_store
            target_store (MongoStore): Store where the summary/aggregated
                output documents produced will be stored
            fields_to_project ([List,Dict]): If provided, the order of items in
                this list must correspond to source_stores. By default, all
                fields of source_stores are projected into target_store.
                List elements can be provided as 1) a list of strings specifying
                the fields to pull from each input store, or 2) a dictionary
                where the values specify the fields to pull from the input store and
                the keys specify what field that will be used in the target store.

                e.g. ["field1","field2"] would be equivalent to
                {"field1":"field1", "field2":"field2"}
                Or fields could be renamed in the target stores via
                {"newname1":"field1", "newname2":"field2"}

                If an empty list or dictionary is provided, all fields of that
                input store will be projected.

                Note fields_to_project is converted into the projection_mapping
                attribute of this builder. There are no checks for possible
                overwrite errors in output docs for the target_store.

            query_by_key (List): Provide a list of keys to limit this builder to a
                only consider a subset of docs with these key values. By default,
                every document from the input stores will be projected.
        """
        # check for user input errors
        if isinstance(source_stores, list) is False:
            raise TypeError("Input source_stores must be provided in a list")
        if isinstance(fields_to_project, list):
            if len(source_stores) != len(fields_to_project):
                raise ValueError("There must be an equal number of elements in source_stores and fields_to_project")
        elif fields_to_project is not None:
            raise TypeError("Input fields_to_project must be a list. E.g. [['str1','str2'],{'A':'str1','B':str2'}]")

        # interpret fields_to_project to create projection_mapping attribute
        projection_mapping: list[dict]  # PEP 484 Type Hinting
        if fields_to_project is None:
            projection_mapping = [{}] * len(source_stores)
        else:
            projection_mapping = []
            for f in fields_to_project:
                if isinstance(f, (list)):
                    projection_mapping.append({i: i for i in f})
                elif isinstance(f, (dict)):
                    projection_mapping.append(f)
                else:
                    raise TypeError(
                        """Input fields_to_project elements must be a list or dict.
                        E.g. [['str1','str2'],{'A':'str1','B':str2'}]"""
                    )
            # ensure key is included in projection for get_items query
            for store, p in zip(source_stores, projection_mapping):
                if p != {}:
                    p.update({target_store.key: store.key})
        self.projection_mapping = projection_mapping

        # establish other attributes and initialization
        self.query_by_key = query_by_key or []
        self.target = target_store
        super().__init__(sources=source_stores, targets=target_store, **kwargs)
        self.ensure_indexes()

    def ensure_indexes(self):
        """
        Ensures key fields are indexed to improve querying efficiency.
        """
        index_checks = [s.ensure_index(s.key) for s in self.sources]

        if not all(index_checks):
            self.logger.warning("Missing indices for key fields on stores.")

    def get_items(self) -> Iterable:
        """
        Gets items from source_stores for processing.
        Items are retrieved in chunks based on a subset of
        key values set by chunk_size but are unsorted.

        Returns:
            generator of items to process
        """
        self.logger.info(f"Starting {self.__class__.__name__} get_items...")

        # get distinct key values
        if len(self.query_by_key) > 0:
            keys = self.query_by_key
        else:
            unique_keys = set()  # type: Set
            for store in self.sources:
                store_keys = store.distinct(field=store.key)
                unique_keys.update(store_keys)
                if None in store_keys:
                    self.logger.debug(
                        f"None found as a key value for store {store.collection_name} with key {store.key}"
                    )
            keys = list(unique_keys)
            self.logger.info(f"{len(keys)} distinct key values found")
            self.logger.debug(f"None found in key values? {None in keys}")

        # for every key (in chunks), query from each store and
        # project fields specified by projection_mapping
        for chunked_keys in grouper(keys, self.chunk_size):
            chunked_keys = [k for k in chunked_keys if k is not None]
            self.logger.debug(f"Querying by chunked_keys: {chunked_keys}")

            unsorted_items_to_process = []
            for store, projection in zip(self.sources, self.projection_mapping):
                # project all fields from store if corresponding element
                # in projection_mapping is an empty dict,
                # else only project the specified fields
                properties: Union[list, None]
                if projection == {}:  # all fields are projected
                    properties = None
                    self.logger.debug(f"For store {store.collection_name} getting all properties")
                else:  # only specified fields are projected
                    properties = list(projection.values())
                    self.logger.debug(f"For {store.collection_name} store getting properties: {properties}")

                # get docs from store for given chunk of key values,
                # rename fields if specified by projection mapping,
                # and put in list of unsorted items to be processed
                docs = store.query(criteria={store.key: {"$in": chunked_keys}}, properties=properties)
                for d in docs:
                    if properties is None:  # all fields are projected as is
                        item = deepcopy(d)
                    else:  # specified fields are renamed
                        item = dict()
                        for k, v in projection.items():
                            item[k] = get(d, v)

                    # remove unneeded fields and add key value to each item
                    # key value stored under target_key is used for sorting
                    # items during the process_items step
                    for k in ["_id", store.last_updated_field]:
                        if k in item:
                            del item[k]
                    item[self.target.key] = d[store.key]

                    unsorted_items_to_process.append(item)

                self.logger.debug(
                    f"Example fields of one output item from {store.collection_name} store sent to"
                    "process_items: {item.keys()}"
                )

            yield unsorted_items_to_process

    def process_item(self, items: Union[list, Iterable]) -> list[dict]:
        """
        Takes a chunk of items belonging to a subset of key values
        and groups them by key value. Combines items for each
        key value into one single doc for the target store.

        Arguments:
            items: items should all belong to a subset of
                key values but are not in any particular order
        Returns:
            items_for_target: a list of items where now each
                item corresponds to a single key value
        """
        self.logger.info("Processing items: sorting by key values...")
        key = self.target.key
        items_sorted_by_key = {}  # type: Dict
        for i in items:
            key_value = i[key]
            if key_value not in items_sorted_by_key:
                items_sorted_by_key[key_value] = []
            items_sorted_by_key[key_value].append(i)

        items_for_target = []
        for k, i_sorted in items_sorted_by_key.items():
            self.logger.debug(f"Combined items for {key}: {k}")
            target_doc: dict = {}
            for i in i_sorted:
                target_doc.update(i)
            # last modification is adding key value avoid overwriting
            target_doc[key] = k
            items_for_target.append(target_doc)
        # note target last_updated_field will be added during update_targets()

        return items_for_target

    def update_targets(self, items: list):
        """
        Adds a last_updated field to items and then adds
        them to the target store.

        Arguments:
            items: a list of items where each item contains
                all the information from the source_stores
                corresponding to a single key value
        """
        items = list(filter(None, chain.from_iterable(items)))
        num_items = len(items)
        self.logger.info(f"Updating target with {num_items} items...")
        target = self.target

        target_insertion_time = datetime.utcnow()
        for item in items:
            item[target.last_updated_field] = target_insertion_time

        if num_items > 0:
            target.update(items)
