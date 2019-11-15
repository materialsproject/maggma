# coding: utf-8
"""
Module containing the core builder definition
"""
from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from typing import Union, Optional, Dict, List, Iterator, Any

from monty.json import MSONable, MontyDecoder
from maggma.utils import grouper
from maggma.core import Store


class Builder(MSONable, metaclass=ABCMeta):
    """
    Base Builder class
    At minimum this class should implement:
    get_items - Get items from the sources
    update_targets - Updates the sources with results

    Multiprocessing and MPI processing can be used if all
    the data processing is  limited to process_items
    """

    def __init__(
        self,
        sources: Union[List[Store], Store],
        targets: Union[List[Store], Store],
        chunk_size: int = 1000,
        query: Optional[Dict] = None,
    ):
        """
        Initialize the builder the framework.

        Args:
            sources: source Store(s)
            targets: target Store(s)
            chunk_size: chunk size for processing
            query: dictionary of options to utilize on a source;
                   Each builder has internal logic on which souce this will apply to
        """
        self.sources = sources if isinstance(sources, list) else [sources]
        self.targets = targets if isinstance(targets, list) else [targets]
        self.chunk_size = chunk_size
        self.query = query
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
    def get_items(self) -> Iterator:
        """
        Returns all the items to process.

        Returns:
            generator or list of items to process
        """
        pass

    def process_item(self, item: Any) -> Any:
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
    def update_targets(self, items: List):
        """
        Takes a dictionary of targets and items from process item and updates them
        Can also perform other book keeping in the process such as storing gridfs oids, etc.

        Args:
            items:

        Returns:

        """
        pass

    def finalize(self):
        """
        Perform any final clean up.
        """
        # Close any Mongo connections.
        for store in self.sources + self.targets:
            try:
                store.close()
            except AttributeError:
                continue

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
            processed_items = [
                self.process_item(item) for item in chunk if item is not None
            ]
            self.update_targets(processed_items)

        self.finalize(cursor)

    def __getstate__(self):
        return self.as_dict()

    def __setstate__(self, d):
        d = {k: v for k, v in d.items() if not k.startswith("@")}
        d = MontyDecoder().process_decoded(d)
        self.__init__(**d)
