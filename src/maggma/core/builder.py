# coding: utf-8
"""
Module containing the core builder definition
"""

import logging
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Iterable, List, Union

from monty.json import MontyDecoder, MSONable

from maggma.core.store import Store
from maggma.utils import TqdmLoggingHandler, grouper, tqdm


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
    ):
        """
        Initialize the builder the framework.

        Arguments:
            sources: source Store(s)
            targets: target Store(s)
            chunk_size: chunk size for processing
        """
        self.sources = sources if isinstance(sources, list) else [sources]
        self.targets = targets if isinstance(targets, list) else [targets]
        self.chunk_size = chunk_size
        self.total = None  # type: Optional[int]
        self.logger = logging.getLogger(type(self).__name__)
        self.logger.addHandler(logging.NullHandler())

    def connect(self):
        """
        Connect to the builder sources and targets.
        """
        for s in self.sources + self.targets:
            s.connect()

    def prechunk(self, number_splits: int) -> Iterable[Dict]:
        """
        Part of a domain-decomposition paradigm to allow the builder to operate on
        multiple nodes by divinding up the IO as well as the compute
        This function should return an iterator of dictionaries that can be distributed
        to multiple instances of the builder to get/process/udpate on

        Arguments:
            number_splits: The number of groups to split the documents to work on
        """
        self.logger.info(
            f"{self.__class__.__name__} doesn't have distributed processing capabillities."
            " Instead this builder will run on just one worker for all processing"
        )
        raise NotImplementedError(
            f"{self.__class__.__name__} doesn't have distributed processing capabillities."
            " Instead this builder will run on just one worker for all processing"
        )

    @abstractmethod
    def get_items(self) -> Iterable:
        """
        Returns all the items to process.

        Returns:
            generator or list of items to process
        """
        pass

    def process_item(self, item: Any) -> Any:
        """
        Process an item. There should be no database operations in this method.
        Default behavior is to return the item.
        Arguments:
            item:

        Returns:
           item: an item to update
        """
        return item

    @abstractmethod
    def update_targets(self, items: List):
        """
        Takes a list of items from process item and updates the targets with them.
        Can also perform other book keeping in the process such as storing gridfs oids, etc.

        Arguments:
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

    def run(self, log_level=logging.DEBUG):
        """
        Run the builder serially
        This is only intended for diagnostic purposes
        """
        # Set up logging
        root = logging.getLogger()
        root.setLevel(log_level)
        ch = TqdmLoggingHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        ch.setFormatter(formatter)
        root.addHandler(ch)

        self.connect()

        cursor = self.get_items()

        for chunk in grouper(tqdm(cursor), self.chunk_size):
            self.logger.info("Processing batch of {} items".format(self.chunk_size))
            processed_chunk = [self.process_item(item) for item in chunk]
            processed_items = [item for item in processed_chunk if item is not None]
            self.update_targets(processed_items)

        self.finalize()

    def __getstate__(self):
        return self.as_dict()

    def __setstate__(self, d):
        d = {k: v for k, v in d.items() if not k.startswith("@")}
        d = MontyDecoder().process_decoded(d)
        self.__init__(**d)
