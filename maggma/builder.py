from abc import ABCMeta, abstractmethod

from monty.json import MSONable


class Builder(MSONable, metaclass=ABCMeta):

    def __init__(self, sources, targets, get_chunk_size=1000, process_chunk_size=1):
        """
        Initialize the builder the framework.

        Args:
            sources([Store]): list of source stores
            targets([Store]): list of target stores
            get_chunk_size(int): chunk size for get_items
            process_chunk_size(int): chunk size for process items
        """
        self.sources = sources
        self.targets = targets
        self.process_chunk_size = process_chunk_size
        self.get_chunk_size = get_chunk_size

    def connect(self, sources=True):
        """
        Connect to the builder sources or targets.

        Args:
            sources (bool): if True connect to sources else targets
        """
        stores = self.sources if sources else self.targets
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

    def update_targets(self, items):
        """
        Takes a dictionary of targets and items from process item and updates them
        Can also perform other book keeping in the process such as storing gridfs oids, etc.

        Ars:
            items:

        Returns:

        """
        pass

    @abstractmethod
    def finalize(self):
        """
        Perform any final clean up.
        """
        pass
