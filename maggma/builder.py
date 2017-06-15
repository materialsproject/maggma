import six
import abc

from monty.json import MSONable


@six.add_metaclass(abc.ABCMeta)
class Builder(MSONable):
    def __init__(self, sources, targets, get_chunk_size=1000, process_chunk_size=1):
        """
        Initialize the builder the framework
        :param sources: list of source stores
        :param targets: list of target stores
        :param get_chunk_size: chunk size for get_items
        :param process_chunk_size: chunk size for process items
        """
        self.sources = sources
        self.targets = targets
        self.process_chunk_size = process_chunk_size
        self.get_chunk_size = get_chunk_size

    @abc.abstractmethod
    def get_items(self):
        """
        Returns all the items to process
        :return: generator or list of items to process
        """
        pass

    @abc.abstractmethod
    def process_item(self, item):
        """
        Process an item. Should not expect DB access as this can be run MPI
        :param item: 
        :return: dict of {target: item to insert}
        """
        pass

    @abstractmethod
    def update_targets(self, items):
        """
        Takes a dictionary of targets and items from process item and updates them
        Can also perform other book keeping in the process such as storing gridfs oids, etc.
        :param items: 
        :return: 
        """
        pass

    @abc.abstractmethod
    def finalize(self):
        """
        Perform any final clean up
        :return: 
        """
        pass
