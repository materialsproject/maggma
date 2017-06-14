from monty.json import MSONable
from maggma.builder import Builder

class Runner(MSONable):


    def __init__(self, builders):
        """
        Initialize with a lit of builders
        :param builders: list of builders
        """
        self.builders = builders
        pass

    def run(self):
    """
    # 1.) use targets and sources of builders to determine interdependencies
    # 2.) order builders according to interdependencies
    # 3.) For each builder:
    #   a.) Setup all targets and sources
    #   b.) pull get_chunk_size items from get_items
    #   c.) process process_chunk_size items
    #       i) can be via serial, multiprocessing, mpi, or mpi/multiprocessing
    #   d.) update_targets
    #   e.) repeat a-c till no remaining items
    #   f.) finalize
    #   g.) Close all targets and sources
    # 4.) Clean up and exit
    """