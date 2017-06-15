from monty.json import MSONable

from collections import defaultdict


class Runner(MSONable):

    def __init__(self, builders):
        """
        Initialize with a lit of builders

        Args:
            builders(list): list of builders
        """
        self.builders = builders
        self.dependency_graph = self._get_builder_dependency_graph()

    def run(self):
        """
        For each builder:
            a.) Setup all sources and targets
            b.) pull get_chunk_size items from get_items
            c.) process process_chunk_size items
                   i) can be via serial, multiprocessing, mpi, or mpi/multiprocessing
            d.) update_targets
            e.) repeat a-c till no remaining items
            f.) finalize
            g.) Close all targets and sources
        Clean up and exit
        """
    pass

    # TODO: make it efficient, O(N^2) complexity at the moment, might be ok(not many builders)?
    def _get_builder_dependency_graph(self):
        """
        Does the following:
        1.) use targets and sources of builders to determine interdependencies
        2.) order builders according to interdependencies

        Returns:
            dict
        """
        # key = index of the builder in the self.builders list
        # value = list of indices of builders that the key depends on i.e these must run before
        # the builder corresponding to the key.
        links_dict = defaultdict(list)
        for i, bi in enumerate(self.builders):
            for j, bj in enumerate(self.builders):
                if i != j:
                    for s in bi.sources:
                        if s in bj.targets:
                            links_dict[i].append(j)
        return links_dict
