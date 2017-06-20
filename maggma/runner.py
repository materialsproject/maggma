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
        self.has_run = []  # for bookkeeping
        for i, b in enumerate(self.builders):
            self._recursive_run(i)
    
    def _recursive_run(self, i):
        """
        Run the builders by recursively traversing through the dependency graph.

        Args:
            i (int): builder index
        """
        if i in self.has_run:
            return
        else:
            if self.dependency_graph[i]:
                for j in self.dependency_graph[i]:
                    self._recursive_run(j)
            self._run_builder(i)
            self.has_run.append(i)

    def _run_builder(self, i):
        """
        Run the i'th builder i.e. self.builders[i]

        Args:
            i (int): builder index

        Returns:

        """
        #builder = self.builders[i]
        #items = list(builder.get_items())
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
