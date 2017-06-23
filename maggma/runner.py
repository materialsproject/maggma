import logging
from collections import defaultdict

from monty.json import MSONable

logger = logging.getLogger(__name__)


# TODO: add tests
class Runner(MSONable):

    def __init__(self, builders, use_mpi=True, nprocs=1):
        """
        Initialize with a lit of builders

        Args:
            builders(list): list of builders
            use_mpi (bool): if True its is assumed that the building is done via MPI, else
                multiprocessing is used.
            nprocs (int): number of processes. Used only for multiprocessing.
        """
        self.builders = builders
        self.use_mpi = use_mpi
        self.nprocs = nprocs
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

    # TODO: cleanup/refactor -KM
    def _run_builder(self, i):
        """
        Run the i'th builder i.e. self.builders[i]

        Args:
            i (int): builder index

        Returns:

        """
        builder = self.builders[i]

        if self.use_mpi:
            self._run_builder_in_mpi(builder)
        else:
            self._run_builder_in_multiproc(builder)

        # cleanup
        builder.finalize()

    # TODO: replace this piece of horriblness!! -KM
    def _run_builder_in_mpi(self, builder):
        """

        Args:
            builder:

        Returns:

        """

        try:
            from mpi4py import MPI

            comm = MPI.COMM_WORLD
            rank = comm.Get_rank()
            size = comm.Get_size()
        except ImportError:
            comm = None
            rank = 0
            size = 1
            logger.warning("No MPI")

        items = None

        # get the items to process at the master and broadcast it to all slaves
        if rank == 0:
            items = list(builder.get_items())

        items = comm.bcast(items, root=0) if comm else items

        n = len(items)
        chunk_size = n // size

        # adjust chuck size if the data size is not divisible by the
        # number of processors
        if rank == 0:
            if n % size != 0:
                chunk_size = chunk_size + n % size

        items_chunk = items[rank:rank + chunk_size]

        for itm in items_chunk:
            builder.process_item(itm)

    # TODO: replace this piece of horriblness!! -KM            
    def _run_builder_in_multiproc(self, builder):
        """

        Args:
            builder:

        Returns:

        """

        from multiprocessing import Pool

        p = Pool(self.nprocs)
        p.map(builder.process_item, [itm for itm in builder.get_items()])

    # TODO: make it efficient, O(N^2) complexity at the moment, might be ok(not many builders)? - KM
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
