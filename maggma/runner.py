import logging
from collections import defaultdict
from itertools import cycle
import multiprocessing
import queue

from monty.json import MSONable

logger = logging.getLogger(__name__)


# TODO: add tests
class Runner(MSONable):

    def __init__(self, builders, use_mpi=True, num_workers=1):
        """
        Initialize with a lit of builders

        Args:
            builders(list): list of builders
            use_mpi (bool): if True its is assumed that the building is done via MPI, else
                multiprocessing is used.
            num_workers (int): number of processes. Used only for multiprocessing.
        """
        self.builders = builders
        self.use_mpi = use_mpi
        self.num_workers = num_workers
        self._queue = multiprocessing.Queue()
        self.dependency_graph = self._get_builder_dependency_graph()
        self.status = []


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
            self._run_builder_in_mpi(i)
        else:
            self._run_builder_in_multiproc(i)

        if all(self.status):
            builder.finalize()

    def _run_builder_in_mpi(self, i):
        """

        Args:
            builder:
        """
        (comm, rank, size) = get_mpi()

        # master: doesnt do any 'work', just distributes the workload.
        if rank == 0:
            builder = self.builders[i]
            # TODO: establish the builder's connection to the db here, before the loop.
            # cycle through the workers, there could be less workers than the items to process
            worker = cycle(range(1, size))

            # distribute the items to process
            for item in builder.get_items():
                comm.send(item, dest=next(worker))

            # get job status from the workers
            for i in range(1, size):
                status = comm.recv(source=i)
                self.status.append(status)

            # kill workers
            for _ in range(size - 1):
                comm.send(None, dest=next(worker))

        # workers:
        #   - process item
        #   - update target
        else:
            self.worker(i, comm)

    # TODO: scrape this? - KM
    def _run_builder_in_mpi_collective_comm(self, builder, scatter=True):
        """
        Since all the items to be processed are fetched on the master node at once, this
        implementation could be problematic if there are large number of items or small number of
        large items.

        At the moment it is hard to get around this: only pickleable objects can be passed
        around using MPI and generators/Queues(uses thread locking internally) are not pickleable!!

        Args:
            builder (Builder): Any object of class that subclasses Builder
            scatter (bool): if True then the items are scattered from the master to slaves, else
                broadcasted.
        """

        (comm, rank, size) = get_mpi()

        items = None

        # get all items at the master
        if rank == 0:
            items = list(builder.get_items())

        # pad items if necessary and scatter it to the slaves
        # ==>
        # large memory consumption(if the number and/or size of the items are large) ONLY at the master
        if scatter:
            itm = None
            if rank == 0:
                n = len(items)
                chunk_size, n_chunks = (n//size, size) if size <= n else (1, n)
                itm = [items[r * chunk_size:(r + 1) * chunk_size] for r in range(n_chunks)]
                # if there are processes than elements, pad the scattering list
                itm.extend([[] for _ in range(max(0, size - n))])
                if 0 < n % size < n:
                    itm[-1].extend(items[size * chunk_size:])
                # print("size", size, chunk_size)

            itm = comm.scatter(itm, root=0)

            builder.process_item(itm)

        # broadcast all items from the master to the slaves.
        # ==>
        # large memory consumption(if the number and/or size of the items are large) on ALL NODES.
        else:
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

    def _run_builder_in_multiproc(self, builder_id):
        """

        Args:
            builder:

        Returns:

        """
        processes = []
        builder = self.builders[builder_id]

        # send items to process
        for item in builder.get_items():
            self._queue.put(item)

        # start the workers
        for i in range(self.num_workers):
            proc = multiprocessing.Process(target=self.worker, args=(builder_id,))
            proc.start()
            processes.append(proc)

        # get job status from the workers
        for i in range(self.num_workers):
            processes[i].join()
            code = processes[i].exitcode
            self.status.append(not bool(code))

    def worker(self, builder_id, comm=None):
        if self.use_mpi:
            while True:
                item = comm.recv(source=0)
                if item is None:
                    break
                processed_item = self.builders[builder_id].process_item(item)
                self.builders[builder_id].update_targets(processed_item)
                comm.ssend(True, 0)
        else:
            while True:
                try:
                    item = self._queue.get(timeout=2)
                    processed_item = self.builders[builder_id].process_item(item)
                    self.builders[builder_id].update_targets(processed_item)
                except queue.Empty:
                    break

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


def get_mpi():
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

    return comm, rank, size

