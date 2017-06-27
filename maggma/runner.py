import logging
import multiprocessing
import queue
import sys
from collections import defaultdict
from itertools import cycle

from monty.json import MSONable

from maggma.helpers import get_mpi

logger = logging.getLogger(__name__)
sh = logging.StreamHandler(stream=sys.stdout)
sh.setLevel(logging.DEBUG)
sh.setFormatter('%(asctime)s %(levelname)s %(message)s')
logger.addHandler(sh)


class Runner(MSONable):

    def __init__(self, builders, num_workers=0):
        """
        Initialize with a list of builders

        Args:
            builders(list): list of builders
            num_workers (int): number of processes. Used only for multiprocessing.
                Will be automatically set to (number of cpus - 1) if set to 0.
        """
        self.builders = builders

        try:
            (comm, rank, size) = get_mpi()
            self.use_mpi = True if size > 1 else False
        except ImportError:
            print("either 'mpi4py' is not installed or issue with the installation. "
                  "Proceeding with mulitprocessing.")
            self.use_mpi = False

        # multiprocessing only if mpi is not used, no mixing
        self.num_workers = num_workers if num_workers > 0 else multiprocessing.cpu_count()-1
        if not self.use_mpi:
            if self.num_workers > 0:
                print("Building with multiprocessing, {} workers in the pool".format(self.num_workers))
                self._queue = multiprocessing.Queue()
                manager = multiprocessing.Manager()
                self.processed_items = manager.dict()
            # serial
            else:
                print("Building serially")
                self._queue = queue.Queue()
                self.processed_items = dict()
        else:
            if rank == 0:
                print("Building with MPI. {} workers in the pool.".format(size-1))

        self.dependency_graph = self._get_builder_dependency_graph()
        self.has_run = []  # for bookkeeping builder runs
        self.status = []  # builder run status

    # TODO: make it efficient, O(N^2) complexity at the moment,
    # might be ok(not many builders)? - KM
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

    def run(self):
        """
        Does the following:
            - traverse through the builder dependency graph and does the following to
              each builder
                - connect to sources
                - get items and feed it to the processing pipeline
                - process each item
                    - supported options: serial, MPI or the builtin multiprocessing
                - collect all processed items
                - connect to the targets
                - update targets
                - finalize aka cleanup(close all connections etc)
        """
        for i in range(len(self.builders)):
            self._build_dependencies(i)
    
    def _build_dependencies(self, builder_id):
        """
        Run the builders by recursively traversing through the dependency graph.

        Args:
            builder_id (int): builder index
        """
        if builder_id in self.has_run:
            return
        else:
            if self.dependency_graph[builder_id]:
                for j in self.dependency_graph[builder_id]:
                    self._build_dependencies(j)
            self._run_builder(builder_id)
            self.has_run.append(builder_id)

    def _run_builder(self, builder_id):
        """
        Run builder: self.builders[builder_id]

        Args:
            builder_id (int): builder index

        Returns:

        """
        if self.use_mpi:
            logger.info("building: ", builder_id)
            self._run_builder_in_mpi(builder_id)
        else:
            self._run_builder_in_multiproc(builder_id)

    def _run_builder_in_mpi(self, builder_id):
        """
        Run the builder using MPI protocol.

        Args:
            builder_id (int): the index of the builder in the builders list
        """
        (comm, rank, size) = get_mpi()
        processed_items_dict = {}

        # master: doesnt do any 'work', just distributes the workload.
        if rank == 0:
            builder = self.builders[builder_id]
            # establish connection to the sources
            builder.connect(sources=True)

            # cycle through the workers, there could be less workers than the items to process
            worker_id = cycle(range(1, size))

            n = 0
            # distribute the items to process
            for item in builder.get_items():
                packet = (builder_id, item)
                comm.send(packet, dest=next(worker_id))
                n = n+1

            logger.info("{} items sent for processing".format(n))

            # get processed item from the workers
            for i in range(n):
                try:
                    processed_item = comm.recv()
                    self.status.append(True)
                    processed_items_dict.update(processed_item)
                except:
                    raise

            # kill workers
            for _ in range(size - 1):
                comm.send(None, dest=next(worker_id))

            # update the targets
            builder.connect(sources=False)
            builder.update_targets(processed_items_dict)

            if all(self.status):
                builder.finalize()

        # workers:
        #   - process item
        #   - update target
        #   - report status
        else:
            self.worker(comm)

    def _run_builder_in_multiproc(self, builder_id):
        """
        Run the builder using the builtin multiprocessing.
        Adapted from pymatgen-db

        Args:
            builder_id (int): the index of the builder in the builders list
        """
        processes = []
        builder = self.builders[builder_id]

        # establish connection to the sources
        builder.connect(sources=True)

        # send items to process
        for item in builder.get_items():
            packet = (builder_id, item)
            self._queue.put(packet)

        if self.num_workers > 0:
            # start the workers
            for i in range(self.num_workers):
                proc = multiprocessing.Process(target=self.worker, args=(None,))
                proc.start()
                processes.append(proc)

            # get job status from the workers
            for i in range(self.num_workers):
                processes[i].join()
                code = processes[i].exitcode
                self.status.append(not bool(code))
        # serial execution
        else:
            try:
                self.worker()
                self.status.append(True)
            except:
                raise

        # update the targets
        builder.connect(sources=False)
        builder.update_targets(self.processed_items)

        if all(self.status):
            self.builders[builder_id].finalize()

    def worker(self, comm=None):
        """
        Where shit gets done!
        Call the builder's process_item method and send back(or put it in the shared dict if
        multiprocess) the processed item

        Args:
            comm (MPI.comm): mpi communicator, must be given when using MPI.
        """
        while True:
            if self.use_mpi:
                    packet = comm.recv(source=0)
                    if packet is None:
                        break
                    builder_id, item = packet
                    processed_item = self.builders[builder_id].process_item(item)
                    comm.ssend(processed_item, 0)
            else:
                    try:
                        packet = self._queue.get(timeout=2)
                        builder_id, item = packet
                        processed_item = self.builders[builder_id].process_item(item)
                        self.processed_items.update(processed_item)
                    except queue.Empty:
                        break
