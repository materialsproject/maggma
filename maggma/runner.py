import logging
import multiprocessing
import queue
import sys
from collections import defaultdict
from itertools import cycle
import abc

from monty.json import MSONable

from maggma.helpers import get_mpi

logger = logging.getLogger(__name__)
sh = logging.StreamHandler(stream=sys.stdout)
sh.setLevel(logging.DEBUG)
sh.setFormatter('%(asctime)s %(levelname)s %(message)s')
logger.addHandler(sh)


class BaseProcessor(MSONable, metaclass=abc.ABCMeta):

    def __init__(self, builders, num_workers=0):
        """
        Initialize with a list of builders

        Args:
            builders(list): list of builders
            num_workers (int): number of processes. Used only for multiprocessing.
                Will be automatically set to (number of cpus - 1) if set to 0.
        """
        self.builders = builders
        self.num_workers = num_workers
        self.status = []

    @abc.abstractmethod
    def process(self):
        """
        Does the processing. e.g. send work to workers(in MPI) or start the processes in 
        multiprocessing.
        """        
        pass

    @abc.abstractmethod
    def worker(self):
        """
        Defines what a worker(slave in MPI or process in multiprocessing) does.
        """
        pass


class MPIProcessor(BaseProcessor):

    def __init__(self, builders):
        (self.comm, self.rank, self.size) = get_mpi()        
        super(MPIProcessor, self).__init__(builders)

    def process(self, builder_id):
        """
        Run the builder using MPI protocol.

        Args:
            builder_id (int): the index of the builder in the builders list
        """
        self.comm.Barrier()
        # master: doesnt do any 'work', just distributes the workload.
        if self.rank == 0:
            self.master(builder_id)
        # worker: process item
        else:
            self.worker()

    def master(self, builder_id):
        print("Building with MPI. {} workers in the pool.".format(self.size - 1))
        
        processed_items = []
        builder = self.builders[builder_id]
        chunk_size = builder.get_chunk_size
        
        # establish connection to the sources and targets
        builder.connect()

        # cycle through the workers, there could be less workers than the items to process
        worker_id = cycle(range(1, self.size))

        n = 0
        workers = []
        # distribute the items to process (in chunks of size chunk_size)
        for item in builder.get_items():
            if n % chunk_size == 0:
                print("processing chunks of size {}".format(chunk_size))
                processed_chunk, status = self._process_chunk(chunk_size, workers)
                processed_items.extend(processed_chunk)

                if status:
                    if not all(status):
                        raise RuntimeError("processing failed")
                    workers = []

            packet = (builder_id, item)
            wid = next(worker_id)
            workers.append(wid)
            self.comm.send(packet, dest=wid)
            n += 1

        # incase the total number of items is not divisible by chunk_size, process the leftovers.
        if workers:
            processed_chunk, status = self._process_chunk(chunk_size, workers)
            processed_items.extend(processed_chunk)
            if not all(status):
                raise

        # kill workers
        for _ in range(self.size - 1):
            self.comm.send(None, dest=next(worker_id))

        # update the targets
        builder.update_targets(processed_items)

        if all(self.status):
            builder.finalize()

    def _process_chunk(self, chunk_size, workers):
        """
        process chunk_size items.

        Args:
            chunk_size (int):
            workers (list): lis tpf worker ids

        Returns:
            (list, list): list of processed items, each process' status list
        """
        status = []
        processed_chunk = []
        logger.info("{} items sent for processing".format(chunk_size))

        # get processed item from the workers
        if workers:
            for _ in workers:
                try:
                    processed_item = self.comm.recv()
                    status.append(True)
                    processed_chunk.append(processed_item)
                except:
                    raise

        return processed_chunk, status

    def worker(self):
        """
        Where shit gets done!
        Call the builder's process_item method and send back the processed item

        Args:
            comm (MPI.comm): mpi communicator, must be given when using MPI.
        """
        while True:
            packet = self.comm.recv(source=0)
            if packet is None:
                break
            builder_id, item = packet
            processed_item = self.builders[builder_id].process_item(item)
            self.comm.ssend(processed_item, 0)


class MultiprocProcessor(BaseProcessor):

    def __init__(self, builders, num_workers):
        # multiprocessing only if mpi is not used, no mixing
        self.num_workers = num_workers if num_workers > 0 else multiprocessing.cpu_count() - 1
        if self.num_workers > 0:
            print("Building with multiprocessing, {} workers in the pool".format(
                self.num_workers))
            self._queue = multiprocessing.Queue()
            manager = multiprocessing.Manager()
            self.processed_items = manager.list()
        # serial
        else:
            print("Building serially")
            self._queue = queue.Queue()
            self.processed_items = []
        super(MultiprocProcessor, self).__init__(builders, num_workers)

    def process(self, builder_id):
        """
        Run the builder using the builtin multiprocessing.
        Adapted from pymatgen-db

        Args:
            builder_id (int): the index of the builder in the builders list
        """
        processes = []
        builder = self.builders[builder_id]

        # establish connection to the sources and targets
        builder.connect()

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
        builder.update_targets(self.processed_items)

        if all(self.status):
            self.builders[builder_id].finalize()

    def worker(self):
        """
        Call the builder's process_item method and put the processed item in the shared dict.
        """
        while True:
            try:
                packet = self._queue.get(timeout=2)
                builder_id, item = packet
                processed_item = self.builders[builder_id].process_item(item)
                self.processed_items.append(processed_item)
            except queue.Empty:
                break


class Runner(MSONable):

    def __init__(self, builders, num_workers=0, processor=None):
        """
        Initialize with a list of builders

        Args:
            builders(list): list of builders
            num_workers (int): number of processes. Used only for multiprocessing.
                Will be automatically set to (number of cpus - 1) if set to 0.
            processor(BaseProcessor): set this if custom processor is needed(must 
                subclass BaseProcessor though)
        """
        self.builders = builders
        default_processor = MPIProcessor(builders) if self.use_mpi else MultiprocProcessor(builders, num_workers)
        self.processor = default_processor if processor is None else processor
        self.dependency_graph = self._get_builder_dependency_graph()
        self.has_run = []  # for bookkeeping builder runs

    @property
    def use_mpi(self):
        try:
            (_, _, size) = get_mpi()
            use_mpi = True if size > 1 else False
        except ImportError:
            print("either 'mpi4py' is not installed or issue with the installation. "
                  "Proceeding with mulitprocessing.")
            use_mpi = False
        return use_mpi

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
        logger.info("building: ", builder_id)
        self.processor.process(builder_id)
