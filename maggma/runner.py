import logging
import multiprocessing
import queue
from collections import defaultdict
from itertools import cycle
import abc
import traceback


from monty.json import MSONable

from maggma.helpers import get_mpi
from maggma.utils import grouper


class BaseProcessor(MSONable, metaclass=abc.ABCMeta):

    def __init__(self, builders):
        """
        Initialize with a list of builders

        Args:
            builders(list): list of builders
        """
        self.builders = builders

        self.logger = logging.getLogger(type(self).__name__)
        self.logger.addHandler(logging.NullHandler())

    @abc.abstractmethod
    def process(self, builder_id):
        """
        Does the processing. e.g. send work to workers(in MPI) or start the processes in
        multiprocessing.

        Args:
            builder_id (int): process the builder_id th builder i.e
                process_item --> update_targets --> finalize
        """
        pass


class SerialProcessor(BaseProcessor):
    """
    Simple serial processor. Usefull for debugging or example code
    """

    def process(self, builder_id):
        """
        Run the builder serially

        Args:
            builder_id (int): the index of the builder in the builders list
        """
        builder = self.builders[builder_id]
        chunk_size = builder.chunk_size

        # establish connection to the sources and targets
        builder.connect()

        cursor = builder.get_items()

        for chunk in grouper(cursor, chunk_size):
            self.logger.info("Processing batch of {} items".format(chunk_size))
            processed_items = [builder.process_item(
                item) for item in filter(None, chunk)]
            builder.update_targets(processed_items)


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
        self.logger.info(
            "Building with MPI. {} workers in the pool.".format(self.size - 1))

        builder = self.builders[builder_id]
        chunk_size = builder.chunk_size

        # establish connection to the sources and targets
        builder.connect()

        # cycle through the workers, there could be less workers than the items
        # to process
        worker_id = cycle(range(1, self.size))

        n = 0
        workers = []
        # distribute the items to process (in chunks of size chunk_size)
        cursor = builder.get_items()
        for item in cursor:
            if n % chunk_size == 0:
                self.logger.info(
                    "processing chunks of size {}".format(chunk_size))
                processed_chunk = self._process_chunk(chunk_size, workers)
                builder.update_targets(processed_chunk)
            packet = (builder_id, item)
            wid = next(worker_id)
            workers.append(wid)
            self.comm.send(packet, dest=wid)
            n += 1

        # in case the total number of items is not divisible by chunk_size,
        # process the leftovers.
        if workers:
            processed_chunk = self._process_chunk(chunk_size, workers)
            builder.update_targets(processed_chunk)

        # kill workers
        for _ in range(self.size - 1):
            self.comm.send(None, dest=next(worker_id))

        # finalize
        builder.finalize(cursor)

    def _process_chunk(self, chunk_size, workers):
        """
        process chunk_size items.

        Args:
            chunk_size (int):
            workers (list): lis tpf worker ids

        Returns:
            list : list of processed items
        """
        status = []
        processed_chunk = []
        self.logger.info("{} items sent for processing".format(chunk_size))

        # get processed item from the workers
        while workers:
            try:
                processed_item = self.comm.recv()
                status.append(True)
                processed_chunk.append(processed_item)
            except:
                raise
            workers.pop()

        if status:
            if not all(status):
                raise RuntimeError("processing failed")

        return processed_chunk

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
        self.num_workers = (num_workers if num_workers > 0
                            else multiprocessing.cpu_count() - 1)
        super(MultiprocProcessor, self).__init__(builders)
        self.logger.info("Building with multiprocessing, {} workers in the pool"
                         .format(self.num_workers))

    def process(self, builder_id):
        """
        Run the builder using the builtin multiprocessing.
        Adapted from pymatgen-db

        Args:
            builder_id (int): the index of the builder in the builders list
        """
        builder = self.builders[builder_id]
        chunk_size = builder.chunk_size
        # Need <=len(self.builders) queues, etc. iff want Runner to run
        # builders in parallel. Holding off for now for simplicity.
        manager = multiprocessing.Manager()
        self._queue = manager.Queue(chunk_size + 1)
        self.processed_items = manager.list()

        # establish connection to the sources and targets
        builder.connect()

        processes = self._start_worker_processes()
        # send items to process
        cursor = builder.get_items()
        for n, item in enumerate(cursor):
            if n == 0:
                self.logger.info(
                    "Waiting for {} processed items before updating targets"
                    .format(chunk_size))
            if len(self.processed_items) >= chunk_size:
                builder.update_targets(self.processed_items[:chunk_size])
                del self.processed_items[:chunk_size]
                self.logger.info(
                    "Waiting for {} processed items before updating targets"
                    .format(chunk_size))
            packet = (builder_id, item)
            self._queue.put(packet)  # blocks when queue is full

        for _ in range(self.num_workers):
            self._queue.put(None)

        # handle the leftovers
        status = []
        for p in processes:
            p.join()
            status.append(not bool(p.exitcode))
        while len(self.processed_items):
            builder.update_targets(self.processed_items[:chunk_size])
            del self.processed_items[:chunk_size]
            self.logger.info(
                "Waiting for {} processed items before updating targets"
                .format(chunk_size))

        # finalize
        if not all(status):
            self.logger.error("Some worker processes exited abnormally.")
        builder.finalize(cursor)

    def _start_worker_processes(self):
        """
        Start worker pool for processing items.
        """
        processes = []

        # start the workers
        for i in range(self.num_workers):
            proc = multiprocessing.Process(target=self.worker)
            proc.start()
            processes.append(proc)

        return processes

    def worker(self):
        """
        Call the builder's process_item method and put the processed item in the shared dict.
        """
        while True:
            try:
                packet = self._queue.get()
                if packet is None:
                    break
                builder_id, item = packet
                try:
                    processed_item = self.builders[builder_id].process_item(item)
                    self.processed_items.append(processed_item)
                except Exception:
                    self.logger.info("Caught exception while building: {}".format(traceback.format_exc()))

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
        self.num_workers = num_workers
        self.logger = logging.getLogger(type(self).__name__)
        self.logger.addHandler(logging.NullHandler())
        default_processor = MPIProcessor(
            builders) if self.use_mpi else MultiprocProcessor(builders, num_workers)
        self.processor = default_processor if processor is None else processor
        self.dependency_graph = self._get_builder_dependency_graph()
        self.has_run = []  # for bookkeeping builder runs

    @property
    def use_mpi(self):
        try:
            (_, _, size) = get_mpi()
            use_mpi = True if size > 1 else False
        except ImportError:
            self.logger.warning("either 'mpi4py' is not installed or issue with the installation. "
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
        self.logger.info("building: {}".format(builder_id))
        self.processor.process(builder_id)
