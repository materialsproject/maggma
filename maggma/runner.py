import logging
import multiprocessing
from collections import defaultdict
from itertools import cycle
import abc

from multiprocessing import Pool
from monty.json import MSONable
from maggma.helpers import get_mpi
from maggma.utils import grouper, reload_msonable_object


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
    def __init__(self, builders, num_workers=None):
        # multiprocessing only if mpi is not used, no mixing
        self.num_workers = num_workers
        super(MultiprocProcessor, self).__init__(builders)
        self.logger.info("Building with multiprocessing, {} workers in the pool".format(self.num_workers))

    def process(self, builder_id):
        """
        Run the builder using the builtin multiprocessing.

        Args:
            builder_id (int): the index of the builder in the builders list
        """
        self.builder = self.builders[builder_id]
        self.builder.connect()

        processing_builder = reload_msonable_object(self.builder)
        cursor = self.builder.get_items()

        self.setup_multithreading()
        self.put_tasks(cursor, processing_builder)
        self.clean_up_data()
        self.builder.finalize(cursor)

    def setup_multithreading(self):
        """
        Sets up objects necessary to store and synchronize data in multiprocessing
        """
        self.data = deque()
        self.task_count = BoundedSemaphore(self.builder.chunk_size)
        self.update_data_condition = Condition()

        self.update_targets_thread = Thread(target=self.update_targets)
        self.update_targets_thread.start()

    def put_tasks(self, cursor, processing_builder):
        """
        Processes all items from builder using a pool of processes
        """
        #1.) setup a process pool
        with concurrent.futures.ProcessPoolExecutor(self.num_workers) as executor:
            # 2.) Ensure we can get data
            while cursor:
                # 3.) Limit total number of queues tasks using a semaphore
                self.task_count.acquire()
                try:
                    # 4.) Submit a task to processing pool
                    f = executor.submit(processing_builder.process_item, next(cursor))
                    # 5.) Add call back to update our data list
                    f.add_done_callback(self.update_data_callback)
                except StopIteration as e:
                    # 6.) No more data so stop itterating
                    cursor = None

    def clean_up_data(self):
        """
        Updates targets with remaining data and then cleans up the data collection
        """
        try:
            # 1.)
            with self.update_data_condition:
                self.builder.update_targets(self.data)
                self.data.clear()
                self.data = None
                self.update_data_condition.notify_all()
        except Exception as e:
            self.logger.debug("Problem in updating targets at end of builder run: ", e)

        self.update_targets_thread.join()

    def update_data_callback(self, future):
        """
        Call back to add data into a list in thread safe manner and signal other threads to add more tasks or update_targets
        """

        with self.update_data_condition:
            self.data.append(future.result())
            self.update_data_condition.notify_all()

        self.task_count.release()

    def update_targets(self):
        """
        Thread to update targets periodically
        """
        while self.data:
            with self.update_data_condition:
                self.update_data_condition.wait_for(lambda: len(self.data) > self.builder.chunk_size)
                try:
                    self.builder.update_targets(data)
                    self.data.clear()
                except Exception as e:
                    self.logger.debug("Problem in updating targets in builder run: {}".format(e))


class Runner(MSONable):
    def __init__(self, builders, num_workers=None):
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
        (_, mpi_rank, mpi_size) = get_mpi()
        if mpi_size > 1:
            self.logger.info("Running with MPI Rank: {}".format(mpi_rank))
            self.processor = MPIProcessor(builders)
        else:
            self.logger.info("Running with Multiprocessing")
            self.processor = MultiprocProcessor(builders, num_workers)
        self.dependency_graph = self._get_builder_dependency_graph()
        self.has_run = []  # for bookkeeping builder runs

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
        self.logger.debug("Building: {}".format(builder_id))
        self.processor.process(builder_id)
