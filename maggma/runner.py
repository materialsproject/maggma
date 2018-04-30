# coding: utf-8
"""
Module defining objects to run builders in various modes
including serial processing, multiprocessing on a single computer,
and processing via MPI
"""

import abc
import logging
import types
from collections import defaultdict, deque
from threading import Thread, Condition, BoundedSemaphore
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from monty.json import MSONable
from maggma.utils import get_mpi, grouper
from tqdm import tqdm


class BaseProcessor(MSONable, metaclass=abc.ABCMeta):
    """
    Base processor class for multiprocessing paradigms
    """

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
            processed_items = [builder.process_item(item) for item in chunk if item is not None]
            builder.update_targets(processed_items)


class MPIProcessor(BaseProcessor):
    """
    Processor to distribute work using MPI
    """

    def __init__(self, builders):
        (self.comm, self.rank, self.size) = get_mpi()
        if not self.comm:
            raise Exception(
                "MPI not working properly, check your mpi4py installation and ensure this is running under mpi")
        self.comm.barrier()
        super(MPIProcessor, self).__init__(builders)

    def process(self, builder_id):
        """
        Run the builder using MPI protocol.

        Args:
            builder_id (int): the index of the builder in the builders list
        """
        self.comm.barrier()
        if self.rank == 0:
            self.process_master(builder_id)
        else:
            self.process_worker()

    def setup_multithreading(self):
        """
        Setup structures for managing data to/from MPI Workers
        """
        self.data = deque()
        self.ranks = deque([i + 1 for i in range(self.size - 1)])
        self.task_count = BoundedSemaphore(self.builder.chunk_size)
        self.update_data_condition = Condition()

        self.run_update_targets = True
        self.update_targets_thread = Thread(target=self.update_targets)
        self.update_targets_thread.start()

    def process_master(self, builder_id):
        """
        Master process for MPI processing
        Handles Data IO to Stores and to MPI Workers
        """
        self.builder = self.builders[builder_id]
        self.builder.connect()

        cursor = self.builder.get_items()

        self.setup_pbars(cursor)
        self.setup_multithreading()
        self.put_tasks(builder_id)
        self.clean_up_workers()
        self.clean_up_data()
        self.builder.finalize(cursor)
        self.cleanup_pbars()

    def process_worker(self):
        """
        MPI Worker process
        """
        is_valid = True

        while is_valid:
            packet = self.comm.recv(source=0)
            if packet["type"] == "process":
                builder_id = packet["builder_id"]
                data = packet["data"]
                try:
                    result = self.builders[builder_id].process_item(data)
                    self.comm.send({"type": "return", "return": result}, dest=0)
                except e:
                    self.comm.send({"type": "error", "error": e})
            elif packet["type"] == "shutdown":
                is_valid = False

    def setup_pbars(self, cursor):
        """
        Sets up progress bars
        """
        total = None
        if hasattr(cursor, "__len__"):
            total = len(cursor)
        elif hasattr(cursor, "count"):
            total = cursor.count()
        elif hasattr(self.builder, "total"):
            total = self.builder.total

        self.get_pbar = tqdm(cursor, desc="Get Items", total=total)
        self.process_pbar = tqdm(desc="Processing Item", total=total)
        self.update_pbar = tqdm(desc="Updating Targets", total=total)

    def cleanup_pbars(self):
        """
        Cleans up the TQDM bars
        """
        self.get_pbar.close()
        self.process_pbar.close()
        self.update_pbar.close()

    def put_tasks(self, builder_id):
        """
        Submit tasks from cursor to MPI workers
        """
        # 1.) Setup thread pool
        with ThreadPoolExecutor(max_workers=self.size - 1) as executor:
            # 2.) Loop over every item wrapped in a tqdm bar
            for item in self.get_pbar:
                # 3.) Limit total number of queued tasks using a semaphore
                self.task_count.acquire()
                # 4.) Submit the item to a worker
                f = executor.submit(self.submit_item, builder_id, item)

    def submit_item(self, builder_id, data):
        """
        Thread to submit an item to MPI Workers and get data back

        """

        # 1.) Find free rank and take it
        mpi_rank = self.ranks.pop()
        # 2.) Submit the job to that rank
        self.comm.send({"type": "process", "builder_id": builder_id, "data": data}, dest=mpi_rank)
        # 3.) Periodically poll for data back
        result = None
        while not result:
            packet = self.comm.recv(source=mpi_rank)
            if packet["type"] == "return":
                result = packet["return"]
                self.task_count.release()
            elif packet["type"] == "error":
                self.logger.error("MPI Rank {} Errored on Builder ID {}:\n{}".format(
                    mpi_rank, builder_id, packet["error"]))
                self.task_count.release()
                return
            else:
                self.task_count.release()
                return  # don't know what happened here, just quit

        # 6.) Update process progress bar
        self.process_pbar.update(1)

        # 7.) Save data
        with self.update_data_condition:
            self.data.append(result)
            self.update_data_condition.notify_all()
        # 8.) Return rank
        self.ranks.append(mpi_rank)

    def clean_up_workers(self):
        """
        Sends shutdown signal to all MPI workers
        """
        for i in range(self.size - 1):
            self.comm.send({"type": "shutdown"}, dest=i + 1)

    def clean_up_data(self):
        """
        Call back to add data into a list in thread safe manner and signal other threads to add more tasks or update_targets
        """
        self.logger.debug("Cleaning up data queue")
        try:
            with self.update_data_condition:
                self.run_update_targets = False
                self.update_data_condition.notify_all()
        except Exception as e:
            self.logger.debug("Problem in updating targets at end of builder run: {}".format(e))

        self.update_targets_thread.join()

    def update_targets(self):
        """
        Thread to update targets periodically
        """
        while self.run_update_targets:
            with self.update_data_condition:
                self.update_data_condition.wait_for(
                    lambda: not self.run_update_targets or len(self.data) > self.builder.chunk_size)
                try:
                    self.builder.update_targets(self.data)
                    self.update_pbar.update(len(self.data))
                    self.data.clear()
                except Exception as e:
                    self.logger.debug("Problem in updating targets in builder run: {}".format(e))


class MultiprocProcessor(BaseProcessor):
    """
    Processor to run builders using python multiprocessing
    """

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

        cursor = self.builder.get_items()

        self.setup_pbars(cursor)

        self.setup_multithreading()
        self.put_tasks()
        self.clean_up_data()
        self.builder.finalize(cursor)
        self.cleanup_pbars()

    def setup_pbars(self, cursor):
        """
        Sets up progress bars
        """
        total = None
        if hasattr(cursor, "__len__"):
            total = len(cursor)
        elif hasattr(cursor, "count"):
            total = cursor.count()
        elif isinstance(cursor,types.GeneratorType):
            cursor.send(None)
            if hasattr(self.builder, "total"):
                total = self.builder.total

        self.get_pbar = tqdm(cursor, desc="Get Items", total=total)
        self.process_pbar = tqdm(desc="Processing Item", total=total)
        self.update_pbar = tqdm(desc="Updating Targets", total=total)

    def cleanup_pbars(self):
        """
        Cleans up the TQDM bars
        """
        self.get_pbar.close()
        self.process_pbar.close()
        self.update_pbar.close()

    def setup_multithreading(self):
        """
        Sets up objects necessary to store and synchronize data in multiprocessing
        """
        self.data = deque()
        self.task_count = BoundedSemaphore(self.builder.chunk_size)
        self.update_data_condition = Condition()

        self.run_update_targets = True
        self.update_targets_thread = Thread(target=self.update_targets)
        self.update_targets_thread.start()

    def put_tasks(self):
        """
        Processes all items from builder using a pool of processes
        """
        # 1.) setup a process pool
        with ProcessPoolExecutor(self.num_workers) as executor:
            # 2.) Loop over every item wrapped in a tqdm bar
            for item in self.get_pbar:
                # 3.) Limit total number of queues tasks using a semaphore
                self.task_count.acquire()
                # 4.) Submit a task to processing pool
                f = executor.submit(self.builder.process_item, item)
                # 5.) Add call back to update our data list
                f.add_done_callback(self.update_data_callback)

    def clean_up_data(self):
        """
        Updates targets with remaining data and then cleans up the data collection
        """
        try:
            with self.update_data_condition:
                self.run_update_targets = False
                self.update_data_condition.notify_all()
        except Exception as e:
            self.logger.debug("Problem in updating targets at end of builder run: {}".format(e))

        self.update_targets_thread.join()

    def update_data_callback(self, future):
        """
        Call back to add data into a list in thread safe manner and signal other threads to add more tasks or update_targets
        """
        with self.update_data_condition:
            self.process_pbar.update(1)
            self.data.append(future.result())
            self.update_data_condition.notify_all()

        self.task_count.release()

    def update_targets(self):
        """
        Thread to update targets periodically
        """

        while self.run_update_targets:
            with self.update_data_condition:
                self.update_data_condition.wait_for(
                    lambda: not self.run_update_targets or len(self.data) > self.builder.chunk_size)
                try:
                    if self.data is not None:
                        self.update_pbar.unpause()
                        self.builder.update_targets(self.data)
                        self.update_pbar.update(len(self.data))
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
