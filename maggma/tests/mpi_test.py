# coding: utf-8
"""
MPI Tests for MPI Processor
"""
import sys
import logging
import numpy as np
from maggma.builder import Builder
from maggma.stores import MemoryStore
from maggma.runner import MPIProcessor


class DummyBuilder(Builder):

    def __init__(self, temp_storage_store):
        self.temp_storage_store = temp_storage_store
        super(DummyBuilder, self).__init__(sources=[], targets=[temp_storage_store],chunk_size=100)

    def get_items(self):
        self.logger.info("Getting Items")
        for i in range(1000):
            yield {"val": i, "task_id": i}

    def process_item(self, item):
        if item["val"] % 10 == 0:
            self.logger.debug("Processing: {}".format(item["val"]))
        proc_val = np.sqrt(np.square(float(item["val"])))
        item["proc_val"] = proc_val
        return item

    def update_targets(self, items):
        self.logger.info("Updating {} items".format(len(items)))
        self.temp_storage_store.update(items)

if __name__ == "__main__":

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    root.addHandler(ch)

    mem = MemoryStore("processed")
    bldr = DummyBuilder(mem)

    mpi_proc = MPIProcessor([bldr])
    mpi_proc.process(0)
