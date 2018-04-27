"""
 Usage:
 with multiprocessing:
    python runner_sample.py
 with mpi(need mpi4py pacakge):
    mpiexec -n 5 python runner_sample.py
"""

from maggma.stores import MemoryStore
from maggma.builder import Builder
from maggma.runner import Runner
from maggma.lava.util import logstreamhandle

__author__ = "Kiran Mathew"


class MyDumbBuilder(Builder):
    def __init__(self, N, sources, targets, chunk_size=1):
        super(MyDumbBuilder, self).__init__(sources, targets, chunk_size)
        self.N = N

    def get_items(self):
        for i in range(self.N):
            yield i

    def process_item(self, item):
        self.logger.info("processing item: {}".format(item))
        # time.sleep(random.randint(0,3))
        return {item: "processed"}

    def update_targets(self, items):
        self.logger.info("Updating targets ...")
        self.logger.info("Received {} processed items".format(len(items)))
        self.logger.info("Updated items: {}".format(list(items)))

    def finalize(self, cursor=None):
        self.logger.info("Finalizing ...")
        self.logger.info("DONE!")


if __name__ == '__main__':
    N = 10
    chunk_size = 3
    stores = [MemoryStore(str(i)) for i in range(7)]

    sources = [stores[0], stores[1], stores[3]]
    targets = [stores[3], stores[6]]

    mdb = MyDumbBuilder(N, sources, targets, chunk_size=chunk_size)

    builders = [mdb]

    runner = Runner(builders)

    logstreamhandle(runner)
    runner.run()
