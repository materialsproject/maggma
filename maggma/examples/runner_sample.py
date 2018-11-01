"""
 Example Usage:
 with serial processing:
    python runner_sample.py
 with multiprocessing (use max cores available):
    python runner_sample.py -n 0
 with multiprocessing (use up to 3 cores):
    python runner_sample.py -n 3
 with mpi(need mpi4py package) size 3:
    mpiexec -n 3 python runner_sample.py --mpi
"""

import argparse
import logging

from maggma.stores import MemoryStore
from maggma.builders import Builder
from maggma.runner import Runner

__author__ = "Kiran Mathew, Donny Winston"


class MyDumbBuilder(Builder):
    """This builder builds."""
    def __init__(self, N, sources, targets, chunk_size=1):
        super().__init__(sources, targets, chunk_size)
        self.N = N

    def get_items(self):
        for i in range(self.N):
            yield i

    def process_item(self, item):
        self.logger.info("processing item: {}".format(item))
        return {item: "processed"}

    def update_targets(self, items):
        self.logger.info("Updating targets ...")
        self.logger.info("Received {} processed items".format(len(items)))
        self.logger.info("Updated items: {}".format(list(items)))

    def finalize(self, cursor=None):
        self.logger.info("Finalizing ...")
        self.logger.info("DONE!")


def logstreamhandle(runner, level=logging.INFO, stream=None):
    """
    Log output of runner and its processors and builders to stream at level.

    Defaults: output to sys.stderr at INFO level.

    Args:
        runner (Runner): the runner.
        level (int): logging level. DEBUG, INFO, WARNING, ERROR, or CRITICAL.
        stream: any stream (sys.stdout, sys.stderr, etc.) or file-like object.
    """
    loggers = [runner.logger, runner.processor.logger]
    loggers.extend(b.logger for b in runner.builders)
    for l in loggers:
        l.setLevel(level)
        ch = logging.StreamHandler(stream=stream)
        ch.setLevel(level)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        l.addHandler(ch)


if __name__ == '__main__':
    N = 10
    chunk_size = 3
    stores = [MemoryStore(str(i)) for i in range(7)]

    sources = [stores[0], stores[1], stores[3]]
    targets = [stores[3], stores[6]]

    mdb = MyDumbBuilder(N, sources, targets, chunk_size=chunk_size)

    builders = [mdb]

    parser = argparse.ArgumentParser(description='Run a sample runner.')
    parser.add_argument('--nworkers', '-n', type=int, default=1,
                        help='number of workers (0 for max available)')
    parser.add_argument('--mpi', dest='mpi', action='store_true')
    parser.add_argument('--no-mpi', dest='mpi', action='store_false')
    parser.set_defaults(mpi=False)

    args = parser.parse_args()
    runner = Runner(builders, max_workers=args.nworkers, mpi=args.mpi)

    logstreamhandle(runner)
    runner.run()
