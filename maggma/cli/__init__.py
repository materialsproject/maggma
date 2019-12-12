#!/usr/bin/env python
# coding utf-8


import logging
import click
import asyncio
from itertools import chain
from monty.serialization import loadfn
from maggma.utils import TqdmLoggingHandler
from maggma.cli.serial import serial
from maggma.cli.multiprocessing import multi


""""
mrun script1
mrun script1 script2 script3
mrun -n  32 script1 script2





mrun master -N 4 sciprt1 script2 <-- have to deploy workers
mrun worker -n 32 127.0.0.1:70001
mrun worker -n 32 127.0.0.1:70001
mrun worker -n 32 127.0.0.1:70001


mrun master -N 4 script1 script 2
mpirun -N 4 mrun worker -n 32 script1 script 2



"""


@click.command()
@click.argument("builders", nargs=-1, type=click.Path(exists=True))
@click.option(
    "-v",
    "--verbose",
    "verbosity",
    count=True,
    help="Controls logging level per number of v's",
    default=0,
)
@click.option(
    "-n",
    "--num-workers",
    "num_workers",
    help="Number of worker processes. Defaults to single processing",
    default=1,
    type=click.IntRange(1),
)
def run(builders, verbosity, num_workers):

    # Set Logging
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, verbosity)]  # capped to number of levels
    root = logging.getLogger()
    root.setLevel(level)
    ch = TqdmLoggingHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    root.addHandler(ch)

    builders = [loadfn(b) for b in builders]
    builders = [b if isinstance(b, list) else [b] for b in builders]
    builders = list(chain.from_iterable(builders))

    if num_workers == 1:
        for builder in builders:
            serial(builder)
    else:
        for builder in builders:
            asyncio.run(multi(builder, num_workers))
