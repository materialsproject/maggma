#!/usr/bin/env python
# coding utf-8


import logging
import click
import asyncio
from itertools import chain
from monty.serialization import loadfn
from maggma.utils import TqdmLoggingHandler, ReportingHandler
from maggma.cli.serial import serial
from maggma.cli.multiprocessing import multi
from maggma.cli.distributed import master, worker


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
@click.option(
    "-r",
    "--reporting",
    "reporting_store",
    help="Store in JSON/YAML form to send reporting data to",
    type=click.Path(exists=True),
)
@click.option("-u", "--url", "url", default=None, type=str)
@click.option("-N", "--num-chunks", "num_chunks", default=0, type=int)
def run(builders, verbosity, reporting_store, num_workers, url, num_chunks):

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

    if reporting_store:
        reporting_store = loadfn(reporting_store)
        root.addHandler(ReportingHandler(reporting_store))

    if url:
        if num_chunks > 0:
            # Master
            asyncio.run(master(url, builders, num_chunks))
        else:
            # worker
            asyncio.run(worker(url, num_workers))
    else:
        if num_workers == 1:
            for builder in builders:
                serial(builder)
        else:
            for builder in builders:
                asyncio.run(multi(builder, num_workers))
