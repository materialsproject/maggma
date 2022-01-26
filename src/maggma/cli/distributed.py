#!/usr/bin/env python
# coding utf-8

import json
from logging import getLogger
import socket as pysocket
from typing import List

from monty.json import jsanitize
from monty.serialization import MontyDecoder

from maggma.cli.multiprocessing import multi
from maggma.core import Builder
from maggma.utils import tqdm

from zmq import REP, REQ
import zmq.asyncio as zmq


def find_port():
    sock = pysocket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


async def manager(
    url: str, port: int, builders: List[Builder], num_chunks: int, num_workers: int
):
    """
    Really simple manager for distributed processing that uses a builder prechunk to modify
    the builder and send out modified builders for each worker to run
    """
    logger = getLogger("Manager")

    if not (num_chunks and num_workers):
        raise ValueError("Both num_chunks and num_workers must be non-zero")

    logger.info(f"Binding to Manager URL {url}:{port}")
    context = zmq.Context()
    socket = context.socket(REP)
    socket.bind(f"{url}:{port}")

    for builder in builders:
        logger.info(f"Working on {builder.__class__.__name__}")
        builder_dict = builder.as_dict()

        try:

            builder.connect()
            chunks_tuples = [(d, False) for d in builder.prechunk(num_chunks)]

            logger.info(f"Distributing {len(chunks_tuples)} chunks to workers")

            for chunk_dict, distributed in tqdm(chunks_tuples, desc="Chunks"):
                while not distributed:
                    if num_workers <= 0:
                        socket.close()
                        raise RuntimeError("No workers left to distribute chunks to")

                    temp_builder_dict = dict(**builder_dict)
                    temp_builder_dict.update(chunk_dict)
                    temp_builder_dict = jsanitize(temp_builder_dict)

                    # Wait for client connection that announces client and says it is ready to do work
                    logger.debug("Waiting for a worker")

                    worker = await socket.recv()

                    if worker.decode("utf-8") == "ERROR":
                        num_workers -= 1
                    else:
                        logger.debug(
                            f"Got connection from worker: {worker.decode('utf-8')}"
                        )
                        # Send out the next chunk
                        await socket.send(json.dumps(temp_builder_dict).encode("utf-8"))
                        distributed = True

            logger.info("Sending exit messages to workers")
            for _ in range(num_workers):
                await socket.recv()
                await socket.send_json("EXIT")

        except NotImplementedError:
            logger.error(
                f"Can't distributed process {builder.__class__.__name__}. Skipping for now"
            )

    socket.close()


async def worker(url: str, port: int, num_processes: int):
    """
    Simple distributed worker that connects to a manager asks for work and deploys
    using multiprocessing
    """
    # Should this have some sort of unique ID?
    logger = getLogger("Worker")

    logger.info(f"Connnecting to Manager at {url}:{port}")
    context = zmq.Context()
    socket = context.socket(REQ)
    socket.connect(f"{url}:{port}")

    # Initial message package
    hostname = pysocket.gethostname()

    try:
        running = True
        while running:
            await socket.send(hostname.encode("utf-8"))
            message = await socket.recv()
            work = json.loads(message.decode("utf-8"))
            if "@class" in work and "@module" in work:
                # We have a valid builder
                builder = MontyDecoder().process_decoded(work)
                await multi(builder, num_processes)
            elif work == "EXIT":
                # End the worker
                running = False

    except Exception as e:
        logger.error(f"A worker failed with error: {e}")
        await socket.send("ERROR".encode("utf-8"))

        socket.close()

    socket.close()
