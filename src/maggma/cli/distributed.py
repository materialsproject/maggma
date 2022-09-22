#!/usr/bin/env python
# coding utf-8

import json
from logging import getLogger
import socket as pysocket
from tkinter import W
from typing import List
import numpy as np
from time import perf_counter

from monty.json import jsanitize
from monty.serialization import MontyDecoder

from maggma.cli.multiprocessing import multi
from maggma.core import Builder
from maggma.utils import tqdm

import zmq
import zmq.asyncio as azmq

TIMEOUT = 1200  # max timeout in seconds for a worker


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
    socket = context.socket(zmq.ROUTER)
    socket.bind(f"{url}:{port}")

    poll = zmq.Poller()
    poll.register(socket, zmq.POLLIN)

    workers = {}
    workers_seen = 0
    num_errors = 0

    for builder in builders:
        logger.info(f"Working on {builder.__class__.__name__}")
        builder_dict = builder.as_dict()

        try:

            builder.connect()
            chunks_tuples = [[d, False] for d in builder.prechunk(num_chunks)]
            logger.info(f"Distributing {len(chunks_tuples)} chunks to workers")

            for work_index, (chunk_dict, distributed) in tqdm(
                enumerate(chunks_tuples), desc="Chunks", total=num_chunks
            ):
                temp_builder_dict = dict(**builder_dict)
                temp_builder_dict.update(chunk_dict)
                temp_builder_dict = jsanitize(temp_builder_dict)

                while not distributed:

                    if num_workers <= 0:
                        socket.close()
                        raise RuntimeError("No workers left to distribute chunks to")

                    if num_errors / workers_seen > 0.5:
                        socket.close()
                        raise RuntimeError(
                            "More than half of the chunks sent to workers have failed. Stopping distributed build."
                        )

                    # Poll and look for messages from workers
                    logger.debug("Manager started and looking for workers")

                    connections = dict(poll.poll(1000))

                    # If workers send messages decode and figure out what do
                    if connections:
                        identity, _, msg = socket.recv_multipart()

                        msg = msg.decode("utf-8")
                        print(msg)

                        if "READY" in msg:
                            if identity not in workers:
                                logger.debug(
                                    f"Got connection from worker: {msg.split('_')[1]}"
                                )
                                workers[identity] = {
                                    "working": False,
                                    "heartbeats": 1,
                                    "last_ping": perf_counter(),
                                    "work_index": -1,
                                }

                                workers_seen += 1

                            else:
                                workers[identity]["working"] = False
                        elif msg == "ERROR":
                            # Remove worker and requeue work sent to it
                            chunks_tuples[workers[identity]["work_index"]][1] = False  # type: ignore
                            workers.pop(identity)
                            num_errors += 1

                        elif msg == "PING":
                            # Respond to heartbeat
                            socket.send_multipart([identity, b"", b"PONG"])
                            workers[identity]["last_ping"] = perf_counter()
                            workers[identity]["heartbeats"] += 1

                    # Decide if any workers are dead and need to be removed
                    handle_dead_workers(workers, chunks_tuples, num_errors)

                    # Send work for available workers
                    for identity in workers:
                        if not workers[identity]["working"]:

                            # Send out a chunk to idle worker
                            socket.send_multipart(
                                [
                                    identity,
                                    b"",
                                    json.dumps(temp_builder_dict).encode("utf-8"),
                                ]
                            )

                            workers[identity]["work_index"] = work_index
                            workers[identity]["working"] = True

                            distributed = True

            logger.info("Sending exit messages to workers")
            for identity in workers:
                socket.send_multipart([identity, b"", b"EXIT"])

        except NotImplementedError:
            logger.error(
                f"Can't distributed process {builder.__class__.__name__}. Skipping for now"
            )

    socket.close()


def handle_dead_workers(workers, chunks_tuples, num_errors):
    if len(workers) == 1:
        # Use global timeout
        identity = list(workers.keys())[0]
        if (perf_counter() - workers[identity]["last_ping"]) >= TIMEOUT:
            chunks_tuples[workers[identity]["work_index"]][1] = False  # type: ignore
            workers.pop(identity)
            num_errors += 1

    elif len(workers) == 2:
        # Use 10% ratio between workers
        workers_sorted = sorted(list(workers.items()), key=lambda x: x[1]["heartbeats"])
        print(workers_sorted)

        ratio = workers_sorted[1][1]["heartbeats"] / workers_sorted[0][1]["heartbeats"]

        if ratio <= 0.1:
            chunks_tuples[workers[workers_sorted[0][0]]["work_index"]][  # type: ignore
                1
            ] = False
            workers.pop(identity)
            num_errors += 1

    elif len(workers) > 2:
        # Calculate modified z-score of heartbeat counts and remove those <= -3.5
        # Re-queue work sent to dead worker
        hearbeat_vals = [w["heartbeats"] for w in workers.values()]
        median = np.median(hearbeat_vals)
        mad = np.median([abs(i - median) for i in hearbeat_vals])
        for identity in list(workers.keys()):
            z_score = 0.6745 * (workers[identity]["heartbeat"] - median) / mad
            if z_score <= -3.5:
                # Remove worker and requeue work sent to it
                chunks_tuples[workers[identity]["work_index"]][  # type: ignore
                    1
                ] = False
                workers.pop(identity)
                num_errors += 1


async def worker(url: str, port: int, num_processes: int):
    """
    Simple distributed worker that connects to a manager asks for work and deploys
    using multiprocessing
    """
    # Should this have some sort of unique ID?
    logger = getLogger("Worker")

    logger.info(f"Connnecting to Manager at {url}:{port}")
    context = azmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"{url}:{port}")

    # Initial message package
    hostname = pysocket.gethostname()

    try:
        running = True
        while running:
            await socket.send("READY_{}".format(hostname).encode("utf-8"))
            message = await socket.recv()
            work = json.loads(message.decode("utf-8"))
            if "@class" in work and "@module" in work:
                # We have a valid builder
                builder = MontyDecoder().process_decoded(work)
                await multi(builder, num_processes, socket=socket)
            elif work == "EXIT":
                # End the worker
                running = False

    except Exception as e:
        logger.error(f"A worker failed with error: {e}")
        await socket.send("ERROR".encode("utf-8"))
        socket.close()

    socket.close()
