#!/usr/bin/env python
# coding utf-8

import json
from logging import getLogger
import socket as pysocket
from typing import List
import numpy as np
from time import perf_counter
import asyncio
from random import randint

from monty.json import jsanitize
from monty.serialization import MontyDecoder

from maggma.cli.multiprocessing import multi, MANAGER_TIMEOUT
from maggma.core import Builder
from maggma.utils import tqdm

import zmq
import zmq.asyncio as azmq

WORKER_TIMEOUT = 5400  # max timeout in seconds for a worker


def find_port():
    sock = pysocket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


def manager(
    url: str, port: int, builders: List[Builder], num_chunks: int, num_workers: int
):
    """
    Really simple manager for distributed processing that uses a builder prechunk to modify
    the builder and send out modified builders for each worker to run.

    The manager will try and keep track of workers, including which error out and which complete.
    Currently, if a single workers fails the entire distributed job will be stopped.
    """
    logger = getLogger("Manager")

    if not (num_chunks and num_workers):
        raise ValueError("Both num_chunks and num_workers must be non-zero")

    logger.info(f"Binding to Manager URL {url}:{port}")
    context = zmq.Context()
    context.setsockopt(opt=zmq.SocketOption.ROUTER_MANDATORY, value=1)
    context.setsockopt(opt=zmq.SNDHWM, value=0)
    context.setsockopt(opt=zmq.RCVHWM, value=0)
    socket = context.socket(zmq.ROUTER)
    socket.bind(f"{url}:{port}")

    poll = zmq.Poller()
    poll.register(socket, zmq.POLLIN)

    workers = {}  # type: ignore

    logger.debug("Manager started and looking for workers")

    for builder in builders:
        logger.info(f"Working on {builder.__class__.__name__}")
        builder_dict = builder.as_dict()

        try:
            builder.connect()
            chunk_dicts = [
                {"chunk": d, "distributed": False, "completed": False}
                for d in builder.prechunk(num_chunks)
            ]
            pbar_distributed = tqdm(
                total=len(chunk_dicts),
                desc="Distributed chunks for {}".format(builder.__class__.__name__),
            )

            pbar_completed = tqdm(
                total=len(chunk_dicts),
                desc="Completed chunks for {}".format(builder.__class__.__name__),
            )

            logger.info(f"Distributing {len(chunk_dicts)} chunks to workers")

        except NotImplementedError:
            attempt_graceful_shutdown(workers, socket)
            raise RuntimeError(
                f"Can't distribute process {builder.__class__.__name__} as no prechunk method exists."
            )

        completed = False

        while not completed:

            completed = all(chunk["completed"] for chunk in chunk_dicts)

            if num_workers <= 0:
                socket.close()
                raise RuntimeError("No workers to distribute chunks to")

            # Poll and look for messages from workers
            connections = dict(poll.poll(100))

            # If workers send messages decode and figure out what do
            if connections:
                identity, _, bmsg = socket.recv_multipart()

                msg = bmsg.decode("utf-8")

                if "READY" in msg:
                    if identity not in workers:
                        logger.debug(f"Got connection from worker: {msg.split('_')[1]}")
                        workers[identity] = {
                            "working": False,
                            "heartbeats": 1,
                            "last_ping": perf_counter(),
                            "work_index": -1,
                        }

                    else:
                        workers[identity]["working"] = False
                        work_ind = workers[identity]["work_index"]
                        if work_ind != -1:
                            chunk_dicts[work_ind]["completed"] = True  # type: ignore
                            pbar_completed.update(1)

                            # If everything is distributed, send EXIT to the worker
                            if all(chunk["distributed"] for chunk in chunk_dicts):
                                logger.debug(
                                    f"Sending exit signal to worker: {msg.split('_')[1]}"
                                )
                                socket.send_multipart([identity, b"", b"EXIT"])
                                workers.pop(identity)

                elif "ERROR" in msg:
                    # Remove worker and requeue work sent to it
                    attempt_graceful_shutdown(workers, socket)
                    raise RuntimeError(
                        "At least one worker has stopped with error message: {}".format(
                            msg.split("_")[1]
                        )
                    )

                elif msg == "PING":
                    # Respond to heartbeat
                    socket.send_multipart([identity, b"", b"PONG"])
                    workers[identity]["last_ping"] = perf_counter()
                    workers[identity]["heartbeats"] += 1

            # Decide if any workers are dead and need to be removed
            handle_dead_workers(workers, socket)

            for work_index, chunk_dict in enumerate(chunk_dicts):
                if not chunk_dict["distributed"]:

                    temp_builder_dict = dict(**builder_dict)
                    temp_builder_dict.update(chunk_dict["chunk"])  # type: ignore
                    temp_builder_dict = jsanitize(temp_builder_dict)

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
                            chunk_dicts[work_index]["distributed"] = True
                            pbar_distributed.update(1)

    # Send EXIT to any remaining workers
    logger.info("Sending exit messages to workers once they are done")
    attempt_graceful_shutdown(workers, socket)


def attempt_graceful_shutdown(workers, socket):
    for identity in workers:
        socket.send_multipart([identity, b"", b"EXIT"])
    socket.close()


def handle_dead_workers(workers, socket):
    if len(workers) == 1:
        # Use global timeout
        identity = list(workers.keys())[0]
        if (perf_counter() - workers[identity]["last_ping"]) >= WORKER_TIMEOUT:
            attempt_graceful_shutdown(workers, socket)
            raise RuntimeError("Worker has timed out. Stopping distributed build.")

    elif len(workers) == 2:
        # Use 10% ratio between workers
        workers_sorted = sorted(list(workers.items()), key=lambda x: x[1]["heartbeats"])

        ratio = workers_sorted[1][1]["heartbeats"] / workers_sorted[0][1]["heartbeats"]

        if ratio <= 0.1:
            attempt_graceful_shutdown(workers, socket)
            raise RuntimeError("One worker has timed out. Stopping distributed build.")

    elif len(workers) > 2:
        # Calculate modified z-score of heartbeat counts and see if any are <= -3.5
        hearbeat_vals = [w["heartbeats"] for w in workers.values()]
        median = np.median(hearbeat_vals)
        mad = np.median([abs(i - median) for i in hearbeat_vals])
        if mad > 0:
            for identity in list(workers.keys()):
                z_score = 0.6745 * (workers[identity]["heartbeats"] - median) / mad
                if z_score <= -3.5:
                    attempt_graceful_shutdown(workers, socket)
                    raise RuntimeError(
                        "At least one worker has timed out. Stopping distributed build."
                    )


async def worker(url: str, port: int, num_processes: int, no_bars: bool):
    """
    Simple distributed worker that connects to a manager asks for work and deploys
    using multiprocessing
    """
    identity = "%04X-%04X" % (randint(0, 0x10000), randint(0, 0x10000))
    logger = getLogger(f"Worker {identity}")

    logger.info(f"Connnecting to Manager at {url}:{port}")
    context = azmq.Context()
    socket = context.socket(zmq.REQ)

    socket.setsockopt_string(zmq.IDENTITY, identity)
    socket.connect(f"{url}:{port}")

    # Initial message package
    hostname = pysocket.gethostname()

    try:
        running = True
        while running:
            await socket.send("READY_{}".format(hostname).encode("utf-8"))
            try:
                bmessage: bytes = await asyncio.wait_for(socket.recv(), timeout=MANAGER_TIMEOUT)  # type: ignore
            except asyncio.TimeoutError:
                socket.close()
                raise RuntimeError("Stopping work as manager timed out.")

            message = bmessage.decode("utf-8")
            if "@class" in message and "@module" in message:
                # We have a valid builder
                work = json.loads(message)
                builder = MontyDecoder().process_decoded(work)
                await multi(builder, num_processes, socket=socket, no_bars=no_bars)
            elif message == "EXIT":
                # End the worker
                running = False

    except Exception as e:
        logger.error(f"A worker failed with error: {e}")
        await socket.send("ERROR_{}".format(e).encode("utf-8"))
        socket.close()

    socket.close()
