#!/usr/bin/env python
# coding utf-8

import asyncio
import json
import socket as pysocket
from logging import getLogger
from random import randint
from time import perf_counter

import numpy as np
import zmq
from monty.json import jsanitize
from monty.serialization import MontyDecoder

from maggma.cli.multiprocessing import multi
from maggma.cli.settings import CLISettings
from maggma.core import Builder
from maggma.utils import tqdm

settings = CLISettings()


def find_port():
    sock = pysocket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


def manager(url: str, port: int, builders: list[Builder], num_chunks: int, num_workers: int):
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

    # Setup socket and polling
    socket, poll = setup(url, port)

    workers = {}  # type: ignore

    logger.debug("Manager started and looking for workers")

    for builder in builders:
        logger.info(f"Working on {builder.__class__.__name__}")
        builder_dict = builder.as_dict()

        try:
            builder.connect()
            chunk_dicts = [{"chunk": d, "distributed": False, "completed": False} for d in builder.prechunk(num_chunks)]
            pbar_distributed = tqdm(
                total=len(chunk_dicts),
                desc=f"Distributed chunks for {builder.__class__.__name__}",
            )

            pbar_completed = tqdm(
                total=len(chunk_dicts),
                desc=f"Completed chunks for {builder.__class__.__name__}",
            )

            logger.info(f"Distributing {len(chunk_dicts)} chunks to workers")

        except NotImplementedError:
            attempt_graceful_shutdown(workers, socket)
            raise RuntimeError(f"Can't distribute process {builder.__class__.__name__} as no prechunk method exists.")

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
                                logger.debug(f"Sending exit signal to worker: {msg.split('_')[1]}")
                                socket.send_multipart([identity, b"", b"EXIT"])
                                workers.pop(identity)

                elif "ERROR" in msg:
                    # Remove worker and requeue work sent to it
                    attempt_graceful_shutdown(workers, socket)
                    raise RuntimeError(
                        "At least one worker has stopped with error message: {}".format(msg.split("_")[1])
                    )

                elif msg == "PING":
                    # Respond to heartbeat
                    socket.send_multipart([identity, b"", b"PONG"])
                    workers[identity]["last_ping"] = perf_counter()
                    workers[identity]["heartbeats"] += 1

            # Decide if any workers are dead and need to be removed
            if settings.WORKER_TIMEOUT is not None:
                handle_dead_workers(workers, socket)

            for work_index, chunk_dict in enumerate(chunk_dicts):
                if not chunk_dict["distributed"]:
                    temp_builder_dict = dict(**builder_dict)
                    temp_builder_dict.update(chunk_dict["chunk"])  # type: ignore
                    temp_builder_dict = jsanitize(temp_builder_dict, recursive_msonable=True)

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


def setup(url, port):
    context = zmq.Context()
    context.setsockopt(opt=zmq.SocketOption.ROUTER_MANDATORY, value=1)
    context.setsockopt(opt=zmq.SNDHWM, value=0)
    context.setsockopt(opt=zmq.RCVHWM, value=0)
    socket = context.socket(zmq.ROUTER)
    socket.bind(f"{url}:{port}")

    poll = zmq.Poller()
    poll.register(socket, zmq.POLLIN)
    return socket, poll


def attempt_graceful_shutdown(workers, socket):
    for identity in workers:
        socket.send_multipart([identity, b"", b"EXIT"])
    socket.close()


def handle_dead_workers(workers, socket):
    if len(workers) == 1:
        # Use global timeout
        identity = next(iter(workers.keys()))
        if (perf_counter() - workers[identity]["last_ping"]) >= settings.WORKER_TIMEOUT:
            attempt_graceful_shutdown(workers, socket)
            raise RuntimeError("Worker has timed out. Stopping distributed build.")

    elif len(workers) == 2:
        # Use 10% ratio between workers
        workers_sorted = sorted(workers.items(), key=lambda x: x[1]["heartbeats"])

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
                    raise RuntimeError("At least one worker has timed out. Stopping distributed build.")


def worker(url: str, port: int, num_processes: int, no_bars: bool):
    """
    Simple distributed worker that connects to a manager asks for work and deploys
    using multiprocessing.
    """
    identity = f"{randint(0, 0x10000):04X}-{randint(0, 0x10000):04X}"
    logger = getLogger(f"Worker {identity}")

    logger.info(f"Connecting to Manager at {url}:{port}")
    context = zmq.Context()
    socket: zmq.Socket = context.socket(zmq.REQ)

    socket.setsockopt_string(zmq.IDENTITY, identity)
    socket.connect(f"{url}:{port}")

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)

    # Initial message package
    hostname = pysocket.gethostname()

    try:
        running = True
        while running:
            socket.send(f"READY_{hostname}".encode())

            # Poll for MANAGER_TIMEOUT seconds, if nothing is given then assume manager is dead and timeout
            connections = dict(poller.poll(settings.MANAGER_TIMEOUT * 1000))
            if not connections:
                socket.close()
                raise RuntimeError("Stopping work as manager timed out.")

            bmessage: bytes = socket.recv()

            message = bmessage.decode("utf-8")
            if "@class" in message and "@module" in message:
                # We have a valid builder
                work = json.loads(message)
                builder = MontyDecoder().process_decoded(work)

                asyncio.run(
                    multi(
                        builder,
                        num_processes,
                        no_bars=no_bars,
                        heartbeat_func=ping_manager,
                        heartbeat_func_kwargs={"socket": socket, "poller": poller},
                    )
                )
            elif message == "EXIT":
                # End the worker
                running = False

    except Exception as e:
        logger.error(f"A worker failed with error: {e}")
        socket.send(f"ERROR_{e}".encode())
        socket.close()

    socket.close()


def ping_manager(socket, poller):
    socket.send_string("PING")

    # Poll for MANAGER_TIMEOUT seconds, if nothing is given then assume manager is dead and timeout
    connections = dict(poller.poll(settings.MANAGER_TIMEOUT * 1000))
    if not connections:
        socket.close()
        raise RuntimeError("Stopping work as manager timed out.")

    message: bytes = socket.recv()
    if message.decode("utf-8") != "PONG":
        socket.close()
        raise RuntimeError("Stopping work as manager did not respond to heartbeat from worker.")
