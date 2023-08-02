#!/usr/bin/env python
# coding utf-8

import asyncio
import json
import socket as pysocket
from logging import getLogger
from random import randint
from time import perf_counter
from typing import List, Literal

import numpy as np
from monty.json import jsanitize
from monty.serialization import MontyDecoder

from maggma.cli.multiprocessing import multi
from maggma.cli.settings import CLISettings
from maggma.core import Builder
from maggma.utils import Timeout, tqdm

try:
    import pika
except ImportError:
    raise ImportError("Both pika and aio-pika are required to use RabbitMQ as a broker")

settings = CLISettings()


def find_port():
    sock = pysocket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


def manager(
    url: str,
    builders: List[Builder],
    num_chunks: int,
    num_workers: int,
    queue_prefix: str,
    port: int = 5672,
):
    """
    Rabbit MQ manager for distributed processing that uses a builder prechunk to modify
    the builder and send them out each worker to run.
    """
    logger = getLogger("Manager")

    if not (num_chunks and num_workers):
        raise ValueError("Both num_chunks and num_workers must be non-zero")

    url = url.split("//")[-1]

    logger.info(f"Binding to Manager URL {url}:{port}")

    # Setup connection to RabbitMQ and ensure on all queues is one unit
    connection, channel, status_queue, worker_queue = setup_rabbitmq(url, queue_prefix, port, "work")

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
            attempt_graceful_shutdown(connection, workers, channel, worker_queue)
            raise RuntimeError(f"Can't distribute process {builder.__class__.__name__} as no prechunk method exists.")

        completed = False

        while not completed:
            completed = all(chunk["completed"] for chunk in chunk_dicts)

            if num_workers <= 0:
                connection.close()
                raise RuntimeError("No workers to distribute chunks to")

            # If workers send messages decode and figure out what do

            _, _, body = channel.basic_get(queue=status_queue, auto_ack=True)

            if body is not None:
                msg = body.decode("utf-8")
                identity = msg.split("_")[-1]

                if "READY" in msg:
                    if identity not in workers:
                        logger.debug(f"Got connection from worker: {msg.split('_')[1]}")
                        workers[identity] = {
                            "working": False,
                            "heartbeats": 1,
                            "last_ping": perf_counter(),
                            "work_index": -1,
                        }

                elif "DONE" in msg:
                    workers[identity]["working"] = False
                    work_ind = workers[identity]["work_index"]
                    if work_ind != -1:
                        chunk_dicts[work_ind]["completed"] = True  # type: ignore
                        pbar_completed.update(1)

                elif "ERROR" in msg:
                    # Remove worker and requeue work sent to it
                    attempt_graceful_shutdown(connection, workers, channel, worker_queue)
                    raise RuntimeError(
                        "At least one worker has stopped with error message: {}".format(msg.split("_")[1])
                    )

                elif "PING" in msg:
                    # Heartbeat from worker (no pong response)
                    workers[identity]["last_ping"] = perf_counter()
                    workers[identity]["heartbeats"] += 1

            # Decide if any workers are dead and need to be removed
            handle_dead_workers(connection, workers, channel, worker_queue)

            for work_index, chunk_dict in enumerate(chunk_dicts):
                if not chunk_dict["distributed"]:
                    temp_builder_dict = dict(**builder_dict)
                    temp_builder_dict.update(chunk_dict["chunk"])  # type: ignore
                    temp_builder_dict = jsanitize(temp_builder_dict)

                    # Send work for available workers
                    for identity in workers:
                        if not workers[identity]["working"]:
                            # Send out a chunk to idle worker
                            channel.basic_publish(
                                exchange="",
                                routing_key=worker_queue,
                                body=json.dumps(temp_builder_dict).encode("utf-8"),
                            )

                            workers[identity]["work_index"] = work_index
                            workers[identity]["working"] = True
                            chunk_dicts[work_index]["distributed"] = True
                            pbar_distributed.update(1)

    # Send EXIT to any remaining workers
    logger.info("Sending exit messages to workers once they are done")
    attempt_graceful_shutdown(connection, workers, channel, worker_queue)


def setup_rabbitmq(url: str, queue_prefix: str, port: int, outbound_queue: Literal["status", "work"]):
    connection = pika.BlockingConnection(pika.ConnectionParameters(url, port))
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1, global_qos=True)

    # Ensure both worker status and work distribution queues exist
    status_queue = queue_prefix + "_status"
    worker_queue = queue_prefix + "_work"

    channel.queue_declare(queue=status_queue, auto_delete=True)
    channel.queue_declare(queue=worker_queue, auto_delete=True)

    # Clear out outbound queue
    if outbound_queue == "work":
        channel.queue_purge(queue=worker_queue)
    else:
        channel.queue_purge(queue=status_queue)

    return connection, channel, status_queue, worker_queue


def attempt_graceful_shutdown(connection, workers, channel, worker_queue):
    for _ in workers:
        channel.basic_publish(
            exchange="",
            routing_key=worker_queue,
            body=b"EXIT",
        )
    connection.close()


def handle_dead_workers(connection, workers, channel, worker_queue):
    if len(workers) == 1:
        # Use global timeout
        identity = next(iter(workers.keys()))
        if (perf_counter() - workers[identity]["last_ping"]) >= settings.WORKER_TIMEOUT:
            attempt_graceful_shutdown(connection, workers, channel, worker_queue)
            raise RuntimeError("Worker has timed out. Stopping distributed build.")

    elif len(workers) == 2:
        # Use 10% ratio between workers
        workers_sorted = sorted(workers.items(), key=lambda x: x[1]["heartbeats"])

        ratio = workers_sorted[1][1]["heartbeats"] / workers_sorted[0][1]["heartbeats"]

        if ratio <= 0.1:
            attempt_graceful_shutdown(connection, workers, channel, worker_queue)
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
                    attempt_graceful_shutdown(connection, workers, channel, worker_queue)
                    raise RuntimeError("At least one worker has timed out. Stopping distributed build.")


def worker(url: str, port: int, num_processes: int, no_bars: bool, queue_prefix: str):
    """
    Simple distributed worker that connects to a manager asks for work and deploys
    using multiprocessing
    """
    identity = f"{randint(0, 0x10000):04X}-{randint(0, 0x10000):04X}"
    logger = getLogger(f"Worker {identity}")

    url = url.split("//")[-1]

    logger.info(f"Connecting to Manager at {url}:{port}")

    # Setup connection to RabbitMQ and ensure on all queues is one unit
    connection, channel, status_queue, worker_queue = setup_rabbitmq(url, queue_prefix, port, "status")

    # Send ready signal to status queue
    channel.basic_publish(
        exchange="",
        routing_key=status_queue,
        body=f"READY_{identity}".encode(),
    )

    try:
        running = True
        while running:
            # Wait for work from manager
            with Timeout(seconds=settings.MANAGER_TIMEOUT):
                _, _, body = channel.basic_get(queue=worker_queue, auto_ack=True)

            if body is not None:
                message = body.decode("utf-8")

                if "@class" in message and "@module" in message:
                    # We have a valid builder
                    work = json.loads(message)
                    builder = MontyDecoder().process_decoded(work)

                    logger.info(f"Working on builder {builder.__class__}")

                    channel.basic_publish(
                        exchange="",
                        routing_key=status_queue,
                        body=f"WORKING_{identity}".encode(),
                    )
                    work = json.loads(message)
                    builder = MontyDecoder().process_decoded(work)

                    asyncio.run(
                        multi(
                            builder,
                            num_processes,
                            no_bars=no_bars,
                            heartbeat_func=ping_manager,
                            heartbeat_func_kwargs={
                                "channel": channel,
                                "identity": identity,
                                "status_queue": status_queue,
                            },
                        )
                    )

                    channel.basic_publish(
                        exchange="",
                        routing_key=status_queue,
                        body=f"DONE_{identity}".encode(),
                    )

                elif message == "EXIT":
                    # End the worker
                    running = False

    except Exception as e:
        logger.error(f"A worker failed with error: {e!r}")
        channel.basic_publish(
            exchange="",
            routing_key=status_queue,
            body=f"ERROR_{identity}".encode(),
        )
        connection.close()

    connection.close()


def ping_manager(channel, identity, status_queue):
    channel.basic_publish(
        exchange="",
        routing_key=status_queue,
        body=f"PING_{identity}".encode(),
    )
