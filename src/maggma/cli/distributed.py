#!/usr/bin/env python
# coding utf-8

import json
from asyncio import wait
from logging import getLogger
from typing import List

from monty.json import jsanitize
from monty.serialization import MontyDecoder
from pynng import Pair1

from maggma.cli.multiprocessing import multi
from maggma.core import Builder
from maggma.utils import tqdm


async def master(url: str, builders: List[Builder], num_chunks: int):
    """
    Really simple master for distributed processing that uses a builder prechunk to modify
    the builder and send out modified builders for each worker to run
    """
    logger = getLogger("Master")

    logger.info(f"Binding to Master URL {url}")
    with Pair1(listen=url, polyamorous=True) as workers:

        for builder in builders:
            logger.info(f"Working on {builder.__class__.__name__}")
            builder_dict = builder.as_dict()

            try:

                builder.connect()
                chunks_dicts = list(builder.prechunk(num_chunks))

                logger.info(f"Distributing {len(chunks_dicts)} chunks to workers")
                for chunk_dict in tqdm(chunks_dicts, desc="Chunks"):
                    temp_builder_dict = dict(**builder_dict)
                    temp_builder_dict.update(chunk_dict)
                    temp_builder_dict = jsanitize(temp_builder_dict)

                    # Wait for client connection that announces client and says it is ready to do work
                    logger.debug("Waiting for a worker")
                    worker = await workers.arecv_msg()
                    logger.debug(
                        f"Got connection from worker: {worker.pipe.remote_address}"
                    )
                    # Send out the next chunk
                    await worker.pipe.asend(
                        json.dumps(temp_builder_dict).encode("utf-8")
                    )
            except NotImplementedError:
                logger.error(
                    f"Can't distributed process {builder.__class__.__name__}. Skipping for now"
                )

        # Clean up and tell workers to shut down
        await wait(
            [pipe.asend(json.dumps({}).encode("utf-8")) for pipe in workers.pipes]
        )


async def worker(url: str, num_workers: int):
    """
    Simple distributed worker that connects to a master asks for work and deploys
    using multiprocessing
    """
    # Should this have some sort of unique ID?
    logger = getLogger("Worker")

    logger.info(f"Connnecting to Master at {url}")
    with Pair1(dial=url, polyamorous=True) as master:
        logger.info(f"Connected to Master at {url}")
        running = True
        while running:
            await master.asend(b"Ready")
            message = await master.arecv()
            work = json.loads(message.decode("utf-8"))
            if "@class" in work and "@module" in work:
                # We have a valid builder
                builder = MontyDecoder().process_decoded(work)
                await multi(builder, num_workers)
            else:
                # End the worker
                # This should look for a specific message ?
                running = False
