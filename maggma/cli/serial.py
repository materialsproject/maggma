#!/usr/bin/env python
# coding utf-8

import logging
from tqdm import tqdm

from maggma.utils import grouper
from maggma.core import Builder


def serial(builder: Builder):
    """
    Runs the builders using a single process
    """

    logger = logging.getLogger("SerialProcessor")

    builder.connect()

    cursor = builder.get_items()

    for chunk in grouper(tqdm(cursor), builder.chunk_size):
        logger.info("Processing batch of {} items".format(builder.chunk_size))
        processed_items = [
            builder.process_item(item) for item in chunk if item is not None
        ]
        builder.update_targets(processed_items)

    builder.finalize()
