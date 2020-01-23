#!/usr/bin/env python
# coding utf-8

import logging
from types import GeneratorType
from tqdm import tqdm

from maggma.utils import grouper, primed
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
    total = None
    if isinstance(cursor, GeneratorType):
        try:
            cursor = primed(cursor)
            if hasattr(builder, "total"):
                total = builder.total
        except StopIteration:
            pass

    elif hasattr(cursor, "__len__"):
        total = len(cursor)
    elif hasattr(cursor, "count"):
        total = cursor.count()

    for chunk in grouper(tqdm(cursor, total=total), builder.chunk_size):
        processed_items = [builder.process_item(item) for item in chunk]
        builder.update_targets(processed_items)

    builder.finalize()
