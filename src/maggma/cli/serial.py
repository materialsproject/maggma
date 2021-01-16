#!/usr/bin/env python
# coding utf-8

import logging
from types import GeneratorType

from tqdm import tqdm

from maggma.core import Builder
from maggma.utils import grouper, primed


def serial(builder: Builder, no_bars=False):
    """
    Runs the builders using a single process
    """

    logger = logging.getLogger("SerialProcessor")

    builder.connect()

    cursor = builder.get_items()

    total = None
    if isinstance(cursor, GeneratorType):
        try:
            cursor = primed(cursor)
            if hasattr(builder, "total"):
                total = builder.total
        except StopIteration:
            pass

    elif hasattr(cursor, "__len__"):
        total = len(cursor)  # type: ignore
    elif hasattr(cursor, "count"):
        total = cursor.count()  # type: ignore

    logger.info(
        f"Starting serial processing: {builder.__class__.__name__}",
        extra={
            "maggma": {
                "event": "BUILD_STARTED",
                "total": total,
                "builder": builder.__class__.__name__,
                "sources": [source.name for source in builder.sources],
                "targets": [target.name for target in builder.targets],
            }
        },
    )
    for chunk in grouper(
        tqdm(cursor, total=total, disable=no_bars), builder.chunk_size
    ):
        logger.info(
            "Processing batch of {} items".format(builder.chunk_size),
            extra={
                "maggma": {
                    "event": "UPDATE",
                    "items": len(chunk),
                    "builder": builder.__class__.__name__,
                }
            },
        )
        processed_chunk = [builder.process_item(item) for item in chunk]
        processed_items = [item for item in processed_chunk if item is not None]
        builder.update_targets(processed_items)

    logger.info(
        f"Ended serial processing: {builder.__class__.__name__}",
        extra={
            "maggma": {"event": "BUILD_ENDED", "builder": builder.__class__.__name__}
        },
    )
    builder.finalize()
