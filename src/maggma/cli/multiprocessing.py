#!/usr/bin/env python
# coding utf-8

from logging import getLogger
from types import GeneratorType
from asyncio import BoundedSemaphore, get_running_loop, gather
from aioitertools import zip_longest
from concurrent.futures import ProcessPoolExecutor
from maggma.utils import primed
from tqdm import tqdm


class ProcessItemsSemaphore(BoundedSemaphore):
    """
    Modified BoundedSemaphore to update a TQDM bar
    for process_items
    """

    def __init__(self, total=None, *args, **kwargs):
        self.tqdm = tqdm(total=total, desc="Process Items")
        super().__init__(*args, **kwargs)

    def release(self):
        self.tqdm.update(1)
        super().release()


class AsyncBackPressuredMap:
    """
    Wrapper for an iterator to provide
    async access with backpressure
    """

    def __init__(self, iterator, func, max_run, executor, total=None):
        self.iterator = iter(iterator)
        self.func = func
        self.executor = executor
        self.back_pressure = ProcessItemsSemaphore(value=max_run, total=total)

    def __aiter__(self):
        return self

    async def __anext__(self):
        await self.back_pressure.acquire()
        loop = get_running_loop()

        try:
            item = next(self.iterator)
        except StopIteration:
            raise StopAsyncIteration

        future = loop.run_in_executor(self.executor, self.func, item)

        async def process_and_release():
            await future
            self.back_pressure.release()
            return future

        return process_and_release()


async def grouper(iterable, n, fillvalue=None):
    """
    Collect data into fixed-length chunks or blocks.
    """
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iterable] * n
    iterator = zip_longest(*args, fillvalue=fillvalue)

    async for group in iterator:
        group = [g for g in group if g is not None]
        yield group


async def multi(builder, num_workers):
    logger = getLogger("MultiProcessor")

    builder.connect()
    cursor = builder.get_items()
    executor = ProcessPoolExecutor(num_workers)

    # Gets the total number of items to process by priming
    # the cursor
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

    mapper = AsyncBackPressuredMap(
        iterator=tqdm(cursor, desc="Get", total=total),
        func=builder.process_item,
        max_run=builder.chunk_size,
        executor=executor,
        total=total,
    )
    update_items = tqdm(total=total, desc="Update Targets")

    logger.info(
        f"Starting multiprocessing: {builder.__class__.__name__}",
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
    async for chunk in grouper(mapper, builder.chunk_size, fillvalue=None):
        logger.info(
            "Processing batch of {} items".format(builder.chunk_size),
            extra={
                "maggma": {
                    "event": "UPDATE",
                    "items": len(chunk),
                    "builder": builder.__class__.__name__,
                    "sources": [source.name for source in builder.sources],
                    "targets": [target.name for target in builder.targets],
                }
            },
        )
        chunk = await gather(*chunk)
        processed_items = [c.result() for c in chunk if chunk is not None]
        builder.update_targets(processed_items)
        update_items.update(len(processed_items))

    logger.info(
        f"Ended multiprocessing: {builder.__class__.__name__}",
        extra={
            "maggma": {
                "event": "BUILD_ENDED",
                "builder": builder.__class__.__name__,
                "sources": [source.name for source in builder.sources],
                "targets": [target.name for target in builder.targets],
            }
        },
    )
    builder.finalize()
