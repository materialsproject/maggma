#!/usr/bin/env python
# coding utf-8

from asyncio import BoundedSemaphore, Queue, gather, get_event_loop
from concurrent.futures import ProcessPoolExecutor
from logging import getLogger
from types import GeneratorType
from typing import Any, Callable, Dict, Optional

from aioitertools import enumerate
from tqdm.auto import tqdm

from maggma.utils import primed

logger = getLogger("MultiProcessor")


class BackPressure:
    """
    Wrapper for an iterator to provide
    async access with backpressure
    """

    def __init__(self, iterator, n):
        self.iterator = iter(iterator)
        self.back_pressure = BoundedSemaphore(n)

    def __aiter__(self):
        return self

    async def __anext__(self):
        await self.back_pressure.acquire()

        try:
            return next(self.iterator)
        except StopIteration:
            raise StopAsyncIteration

    async def release(self, async_iterator):
        """
        release iterator to pipeline the backpressure
        """
        async for item in async_iterator:
            try:
                self.back_pressure.release()
            except ValueError:
                pass

            yield item


class AsyncUnorderedMap:
    """
    Async iterator that maps a function to an async iterator
    usign an executor and returns items as they are done
    This does not guarantee order
    """

    def __init__(self, func, async_iterator, executor):
        self.iterator = async_iterator
        self.func = func
        self.executor = executor

        loop = get_event_loop()

        self.fill_task = loop.create_task(self.get_from_iterator())

        self.done_sentinel = object()
        self.results = Queue()
        self.tasks = {}

    async def process_and_release(self, idx):
        future = self.tasks[idx]
        try:
            item = await future
            self.results.put_nowait(item)
        except Exception:
            pass
        finally:
            self.tasks.pop(idx)

    async def get_from_iterator(self):
        loop = get_event_loop()
        async for idx, item in enumerate(self.iterator):
            future = loop.run_in_executor(
                self.executor, safe_dispatch, (self.func, item)
            )

            self.tasks[idx] = future

            loop.create_task(self.process_and_release(idx))

        await gather(*self.tasks.values())
        self.results.put_nowait(self.done_sentinel)

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.results.get()

        if item == self.done_sentinel:
            raise StopAsyncIteration
        else:
            return item


async def atqdm(async_iterator, *args, **kwargs):
    """
    Wrapper around tqdm for async generators
    """
    _tqdm = tqdm(*args, **kwargs)
    async for item in async_iterator:
        _tqdm.update()
        yield item

    _tqdm.close()


async def grouper(async_iterator, n: int):
    """
    Collect data into fixed-length chunks or blocks.
    >>> list(grouper(3, 'ABCDEFG'))
    [['A', 'B', 'C'], ['D', 'E', 'F'], ['G']]

    Updated from:
    https://stackoverflow.com/questions/31164731/python-chunking-csv-file-multiproccessing/31170795#31170795

    Modified for async
    """
    chunk = []
    async for item in async_iterator:
        chunk.append(item)
        if len(chunk) >= n:
            yield chunk
            chunk.clear()
    if chunk != []:
        yield chunk


def safe_dispatch(val):
    func, item = val
    try:
        return func(item)
    except Exception as e:
        logger.error(e)
        return None


async def multi(
    builder,
    num_processes,
    no_bars=False,
    heartbeat_func: Optional[Callable[..., Any]] = None,
    heartbeat_func_kwargs: Dict[Any, Any] = {},
):
    builder.connect()
    cursor = builder.get_items()
    executor = ProcessPoolExecutor(num_processes)

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

    back_pressured_get = BackPressure(
        iterator=tqdm(cursor, desc="Get", total=total, disable=no_bars),
        n=builder.chunk_size,
    )

    processed_items = atqdm(
        async_iterator=AsyncUnorderedMap(
            func=builder.process_item,
            async_iterator=back_pressured_get,
            executor=executor,
        ),
        total=total,
        desc="Process Items",
        disable=no_bars,
    )

    if heartbeat_func:
        heartbeat_func(**heartbeat_func_kwargs)

    back_pressure_relief = back_pressured_get.release(processed_items)

    update_items = tqdm(total=total, desc="Update Targets", disable=no_bars)

    async for chunk in grouper(back_pressure_relief, n=builder.chunk_size):
        logger.info(
            "Processed batch of {} items".format(builder.chunk_size),
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
        processed_items = [item for item in chunk if item is not None]
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

    update_items.close()
    builder.finalize()
