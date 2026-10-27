import asyncio
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool

import pytest

from maggma.cli.multiprocessing import AsyncUnorderedMap, BackPressure, grouper, multi, safe_dispatch

from .builder_for_test import DummyBuilder


@pytest.mark.asyncio()
async def test_grouper():
    async def arange(count):
        for i in range(count):
            yield (i)

    async for group in grouper(arange(100), n=10):
        assert len(group) == 10

    async for group in grouper(arange(9), n=10):
        assert len(group) == 9


def wait_and_return(x):
    time.sleep(1)
    return x * x


async def arange(n):
    for num in range(n):
        yield num


@pytest.mark.asyncio()
async def test_backpressure():
    iterable = range(10)
    backpressure = BackPressure(iterable, 2)

    # Put two items into the process queue
    await backpressure.__anext__()
    await backpressure.__anext__()

    # Ensure back_pressure enabled
    assert backpressure.back_pressure.locked()

    # Release back pressure
    releaser = backpressure.release(arange(10))
    await releaser.__anext__()
    assert not backpressure.back_pressure.locked()

    # Ensure can keep releasing backing pressure and won't error
    await releaser.__anext__()
    await releaser.__anext__()

    # Ensure stop iteration works
    with pytest.raises(StopAsyncIteration):  # noqa: PT012
        for _i in range(10):
            await releaser.__anext__()

    assert not backpressure.back_pressure.locked()


@pytest.mark.asyncio()
async def test_async_map():
    executor = ThreadPoolExecutor(1)
    amap = AsyncUnorderedMap(wait_and_return, arange(3), executor)
    true_values = {x * x for x in range(3)}

    finished_vals = set()
    async for finished_val in amap:
        finished_vals.add(finished_val)

    assert finished_vals == true_values


def test_safe_dispatch():
    def bad_func(val):
        raise ValueError("AAAH")

    safe_dispatch((bad_func, ""))


def die(x):
    # Simulate a worker that is killed (e.g. by the OOM killer)
    os._exit(1)


@pytest.mark.asyncio()
async def test_async_map_worker_dies():
    with ProcessPoolExecutor(1) as executor:
        amap = AsyncUnorderedMap(die, arange(3), executor)

        with pytest.raises(BrokenProcessPool):
            await asyncio.wait_for(_collect(amap), timeout=30)


@pytest.mark.asyncio()
async def test_async_map_iterator_error():
    async def bad_iterator():
        yield 1
        raise ValueError("bad source")

    executor = ThreadPoolExecutor(1)
    amap = AsyncUnorderedMap(wait_and_return, bad_iterator(), executor)

    with pytest.raises(ValueError, match="bad source"):
        await asyncio.wait_for(_collect(amap), timeout=30)


@pytest.mark.asyncio()
async def test_async_map_unpicklable_item():
    # An item that cannot be sent to a worker is skipped, not left hanging
    async def items():
        for item in [1, lambda: None, 2]:
            yield item

    with ProcessPoolExecutor(1) as executor:
        amap = AsyncUnorderedMap(square, items(), executor)
        results = await asyncio.wait_for(_collect(amap), timeout=30)

    assert sorted(r for r in results if r is not None) == [1, 4]
    assert results.count(None) == 1


async def _collect(amap):
    return [item async for item in amap]


def square(x):
    return x * x


def test_multi_shuts_down_executor(mocker):
    executors = []

    class RecordingExecutor(ProcessPoolExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.shutdown_called = False
            executors.append(self)

        def shutdown(self, *args, **kwargs):
            self.shutdown_called = True
            super().shutdown(*args, **kwargs)

    mocker.patch("maggma.cli.multiprocessing.ProcessPoolExecutor", RecordingExecutor)

    builder = DummyBuilder(total=5)
    asyncio.run(multi(builder=builder, num_processes=2, no_bars=True))
    assert executors[0].shutdown_called

    builder = DummyBuilder(total=5)
    mocker.patch.object(builder, "update_targets", side_effect=RuntimeError("update failed"))
    with pytest.raises(RuntimeError, match="update failed"):
        asyncio.run(multi(builder=builder, num_processes=2, no_bars=True))
    assert executors[1].shutdown_called
