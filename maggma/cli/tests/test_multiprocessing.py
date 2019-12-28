import pytest
import time
import asyncio
from maggma.cli.multiprocessing import AsyncBackPressuredMap, grouper
from concurrent.futures import ThreadPoolExecutor


@pytest.mark.asyncio
async def test_grouper():
    async def arange(count):
        for i in range(count):
            yield (i)

    async for group in grouper(arange(100), n=10):
        assert len(group) == 10

    async for group in grouper(arange(9), n=10, fillvalue="s"):
        assert len(group) == 10

    async for group in grouper(arange(9), n=10):
        assert len(group) == 9


def wait_and_return(x):
    time.sleep(1)
    return x * x


@pytest.mark.asyncio
async def test_backpressure_map():

    executor = ThreadPoolExecutor(1)
    mapper = AsyncBackPressuredMap(
        iterator=range(3), func=wait_and_return, max_run=2, executor=executor
    )

    true_values = [x * x for x in range(3)]
    async for finished_val in mapper:
        finished_val = await finished_val
        assert finished_val.result() == true_values.pop(0)

    mapper = AsyncBackPressuredMap(
        iterator=range(3), func=wait_and_return, max_run=2, executor=executor
    )

    # Put two items into the process queue
    futures = [await mapper.__anext__(), await mapper.__anext__()]
    # Ensure back_pressure enabled
    assert mapper.back_pressure.locked()
    await asyncio.sleep(2)
    # Ensure back_pressure enabled till data is dequeued from process_pipeline
    assert mapper.back_pressure.locked()
    # Dequeue futures and ensure back_pressure is gone
    await asyncio.gather(*futures)
    assert not mapper.back_pressure.locked()
