import asyncio
import json

import pytest
from pynng import Pair1
from pynng.exceptions import Timeout

from maggma.cli.distributed import find_port, manager, worker
from maggma.core import Builder


class DummyBuilderWithNoPrechunk(Builder):
    def __init__(self, dummy_prechunk: bool, val: int = -1, **kwargs):
        self.dummy_prechunk = dummy_prechunk
        self.connected = False
        self.kwargs = kwargs
        self.val = val
        super().__init__(sources=[], targets=[])

    def connect(self):
        self.connected = True

    def get_items(self):
        return list(range(10))

    def process_items(self, items):
        pass

    def update_targets(self, items):
        pass


class DummyBuilder(DummyBuilderWithNoPrechunk):
    def prechunk(self, num_chunks):
        return [{"val": i} for i in range(num_chunks)]


SERVER_URL = "tcp://127.0.0.1"
SERVER_PORT = 8234


@pytest.fixture(scope="function")
async def manager_server(event_loop, log_to_stdout):

    task = asyncio.create_task(
        manager(
            SERVER_URL, SERVER_PORT, [DummyBuilder(dummy_prechunk=False)], num_chunks=10
        )
    )
    yield task
    task.cancel()


@pytest.mark.asyncio
async def test_manager_wait_for_ready(manager_server):
    with Pair1(
        dial=f"{SERVER_URL}:{SERVER_PORT}", polyamorous=True, recv_timeout=100
    ) as manager:
        with pytest.raises(Timeout):
            manager.recv()


@pytest.mark.asyncio
async def test_manager_give_out_chunks(manager_server, log_to_stdout):
    with Pair1(
        dial=f"{SERVER_URL}:{SERVER_PORT}", polyamorous=True, recv_timeout=500
    ) as manager_socket:

        for i in range(0, 10):
            log_to_stdout.debug(f"Going to ask Manager for work: {i}")
            await manager_socket.asend(b"Ready")
            message = await manager_socket.arecv()
            print(message)
            work = json.loads(message.decode("utf-8"))

            assert work["@class"] == "DummyBuilder"
            assert work["@module"] == "tests.cli.test_distributed"
            assert work["val"] == i

        await manager_socket.asend(b"Ready")
        message = await manager_socket.arecv()
        work = json.loads(message.decode("utf-8"))
        assert work == {}


@pytest.mark.asyncio
async def test_worker():
    with Pair1(
        listen=f"{SERVER_URL}:{SERVER_PORT}", polyamorous=True, recv_timeout=500
    ) as worker_socket:

        worker_task = asyncio.create_task(
            worker(SERVER_URL, SERVER_PORT, num_workers=1)
        )

        message = await worker_socket.arecv()
        assert message == b"Ready"

        dummy_work = {
            "@module": "tests.cli.test_distributed",
            "@class": "DummyBuilder",
            "@version": None,
            "dummy_prechunk": False,
            "val": 0,
        }
        for i in range(2):
            await worker_socket.asend(json.dumps(dummy_work).encode("utf-8"))
            await asyncio.sleep(1)
            message = await worker_socket.arecv()
            assert message == b"Ready"

        await worker_socket.asend(json.dumps({}).encode("utf-8"))
        with pytest.raises(Timeout):
            await worker_socket.arecv()

        assert len(worker_socket.pipes) == 0

        worker_task.cancel()


@pytest.mark.asyncio
async def test_no_prechunk(caplog):

    asyncio.create_task(
        manager(
            SERVER_URL,
            SERVER_PORT,
            [DummyBuilderWithNoPrechunk(dummy_prechunk=False)],
            num_chunks=10,
        )
    )
    await asyncio.sleep(1)
    assert (
        "Can't distributed process DummyBuilderWithNoPrechunk. Skipping for now"
        in caplog.text
    )


def test_find_port():
    assert find_port() > 0
