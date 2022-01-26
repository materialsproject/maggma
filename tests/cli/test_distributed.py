import asyncio
import json

import pytest

from maggma.cli.distributed import find_port, manager, worker
from maggma.core import Builder

from zmq import REP, REQ
import zmq.asyncio as zmq
import socket as pysocket

# TODO: Timeout errors?

HOSTNAME = pysocket.gethostname()


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


class DummyBuilderError(DummyBuilderWithNoPrechunk):
    def prechunk(self, num_chunks):
        return [{"val": i} for i in range(num_chunks)]

    def get_items(self):
        raise ValueError("Dummy error")

    def process_items(self, items):
        raise ValueError("Dummy error")


SERVER_URL = "tcp://127.0.0.1"
SERVER_PORT = 8234


@pytest.mark.xfail(raises=ValueError)
@pytest.mark.asyncio
async def test_wrong_worker_input(log_to_stdout):

    manager_server = asyncio.create_task(
        manager(
            SERVER_URL,
            SERVER_PORT,
            [DummyBuilder(dummy_prechunk=False)],
            num_chunks=2,
            num_workers=0,
        )
    )

    await asyncio.sleep(1)
    manager_server.result()


@pytest.mark.asyncio
async def test_manager_give_out_chunks(log_to_stdout):

    manager_server = asyncio.create_task(
        manager(
            SERVER_URL,
            SERVER_PORT,
            [DummyBuilder(dummy_prechunk=False)],
            num_chunks=10,
            num_workers=10,
        )
    )

    context = zmq.Context()
    socket = context.socket(REQ)
    socket.connect(f"{SERVER_URL}:{SERVER_PORT}")

    for i in range(0, 10):
        log_to_stdout.debug(f"Going to ask Manager for work: {i}")
        await socket.send(b"Ready")
        message = await socket.recv()

        work = json.loads(message.decode("utf-8"))

        assert work["@class"] == "DummyBuilder"
        assert work["@module"] == "tests.cli.test_distributed"
        assert work["val"] == i

    for i in range(0, 10):
        await socket.send(b"Ready")
        message = await socket.recv()
        assert message == b'"EXIT"'

    manager_server.cancel()


@pytest.mark.asyncio
async def test_manager_worker_error(log_to_stdout):

    manager_server = asyncio.create_task(
        manager(
            SERVER_URL,
            SERVER_PORT,
            [DummyBuilder(dummy_prechunk=False)],
            num_chunks=10,
            num_workers=1,
        )
    )

    context = zmq.Context()
    socket = context.socket(REQ)
    socket.connect(f"{SERVER_URL}:{SERVER_PORT}")

    await socket.send("ERROR".encode("utf-8"))
    await asyncio.sleep(1)
    assert manager_server.done()
    manager_server.cancel()


@pytest.mark.asyncio
async def test_worker():
    context = zmq.Context()
    socket = context.socket(REP)
    socket.bind(f"{SERVER_URL}:{SERVER_PORT}")

    worker_task = asyncio.create_task(worker(SERVER_URL, SERVER_PORT, num_processes=1))

    message = await socket.recv()

    dummy_work = {
        "@module": "tests.cli.test_distributed",
        "@class": "DummyBuilder",
        "@version": None,
        "dummy_prechunk": False,
        "val": 0,
    }
    for i in range(2):
        await socket.send(json.dumps(dummy_work).encode("utf-8"))
        await asyncio.sleep(1)
        message = await socket.recv()
        assert message == HOSTNAME.encode("utf-8")

    worker_task.cancel()


@pytest.mark.asyncio
async def test_worker_error():
    context = zmq.Context()
    socket = context.socket(REP)
    socket.bind(f"{SERVER_URL}:{SERVER_PORT}")

    worker_task = asyncio.create_task(worker(SERVER_URL, SERVER_PORT, num_processes=1))

    message = await socket.recv()
    assert message == HOSTNAME.encode("utf-8")

    dummy_work = {
        "@module": "tests.cli.test_distributed",
        "@class": "DummyBuilderError",
        "@version": None,
        "dummy_prechunk": False,
        "val": 0,
    }

    await socket.send(json.dumps(dummy_work).encode("utf-8"))
    await asyncio.sleep(1)
    message = await socket.recv()
    assert message.decode("utf-8") == "ERROR"

    worker_task.cancel()


@pytest.mark.asyncio
async def test_worker_exit():
    context = zmq.Context()
    socket = context.socket(REP)
    socket.bind(f"{SERVER_URL}:{SERVER_PORT}")

    worker_task = asyncio.create_task(worker(SERVER_URL, SERVER_PORT, num_processes=1))

    message = await socket.recv()
    assert message == HOSTNAME.encode("utf-8")

    await socket.send_json("EXIT")
    await asyncio.sleep(1)
    assert worker_task.done()

    worker_task.cancel()


@pytest.mark.asyncio
async def test_no_prechunk(caplog):

    asyncio.create_task(
        manager(
            SERVER_URL,
            SERVER_PORT,
            [DummyBuilderWithNoPrechunk(dummy_prechunk=False)],
            num_chunks=10,
            num_workers=10,
        )
    )
    await asyncio.sleep(1)
    assert (
        "Can't distributed process DummyBuilderWithNoPrechunk. Skipping for now"
        in caplog.text
    )


def test_find_port():
    assert find_port() > 0
