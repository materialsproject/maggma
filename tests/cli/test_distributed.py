import asyncio
import json
from multiprocessing.sharedctypes import Value
import threading
import time

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
SERVER_PORT = 1234


@pytest.mark.xfail(raises=ValueError)
def test_wrong_worker_input(log_to_stdout):

    manager(
        SERVER_URL,
        SERVER_PORT,
        [DummyBuilder(dummy_prechunk=False)],
        num_chunks=2,
        num_workers=0,
    )


@pytest.mark.asyncio
async def test_manager_and_worker(log_to_stdout):

    manager_thread = threading.Thread(
        target=manager,
        args=(SERVER_URL, SERVER_PORT, [DummyBuilder(dummy_prechunk=False)], 5, 5),
    )
    manager_thread.start()

    tasks = [worker(SERVER_URL, SERVER_PORT, num_processes=1, no_bars=True) for _ in range(5)]
    await asyncio.gather(*tasks)

    manager_thread.join()


@pytest.mark.asyncio
async def test_manager_worker_error(log_to_stdout):

    manager_thread = threading.Thread(
        target=manager,
        args=(SERVER_URL, SERVER_PORT, [DummyBuilder(dummy_prechunk=False)], 10, 1),
    )
    manager_thread.start()

    context = zmq.Context()
    socket = context.socket(REQ)
    socket.connect(f"{SERVER_URL}:{SERVER_PORT}")

    await socket.send("ERROR_testerror".encode("utf-8"))
    await asyncio.sleep(1)

    manager_thread.join()


@pytest.mark.asyncio
async def test_worker_error():
    context = zmq.Context()
    socket = context.socket(REP)
    socket.bind(f"{SERVER_URL}:{SERVER_PORT}")

    worker_task = asyncio.create_task(worker(SERVER_URL, SERVER_PORT, num_processes=1, no_bars=True))

    message = await socket.recv()
    assert message == "READY_{}".format(HOSTNAME).encode("utf-8")

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
    assert message.decode("utf-8") == "ERROR_Dummy error"

    worker_task.cancel()


@pytest.mark.asyncio
async def test_worker_exit():
    context = zmq.Context()
    socket = context.socket(REP)
    socket.bind(f"{SERVER_URL}:{SERVER_PORT}")

    worker_task = asyncio.create_task(worker(SERVER_URL, SERVER_PORT, num_processes=1, no_bars=True))

    message = await socket.recv()
    assert message == "READY_{}".format(HOSTNAME).encode("utf-8")
    await asyncio.sleep(1)
    await socket.send(b"EXIT")
    await asyncio.sleep(1)
    assert worker_task.done()

    worker_task.cancel()


@pytest.mark.xfail
def test_no_prechunk(caplog):
    manager(
        SERVER_URL,
        SERVER_PORT,
        [DummyBuilderWithNoPrechunk(dummy_prechunk=False)],
        10,
        1,
    )


def test_find_port():
    assert find_port() > 0
