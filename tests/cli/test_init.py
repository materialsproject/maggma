import pytest
from click.testing import CliRunner
from maggma.cli import run
from maggma.stores import MongoStore, MemoryStore
from maggma.builders import CopyBuilder
from monty.serialization import dumpfn
from datetime import datetime


@pytest.fixture
def mongostore():
    store = MongoStore("maggma_test", "test")
    store.connect()
    yield store
    store._collection.drop()


def test_basic_run():

    runner = CliRunner()
    result = runner.invoke(run, ["--help"])
    assert result.exit_code == 0

    result = runner.invoke(run)
    assert result.exit_code == 0


def test_run_builder(mongostore):

    memorystore = MemoryStore("temp")
    builder = CopyBuilder(mongostore, memorystore)

    mongostore.update(
        [
            {mongostore.key: i, mongostore.last_updated_field: datetime.utcnow()}
            for i in range(10)
        ]
    )

    runner = CliRunner()
    with runner.isolated_filesystem():
        dumpfn(builder, "test_builder.json")
        result = runner.invoke(run, ["-v", "test_builder.json"])
        assert result.exit_code == 0
        assert "CopyBuilder" in result.output
        assert "SerialProcessor" in result.output

        result = runner.invoke(run, ["-v", "-n", "2", "test_builder.json"])
        assert result.exit_code == 0
        assert "CopyBuilder" in result.output
        assert "MultiProcessor" in result.output
