import shutil
from datetime import datetime
from pathlib import Path

import pytest
from click.testing import CliRunner
from monty.serialization import dumpfn

from maggma.builders import CopyBuilder
from maggma.cli import run
from maggma.stores import MemoryStore, MongoStore


@pytest.fixture
def mongostore():
    store = MongoStore("maggma_test", "test")
    store.connect()
    store.remove_docs({})
    yield store
    store.remove_docs({})
    store._collection.drop()


@pytest.fixture
def reporting_store():
    store = MongoStore("maggma_test", "reporting")
    store.connect()
    store.remove_docs({})
    yield store
    store.remove_docs({})
    store._collection.drop()


def test_basic_run():

    runner = CliRunner()
    result = runner.invoke(run, ["--help"])
    assert result.exit_code == 0

    # Ensure running without any builders fail
    result = runner.invoke(run)
    assert result.exit_code != 0


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

        result = runner.invoke(run, ["-vvv", "--no_bars", "test_builder.json"])
        assert result.exit_code == 0
        assert "Get" not in result.output
        assert "Update" not in result.output

        result = runner.invoke(run, ["-v", "-n", "2", "test_builder.json"])
        assert result.exit_code == 0
        assert "CopyBuilder" in result.output
        assert "MultiProcessor" in result.output

        result = runner.invoke(
            run, ["-vvv", "-n", "2", "--no_bars", "test_builder.json"]
        )
        assert result.exit_code == 0
        assert "Get" not in result.output
        assert "Update" not in result.output


def test_run_builder_chain(mongostore):

    memorystore = MemoryStore("temp")
    builder1 = CopyBuilder(mongostore, memorystore)
    builder2 = CopyBuilder(mongostore, memorystore)

    mongostore.update(
        [
            {mongostore.key: i, mongostore.last_updated_field: datetime.utcnow()}
            for i in range(10)
        ]
    )

    runner = CliRunner()
    with runner.isolated_filesystem():
        dumpfn([builder1, builder2], "test_builders.json")
        result = runner.invoke(run, ["-v", "test_builders.json"])
        assert result.exit_code == 0
        assert "CopyBuilder" in result.output
        assert "SerialProcessor" in result.output

        result = runner.invoke(run, ["-vvv", "--no_bars", "test_builders.json"])
        assert result.exit_code == 0
        assert "Get" not in result.output
        assert "Update" not in result.output

        result = runner.invoke(run, ["-v", "-n", "2", "test_builders.json"])
        assert result.exit_code == 0
        assert "CopyBuilder" in result.output
        assert "MultiProcessor" in result.output

        result = runner.invoke(
            run, ["-vvv", "-n", "2", "--no_bars", "test_builders.json"]
        )
        assert result.exit_code == 0
        assert "Get" not in result.output
        assert "Update" not in result.output


def test_reporting(mongostore, reporting_store):

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
        dumpfn(reporting_store, "test_reporting_store.json")
        result = runner.invoke(
            run, ["-v", "test_builder.json", "-r", "test_reporting_store.json"]
        )
        assert result.exit_code == 0

        report_docs = list(reporting_store.query())
        assert len(report_docs) == 3

        start_doc = next(d for d in report_docs if d["event"] == "BUILD_STARTED")
        assert "sources" in start_doc
        assert "targets" in start_doc

        end_doc = next(d for d in report_docs if d["event"] == "BUILD_ENDED")
        assert "errors" in end_doc
        assert "warnings" in end_doc

        update_doc = next(d for d in report_docs if d["event"] == "UPDATE")
        assert "items" in update_doc


def test_python_source():

    runner = CliRunner()

    with runner.isolated_filesystem():
        shutil.copy2(
            src=Path(__file__).parent / "builder_for_test.py", dst=Path(".").resolve()
        )
        result = runner.invoke(run, ["-v", "-n", "2", "builder_for_test.py"])

    assert result.exit_code == 0
    assert "Ended multiprocessing: DummyBuilder" in result.output


def test_python_notebook_source():

    runner = CliRunner()

    with runner.isolated_filesystem():
        shutil.copy2(
            src=Path(__file__).parent / "builder_notebook_for_test.ipynb",
            dst=Path(".").resolve(),
        )
        result = runner.invoke(
            run, ["-v", "-n", "2", "builder_notebook_for_test.ipynb"]
        )

    assert result.exit_code == 0
    assert "Ended multiprocessing: DummyBuilder" in result.output
