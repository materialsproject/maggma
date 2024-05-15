import logging
import sys
from pathlib import Path

import pytest


@pytest.fixture()
def tmp_dir():  # noqa: PT004
    """
    Create a clean directory and cd into it.

    The directory will be removed at the end of the test.
    """
    import os
    import shutil
    import tempfile

    old_cwd = os.getcwd()
    newpath = tempfile.mkdtemp()
    os.chdir(newpath)
    yield
    os.chdir(old_cwd)
    shutil.rmtree(newpath)


@pytest.fixture()
def test_dir():
    module_dir = Path(__file__).resolve().parent
    test_dir = module_dir / "test_files"
    return test_dir.resolve()


@pytest.fixture()
def db_json(test_dir):
    db_dir = test_dir / "settings_files"
    db_json = db_dir / "db.json"
    return db_json.resolve()


@pytest.fixture()
def lp_file(test_dir):
    db_dir = test_dir / "settings_files"
    lp_file = db_dir / "my_launchpad.yaml"
    return lp_file.resolve()


@pytest.fixture()
def log_to_stdout():
    # Set Logging
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    root.addHandler(ch)
    return root


def pytest_itemcollected(item):
    """Make tests names more readable in the tests output."""
    item._nodeid = (
        item._nodeid.replace(".py", "")
        .replace("tests/", "")
        .replace("test_", "")
        .replace("_", " ")
        .replace("Test", "")
        .replace("Class", " class")
        .lower()
    )
    doc = item.obj.__doc__.strip() if item.obj.__doc__ else ""
    if doc:
        item._nodeid = item._nodeid.split("::")[0] + "::" + doc
