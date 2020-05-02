import logging
import sys
from pathlib import Path

import pytest


@pytest.fixture
def test_dir():
    module_dir = Path(__file__).resolve().parent
    test_dir = module_dir / "test_files"
    return test_dir.resolve()


@pytest.fixture
def db_json(test_dir):
    db_dir = test_dir / "settings_files"
    db_json = db_dir / "db.json"
    return db_json.resolve()


@pytest.fixture
def log_to_stdout():
    # Set Logging
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    root.addHandler(ch)
    return root
