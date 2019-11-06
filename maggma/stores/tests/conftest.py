from pathlib import Path
import pytest


@pytest.fixture("session")
def db_json():
    module_dir = Path(__file__).resolve().parent
    db_dir = module_dir / ".." / ".." / ".." / "test_files" / "settings_files"
    db_json = db_dir / "db.json"
    return db_json.resolve()
