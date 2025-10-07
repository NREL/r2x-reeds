from pathlib import Path

import pytest


@pytest.fixture(scope="function")
def empty_file(tmp_path) -> Path:
    empty_fpath = tmp_path / "test.csv"
    empty_fpath.write_text("")
    yield empty_fpath
    empty_fpath.unlink()
