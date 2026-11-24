"""Shared pytest fixtures: fresh per-test database and TestClient."""
import importlib
import os

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def app_module(tmp_path, monkeypatch):
    db_path = tmp_path / "line.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    import app.main as main
    importlib.reload(main)
    yield main, db_path
    main.repo.close()


@pytest.fixture
def client(app_module):
    main, _ = app_module
    with TestClient(main.app) as c:
        yield c


@pytest.fixture
def repo(app_module):
    main, _ = app_module
    return main.repo


def reference_move(arr: list[int], start: int, end: int,
                   insert_after: int) -> list[int]:
    """List-based reference implementation of the documented move semantics."""
    n = len(arr)
    segment = arr[start - 1:end]
    remaining = arr[:start - 1] + arr[end:]
    seg_len = end - start + 1
    gap = insert_after - seg_len if insert_after >= end else insert_after
    return remaining[:gap] + segment + remaining[gap:]
