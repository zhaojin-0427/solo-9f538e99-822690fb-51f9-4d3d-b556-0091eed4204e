"""End-to-end API tests against a real SQLite database."""
import importlib
import sqlite3

import pytest
from fastapi.testclient import TestClient

from tests.conftest import reference_move


def test_initialize_version_one_keeps_original_order(client):
    r = client.post("/line/initialize", json={"total_traces": 20})
    assert r.status_code == 201
    assert r.json()["version"] == 1

    r = client.get("/line")
    assert r.json() == {
        "total_traces": 20,
        "current_version": 1,
        "current_root": r.json()["current_root"],
    }

    for pos in (1, 7, 20):
        r = client.get("/line/lookup",
                       params={"version": 1, "position": pos})
        assert r.status_code == 200
        body = r.json()
        assert body["original_trace"] == pos
        assert body["current_version"] == 1
        assert body["current_position"] == pos


def test_double_initialize_rejected(client):
    client.post("/line/initialize", json={"total_traces": 10})
    r = client.post("/line/initialize", json={"total_traces": 10})
    assert r.status_code == 422


def test_move_to_end_and_back_preserves_segment_order(client):
    n = 20
    client.post("/line/initialize", json={"total_traces": n})
    r = client.post("/line/move", json={
        "base_version": 1, "start": 4, "end": 8, "insert_after": n})
    assert r.status_code == 200
    assert r.json()["version"] == 2

    expected = reference_move(list(range(1, n + 1)), 4, 8, n)
    for pos, trace in enumerate(expected, start=1):
        body = client.get("/line/lookup",
                          params={"version": 2, "position": pos}).json()
        assert body["original_trace"] == trace
        assert body["current_version"] == 2
        assert body["current_position"] == pos

    # Move the same segment back to the front.
    r = client.post("/line/move", json={
        "base_version": 2,
        "start": n - 5 + 1, "end": n,
        "insert_after": 0})
    assert r.status_code == 200
    assert r.json()["version"] == 3
    expected2 = reference_move(expected, n - 4, n, 0)
    got = [
        client.get("/line/lookup",
                   params={"version": 3, "position": p}).json()
        ["original_trace"]
        for p in range(1, n + 1)
    ]
    assert got == expected2
    assert expected2[:5] == [4, 5, 6, 7, 8]  # segment internal order kept


def test_stale_submission_creates_no_version(client):
    client.post("/line/initialize", json={"total_traces": 10})
    client.post("/line/move", json={
        "base_version": 1, "start": 2, "end": 3, "insert_after": 10})

    r = client.post("/line/move", json={
        "base_version": 1, "start": 1, "end": 1, "insert_after": 10})
    assert r.status_code == 409
    assert "stale" in r.json()["detail"]

    meta = client.get("/line").json()
    versions = client.get("/line/versions").json()["versions"]
    assert meta["current_version"] == 2
    assert [v["version"] for v in versions] == [1, 2]


def test_invalid_moves_rejected_without_new_version(client):
    n = 10
    client.post("/line/initialize", json={"total_traces": n})
    bad_payloads = [
        {"base_version": 1, "start": 0, "end": 3, "insert_after": 10},
        {"base_version": 1, "start": 5, "end": 3, "insert_after": 10},
        {"base_version": 1, "start": 2, "end": 4, "insert_after": 2},
        {"base_version": 1, "start": 2, "end": 4, "insert_after": n + 1},
    ]
    for payload in bad_payloads:
        r = client.post("/line/move", json=payload)
        assert r.status_code == 422, payload
    assert client.get("/line").json()["current_version"] == 1


def test_lookup_unknown_version_and_position(client):
    client.post("/line/initialize", json={"total_traces": 10})
    assert client.get("/line/lookup",
                      params={"version": 99, "position": 1}).status_code == 404
    assert client.get("/line/lookup",
                      params={"version": 1, "position": 0}).status_code == 422
    assert client.get("/line/lookup",
                      params={"version": 1, "position": 11}).status_code == 422


def test_old_version_remains_queryable_after_two_moves(client):
    n = 50
    client.post("/line/initialize", json={"total_traces": n})
    client.post("/line/move", json={
        "base_version": 1, "start": 10, "end": 20, "insert_after": n})
    client.post("/line/move", json={
        "base_version": 2, "start": 1, "end": 5, "insert_after": 30})

    # Version 1 is immutable: position p still maps to original trace p.
    for pos in (1, 10, 20, 50):
        body = client.get("/line/lookup",
                          params={"version": 1, "position": pos}).json()
        assert body["original_trace"] == pos
        # Current position must match where that trace lives in version 3.
        cur = client.get(
            f"/line/traces/{body['original_trace']}").json()
        assert body["current_version"] == 3
        assert body["current_position"] == cur["current_position"]


def test_trace_endpoint_matches_position_lookup(client):
    n = 30
    client.post("/line/initialize", json={"total_traces": n})
    client.post("/line/move", json={
        "base_version": 1, "start": 3, "end": 7, "insert_after": n})
    for trace in (1, 5, 30):
        cur = client.get(f"/line/traces/{trace}").json()
        back = client.get("/line/lookup", params={
            "version": cur["current_version"],
            "position": cur["current_position"],
        }).json()
        assert back["original_trace"] == trace


def test_persistence_across_restart(app_module):
    main, db_path = app_module
    with TestClient(main.app) as c:
        c.post("/line/initialize", json={"total_traces": 30})
        c.post("/line/move", json={
            "base_version": 1, "start": 5, "end": 9, "insert_after": 30})
        before = c.get("/line/lookup",
                       params={"version": 1, "position": 7}).json()
    main.repo.close()

    # Reopen the same database file, simulating a process restart.
    import app.main as main2
    importlib.reload(main2)
    try:
        with TestClient(main2.app) as c:
            assert c.get("/health").json() == {"status": "ok",
                                               "initialized": True}
            after = c.get("/line/lookup",
                          params={"version": 1, "position": 7}).json()
            assert after == before
            assert c.get("/line").json()["current_version"] == 2
            assert c.get("/line/versions").json()["versions"][1][
                "base_version"] == 1
    finally:
        main2.repo.close()


def test_move_is_atomic_when_write_fails(app_module, monkeypatch):
    main, _ = app_module
    client = TestClient(main.app, raise_server_exceptions=False)
    client.post("/line/initialize", json={"total_traces": 10})

    from app import storage as storage_mod

    def boom(self):
        raise sqlite3.OperationalError("simulated disk failure")

    monkeypatch.setattr(storage_mod.EagerSqliteStore, "flush", boom)
    r = client.post("/line/move", json={
        "base_version": 1, "start": 2, "end": 4, "insert_after": 10})
    assert r.status_code == 500

    # No new version, no dangling nodes, line still usable.
    assert client.get("/line").json()["current_version"] == 1
    versions = client.get("/line/versions").json()["versions"]
    assert [v["version"] for v in versions] == [1]
    body = client.get("/line/lookup",
                      params={"version": 1, "position": 3}).json()
    assert body["original_trace"] == 3

    # A subsequent valid move still succeeds.
    monkeypatch.undo()
    r = client.post("/line/move", json={
        "base_version": 1, "start": 2, "end": 4, "insert_after": 10})
    assert r.status_code == 200
    assert r.json()["version"] == 2


@pytest.mark.parametrize("start,end,gap", [
    (1, 1, 5), (6, 9, 0), (3, 7, 10), (10, 10, 10), (2, 2, 0),
])
def test_random_move_matches_reference(client, start, end, gap):
    n = 10
    client.post("/line/initialize", json={"total_traces": n})
    r = client.post("/line/move", json={
        "base_version": 1, "start": start, "end": end,
        "insert_after": gap})
    assert r.status_code == 200
    expected = reference_move(list(range(1, n + 1)), start, end, gap)
    got = [
        client.get("/line/lookup",
                   params={"version": 2, "position": p}).json()
        ["original_trace"]
        for p in range(1, n + 1)
    ]
    assert got == expected
