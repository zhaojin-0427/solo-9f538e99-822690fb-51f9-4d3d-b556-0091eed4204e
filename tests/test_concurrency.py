"""Concurrency / snapshot tests.

A lookup must observe one committed state even while a move transaction is
in progress, and stale move submissions must never create versions.
"""
import threading

from tests.conftest import reference_move


def test_concurrent_writers_one_winner_only(client):
    client.post("/line/initialize", json={"total_traces": 100})
    results: list[int] = []
    barrier = threading.Barrier(4)

    def submit(base: int):
        barrier.wait()
        r = client.post("/line/move", json={
            "base_version": base, "start": 1, "end": 5,
            "insert_after": 100})
        results.append(r.status_code)

    threads = [threading.Thread(target=submit, args=(1,)) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(200) == 1
    assert results.count(409) == 3
    meta = client.get("/line").json()
    assert meta["current_version"] == 2
    versions = client.get("/line/versions").json()["versions"]
    assert [v["version"] for v in versions] == [1, 2]


def test_snapshot_read_during_move(app_module):
    """A read transaction opened before the move commits keeps seeing the
    old version for its whole duration."""
    main, _ = app_module
    repo = main.repo
    repo.initialize(50)

    import sqlite3
    reader = sqlite3.connect(repo.path, isolation_level=None)
    reader.execute("PRAGMA query_only=ON")
    reader.row_factory = sqlite3.Row
    reader.execute("BEGIN")
    row = reader.execute(
        "SELECT current_version FROM meta WHERE id = 0"
    ).fetchone()
    assert row["current_version"] == 1

    # A move commits in another connection while the read txn is open.
    repo.move_segment(1, 10, 20, 50)

    # The still-open snapshot must not see the new version.
    row = reader.execute(
        "SELECT current_version FROM meta WHERE id = 0"
    ).fetchone()
    assert row["current_version"] == 1
    root_row = reader.execute(
        "SELECT root_node FROM versions WHERE version = 1"
    ).fetchone()
    assert root_row is not None
    reader.execute("COMMIT")
    reader.close()

    # A fresh snapshot sees the committed state.
    assert repo.current_version() == 2


def test_repeated_stale_then_fresh_move_chain(client):
    n = 40
    client.post("/line/initialize", json={"total_traces": n})
    order = list(range(1, n + 1))
    current = 1
    for start, end, gap in [(2, 6, n), (1, 3, 0), (20, 25, 10),
                            (n, n, n - 1)]:
        # stale submissions against every older version are rejected
        for stale in range(1, current):
            r = client.post("/line/move", json={
                "base_version": stale, "start": start, "end": end,
                "insert_after": gap})
            assert r.status_code == 409
        r = client.post("/line/move", json={
            "base_version": current, "start": start, "end": end,
            "insert_after": gap})
        assert r.status_code == 200
        current += 1
        order = reference_move(order, start, end, gap)

    got = [
        client.get("/line/lookup",
                   params={"version": current, "position": p}).json()
        ["original_trace"]
        for p in range(1, n + 1)
    ]
    assert got == order
    assert client.get("/line").json()["current_version"] == current
