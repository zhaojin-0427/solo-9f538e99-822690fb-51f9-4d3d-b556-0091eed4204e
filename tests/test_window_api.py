"""End-to-end tests for bad-trace flags and window picking.

Covers the required workflow:

* flags are registered/cleared by *original* trace number;
* a segment move relocates flags together with their original traces;
* the window endpoint returns the leftmost longest usable segment
  (unmarked + consecutive original ids), or an empty segment;
* one query reads order and flags from one committed snapshot;
* a flipped flag only recomputes one root-to-leaf path;
* flags survive a process restart; a failed write changes nothing.
"""
import importlib
import sqlite3

from fastapi.testclient import TestClient

from tests.conftest import reference_move


# --------------------------------------------------------------- reference
def reference_window(order: list[int], bad: set[int],
                     lo: int, hi: int) -> dict | None:
    """Brute-force longest usable run inside inclusive positions [lo, hi]."""
    best_len = best_start = cur = 0
    cur_start = lo
    prev_good = False
    prev_value = None
    for pos in range(lo, hi + 1):
        trace = order[pos - 1]
        # Continues the current run only when both seam traces are good
        # and the original ids are consecutive; otherwise starts afresh.
        # The first trace of the window has no in-window predecessor.
        continues = (pos > lo and trace not in bad and prev_good
                     and trace == prev_value + 1)
        if trace in bad:
            cur = 0
        else:
            if not continues:
                cur_start = pos
            cur = cur + 1 if continues else 1
        if cur > best_len:
            best_len, best_start = cur, cur_start
        prev_good = trace not in bad
        prev_value = trace
    if best_len == 0:
        return None
    return {
        "start_position": best_start,
        "end_position": best_start + best_len - 1,
        "start_original_trace": order[best_start - 1],
        "end_original_trace": order[best_start + best_len - 2],
        "length": best_len,
    }


def assert_window_matches(client, order, bad, lo, hi, version):
    r = client.get("/line/window", params={"start": lo, "end": hi})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["version"] == version
    assert body["window"] == [lo, hi]
    expected = reference_window(order, bad, lo, hi)
    assert body["segment"] == expected


# ------------------------------------------------------------------ marking
def test_mark_and_clear_bad_trace_by_original_number(client):
    n = 20
    client.post("/line/initialize", json={"total_traces": n})

    r = client.put("/line/bad-traces/7")
    assert r.status_code == 200
    assert r.json() == {"original_trace": 7, "bad": True, "changed": True}
    # Idempotent re-marking: no change reported.
    r = client.put("/line/bad-traces/7")
    assert r.json()["changed"] is False

    r = client.get("/line/bad-traces")
    assert r.json() == {"total_traces": n, "count": 1, "bad_traces": [7]}

    r = client.delete("/line/bad-traces/7")
    assert r.json() == {"original_trace": 7, "bad": False, "changed": True}
    r = client.delete("/line/bad-traces/7")  # idempotent clear
    assert r.json()["changed"] is False
    assert client.get("/line/bad-traces").json()["count"] == 0


def test_bad_trace_validation(client):
    client.post("/line/initialize", json={"total_traces": 10})
    assert client.put("/line/bad-traces/0").status_code == 422
    assert client.put("/line/bad-traces/11").status_code == 422
    assert client.delete("/line/bad-traces/-1").status_code == 422
    assert client.get("/line/window",
                      params={"start": 0, "end": 5}).status_code == 422
    assert client.get("/line/window",
                      params={"start": 6, "end": 5}).status_code == 422
    assert client.get("/line/window",
                      params={"start": 1, "end": 11}).status_code == 422


def test_endpoints_before_initialization(app_module):
    main, _ = app_module
    client = TestClient(main.app, raise_server_exceptions=False)
    assert client.put("/line/bad-traces/1").status_code == 404
    assert client.get("/line/window",
                      params={"start": 1, "end": 1}).status_code == 404
    assert client.get("/line/bad-traces").status_code == 404


# ----------------------------------------------------------------- windows
def test_window_without_bad_traces_returns_whole_range(client):
    n = 30
    client.post("/line/initialize", json={"total_traces": n})
    r = client.get("/line/window", params={"start": 5, "end": 20})
    assert r.json()["segment"] == {
        "start_position": 5, "end_position": 20,
        "start_original_trace": 5, "end_original_trace": 20, "length": 16,
    }
    # Single-position window.
    r = client.get("/line/window", params={"start": 8, "end": 8})
    assert r.json()["segment"]["length"] == 1
    assert r.json()["segment"]["start_original_trace"] == 8


def test_bad_trace_splits_window_and_tie_goes_leftmost(client):
    n = 20
    client.post("/line/initialize", json={"total_traces": n})
    # Mark trace 10 bad: runs [1..9] and [11..20].
    client.put("/line/bad-traces/10")
    r = client.get("/line/window", params={"start": 1, "end": n})
    # lengths 9 and 10 -> right side wins
    assert r.json()["segment"] == {
        "start_position": 11, "end_position": 20,
        "start_original_trace": 11, "end_original_trace": 20, "length": 10,
    }
    # Balanced runs around trace 10: tie keeps the left one.
    r = client.get("/line/window", params={"start": 8, "end": 12})
    assert r.json()["segment"]["start_position"] == 8
    assert r.json()["segment"]["length"] == 2  # traces 8,9


def test_window_with_no_usable_trace_returns_empty(client):
    n = 5
    client.post("/line/initialize", json={"total_traces": n})
    for t in (1, 2, 3):
        client.put(f"/line/bad-traces/{t}")
    r = client.get("/line/window", params={"start": 1, "end": 3})
    assert r.status_code == 200
    body = r.json()
    assert body["segment"] is None and body["version"] == 1


def test_nonconsecutive_original_ids_break_usable_runs(client):
    n = 10
    client.post("/line/initialize", json={"total_traces": n})
    # Move trace 1 to the end: 2,3,...,10,1 -> the seam 10->1 breaks runs.
    client.post("/line/move", json={
        "base_version": 1, "start": 1, "end": 1, "insert_after": n})
    order = reference_move(list(range(1, n + 1)), 1, 1, n)
    r = client.get("/line/window", params={"start": 1, "end": n})
    assert r.json()["segment"] == {
        "start_position": 1, "end_position": 9,
        "start_original_trace": 2, "end_original_trace": 10, "length": 9,
    }
    assert reference_window(order, set(), 1, n)["start_original_trace"] == 2


# ----------------------------------------------------------- move + flags
def test_flags_follow_original_traces_across_moves(client):
    n = 30
    client.post("/line/initialize", json={"total_traces": n})
    # Flag original traces 5 and 12 before any move.
    bad = {5, 12}
    for t in bad:
        client.put(f"/line/bad-traces/{t}")

    order = list(range(1, n + 1))
    moves = [
        (1, 4, 14, n),     # big middle block to the end
        (2, 1, 3, 20),     # small block inward
        (3, n - 2, n, 0),  # tail to the front
    ]
    version = 1
    for base, start, end, gap in moves:
        r = client.post("/line/move", json={
            "base_version": base, "start": start, "end": end,
            "insert_after": gap})
        assert r.status_code == 200
        version += 1
        order = reference_move(order, start, end, gap)

    # The flag set is still expressed by original trace numbers and did
    # not move with positions: bad_traces table is untouched.
    assert set(client.get("/line/bad-traces").json()["bad_traces"]) == bad

    # Exhaustive window check over many sub-ranges against the brute-force
    # reference, including windows containing the relocated bad traces.
    for lo, hi in [(1, n), (1, 10), (5, 25), (11, 20), (20, 30),
                   (1, 1), (n, n), (3, 8), (15, 30)]:
        assert_window_matches(client, order, bad, lo, hi, version)

    # Clear one flag after the moves: the run containing that trace merges.
    client.delete("/line/bad-traces/5")
    bad.discard(5)
    assert_window_matches(client, order, bad, 1, n, version)
    assert_window_matches(client, order, bad, 1, 15, version)


def test_clear_flag_restores_full_run(client):
    n = 15
    client.post("/line/initialize", json={"total_traces": n})
    client.put("/line/bad-traces/8")
    assert client.get("/line/window",
                      params={"start": 1, "end": n}).json()["segment"][
        "length"] == 7  # traces 9..15
    client.delete("/line/bad-traces/8")
    body = client.get("/line/window",
                      params={"start": 1, "end": n}).json()
    assert body["segment"]["length"] == 15
    assert (body["segment"]["start_original_trace"],
            body["segment"]["end_original_trace"]) == (1, 15)


def test_marking_does_not_create_a_version(client):
    client.post("/line/initialize", json={"total_traces": 20})
    client.put("/line/bad-traces/3")
    client.delete("/line/bad-traces/3")
    meta = client.get("/line").json()
    assert meta["current_version"] == 1
    assert [v["version"] for v in
            client.get("/line/versions").json()["versions"]] == [1]


# ------------------------------------------------------------- invariants
def test_current_agg_invariant_after_moves_and_flags(client, repo):
    n = 60
    client.post("/line/initialize", json={"total_traces": n})
    for t in (4, 19, 47):
        client.put(f"/line/bad-traces/{t}")
    client.post("/line/move", json={
        "base_version": 1, "start": 10, "end": 30, "insert_after": n})
    client.put("/line/bad-traces/55")
    client.delete("/line/bad-traces/19")

    # Recompute every stored summary bottom-up from treap structure plus the
    # bad-traces table and compare against current_agg.
    conn = repo._conn
    bad = {r[0] for r in conn.execute(
        "SELECT original_trace FROM bad_traces")}
    from app.window import combine, leaf
    cache = {}

    def check(node_id):
        if node_id == 0:
            return None
        value, left, right = conn.execute(
            "SELECT value, left, right FROM treap_nodes WHERE id = ?",
            (node_id,)).fetchone()
        agg = combine(check(int(left)),
                      combine(leaf(int(value), int(value) in bad),
                              check(int(right))))
        cache[node_id] = agg
        stored = tuple(conn.execute(
            "SELECT size, first_trace, last_trace, first_good, last_good, "
            "all_consec, pref, suff, suff_start, best, best_start "
            "FROM current_agg WHERE node_id = ?",
            (node_id,)).fetchone())
        assert stored == agg, node_id
        return agg

    root = conn.execute("SELECT current_root FROM meta WHERE id = 0"
                        ).fetchone()[0]
    check(root)


def test_flag_flip_only_touches_one_tree_path(app_module, monkeypatch):
    """A single flag change writes O(height) aggregate rows, never O(n)."""
    main, _ = app_module
    repo = main.repo
    n = 2_000
    repo.initialize(n)
    conn = repo._conn
    before = conn.execute("SELECT COUNT(*) FROM current_agg").fetchone()[0]
    repo.set_bad_trace(1_234, True)
    # No current_agg rows are added or removed: the path is updated in place.
    after = conn.execute("SELECT COUNT(*) FROM current_agg").fetchone()[0]
    assert before == after
    repo.set_bad_trace(1_234, False)
    assert conn.execute("SELECT COUNT(*) FROM current_agg"
                        ).fetchone()[0] == before
    # bad_traces holds exactly the live flags
    assert conn.execute("SELECT COUNT(*) FROM bad_traces").fetchone()[0] == 0


def test_move_does_not_rebuild_aggregates_of_shared_subtrees(client, repo):
    """A move inserts summaries only for newly allocated treap nodes."""
    n = 5_000
    client.post("/line/initialize", json={"total_traces": n})
    client.put("/line/bad-traces/2_500")
    conn = repo._conn
    total_before = conn.execute(
        "SELECT COUNT(*) FROM current_agg").fetchone()[0]
    # Count new treap nodes the move allocates.
    node_before = conn.execute(
        "SELECT COALESCE(MAX(id), 0) FROM treap_nodes").fetchone()[0]
    client.post("/line/move", json={
        "base_version": 1, "start": 1_000, "end": 4_000,
        "insert_after": n})
    new_nodes = conn.execute(
        "SELECT COUNT(*) FROM treap_nodes WHERE id > ?",
        (node_before,)).fetchone()[0]
    total_after = conn.execute(
        "SELECT COUNT(*) FROM current_agg").fetchone()[0]
    # The only current_agg rows added belong to the newly allocated nodes;
    # the 3,001-trace moved block's shared subtrees were not rebuilt.
    assert total_after - total_before == new_nodes
    assert new_nodes < 500  # O(height), independent of the 3k block


def test_snapshot_query_does_not_see_concurrent_flag_change(app_module):
    """A window query opened before a flag write keeps the old snapshot."""
    main, _ = app_module
    repo = main.repo
    repo.initialize(40)
    reader = sqlite3.connect(repo.path, isolation_level=None)
    reader.execute("PRAGMA query_only=ON")
    reader.row_factory = sqlite3.Row
    reader.execute("BEGIN")
    # Snapshot opened while no flags exist.
    row = reader.execute(
        "SELECT COUNT(*) AS c FROM bad_traces").fetchone()
    assert row["c"] == 0

    repo.set_bad_trace(20, True)  # commits in another connection

    # The still-open snapshot must not see the new flag.
    assert reader.execute(
        "SELECT COUNT(*) AS c FROM bad_traces").fetchone()["c"] == 0
    reader.execute("COMMIT")
    reader.close()
    # A fresh snapshot sees it, and the window reflects the split.
    body = repo.find_window(1, 40)
    assert body["segment"]["length"] == 20  # traces 21..40


def test_marker_persistence_across_restart(app_module):
    main, db_path = app_module
    with TestClient(main.app) as c:
        c.post("/line/initialize", json={"total_traces": 40})
        c.put("/line/bad-traces/13")
        c.post("/line/move", json={
            "base_version": 1, "start": 1, "end": 5, "insert_after": 40})
        before = c.get("/line/window",
                       params={"start": 1, "end": 40}).json()
    main.repo.close()

    import app.main as main2
    importlib.reload(main2)
    try:
        with TestClient(main2.app) as c:
            assert set(c.get("/line/bad-traces").json()["bad_traces"]) == {13}
            after = c.get("/line/window",
                          params={"start": 1, "end": 40}).json()
            assert after == before
            # Aggregates work after restart: clearing the flag restores the
            # longest run of consecutive original ids. The move put 1..5 at
            # the end (order 6..40,1..5), so the 40 -> 1 seam still bounds
            # the longest run to traces 6..40 (length 35).
            c.delete("/line/bad-traces/13")
            body = c.get("/line/window",
                         params={"start": 1, "end": 40}).json()
            assert body["segment"]["length"] == 35
            assert (body["segment"]["start_original_trace"],
                    body["segment"]["end_original_trace"]) == (6, 40)
    finally:
        main2.repo.close()


def test_failed_flag_write_is_fully_rolled_back(app_module, monkeypatch):
    main, _ = app_module
    repo = main.repo
    repo.initialize(20)
    client = TestClient(main.app, raise_server_exceptions=False)

    def boom(*_a, **_kw):
        raise sqlite3.OperationalError("simulated disk failure")

    monkeypatch.setattr(type(repo), "_recompute_agg_path", boom)
    r = client.put("/line/bad-traces/5")
    assert r.status_code == 500
    # Neither the flag nor any partial aggregate survived.
    assert client.get("/line/bad-traces").json()["count"] == 0
    body = client.get("/line/window",
                      params={"start": 1, "end": 20}).json()
    assert body["segment"]["length"] == 20

    monkeypatch.undo()
    r = client.put("/line/bad-traces/5")
    assert r.status_code == 200 and r.json()["changed"] is True
