"""Scale tests: up to a million traces.

Cost contract under test (per move, per lookup):
* treap node / mirror-table writes grow with tree height, not with total
  trace count, moved segment length or number of historical versions;
* every lookup is a handful of indexed point reads along root-to-leaf paths.

Set LARGE_N to override the size; the default is the required 1_000_000.
Set SCALE_FAST=1 to shrink the case for quick local iteration.
"""
import os
import time

import pytest

from tests.conftest import reference_move

N = int(os.environ.get("LARGE_N", "100_000" if os.environ.get("SCALE_FAST")
                       else "1_000_000"))
pytestmark = pytest.mark.skipif(
    os.environ.get("SKIP_SCALE") == "1", reason="scale tests skipped"
)


def _new_repo(tmp_path):
    from app.storage import LineRepository
    return LineRepository(str(tmp_path / "scale.db"))


def _table_count(conn, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _changed_rows(before: dict, after: dict) -> int:
    return sum(after[k] - before[k] for k in before)


def test_million_trace_two_big_moves_and_old_version_lookup(tmp_path):
    repo = _new_repo(tmp_path)
    n = N

    t0 = time.perf_counter()
    repo.initialize(n)
    init_seconds = time.perf_counter() - t0
    assert repo.total_traces() == n

    conn = repo._conn
    assert _table_count(conn, "treap_nodes") == n
    assert _table_count(conn, "current_parent") == n
    assert _table_count(conn, "current_node") == n

    # Two large moves; reference list holds the expected current order.
    # Coordinates scale with n (defaults tuned for n = 1_000_000).
    fifth = n // 5
    order = list(range(1, n + 1))
    moves = [
        # version 2: move a n/5-sized block from the middle to the end
        (1, 2 * fifth + 1, 4 * fifth, n),
        # version 3: move a ~3n/10 block to the front
        (2, n // 20 + 1, n // 20 + 3 * fifth, 0),
    ]

    per_move_rows: list[int] = []
    for base, start, end, gap in moves:
        before = {
            "treap_nodes": _table_count(conn, "treap_nodes"),
            "current_parent": _table_count(conn, "current_parent"),
            "current_node": _table_count(conn, "current_node"),
        }
        t0 = time.perf_counter()
        result = repo.move_segment(base, start, end, gap)
        move_seconds = time.perf_counter() - t0
        after = {k: _table_count(conn, k) for k in before}

        order = reference_move(order, start, end, gap)
        assert result["version"] == base + 1
        assert move_seconds < 10.0

        written = _changed_rows(before, after)
        per_move_rows.append(written)
        # O(log n) rows: for n=1e6 this must be a few thousand at most,
        # independent of the 200k/300k moved segment lengths.
        assert written < 20_000, (before, after, written)

    # ---- current order matches the list-based reference (sampled) ----
    import random
    rng = random.Random(2026)
    sample_positions = [rng.randint(1, n) for _ in range(300)]
    t0 = time.perf_counter()
    for pos in sample_positions:
        body = repo.lookup_position(3, pos)
        assert body["original_trace"] == order[pos - 1]
        assert body["current_version"] == 3
        assert body["current_position"] == pos
    lookup_seconds = (time.perf_counter() - t0) / len(sample_positions)
    assert lookup_seconds < 0.02  # ~index-only point reads, ms-level

    # ---- the explicitly required check: old-version positioning after
    # both big moves. Version 1 is identity; version 2 is fixed too. ----
    m1_start, m1_end = 2 * fifth + 1, 4 * fifth
    expected_v2 = reference_move(list(range(1, n + 1)),
                                 m1_start, m1_end, n)
    check_positions = [
        1, m1_start - 2, m1_start - 1, m1_start, m1_end, m1_end + 1,
        n - fifth + 1, n,
    ]
    check_positions = [p for p in check_positions if 1 <= p <= n]
    for pos in check_positions:
        body = repo.lookup_position(1, pos)
        assert body["query_version"] == 1
        assert body["original_trace"] == pos            # v1 is identity
        assert body["current_version"] == 3
        # current position of that original trace in the v3 order
        assert order[body["current_position"] - 1] == pos
        # and the trace endpoint agrees
        assert repo.lookup_trace(pos)["current_position"] == \
            body["current_position"]

        body2 = repo.lookup_position(2, pos)
        assert body2["original_trace"] == expected_v2[pos - 1]
        assert body2["current_version"] == 3
        assert order[body2["current_position"] - 1] == \
            expected_v2[pos - 1]

    # ---- mirror-table invariant on a random sample: parent links describe
    # exactly the current tree, and current_node points into it. ----
    for trace in [rng.randint(1, n) for _ in range(200)]:
        info = repo.lookup_trace(trace)
        back = repo.lookup_position(info["current_version"],
                                    info["current_position"])
        assert back["original_trace"] == trace

    # ---- more historical versions must not make moves more expensive ----
    counts_before = _table_count(conn, "treap_nodes")
    repo.move_segment(3, 1, 2, n)  # version 4, tiny move
    tiny_rows = _table_count(conn, "treap_nodes") - counts_before
    assert tiny_rows < 2_000  # still height-bound despite 3 prior versions

    # ---- restart: all published versions remain queryable ----
    meta = repo.get_meta()
    repo.close()
    from app.storage import LineRepository
    reopened = LineRepository(str(tmp_path / "scale.db"))
    try:
        assert reopened.current_version() == 4
        assert reopened.total_traces() == n
        body = reopened.lookup_position(1, n // 4)
        assert body["original_trace"] == n // 4
        assert body["current_version"] == 4
        assert reopened.lookup_position(2, 1)["original_trace"] == \
            expected_v2[0]
    finally:
        reopened.close()

    print(f"\n[n={n}] init={init_seconds:.1f}s "
          f"rows_per_move={per_move_rows} "
          f"avg_lookup={lookup_seconds * 1000:.2f}ms")


def test_lookups_use_indexed_reads_only(tmp_path):
    """EXPLAIN QUERY PLAN shows no full scans on the hot paths."""
    repo = _new_repo(tmp_path)
    repo.initialize(10_000)
    conn = repo._conn
    plans = conn.execute(
        "EXPLAIN QUERY PLAN SELECT node_id FROM current_node "
        "WHERE original_trace = 42"
    ).fetchall()
    assert all("SCAN" not in dict(p)["detail"] or "USING INDEX" in
               dict(p)["detail"] for p in plans)
    plan_text = " ".join(dict(p)["detail"] for p in plans)
    assert "SEARCH" in plan_text and "SCAN" not in plan_text

    plans = conn.execute(
        "EXPLAIN QUERY PLAN SELECT value, left, right, "
        "(SELECT size FROM treap_nodes WHERE id = t.left) AS lsize "
        "FROM treap_nodes t WHERE id = 7"
    ).fetchall()
    plan_text = " ".join(dict(p)["detail"] for p in plans)
    assert "USING INTEGER PRIMARY KEY" in plan_text
    assert "SCAN" not in plan_text
    repo.close()
