"""Unit tests for the windowing monoid (app/window.py).

A brute-force reference scans the sequence, so these tests pin the O(1)
combine used by the storage layer against an obviously correct definition.
"""
import random

from app.window import BEST, BEST_START, PREF, SUFF, combine, fold, leaf


def reference_window(seq: list[tuple[int, bool]]) -> tuple[int, int]:
    """Return (length, 1-based start) of the leftmost longest usable run."""
    best_len = best_start = cur = 0
    cur_start = 1
    prev_good = False
    prev_value = None
    for i, (value, is_bad) in enumerate(seq):
        # Continues the current run only when both seam traces are good
        # and the original ids are consecutive; otherwise starts afresh.
        continues = (not is_bad and prev_good and value == prev_value + 1)
        if not is_bad and not continues:
            cur_start = i + 1
        cur = cur + 1 if continues else (0 if is_bad else 1)
        if cur > best_len:
            best_len, best_start = cur, cur_start
        prev_good = not is_bad
        prev_value = value
    return best_len, best_start


def test_leaf_good_and_bad():
    good = leaf(7, False)
    assert good[BEST] == 1 and good[BEST_START] == 1
    assert good[PREF] == 1 and good[SUFF] == 1
    bad = leaf(7, True)
    assert bad[BEST] == 0 and bad[PREF] == 0 and bad[SUFF] == 0


def test_all_good_consecutive_sequence():
    seq = [(v, False) for v in range(10, 30)]
    agg = fold(seq)
    assert agg[BEST] == 20 and agg[BEST_START] == 1
    assert agg[PREF] == 20 and agg[SUFF] == 20


def test_bad_trace_splits_runs():
    seq = [(v, False) for v in range(1, 11)]
    seq[4] = (5, True)  # trace 5 is bad
    agg = fold(seq)
    # runs: [1..4] length 4, [6..10] length 5 -> leftmost longest starts at 6
    assert agg[BEST] == 5 and agg[BEST_START] == 6
    assert agg[PREF] == 4 and agg[SUFF] == 5


def test_nonconsecutive_original_ids_break_runs():
    seq = [(1, False), (2, False), (5, False), (6, False), (7, False)]
    agg = fold(seq)
    # runs: [1,2] len 2, [5,6,7] len 3
    assert agg[BEST] == 3 and agg[BEST_START] == 3
    assert agg[SUFF] == 3 and agg[PREF] == 2


def test_tie_keeps_leftmost_run():
    seq = [(1, False), (2, False), (9, False), (4, False), (5, False)]
    # runs: [1,2] len2 (start1), [9] len1 (start3), [4,5] len2 (start4)
    agg = fold(seq)
    assert agg[BEST] == 2 and agg[BEST_START] == 1


def test_all_bad_has_no_usable_trace():
    agg = fold([(i, True) for i in range(1, 6)])
    assert agg[BEST] == 0


def test_combine_matches_reference_random_cases():
    rng = random.Random(20261124)
    for _ in range(300):
        m = rng.randint(1, 40)
        seq = []
        prev = rng.randint(1, 5)
        for _ in range(m):
            value = prev + 1 if rng.random() < 0.8 else prev + rng.randint(2, 5)
            seq.append((value, rng.random() < 0.2))
            prev = value
        agg = fold(seq)
        ref_len, ref_start = reference_window(seq)
        assert (agg[BEST], agg[BEST_START]) == (ref_len, ref_start)


def test_fold_is_associative_under_different_groupings():
    rng = random.Random(77)
    seq = [(rng.randint(1, 100), rng.random() < 0.25) for _ in range(24)]
    whole = fold(seq)
    left_assoc = None
    for item in seq:
        left_assoc = combine(left_assoc, leaf(*item))
    mid = len(seq) // 2
    grouped = combine(fold(seq[:mid]), fold(seq[mid:]))
    assert left_assoc == whole
    assert grouped == whole
