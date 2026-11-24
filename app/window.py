"""Windowing monoid for the "longest usable segment" query.

A *usable* trace is one that

1. is not marked bad, and
2. together with the traces around it, carries consecutive original trace
   numbers (each next trace's original id is exactly one greater).

The adjacent-channel denoising trial needs the longest contiguous run of
usable traces inside an arbitrary position window of the *current* line.

This module defines the monoid summarising one treap subtree so that two
adjacent subtrees can be combined in O(1). The summary of a position range
is then a fold over the O(height) treap nodes that cover that range - no
per-trace scan and no comparison of segment lengths one by one.

An aggregate is a tuple ``(size, first, last, first_good, last_good,
all_consec, pref, suff, suff_start, best, best_start)`` where

* ``size``          - number of traces in the subtree;
* ``first``/``last`` - original trace numbers at the two ends;
* ``first_good``/``last_good`` - 1 iff the end traces are unmarked;
* ``all_consec``    - 1 iff original ids increase by exactly one through
                      the whole subtree (goodness is tracked separately);
* ``pref``          - length of the usable run starting at the first trace;
* ``suff``          - length of the usable run ending at the last trace;
* ``suff_start``    - 1-based offset of that suffix run inside the subtree;
* ``best``          - length of the longest usable run in the subtree;
* ``best_start``    - 1-based offset of that longest run. Ties keep the
                      leftmost run, which falls out of using strict ``>``
                      when comparing candidate lengths.

``None`` is the empty aggregate (identity element).
"""
from __future__ import annotations

from typing import Optional

# Tuple field indices, kept as constants so SQL row code stays readable.
(SIZE, FIRST, LAST, FIRST_GOOD, LAST_GOOD, ALL_CONSEC,
 PREF, SUFF, SUFF_START, BEST, BEST_START) = range(11)

Agg = Optional[tuple]


def leaf(value: int, is_bad: bool) -> tuple:
    """Aggregate of a single-trace subtree."""
    good = 0 if is_bad else 1
    # suff_start is vacuously size - suff + 1; with suff == 0 that is 2.
    return (1, value, value, good, good, 1,
            good, good, 1 if good else 2, good, 1)


def combine(a: Agg, b: Agg) -> Agg:
    """Associative merge of two adjacent, non-overlapping aggregates.

    Traces of ``a`` all precede traces of ``b``. A run may cross the seam
    only when both seam traces are good and ``b.first == a.last + 1``.
    """
    if a is None:
        return b
    if b is None:
        return a
    seam_ok = bool(a[LAST_GOOD] and b[FIRST_GOOD]
                   and b[FIRST] == a[LAST] + 1)
    all_consec = int(bool(
        a[ALL_CONSEC] and b[ALL_CONSEC] and b[FIRST] == a[LAST] + 1
    ))

    # Prefix run: reaches into b only when every trace of a belongs to one
    # usable run (its prefix covers the whole subtree) and the seam is
    # itself usable.
    if a[PREF] == a[SIZE] and seam_ok:
        pref = a[SIZE] + b[PREF]
    else:
        pref = a[PREF]

    # Suffix run: reaches back into a only when b is fully one usable run
    # and the seam is usable.
    if b[SUFF] == b[SIZE] and seam_ok:
        suff = b[SIZE] + a[SUFF]
        suff_start = a[SUFF_START]
    else:
        suff = b[SUFF]
        suff_start = a[SIZE] + b[SUFF_START]

    # Longest run: each side's best, plus the joined run at the seam when
    # it is usable. Strict comparison keeps the earliest run on ties.
    best = a[BEST]
    best_start = a[BEST_START]
    if b[BEST] > best:
        best = b[BEST]
        best_start = a[SIZE] + b[BEST_START]
    if seam_ok:
        joined = a[SUFF] + b[PREF]
        if joined > best:
            best = joined
            best_start = a[SUFF_START]

    return (
        a[SIZE] + b[SIZE],
        a[FIRST], b[LAST],
        a[FIRST_GOOD], b[LAST_GOOD],
        all_consec,
        pref, suff, suff_start, best, best_start,
    )


def fold(values: list[tuple[int, bool]]) -> Agg:
    """Fold ``(original_trace, is_bad)`` leaves in sequence order.

    Mainly a convenience for tests and reference checks.
    """
    acc: Agg = None
    for value, is_bad in values:
        acc = combine(acc, leaf(value, is_bad))
    return acc
