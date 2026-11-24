"""Persistent implicit treap primitives (split / merge).

A treap node stores the *original trace number* as its value and the subtree
size, so the k-th position is reached in O(height) steps.

Nodes are immutable once persisted: every structural change allocates new
nodes along the affected paths and shares every unchanged subtree with the
previous version. A published version is therefore fully identified by its
root node id and remains reachable forever.

Node id ``0`` is the null node.
"""
from __future__ import annotations

import random
import sys
from dataclasses import dataclass

sys.setrecursionlimit(10000)


@dataclass(frozen=True, slots=True)
class Node:
    id: int
    value: int
    priority: int
    left: int
    right: int
    size: int


class TreapStore:
    """Pluggable storage backend used by :func:`split` and :func:`merge`."""

    def get_node(self, node_id: int) -> Node:
        raise NotImplementedError

    def size(self, node_id: int) -> int:
        raise NotImplementedError

    def new_node(self, *, value: int, priority: int,
                 left: int, right: int, size: int) -> int:
        """Persist a brand-new node and return its id."""
        raise NotImplementedError


_rng = random.Random()


def random_priority() -> int:
    return _rng.getrandbits(63)


def split(store: TreapStore, root: int, k: int) -> tuple[int, int]:
    """Split ``root`` into (first ``k`` elements, remaining elements)."""
    if root == 0:
        return 0, 0
    node = store.get_node(root)
    left_size = store.size(node.left)
    if k <= left_size:
        first, second = split(store, node.left, k)
        new_root = store.new_node(
            value=node.value, priority=node.priority,
            left=second, right=node.right,
            size=store.size(second) + store.size(node.right) + 1,
        )
        return first, new_root
    if k > left_size + 1:
        first, second = split(store, node.right, k - left_size - 1)
        new_root = store.new_node(
            value=node.value, priority=node.priority,
            left=node.left, right=first,
            size=store.size(node.left) + store.size(first) + 1,
        )
        return new_root, second
    # k == left_size + 1: the first k elements are the left subtree plus
    # this node, so the cut sits immediately after it.
    detached = store.new_node(
        value=node.value, priority=node.priority,
        left=node.left, right=0,
        size=left_size + 1,
    )
    return detached, node.right


def merge(store: TreapStore, a: int, b: int) -> int:
    """Merge two trees where every element of ``a`` precedes every one of ``b``."""
    if a == 0:
        return b
    if b == 0:
        return a
    na = store.get_node(a)
    nb = store.get_node(b)
    if na.priority > nb.priority:
        new_right = merge(store, na.right, b)
        return store.new_node(
            value=na.value, priority=na.priority,
            left=na.left, right=new_right,
            size=store.size(na.left) + store.size(new_right) + 1,
        )
    new_left = merge(store, a, nb.left)
    return store.new_node(
        value=nb.value, priority=nb.priority,
        left=new_left, right=nb.right,
        size=store.size(new_left) + store.size(nb.right) + 1,
    )
