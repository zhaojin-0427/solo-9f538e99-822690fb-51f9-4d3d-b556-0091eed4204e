"""Unit tests for the persistent implicit treap primitives."""
import random

import pytest

from app.treap import Node, TreapStore, merge, split


class MemoryStore(TreapStore):
    def __init__(self):
        self.nodes: dict[int, Node] = {}
        self._next = 1
        self.created = 0

    def get_node(self, node_id: int) -> Node:
        return self.nodes[node_id]

    def size(self, node_id: int) -> int:
        return 0 if node_id == 0 else self.nodes[node_id].size

    def new_node(self, *, value, priority, left, right, size) -> int:
        node_id = self._next
        self._next += 1
        self.created += 1
        self.nodes[node_id] = Node(node_id, value, priority, left, right, size)
        return node_id


def inorder(store: MemoryStore, root: int) -> list[int]:
    out: list[int] = []
    stack: list[int | tuple] = [root]
    while stack:
        cur = stack.pop()
        if isinstance(cur, tuple):
            out.append(cur[0])
            continue
        node = store.nodes[cur]
        if node.right:
            stack.append(node.right)
        stack.append((node.value,))
        if node.left:
            stack.append(node.left)
    return out


def build(store: MemoryStore, values: list[int]) -> int:
    root = 0
    for v in values:
        root = merge(store, root, store.new_node(
            value=v, priority=random.getrandbits(63),
            left=0, right=0, size=1))
    return root


def check_heap_and_sizes(store: MemoryStore, root: int) -> None:
    if root == 0:
        return
    node = store.nodes[root]
    assert node.size == store.size(node.left) + store.size(node.right) + 1
    if node.left:
        assert node.priority > store.nodes[node.left].priority
        check_heap_and_sizes(store, node.left)
    if node.right:
        assert node.priority > store.nodes[node.right].priority
        check_heap_and_sizes(store, node.right)


def test_split_merge_preserve_order_and_heap():
    random.seed(7)
    store = MemoryStore()
    root = build(store, list(range(1, 201)))
    check_heap_and_sizes(store, root)

    a, b = split(store, root, 73)
    assert inorder(store, a) == list(range(1, 74))
    assert inorder(store, b) == list(range(74, 201))
    check_heap_and_sizes(store, a)
    check_heap_and_sizes(store, b)

    rejoined = merge(store, a, b)
    assert inorder(store, rejoined) == list(range(1, 201))
    check_heap_and_sizes(store, rejoined)


def test_persistence_keeps_old_root_intact():
    random.seed(11)
    store = MemoryStore()
    root = build(store, list(range(1, 101)))
    before = inorder(store, root)

    a, b = split(store, root, 40)
    new_root = merge(store, b, a)

    # Old root still describes the original sequence.
    assert inorder(store, root) == before
    assert inorder(store, new_root) == list(range(41, 101)) + list(range(1, 41))
    check_heap_and_sizes(store, new_root)


def test_split_edge_cases():
    random.seed(3)
    store = MemoryStore()
    root = build(store, list(range(1, 11)))
    a, b = split(store, root, 0)
    assert a == 0 and inorder(store, b) == list(range(1, 11))
    a, b = split(store, root, 10)
    assert inorder(store, a) == list(range(1, 11)) and b == 0


def test_move_semantics_via_treap_operations():
    """Cut [s, e] and reinsert after gap, matching repository semantics."""
    random.seed(21)
    values = list(range(1, 21))
    store = MemoryStore()
    root = build(store, values)

    s, e, insert_after = 4, 8, 15
    left, rest = split(store, root, s - 1)
    segment, right = split(store, rest, e - s + 1)
    gap = insert_after - (e - s + 1) if insert_after >= e else insert_after
    before, after = split(store, merge(store, left, right), gap)
    new_root = merge(store, merge(store, before, segment), after)

    expected = [1, 2, 3, 9, 10, 11, 12, 13, 14, 15,
                4, 5, 6, 7, 8, 16, 17, 18, 19, 20]
    assert inorder(store, new_root) == expected


def test_node_allocation_is_logarithmic():
    random.seed(31)
    store = MemoryStore()
    root = build(store, list(range(1, 10_001)))
    baseline = store.created
    for k in (10, 5_000, 9_999):
        a, b = split(store, root, k)
        merge(store, a, b)
    per_op = (store.created - baseline) / 3
    assert per_op < 200  # far below n = 10k; expected ~4 log2(n)
