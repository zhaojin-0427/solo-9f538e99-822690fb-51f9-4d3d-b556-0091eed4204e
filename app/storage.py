"""SQLite-backed persistence for the single survey line ordering service.

Schema
------
meta
    Singleton row (id = 0): total trace count, current committed version
    number and the root node id of that version.

versions
    One immutable row per published version. ``base_version`` is the version
    the client based the move on; a move whose base is no longer current is
    stale and rejected, so no new version is produced.

treap_nodes
    Immutable persistent-treap nodes shared across versions. A move only
    allocates O(log n) nodes because every untouched subtree is shared.
    Node id 0 is the null node.

current_parent
    Mutable mirror describing *only* the current tree: node id -> parent
    node id (a root's parent is 0). A move rewrites only the O(log n) rows
    on the changed root-to-leaf paths. This is what makes current-position
    lookup and segment moves independent of total trace count, segment
    length and the number of historical versions.

current_node
    Mutable mirror: original trace number -> node id in the current tree,
    likewise O(log n) rewrites per move.

bad_traces
    Original trace numbers flagged as bad. Keyed by *original* trace number,
    not by position, so a move never has to touch these rows: the flag stays
    attached to the physical trace wherever the move relocates it.

current_agg
    Mutable windowing mirror: for every node of the current tree, the
    windowing monoid summary (see :mod:`app.window`) of its subtree.
    A move inserts summaries only for the O(log n) newly allocated nodes -
    shared subtrees keep their rows untouched. Flipping one bad-trace flag
    recomputes only the O(log n) nodes on that trace's root-to-leaf path,
    so neither operation rescans the line nor rebuilds the moved block.
    Rows for nodes that left the current tree are retained but unreferenced.

Concurrency
-----------
A process-local writer lock serialises moves and flag changes; each write
is one SQL transaction, so a failed write rolls back completely and never
creates a version or a partial flag change. Readers open their own snapshot
transactions (WAL mode); an entire lookup or window query therefore sees one
committed state - the trace order and the bad-trace flags come from the same
snapshot - even while a write is in flight.
"""
from __future__ import annotations

import sqlite3
import threading

from .treap import Node, TreapStore, merge, random_priority, split
from .window import BEST, BEST_START, combine, leaf


class StaleVersionError(Exception):
    """The client based its move on a version that is no longer current."""


class InvalidMoveError(Exception):
    """The requested positions are out of range or inconsistent."""


class NotFoundError(Exception):
    """No such line / version / position."""


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    id INTEGER PRIMARY KEY CHECK (id = 0),
    total_traces INTEGER NOT NULL,
    current_version INTEGER NOT NULL,
    current_root INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS versions (
    version INTEGER PRIMARY KEY,
    root_node INTEGER NOT NULL,
    base_version INTEGER NOT NULL,
    move_start INTEGER,
    move_end INTEGER,
    insert_after INTEGER,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE TABLE IF NOT EXISTS treap_nodes (
    id INTEGER PRIMARY KEY,
    value INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    left INTEGER NOT NULL,
    right INTEGER NOT NULL,
    size INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS current_parent (
    node_id INTEGER PRIMARY KEY,
    parent_id INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS current_node (
    original_trace INTEGER PRIMARY KEY,
    node_id INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS bad_traces (
    original_trace INTEGER PRIMARY KEY
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS current_agg (
    node_id INTEGER PRIMARY KEY,
    size INTEGER NOT NULL,
    first_trace INTEGER NOT NULL,
    last_trace INTEGER NOT NULL,
    first_good INTEGER NOT NULL,
    last_good INTEGER NOT NULL,
    all_consec INTEGER NOT NULL,
    pref INTEGER NOT NULL,
    suff INTEGER NOT NULL,
    suff_start INTEGER NOT NULL,
    best INTEGER NOT NULL,
    best_start INTEGER NOT NULL
);
"""

AGG_COLUMNS = (
    "size", "first_trace", "last_trace", "first_good", "last_good",
    "all_consec", "pref", "suff", "suff_start", "best", "best_start",
)


class EagerSqliteStore(TreapStore):
    """Treap store that allocates real ids and batched-INSERTs new nodes."""

    BATCH = 1000

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._cache: dict[int, Node] = {}
        self._next_id = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM treap_nodes"
        ).fetchone()[0] + 1
        self._pending: list[Node] = []

    @property
    def next_id(self) -> int:
        """Id that the next allocated node will receive."""
        return self._next_id

    def get_node(self, node_id: int) -> Node:
        if node_id == 0:
            raise KeyError(0)
        node = self._cache.get(node_id)
        if node is None:
            row = self._conn.execute(
                "SELECT id, value, priority, left, right, size "
                "FROM treap_nodes WHERE id = ?",
                (node_id,),
            ).fetchone()
            if row is None:  # pragma: no cover - defensive
                raise KeyError(node_id)
            node = Node(*row)
            self._cache[node_id] = node
        return node

    def size(self, node_id: int) -> int:
        return 0 if node_id == 0 else self.get_node(node_id).size

    def new_node(self, *, value, priority, left, right, size) -> int:
        node = Node(
            id=self._next_id, value=value, priority=priority,
            left=left, right=right, size=size,
        )
        self._next_id += 1
        self._cache[node.id] = node
        self._pending.append(node)
        if len(self._pending) >= self.BATCH:
            self.flush()
        return node.id

    def flush(self) -> None:
        if not self._pending:
            return
        self._conn.executemany(
            "INSERT INTO treap_nodes (id, value, priority, left, right, size) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [(n.id, n.value, n.priority, n.left, n.right, n.size)
             for n in self._pending],
        )
        self._pending.clear()


class LineRepository:
    """All database access for the one maintained survey line."""

    def __init__(self, path: str):
        self.path = path
        self._writer_lock = threading.Lock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)

    # ------------------------------------------------------------- lifecycle
    def initialize(self, total_traces: int) -> int:
        """Create version 1 with traces 1..total_traces in original order."""
        if total_traces < 1:
            raise InvalidMoveError("total_traces must be >= 1")
        with self._writer_lock:
            if self._conn.execute(
                "SELECT EXISTS(SELECT 1 FROM meta)"
            ).fetchone()[0]:
                raise InvalidMoveError("line is already initialized")
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                root = self._build_initial_tree(total_traces)
                self._conn.execute(
                    "INSERT INTO meta (id, total_traces, current_version, "
                    "current_root) VALUES (0, ?, 1, ?)",
                    (total_traces, root),
                )
                self._conn.execute(
                    "INSERT INTO versions (version, root_node, base_version, "
                    "move_start, move_end, insert_after) "
                    "VALUES (1, ?, 0, NULL, NULL, NULL)",
                    (root,),
                )
                self._populate_initial_mirrors(root)
                self._populate_initial_agg()
                self._conn.execute("COMMIT")
                return 1
            except Exception:
                self._rollback_safely()
                raise

    def is_initialized(self) -> bool:
        return self._conn.execute(
            "SELECT EXISTS(SELECT 1 FROM meta)"
        ).fetchone()[0] == 1

    def total_traces(self) -> int:
        return int(self.get_meta()["total_traces"])

    def current_version(self) -> int:
        return int(self.get_meta()["current_version"])

    def close(self) -> None:
        self._conn.close()

    def _build_initial_tree(self, n: int) -> int:
        """O(n) balanced treap over values 1..n with random heap priorities.

        The full node list is kept in memory (~n small tuples) so the mirror
        tables can be populated without issuing n point queries.
        """
        conn = self._conn
        next_id = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM treap_nodes"
        ).fetchone()[0] + 1
        rows: list[tuple] = []

        def build(lo: int, hi: int) -> tuple[int, int]:
            # Returns (node id, subtree size); no node reads are needed, so
            # building a million-node tree never loads nodes into cache.
            if lo > hi:
                return 0, 0
            nonlocal next_id
            mid = (lo + hi) // 2
            left, lsize = build(lo, mid - 1)
            right, rsize = build(mid + 1, hi)
            node_id = next_id
            next_id += 1
            rows.append(
                (node_id, mid, random_priority(), left, right,
                 lsize + rsize + 1)
            )
            return node_id, lsize + rsize + 1

        root, _ = build(1, n)
        for i in range(0, len(rows), 1000):
            conn.executemany(
                "INSERT INTO treap_nodes (id, value, priority, left, right, "
                "size) VALUES (?, ?, ?, ?, ?, ?)",
                rows[i:i + 1000],
            )
        self._initial_rows = rows
        return root

    def _populate_initial_mirrors(self, root: int) -> None:
        """One-time O(n) fill of the current-version mirror tables, driven by
        the in-memory node list from :meth:`_build_initial_tree`."""
        conn = self._conn
        rows = self._initial_rows
        by_id: dict[int, tuple] = {r[0]: r for r in rows}
        parent_rows: list[tuple] = []
        trace_rows: list[tuple] = []
        stack: list[tuple[int, int]] = [(root, 0)]
        while stack:
            node_id, parent_id = stack.pop()
            r = by_id[node_id]
            parent_rows.append((node_id, parent_id))
            trace_rows.append((r[1], node_id))  # value == original trace
            if r[4]:
                stack.append((r[4], node_id))  # right
            if r[3]:
                stack.append((r[3], node_id))  # left
        for i in range(0, len(parent_rows), 1000):
            conn.executemany(
                "INSERT INTO current_parent (node_id, parent_id) VALUES (?, ?)",
                parent_rows[i:i + 1000],
            )
            conn.executemany(
                "INSERT INTO current_node (original_trace, node_id) "
                "VALUES (?, ?)",
                trace_rows[i:i + 1000],
            )
        # self._initial_rows is released by _populate_initial_agg, which
        # runs next and still needs it.

    def _populate_initial_agg(self) -> None:
        """One-time O(n) fill of current_agg.

        At initialization no trace is bad, so every leaf is a good
        singleton. Nodes are allocated post-order (children before their
        parent), hence each row's children already have a summary when the
        row is visited; the whole fill runs on in-memory data.
        """
        conn = self._conn
        rows = self._initial_rows
        aggs: dict[int, tuple] = {}
        out: list[tuple] = []
        for node_id, value, _prio, left, right, _size in rows:
            agg = combine(
                aggs.get(left),
                combine(leaf(value, False), aggs.get(right)),
            )
            aggs[node_id] = agg
            out.append((node_id, *agg))
        for i in range(0, len(out), 1000):
            conn.executemany(
                "INSERT INTO current_agg (node_id, " + ", ".join(AGG_COLUMNS)
                + ") VALUES (" + ", ".join("?" * (len(AGG_COLUMNS) + 1)) + ")",
                out[i:i + 1000],
            )
        del aggs
        del self._initial_rows  # mirrors and aggregates are now persisted

    # ---------------------------------------------------------------- reads
    def _read_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        conn.execute("PRAGMA query_only=ON")
        conn.row_factory = sqlite3.Row
        return conn

    def get_meta(self) -> dict:
        conn = self._read_conn()
        try:
            row = conn.execute(
                "SELECT * FROM meta WHERE id = 0"
            ).fetchone()
            if row is None:
                raise NotFoundError("line has not been initialized")
            return dict(row)
        finally:
            conn.close()

    def list_versions(self) -> list[dict]:
        conn = self._read_conn()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT version, base_version, move_start, move_end, "
                "insert_after, created_at FROM versions ORDER BY version"
            )]
        finally:
            conn.close()

    def lookup_position(self, version: int, position: int) -> dict:
        """Original trace at ``position`` of any published version, plus its
        current version and position. One snapshot transaction throughout."""
        conn = self._read_conn()
        try:
            conn.execute("BEGIN")
            meta = conn.execute(
                "SELECT total_traces, current_version, current_root "
                "FROM meta WHERE id = 0"
            ).fetchone()
            if meta is None:
                raise NotFoundError("line has not been initialized")
            n = int(meta["total_traces"])
            if not (1 <= position <= n):
                raise InvalidMoveError(
                    f"position must be between 1 and {n}"
                )
            root = self._version_root(conn, version)
            original_trace = self._kth(conn, root, position)
            cur_version = int(meta["current_version"])
            if version == cur_version:
                cur_position = position
            else:
                row = conn.execute(
                    "SELECT node_id FROM current_node WHERE original_trace = ?",
                    (original_trace,),
                ).fetchone()
                if row is None:  # pragma: no cover - invariant
                    raise NotFoundError("current mapping missing")
                cur_position = self._position_of(conn, int(row["node_id"]))
            conn.execute("COMMIT")
            return {
                "query_version": version,
                "query_position": position,
                "original_trace": original_trace,
                "current_version": cur_version,
                "current_position": cur_position,
            }
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def lookup_trace(self, original_trace: int) -> dict:
        """Current position of an original trace."""
        conn = self._read_conn()
        try:
            conn.execute("BEGIN")
            meta = conn.execute(
                "SELECT total_traces, current_version FROM meta WHERE id = 0"
            ).fetchone()
            if meta is None:
                raise NotFoundError("line has not been initialized")
            n = int(meta["total_traces"])
            if not (1 <= original_trace <= n):
                raise InvalidMoveError(
                    f"original_trace must be between 1 and {n}"
                )
            row = conn.execute(
                "SELECT node_id FROM current_node WHERE original_trace = ?",
                (original_trace,),
            ).fetchone()
            if row is None:  # pragma: no cover - invariant
                raise NotFoundError("trace not found in current version")
            position = self._position_of(conn, int(row["node_id"]))
            result = {
                "original_trace": original_trace,
                "current_version": int(meta["current_version"]),
                "current_position": position,
            }
            conn.execute("COMMIT")
            return result
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    @staticmethod
    def _version_root(conn: sqlite3.Connection, version: int) -> int:
        row = conn.execute(
            "SELECT root_node FROM versions WHERE version = ?", (version,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"version {version} does not exist")
        return int(row["root_node"])

    @staticmethod
    def _kth(conn: sqlite3.Connection, root: int, k: int) -> int:
        """Value at 1-based position k; O(height) indexed point reads."""
        node_id = root
        while node_id:
            row = conn.execute(
                "SELECT value, left, right, "
                "(SELECT size FROM treap_nodes WHERE id = t.left) AS lsize "
                "FROM treap_nodes t WHERE id = ?",
                (node_id,),
            ).fetchone()
            left_size = int(row["lsize"] or 0)
            if k == left_size + 1:
                return int(row["value"])
            if k <= left_size:
                node_id = int(row["left"])
            else:
                k -= left_size + 1
                node_id = int(row["right"])
        raise NotFoundError("kth lookup failed")  # pragma: no cover

    @staticmethod
    def _position_of(conn: sqlite3.Connection, node_id: int) -> int:
        """1-based position via parent links; O(height) indexed point reads."""
        row = conn.execute(
            "SELECT left FROM treap_nodes WHERE id = ?", (node_id,)
        ).fetchone()
        position = 1 + LineRepository._subtree_size(conn, int(row["left"]))
        while True:
            parent = conn.execute(
                "SELECT parent_id FROM current_parent WHERE node_id = ?",
                (node_id,),
            ).fetchone()
            if parent is None or int(parent["parent_id"]) == 0:
                return position
            pid = int(parent["parent_id"])
            prow = conn.execute(
                "SELECT left, right FROM treap_nodes WHERE id = ?", (pid,)
            ).fetchone()
            if int(prow["right"]) == node_id:
                position += 1 + LineRepository._subtree_size(
                    conn, int(prow["left"])
                )
            node_id = pid

    @staticmethod
    def _subtree_size(conn: sqlite3.Connection, node_id: int) -> int:
        if node_id == 0:
            return 0
        return int(conn.execute(
            "SELECT size FROM treap_nodes WHERE id = ?", (node_id,)
        ).fetchone()["size"])

    def _rollback_safely(self) -> None:
        try:
            self._conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass

    # ----------------------------------------------------------------- move
    def move_segment(
        self,
        base_version: int,
        start: int,
        end: int,
        insert_after: int,
    ) -> dict:
        """Move positions [start, end] (inclusive) to sit right after
        ``insert_after`` (0 = front, n = end), in ``base_version``'s order.

        Stale base versions and any write failure abort the whole
        transaction, so no new version is ever produced.
        """
        with self._writer_lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                meta = self._conn.execute(
                    "SELECT total_traces, current_version, current_root "
                    "FROM meta WHERE id = 0"
                ).fetchone()
                if meta is None:
                    raise InvalidMoveError("line has not been initialized")
                n = int(meta["total_traces"])
                current = int(meta["current_version"])
                if base_version != current:
                    raise StaleVersionError(
                        f"base version {base_version} is stale; "
                        f"current version is {current}"
                    )
                self._validate_move(n, start, end, insert_after)
                root = int(meta["current_root"])
                store = EagerSqliteStore(self._conn)
                first_new_id = store.next_id

                # Extract [start, end]; internal order of the segment is
                # preserved because split never reorders elements.
                left, rest = split(store, root, start - 1)
                segment, right = split(store, rest, end - start + 1)

                # Translate the insertion gap to the sequence without the
                # segment. Positions strictly before the cut keep their
                # numbering; positions at/after the cut shift left by the
                # segment length. (A gap inside [start, end] is rejected by
                # _validate_move.)
                seg_len = end - start + 1
                gap = insert_after if insert_after < start \
                    else insert_after - seg_len
                remaining = merge(store, left, right)
                before, after = split(store, remaining, gap)
                new_root = merge(
                    store, merge(store, before, segment), after
                )
                store.flush()

                new_version = current + 1
                self._conn.execute(
                    "INSERT INTO versions (version, root_node, base_version, "
                    "move_start, move_end, insert_after) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (new_version, new_root, base_version,
                     start, end, insert_after),
                )
                self._conn.execute(
                    "UPDATE meta SET current_version = ?, current_root = ? "
                    "WHERE id = 0",
                    (new_version, new_root),
                )
                self._sync_mirrors(root, new_root)
                self._sync_window_agg(store, first_new_id)
                self._conn.execute("COMMIT")
                return {
                    "version": new_version,
                    "root_node": new_root,
                    "total_traces": n,
                }
            except Exception:
                self._rollback_safely()
                raise

    @staticmethod
    def _validate_move(n: int, start: int, end: int,
                       insert_after: int) -> None:
        if not (1 <= start <= n):
            raise InvalidMoveError(f"start must be between 1 and {n}")
        if not (start <= end <= n):
            raise InvalidMoveError(
                f"end must satisfy start ({start}) <= end <= {n}"
            )
        if not (0 <= insert_after <= n):
            raise InvalidMoveError(
                f"insert_after must be between 0 (front) and {n} (end)"
            )
        if start <= insert_after < end:
            raise InvalidMoveError(
                f"insert_after ({insert_after}) must not point inside the "
                f"moved segment [{start}, {end}]"
            )

    def _sync_mirrors(self, old_root: int, new_root: int) -> None:
        """Incrementally refresh current_parent / current_node.

        Persistent split/merge only allocates new nodes along O(log n)
        changed root-to-leaf paths; identical subtrees are shared. We walk
        the old and new trees in parallel and prune wherever their node ids
        match, so rows are written only on changed paths plus the single
        root link of each shared subtree that got a new parent.
        """
        conn = self._conn
        parent_rows: list[tuple[int, int]] = []
        trace_rows: list[tuple[int, int]] = []
        stack: list[tuple[int, int, int]] = [(old_root, new_root, 0)]
        while stack:
            old_id, new_id, parent_id = stack.pop()
            if new_id == 0:
                continue
            parent_rows.append((new_id, parent_id))
            if old_id == new_id:
                # Shared subtree: its internal links and trace mappings are
                # unchanged; only the (possibly new) parent link above was
                # just recorded.
                continue
            row = conn.execute(
                "SELECT value, left, right FROM treap_nodes WHERE id = ?",
                (new_id,),
            ).fetchone()
            old_row = conn.execute(
                "SELECT left, right FROM treap_nodes WHERE id = ?",
                (old_id,),
            ).fetchone() if old_id else None
            trace_rows.append((int(row["value"]), new_id))
            stack.append((
                int(old_row["right"]) if old_row is not None else 0,
                int(row["right"]), new_id,
            ))
            stack.append((
                int(old_row["left"]) if old_row is not None else 0,
                int(row["left"]), new_id,
            ))
        conn.executemany(
            "INSERT INTO current_parent (node_id, parent_id) "
            "VALUES (?, ?) ON CONFLICT(node_id) DO UPDATE SET "
            "parent_id = excluded.parent_id",
            parent_rows,
        )
        conn.executemany(
            "INSERT INTO current_node (original_trace, node_id) "
            "VALUES (?, ?) ON CONFLICT(original_trace) DO UPDATE SET "
            "node_id = excluded.node_id",
            trace_rows,
        )
        # Rows belonging to nodes that no longer belong to the current tree
        # are harmless (never referenced from current_node/current_root);
        # they are kept to avoid O(n) deletes.

    # ------------------------------------------------------- bad traces /
    # windowing mirrors
    def _sync_window_agg(self, store: EagerSqliteStore,
                         first_new_id: int) -> None:
        """Insert current_agg rows for the nodes a move just allocated.

        Split/merge create nodes in post-order dependency order (a child is
        always allocated before its parent) with consecutive ids, so
        visiting the new id range in order guarantees that every child
        summary is already available - either in ``current_agg`` for a
        shared subtree or inserted earlier in this same loop for a freshly
        allocated child. The moved block's shared subtrees keep their
        existing rows and are never rebuilt.
        """
        conn = self._conn
        rows = conn.execute(
            "SELECT id, value, left, right FROM treap_nodes "
            "WHERE id >= ? ORDER BY id", (first_new_id,),
        ).fetchall()
        if not rows:
            return
        bad_set = self._load_bad_set(conn, [r[1] for r in rows])
        agg_cache: dict[int, tuple] = {}
        out: list[tuple] = []
        for node_id, value, left, right in rows:
            agg = combine(
                self._agg_of(conn, int(left), agg_cache),
                combine(leaf(int(value), int(value) in bad_set),
                        self._agg_of(conn, int(right), agg_cache)),
            )
            agg_cache[node_id] = agg
            out.append((node_id, *agg))
        placeholders = ", ".join("?" for _ in AGG_COLUMNS)
        conn.executemany(
            f"INSERT INTO current_agg (node_id, {', '.join(AGG_COLUMNS)}) "
            f"VALUES (?, {placeholders})",
            out,
        )

    @staticmethod
    def _load_bad_set(conn: sqlite3.Connection,
                      values: list[int]) -> set[int]:
        """Which of the given original trace numbers are currently bad.

        One indexed IN-query per call regardless of how many traces are
        involved, so a move or a flag flip never scans bad_traces.
        """
        if not values:
            return set()
        unique = list(dict.fromkeys(values))
        # SQLite accepts at most 999 host parameters in one statement.
        bad: set[int] = set()
        for i in range(0, len(unique), 900):
            chunk = unique[i:i + 900]
            placeholders = ", ".join("?" for _ in chunk)
            bad.update(r[0] for r in conn.execute(
                f"SELECT original_trace FROM bad_traces "
                f"WHERE original_trace IN ({placeholders})", chunk))
        return bad

    def _agg_of(self, conn: sqlite3.Connection, node_id: int,
                cache: dict[int, tuple]) -> tuple | None:
        if node_id == 0:
            return None
        cached = cache.get(node_id)
        if cached is not None:
            return cached
        row = conn.execute(
            f"SELECT {', '.join(AGG_COLUMNS)} FROM current_agg "
            "WHERE node_id = ?", (node_id,),
        ).fetchone()
        if row is None:  # pragma: no cover - invariant
            raise NotFoundError(f"window aggregate missing for node {node_id}")
        agg = tuple(row)
        cache[node_id] = agg
        return agg

    def set_bad_trace(self, original_trace: int, bad: bool) -> dict:
        """Register (``bad=True``) or clear (``bad=False``) one bad-trace
        flag, identified by original trace number.

        The flag is keyed by original trace number, so moves carry it along
        automatically. Only the O(height) root-to-leaf path of that trace
        is recomputed in current_agg; the rest of the line is untouched.
        Idempotent: re-applying the current state changes nothing.
        """
        with self._writer_lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                n = self._require_total(self._conn)
                if not (1 <= original_trace <= n):
                    raise InvalidMoveError(
                        f"original_trace must be between 1 and {n}"
                    )
                existed = self._conn.execute(
                    "SELECT 1 FROM bad_traces WHERE original_trace = ?",
                    (original_trace,),
                ).fetchone() is not None
                changed = existed != bad
                if changed:
                    if bad:
                        self._conn.execute(
                            "INSERT INTO bad_traces (original_trace) "
                            "VALUES (?)", (original_trace,))
                    else:
                        self._conn.execute(
                            "DELETE FROM bad_traces WHERE original_trace = ?",
                            (original_trace,))
                    self._recompute_agg_path(original_trace, bad)
                self._conn.execute("COMMIT")
                return {
                    "original_trace": original_trace,
                    "bad": bad,
                    "changed": changed,
                }
            except Exception:
                self._rollback_safely()
                raise

    def _recompute_agg_path(self, original_trace: int, is_bad: bool) -> None:
        """Re-summarise one trace's leaf and every ancestor up to the root.

        O(height) indexed reads/writes; sibling subtrees are read from
        current_agg and never walked.
        """
        conn = self._conn
        node_row = conn.execute(
            "SELECT node_id FROM current_node WHERE original_trace = ?",
            (original_trace,),
        ).fetchone()
        if node_row is None:  # pragma: no cover - invariant
            raise NotFoundError(
                f"trace {original_trace} not found in current version")
        node_id = int(node_row["node_id"])
        cache: dict[int, tuple] = {}
        out: list[tuple] = []
        while node_id:
            row = conn.execute(
                "SELECT value, left, right FROM treap_nodes WHERE id = ?",
                (node_id,),
            ).fetchone()
            if int(row["left"]) == 0 and int(row["right"]) == 0:
                agg = leaf(int(row["value"]), is_bad)
            else:
                agg = combine(
                    self._agg_of(conn, int(row["left"]), cache),
                    combine(
                        leaf(int(row["value"]),
                             self._is_bad(conn, int(row["value"]))),
                        self._agg_of(conn, int(row["right"]), cache),
                    ),
                )
            cache[node_id] = agg
            out.append((node_id, *agg))
            parent = conn.execute(
                "SELECT parent_id FROM current_parent WHERE node_id = ?",
                (node_id,),
            ).fetchone()
            if parent is None or int(parent["parent_id"]) == 0:
                break
            node_id = int(parent["parent_id"])
        placeholders = ", ".join("?" for _ in AGG_COLUMNS)
        conn.executemany(
            f"INSERT INTO current_agg (node_id, {', '.join(AGG_COLUMNS)}) "
            f"VALUES (?, {placeholders}) ON CONFLICT(node_id) DO UPDATE SET "
            + ", ".join(f"{c} = excluded.{c}" for c in AGG_COLUMNS),
            out,
        )

    @staticmethod
    def _is_bad(conn: sqlite3.Connection, original_trace: int) -> bool:
        return conn.execute(
            "SELECT 1 FROM bad_traces WHERE original_trace = ?",
            (original_trace,),
        ).fetchone() is not None

    @staticmethod
    def _require_total(conn: sqlite3.Connection) -> int:
        row = conn.execute(
            "SELECT total_traces FROM meta WHERE id = 0"
        ).fetchone()
        if row is None:
            raise NotFoundError("line has not been initialized")
        return int(row["total_traces"])

    def list_bad_traces(self) -> dict:
        conn = self._read_conn()
        try:
            conn.execute("BEGIN")
            n = self._require_total(conn)
            traces = [r[0] for r in conn.execute(
                "SELECT original_trace FROM bad_traces ORDER BY "
                "original_trace")]
            conn.execute("COMMIT")
            return {"total_traces": n, "count": len(traces),
                    "bad_traces": traces}
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def find_window(self, start: int, end: int) -> dict:
        """Longest usable segment within current positions [start, end].

        Usable = every trace unmarked and original ids increasing by one.
        The range is folded over the O(height) treap nodes covering it, so
        no trace is scanned and candidate segments are never compared one by
        one; the monoid keeps only the leftmost longest run. Ties therefore
        resolve to the earliest segment, and a window without any usable
        trace returns ``segment = null``. Ordering and flags are read inside
        one snapshot transaction, i.e. from the same committed state.
        """
        conn = self._read_conn()
        try:
            conn.execute("BEGIN")
            meta = conn.execute(
                "SELECT total_traces, current_version, current_root "
                "FROM meta WHERE id = 0"
            ).fetchone()
            if meta is None:
                raise NotFoundError("line has not been initialized")
            n = int(meta["total_traces"])
            if not (1 <= start <= n) or not (start <= end <= n):
                raise InvalidMoveError(
                    f"window must satisfy 1 <= start <= end <= {n}"
                )
            version = int(meta["current_version"])
            agg = self._range_agg(conn, int(meta["current_root"]),
                                  start, end, {})
            if agg is None or agg[BEST] == 0:
                conn.execute("COMMIT")
                return {"version": version, "window": [start, end],
                        "segment": None}
            length = int(agg[BEST])
            seg_start = start + int(agg[BEST_START]) - 1
            seg_end = seg_start + length - 1
            first_trace = self._kth(conn, int(meta["current_root"]),
                                    seg_start)
            last_trace = self._kth(conn, int(meta["current_root"]), seg_end)
            conn.execute("COMMIT")
            return {
                "version": version,
                "window": [start, end],
                "segment": {
                    "start_position": seg_start,
                    "end_position": seg_end,
                    "start_original_trace": first_trace,
                    "end_original_trace": last_trace,
                    "length": length,
                },
            }
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _range_agg(self, conn: sqlite3.Connection, root: int,
                   lo: int, hi: int, cache: dict) -> tuple | None:
        """Monoid summary of inclusive 1-based positions [lo, hi].

        Treap recursion follows only the O(height) nodes intersecting the
        range; fully covered subtrees contribute their stored summary in a
        single indexed read, so the number of reads is proportional to tree
        height rather than the window's width. ``cache`` is shared across
        the whole query so a summary read once is never read twice.
        """
        row = conn.execute(
            "SELECT size, left, right FROM treap_nodes WHERE id = ?",
            (root,),
        ).fetchone()
        size = int(row["size"])
        if lo == 1 and hi == size:
            return self._agg_of(conn, root, cache)
        left_id, right_id = int(row["left"]), int(row["right"])
        right_size = int(conn.execute(
            "SELECT size FROM treap_nodes WHERE id = ?", (right_id,)
        ).fetchone()[0]) if right_id else 0
        left_size = size - 1 - right_size
        result: tuple | None = None
        if lo <= left_size:
            result = self._range_agg(
                conn, left_id, lo, min(hi, left_size), cache)
        if lo <= left_size + 1 <= hi:
            value = int(conn.execute(
                "SELECT value FROM treap_nodes WHERE id = ?", (root,)
            ).fetchone()[0])
            result = combine(result, leaf(
                value, self._is_bad(conn, value)))
        if hi > left_size + 1:
            result = combine(result, self._range_agg(
                conn, right_id,
                max(1, lo - left_size - 1), hi - left_size - 1, cache))
        return result

