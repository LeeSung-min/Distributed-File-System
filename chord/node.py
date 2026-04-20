"""
chord/node.py

A single Chord peer. Each node owns a slice of the identifier ring
[0, 2^M). It keeps a finger table for O(log N) routing and tracks
its immediate predecessor so ring consistency can be maintained.

Key design choices worth understanding:
  - M=160 because we use SHA-1, giving a 2^160 ring.
  - Finger table entry i points to successor(n + 2^i mod 2^M).
  - stabilize() and fix_fingers() run periodically so the ring
    self-heals when nodes join or leave.
"""

import hashlib
import threading
import time
import logging
from typing import Optional

log = logging.getLogger(__name__)

M = 160          # SHA-1 bit width
RING_SIZE = 2**M


def sha1_int(data: str) -> int:
    """Map an arbitrary string onto the ring [0, 2^160)."""
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)


def in_range(x: int, lo: int, hi: int, inclusive_hi: bool = False) -> bool:
    """
    Check if x falls in the arc (lo, hi] on the circular ring.
    When lo == hi the full ring is implied, so everything is in range.
    """
    if lo == hi:
        return True
    if lo < hi:
        return lo < x < hi or (inclusive_hi and x == hi)
    # Wraps around zero
    return x > lo or x < hi or (inclusive_hi and x == hi)


class Finger:
    """One row in the finger table."""
    def __init__(self, start: int):
        self.start: int = start       # (n + 2^i) mod 2^M
        self.node: Optional["ChordNode"] = None  # The node responsible for start


class ChordNode:
    """
    A Chord peer.

    In a real deployment each node would live in its own process and
    communicate over sockets. Here every node is a Python object and
    calls are direct method calls — that is fine for learning because
    the routing logic is identical; only the transport layer changes.
    """

    def __init__(self, node_id: Optional[int] = None, name: str = ""):
        # Allow a fixed ID for testing, otherwise pick a random-ish one.
        self.id: int = node_id if node_id is not None else sha1_int(name or str(time.time()))
        self.name: str = name or f"node-{self.id % 10000}"

        self.predecessor: Optional["ChordNode"] = None
        self.successor: "ChordNode" = self   # Points to self until ring forms

        # Build the finger table skeleton; entries are filled by fix_fingers().
        self.fingers: list[Finger] = []
        for i in range(M):
            f = Finger(start=(self.id + 2**i) % RING_SIZE)
            f.node = self   # optimistic default
            self.fingers.append(f)

        # Local key-value store — this is what "local storage" means.
        self._store: dict[int, bytes] = {}
        self._store_lock = threading.Lock()

        # Background maintenance threads
        self._running = False
        self._stabilize_thread: Optional[threading.Thread] = None
        self._fix_thread: Optional[threading.Thread] = None

        log.info("ChordNode %s created (id=%d)", self.name, self.id)

    # ------------------------------------------------------------------
    # Ring bootstrapping
    # ------------------------------------------------------------------

    def create(self):
        """Start a brand-new one-node ring."""
        self.predecessor = None
        self.successor = self
        self._start_maintenance()
        log.info("%s created a new ring", self.name)

    def join(self, bootstrap: "ChordNode"):
        """
        Join an existing ring via a known bootstrap node.
        We only set our successor here; stabilize() will fill the rest.
        """
        self.predecessor = None
        self.successor = bootstrap.find_successor(self.id)
        self._start_maintenance()
        log.info("%s joined via %s, successor=%s", self.name, bootstrap.name, self.successor.name)

    # ------------------------------------------------------------------
    # Core Chord routing
    # ------------------------------------------------------------------

    def find_successor(self, key: int) -> "ChordNode":
        """
        Return the node responsible for `key`.

        If key falls between us and our successor, our successor is the
        answer. Otherwise we forward to the closest preceding finger and
        let that node continue the search — O(log N) hops total.
        """
        if in_range(key, self.id, self.successor.id, inclusive_hi=True):
            return self.successor
        n0 = self._closest_preceding_finger(key)
        if n0 is self:
            return self.successor
        return n0.find_successor(key)

    def _closest_preceding_finger(self, key: int) -> "ChordNode":
        """
        Walk the finger table from largest to smallest, returning the
        finger that is closest to `key` without overshooting it.
        """
        for i in range(M - 1, -1, -1):
            f = self.fingers[i].node
            if f and in_range(f.id, self.id, key):
                return f
        return self

    # ------------------------------------------------------------------
    # Stabilization (keeps the ring consistent after joins/failures)
    # ------------------------------------------------------------------

    def stabilize(self):
        """
        Verify our successor's predecessor pointer and update our own
        successor if a new node slipped in between us.
        """
        x = self.successor.predecessor
        if x and in_range(x.id, self.id, self.successor.id):
            self.successor = x
        self.successor.notify(self)

    def notify(self, candidate: "ChordNode"):
        """
        A candidate claims to be our predecessor. Accept if it falls
        in the right arc or if we have no predecessor yet.
        """
        if (self.predecessor is None or
                in_range(candidate.id, self.predecessor.id, self.id)):
            self.predecessor = candidate

    def fix_fingers(self):
        """Refresh one random finger table entry per call."""
        import random
        i = random.randint(0, M - 1)
        self.fingers[i].node = self.find_successor(self.fingers[i].start)

    # ------------------------------------------------------------------
    # Local key-value storage
    # ------------------------------------------------------------------

    def local_put(self, key: int, value: bytes):
        with self._store_lock:
            self._store[key] = value
        log.debug("%s stored key %d (%d bytes)", self.name, key, len(value))

    def local_get(self, key: int) -> Optional[bytes]:
        with self._store_lock:
            return self._store.get(key)

    def local_delete(self, key: int) -> bool:
        with self._store_lock:
            return self._store.pop(key, None) is not None

    def local_list_keys(self) -> list[int]:
        with self._store_lock:
            return list(self._store.keys())

    # ------------------------------------------------------------------
    # DHT-level routing: find the right node, then do the operation
    # ------------------------------------------------------------------

    def put(self, key: int, value: bytes):
        responsible = self.find_successor(key)
        responsible.local_put(key, value)

    def get(self, key: int) -> Optional[bytes]:
        responsible = self.find_successor(key)
        return responsible.local_get(key)

    def delete(self, key: int) -> bool:
        responsible = self.find_successor(key)
        return responsible.local_delete(key)

    # ------------------------------------------------------------------
    # Background maintenance
    # ------------------------------------------------------------------

    def _start_maintenance(self):
        self._running = True
        self._stabilize_thread = threading.Thread(
            target=self._run_stabilize, daemon=True, name=f"{self.name}-stabilize"
        )
        self._fix_thread = threading.Thread(
            target=self._run_fix_fingers, daemon=True, name=f"{self.name}-fix"
        )
        self._stabilize_thread.start()
        self._fix_thread.start()

    def _run_stabilize(self):
        while self._running:
            try:
                self.stabilize()
            except Exception as e:
                log.warning("%s stabilize error: %s", self.name, e)
            time.sleep(0.5)

    def _run_fix_fingers(self):
        while self._running:
            try:
                self.fix_fingers()
            except Exception as e:
                log.warning("%s fix_fingers error: %s", self.name, e)
            time.sleep(0.3)

    def stop(self):
        self._running = False

    def __repr__(self):
        succ_id = self.successor.id if self.successor else None
        pred_id = self.predecessor.id if self.predecessor else None
        return (f"ChordNode(name={self.name}, id={self.id}, "
                f"succ={succ_id}, pred={pred_id})")
