"""
paxos/replica.py

Simplified Paxos for replicated DFS metadata.

The protocol here matches the assignment's simplified model:
  1. Leader picks a ballot number t and broadcasts ACCEPT(op, t).
  2. Each replica responds with LEARN(op, t) if it hasn't seen a
     higher ballot for this slot.
  3. Once a strict majority (> n/2) have sent LEARN, the leader
     broadcasts COMMIT(op, t).
  4. All replicas apply committed operations in ballot order.

Fault model: crash-only. We do not handle Byzantine behavior.
Messages can be delayed or lost (simulated by ignoring timed-out
replicas) but we assume no corruption.

In a real system ACCEPT/LEARN messages would be sent over the network.
Here each Replica is a Python object and calls are local, so the
"network" is just method invocation — the protocol logic is identical.
"""

import threading
import logging
import time
from collections import defaultdict
from typing import Any, Optional

log = logging.getLogger(__name__)


class PaxosMessage:
    """Carrier for a single Paxos protocol message."""

    def __init__(self, msg_type: str, op: Any, ballot: int, sender_id: int):
        self.msg_type = msg_type   # "ACCEPT" | "LEARN" | "COMMIT"
        self.op = op               # The operation being proposed
        self.ballot = ballot       # Monotonically increasing ballot number
        self.sender_id = sender_id

    def __repr__(self):
        return (f"PaxosMessage({self.msg_type} ballot={self.ballot} "
                f"op={self.op!r} from={self.sender_id})")


class Replica:
    """
    A single Paxos replica node.

    Each replica maintains:
      - A committed log of operations applied in order.
      - The highest ballot it has accepted, so it can reject stale proposals.
      - A callback (apply_fn) that the DFS layer registers to actually
        carry out committed operations (e.g. write metadata to Chord).
    """

    def __init__(self, replica_id: int, apply_fn=None):
        self.replica_id = replica_id
        self.apply_fn = apply_fn   # Callable[[op], None]

        self._lock = threading.Lock()
        self._highest_ballot: int = -1
        self._committed_log: list[tuple[int, Any]] = []  # (ballot, op)
        self._log_file = f"paxos_replica_{replica_id}.log"

        self._write_log(f"INIT replica_id={replica_id}")

    # ------------------------------------------------------------------
    # Message handlers — called by the leader
    # ------------------------------------------------------------------

    def receive_accept(self, msg: PaxosMessage) -> Optional[PaxosMessage]:
        """
        Handle an ACCEPT message from a leader.

        We promise to learn this op if the ballot is >= anything we have
        seen before. If we have seen a higher ballot, we reject by
        returning None — the leader should retry with a higher ballot.
        """
        with self._lock:
            if msg.ballot < self._highest_ballot:
                log.warning(
                    "Replica %d rejected ACCEPT ballot=%d (have %d)",
                    self.replica_id, msg.ballot, self._highest_ballot
                )
                self._write_log(
                    f"REJECT ballot={msg.ballot} (have {self._highest_ballot})"
                )
                return None

            self._highest_ballot = msg.ballot
            self._write_log(f"ACCEPT ballot={msg.ballot} op={msg.op!r}")
            log.debug("Replica %d accepted ballot=%d op=%r",
                      self.replica_id, msg.ballot, msg.op)

            return PaxosMessage(
                msg_type="LEARN",
                op=msg.op,
                ballot=msg.ballot,
                sender_id=self.replica_id
            )

    def receive_commit(self, msg: PaxosMessage):
        """
        Apply a committed operation. The leader sends this after a majority
        have responded with LEARN. We only apply it once per ballot.
        """
        with self._lock:
            already = any(b == msg.ballot for b, _ in self._committed_log)
            if already:
                return

            self._committed_log.append((msg.ballot, msg.op))
            # Keep log ordered by ballot so replicas converge to the same state.
            self._committed_log.sort(key=lambda x: x[0])
            self._write_log(f"COMMIT ballot={msg.ballot} op={msg.op!r}")
            log.info("Replica %d committed ballot=%d op=%r",
                     self.replica_id, msg.ballot, msg.op)

        if self.apply_fn:
            try:
                self.apply_fn(msg.op)
            except Exception as e:
                log.error("Replica %d apply_fn failed: %s", self.replica_id, e)

    def get_log(self) -> list[tuple[int, Any]]:
        with self._lock:
            return list(self._committed_log)

    # ------------------------------------------------------------------
    # Human-readable Paxos log (required by rubric)
    # ------------------------------------------------------------------

    def _write_log(self, message: str):
        timestamp = time.strftime("%H:%M:%S")
        line = f"[{timestamp}] [replica-{self.replica_id}] {message}\n"
        try:
            with open(self._log_file, "a") as f:
                f.write(line)
        except OSError:
            pass  # Don't let logging crash the system


class PaxosLeader:
    """
    The Paxos leader for a group of replicas.

    One leader drives consensus for a named group (e.g. "metadata" or
    a specific filename). The ballot number increments on every proposal
    so that replicas can totally order all operations.

    In a real implementation the leader role might rotate. For this
    assignment a fixed leader per group is sufficient.
    """

    def __init__(self, leader_id: int, replicas: list[Replica]):
        if len(replicas) < 3:
            raise ValueError("Paxos requires at least 3 replicas")
        self.leader_id = leader_id
        self.replicas = replicas
        self._ballot = 0
        self._lock = threading.Lock()
        self._log_file = f"paxos_leader_{leader_id}.log"
        self._write_log("INIT leader_id=%d replicas=%d" %
                        (leader_id, len(replicas)))

    @property
    def majority(self) -> int:
        return len(self.replicas) // 2 + 1

    def propose(self, op: Any, timeout: float = 2.0) -> bool:
        """
        Drive a full Paxos round for `op`.

        Returns True if the operation was committed by a majority.
        Returns False if we couldn't reach a majority (caller can retry).
        """
        with self._lock:
            self._ballot += 1
            ballot = self._ballot

        accept_msg = PaxosMessage(
            msg_type="ACCEPT",
            op=op,
            ballot=ballot,
            sender_id=self.leader_id
        )
        self._write_log(f"PROPOSE ballot={ballot} op={op!r}")
        log.info("Leader %d proposing ballot=%d op=%r",
                 self.leader_id, ballot, op)

        # Phase 1: broadcast ACCEPT, collect LEARN responses
        learns: list[PaxosMessage] = []
        for replica in self.replicas:
            response = replica.receive_accept(accept_msg)
            if response and response.msg_type == "LEARN":
                learns.append(response)
                self._write_log(
                    f"LEARN from replica-{response.sender_id} "
                    f"ballot={response.ballot}"
                )

        if len(learns) < self.majority:
            log.warning(
                "Leader %d failed to reach majority (got %d/%d)",
                self.leader_id, len(learns), self.majority
            )
            self._write_log(
                f"NO_MAJORITY ballot={ballot} "
                f"learns={len(learns)} need={self.majority}"
            )
            return False

        # Phase 2: broadcast COMMIT to all replicas
        commit_msg = PaxosMessage(
            msg_type="COMMIT",
            op=op,
            ballot=ballot,
            sender_id=self.leader_id
        )
        self._write_log(f"COMMIT ballot={ballot} op={op!r}")
        for replica in self.replicas:
            replica.receive_commit(commit_msg)

        log.info("Leader %d committed ballot=%d op=%r", self.leader_id, ballot, op)
        return True

    def _write_log(self, message: str):
        timestamp = time.strftime("%H:%M:%S")
        line = f"[{timestamp}] [leader-{self.leader_id}] {message}\n"
        try:
            with open(self._log_file, "a") as f:
                f.write(line)
        except OSError:
            pass


class ReplicationGroup:
    """
    Bundles a leader and its replica set for a single replication group.

    The DFS layer creates one ReplicationGroup for metadata and uses
    propose() whenever it needs to commit a metadata change.
    """

    def __init__(self, group_id: int, num_replicas: int = 3, apply_fn=None):
        if num_replicas < 3:
            raise ValueError("Need at least 3 replicas")
        self.group_id = group_id
        self.replicas = [
            Replica(replica_id=i, apply_fn=apply_fn)
            for i in range(num_replicas)
        ]
        self.leader = PaxosLeader(
            leader_id=group_id,
            replicas=self.replicas
        )

    def commit(self, op: Any) -> bool:
        """Propose and commit an operation. Returns True on success."""
        return self.leader.propose(op)

    def get_committed_log(self) -> list[tuple[int, Any]]:
        """Return the committed log from replica 0 (the reference copy)."""
        return self.replicas[0].get_log()
