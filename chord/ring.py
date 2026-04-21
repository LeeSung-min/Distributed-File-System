"""
chord/ring.py

Convenience wrapper that spins up a multi-node Chord ring in a single
process.
"""

import time
import logging
from chord.node import ChordNode, sha1_int

log = logging.getLogger(__name__)


class ChordRing:
    """
    Manages a local Chord ring of N nodes.

    Usage:
        ring = ChordRing(num_nodes=5)
        ring.bootstrap()
        node = ring.get_node_for_key(some_key)
    """

    def __init__(self, num_nodes: int = 5):
        if num_nodes < 3:
            raise ValueError("Need at least 3 nodes for meaningful routing")
        self.num_nodes = num_nodes
        self.nodes: list[ChordNode] = []

    def bootstrap(self, settle_time: float = 2.0):
        """
        Create nodes, form the ring, and wait for stabilization.
        """
        # Build nodes with deterministic IDs so tests are reproducible.
        for i in range(self.num_nodes):
            node_id = sha1_int(f"chord-node-{i}")
            node = ChordNode(node_id=node_id, name=f"peer-{i}")
            self.nodes.append(node)

        # Sort by ID so we can verify ring order easily.
        self.nodes.sort(key=lambda n: n.id)

        # First node starts a fresh ring.
        self.nodes[0].create()

        # Every subsequent node joins through node 0.
        for node in self.nodes[1:]:
            node.join(self.nodes[0])
            time.sleep(0.05)  # small gap avoids race on successor pointers

        # Give stabilization threads time to converge.
        log.info("Waiting %.1fs for ring to stabilize...", settle_time)
        time.sleep(settle_time)
        log.info("Ring ready. Node order: %s",
                 [f"{n.name}({n.id % 10000})" for n in self.nodes])

    def get_node_for_key(self, key: int) -> ChordNode:
        """Route a key to its responsible node via the ring."""
        return self.nodes[0].find_successor(key)

    def shutdown(self):
        for node in self.nodes:
            node.stop()

    def debug_ring(self):
        """Print successor/predecessor chains — useful for debugging."""
        print("\n=== Chord Ring State ===")
        for n in self.nodes:
            succ = n.successor.name if n.successor else "None"
            pred = n.predecessor.name if n.predecessor else "None"
            print(f"  {n.name:8s}  id={n.id % 100000:6d}  "
                  f"pred={pred:8s}  succ={succ}")
        print("========================\n")
