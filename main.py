#!/usr/bin/env python3
"""
main.py — entry point for the Distributed File System.

Bootstraps a Chord ring and DFS, then dispatches CLI commands.

Examples
--------
    python main.py touch notes.txt
    python main.py append notes.txt ./local_notes.txt
    python main.py ls
    python main.py sort data.csv sorted.csv
    python main.py verify sorted.csv
    python main.py shell          # interactive mode
"""

import logging
import sys

from chord.ring import ChordRing
from dfs.api import DFS
from dfs.sort import sort_file, verify_sorted
from cli.shell import run_command

# Configure logging — set to INFO for normal use, DEBUG for deep tracing.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)-20s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Quiet the Chord stabilization noise during normal use.
logging.getLogger("chord.node").setLevel(logging.WARNING)


def build_system(num_chord_nodes: int = 5, num_paxos_replicas: int = 3) -> DFS:
    """
    Boot the full stack: Chord ring → DFS API layer.
    """
    ring = ChordRing(num_nodes=num_chord_nodes)
    ring.bootstrap(settle_time=1.5)
    dfs = DFS(ring=ring, num_paxos_replicas=num_paxos_replicas)
    return dfs


def main():
    dfs = build_system(num_chord_nodes=5, num_paxos_replicas=3)

    args = sys.argv[1:]
    if not args:
        # Default: drop into the interactive shell.
        args = ["shell"]

    sys.exit(run_command(dfs, sort_file, verify_sorted, args))


if __name__ == "__main__":
    main()
