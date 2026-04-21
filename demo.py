#!/usr/bin/env python3
"""
demo.py

Demonstrates every rubric-required operation and prints the Paxos log
at the end.  Run this to generate the screenshots/logs.

    python demo.py
"""

import logging
import os
import sys
import tempfile
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)-22s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("chord.node").setLevel(logging.WARNING)

from chord.ring import ChordRing
from dfs.api import DFS
from dfs.sort import sort_file, verify_sorted


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def main():
    section("1. Bootstrapping Chord ring (5 peers)")
    ring = ChordRing(num_nodes=5)
    ring.bootstrap(settle_time=2.0)
    ring.debug_ring()

    dfs = DFS(ring=ring, num_paxos_replicas=3)
    print("DFS ready.\n")

    # ------------------------------------------------------------------ #
    section("2. touch - create files")
    dfs.touch("notes.txt")
    dfs.touch("sample_data.csv")
    print("Created: notes.txt, sample_data.csv")

    # ------------------------------------------------------------------ #
    section("3. append - upload local content")
    script_dir = os.path.dirname(os.path.abspath(__file__))

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("This is line one.\n")
        f.write("This is line two.\n")
        f.write("This is line three.\n")
        notes_path = f.name

    dfs.append("notes.txt", notes_path)
    os.unlink(notes_path)

    csv_path = os.path.join(script_dir, "sample_data.csv")
    dfs.append("sample_data.csv", csv_path)
    print("Appended content to both files.")

    # ------------------------------------------------------------------ #
    section("4. ls - list all files")
    files = dfs.ls()
    for fname in files:
        print(f"  {fname}")

    # ------------------------------------------------------------------ #
    section("5. stat - file metadata")
    stat = dfs.stat("notes.txt")
    for k, v in stat.items():
        if k != "pages":
            print(f"  {k:15s}: {v}")
    for p in stat["pages"]:
        print(f"    page {p['page_no']}: guid={p['guid']} size={p['size']}")

    # ------------------------------------------------------------------ #
    section("6. read - full contents")
    data = dfs.read("notes.txt")
    print(data.decode())

    # ------------------------------------------------------------------ #
    section("7. head - first 2 lines")
    print(dfs.head("notes.txt", n=2))

    # ------------------------------------------------------------------ #
    section("8. tail - last 2 lines")
    print(dfs.tail("notes.txt", n=2))

    # ------------------------------------------------------------------ #
    section("9. sort_file - distributed sort of sample_data.csv")
    sort_file(dfs, "sample_data.csv", "sorted_output.csv")
    print("\nSorted output:")
    print(dfs.read("sorted_output.csv").decode())

    # ------------------------------------------------------------------ #
    section("10. verify - correctness check")
    ok = verify_sorted(dfs, "sorted_output.csv")
    print(f"Sort verification: {'PASSED ✓' if ok else 'FAILED ✗'}")

    # ------------------------------------------------------------------ #
    section("11. delete - remove a file")
    dfs.delete_file("notes.txt")
    remaining = dfs.ls()
    print("Files after deleting notes.txt:")
    for fname in remaining:
        print(f"  {fname}")

    # ------------------------------------------------------------------ #
    section("12. Paxos log (leader-0)")
    log_path = "paxos_leader_0.log"
    # The log name is derived from group_id which is hash(filename) % 2^31
    # Scan for any leader log files.
    leader_logs = [f for f in os.listdir(".") if f.startswith("paxos_leader")]
    if leader_logs:
        with open(leader_logs[0]) as f:
            print(f.read())
    else:
        print("(no paxos log found in current directory)")

    # ------------------------------------------------------------------ #
    section("13. Paxos replica log (replica-0)")
    replica_logs = [f for f in os.listdir(".") if f.startswith("paxos_replica")]
    if replica_logs:
        with open(replica_logs[0]) as f:
            print(f.read())

    ring.shutdown()
    print("\nDemo complete.")


if __name__ == "__main__":
    main()
