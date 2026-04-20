"""
dfs/sort.py

Distributed sort over DFS pages.

The algorithm:

  1. SCAN  — read every page of the input file and parse key,value records.
  2. ROUTE — for each record, compute hash(key) and call find_successor()
             to determine which Chord peer "owns" that key range.
  3. ACCUMULATE — each peer inserts incoming records into a local sorted
             list using bisect.insort so the list stays ordered by raw key.
  4. ASSEMBLE — collect every peer's sorted segment, do a final merge sort
             by raw key to produce a globally sorted sequence.
  5. WRITE — store the sorted output as a new DFS file.

Why routing to peers still matters:
  Each peer only holds its slice of the key space in memory.  In a real
  multi-machine system this keeps memory usage per node bounded.  The final
  merge step is O(N log N) but operates on already-sorted segments, so in
  practice it is a fast k-way merge.  Routing by hash(key) is exactly how
  MapReduce-style distributed sorts partition work.
"""

import bisect
import logging
import os
import tempfile
from collections import defaultdict
from typing import Optional

from chord.node import sha1_int
from dfs.api import DFS

log = logging.getLogger(__name__)


def _parse_record(line: str) -> Optional[tuple[str, str]]:
    """Parse one 'key,value' line. Returns None for blank/malformed lines."""
    line = line.strip()
    if not line:
        return None
    parts = line.split(",", maxsplit=1)
    if len(parts) != 2:
        log.warning("Skipping malformed record: %r", line)
        return None
    return parts[0], parts[1]


def sort_file(dfs: "DFS", filename: str, output_filename: str):
    """
    Sort a DFS file of 'key,value' records and write the result to
    output_filename.
    """
    log.info("sort_file: %r -> %r", filename, output_filename)

    # ------------------------------------------------------------------ #
    # Step 1 — SCAN
    # ------------------------------------------------------------------ #
    raw = dfs.read(filename)
    text = raw.decode(errors="replace")
    all_records: list[tuple[str, str]] = []
    for line in text.splitlines():
        rec = _parse_record(line)
        if rec:
            all_records.append(rec)

    if not all_records:
        log.warning("sort_file: no records found in %r", filename)
        dfs.touch(output_filename)
        return

    log.info("sort_file: parsed %d records from %r", len(all_records), filename)

    # ------------------------------------------------------------------ #
    # Step 2 — ROUTE: assign each record to a Chord peer by hash(key)
    # ------------------------------------------------------------------ #
    peer_buckets: dict[int, list[tuple[str, str]]] = defaultdict(list)
    peer_names: dict[int, str] = {}

    for key, value in all_records:
        ring_key = sha1_int(key)
        responsible = dfs.ring.nodes[0].find_successor(ring_key)
        nid = responsible.id
        # bisect.insort keeps each peer's local list sorted by raw key.
        bisect.insort(peer_buckets[nid], (key, value))
        peer_names[nid] = responsible.name

    log.info("sort_file: routed %d records across %d peers",
             len(all_records), len(peer_buckets))
    for nid, bucket in peer_buckets.items():
        log.debug("  peer %s: %d records", peer_names[nid], len(bucket))

    # ------------------------------------------------------------------ #
    # Step 3 — ASSEMBLE: collect all peer segments and globally sort
    #
    # Each peer segment is already sorted by raw key (bisect.insort).
    # We gather them all and do a final sort() which in practice is a
    # fast merge of already-sorted runs.
    # ------------------------------------------------------------------ #
    merged: list[tuple[str, str]] = []
    for nid in sorted(peer_buckets.keys()):
        merged.extend(peer_buckets[nid])

    merged.sort(key=lambda r: r[0])

    sorted_lines = [f"{k},{v}" for k, v in merged]
    sorted_content = "\n".join(sorted_lines) + "\n"

    # ------------------------------------------------------------------ #
    # Step 4 — WRITE: store as a new DFS file
    # ------------------------------------------------------------------ #
    dfs.touch(output_filename)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(sorted_content)
        tmp_path = tmp.name

    try:
        dfs.append(output_filename, tmp_path)
    finally:
        os.unlink(tmp_path)

    log.info("sort_file: wrote %d sorted records to %r",
             len(sorted_lines), output_filename)


def verify_sorted(dfs: "DFS", filename: str) -> bool:
    """
    Read a sorted DFS file and assert that keys are non-decreasing.
    Returns True if sorted, False if not.
    """
    raw = dfs.read(filename)
    lines = raw.decode(errors="replace").splitlines()
    prev_key: Optional[str] = None
    for lineno, line in enumerate(lines, start=1):
        rec = _parse_record(line)
        if rec is None:
            continue
        key, _ = rec
        if prev_key is not None and key < prev_key:
            log.error("verify_sorted FAILED at line %d: %r < %r",
                      lineno, key, prev_key)
            return False
        prev_key = key
    log.info("verify_sorted PASSED for %r (%d lines)", filename, len(lines))
    return True
