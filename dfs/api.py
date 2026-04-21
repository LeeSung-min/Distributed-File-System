"""
dfs/api.py

Design notes:
  - Every filename maps to exactly one metadata_key via sha1_int().
    This makes metadata placement deterministic and reproducible.
  - Each page is also keyed deterministically: sha1_int(filename:N).
    Any node in the ring can reconstruct a page key from the filename
    alone without consulting the metadata first.
  - All metadata mutations go through Paxos before being written to
    Chord so that replicas stay consistent.
  - Page content is written directly to Chord without Paxos because
    pages are immutable: once written, a page never changes.
    (Append creates new pages; it doesn't overwrite old ones.)
"""

import logging
import os
from typing import Optional

from chord.node import sha1_int
from chord.ring import ChordRing
from paxos.replica import ReplicationGroup
from dfs.models import FileMetadata, PageDescriptor

log = logging.getLogger(__name__)

# Maximum bytes per page. 64 KB is a reasonable default.
PAGE_SIZE = 64 * 1024


def _metadata_key(filename: str) -> int:
    """Stable Chord key for a file's metadata object."""
    return sha1_int(f"metadata:{filename}")


def _page_key(filename: str, page_no: int) -> int:
    """Stable Chord key for one page of a file."""
    return sha1_int(f"{filename}:{page_no}")


class DFSError(Exception):
    pass


class FileNotFoundError(DFSError):
    pass


class FileExistsError(DFSError):
    pass


class DFS:
    """
    Distributed File System API.

    Parameters
    ----------
    ring : ChordRing
        A bootstrapped Chord ring.  The DFS routes all storage through it.
    num_paxos_replicas : int
        How many replicas to keep for metadata.  Must be >= 3.
    """

    def __init__(self, ring: ChordRing, num_paxos_replicas: int = 3):
        self.ring = ring
        self._paxos_groups: dict[str, ReplicationGroup] = {}
        self._num_replicas = num_paxos_replicas

        log.info("DFS initialized with %d Chord peers, %d Paxos replicas",
                 len(ring.nodes), num_paxos_replicas)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_or_create_paxos_group(self, filename: str) -> ReplicationGroup:
        """
        Each file gets its own Paxos replication group for its metadata.

        apply_fn is the callback that actually persists a committed
        metadata object into the Chord ring.  It receives the raw op dict
        that was proposed, extracts the metadata sub-dict, serializes it
        to JSON bytes, and writes those bytes to the correct Chord node.
        """
        if filename not in self._paxos_groups:
            # Capture filename in the closure explicitly.
            _fname = filename
            _ring = self.ring

            def apply_fn(op: dict):
                action = op.get("action")
                if action not in ("touch", "append", "delete"):
                    return
                meta_dict = op.get("metadata")
                if not meta_dict:
                    return

                import json as _json
                key = _metadata_key(_fname)

                if action == "delete":
                    # Tombstone: remove the key so ls() stops seeing it.
                    _ring.get_node_for_key(key).local_delete(key)
                else:
                    # Serialize the plain dict directly.
                    raw = _json.dumps(meta_dict).encode()
                    _ring.get_node_for_key(key).local_put(key, raw)

                log.debug("apply_fn: %s metadata for %r", action, _fname)

            self._paxos_groups[filename] = ReplicationGroup(
                group_id=hash(filename) % (2**31),
                num_replicas=self._num_replicas,
                apply_fn=apply_fn,
            )
        return self._paxos_groups[filename]

    def _load_metadata(self, filename: str) -> Optional[FileMetadata]:
        """Fetch metadata from the Chord ring, or None if not found."""
        key = _metadata_key(filename)
        raw = self.ring.get_node_for_key(key).local_get(key)
        if raw is None:
            return None
        return FileMetadata.from_bytes(raw)

    def _commit_metadata(self, filename: str, metadata: FileMetadata, action: str):
        """
        Push a metadata update through Paxos.

        The Paxos apply_fn will write the committed metadata to Chord.
        If Paxos can't reach a majority, we raise so the caller knows
        the write didn't land.
        """
        group = self._get_or_create_paxos_group(filename)
        op = {
            "action": action,
            "filename": filename,
            "metadata": metadata.to_dict(),
        }
        success = group.commit(op)
        if not success:
            raise DFSError(f"Paxos commit failed for {action!r} on {filename!r} "
                           f"(no majority reached)")
        log.info("Paxos committed %r for file %r (v%d)",
                 action, filename, metadata.version)

    # ------------------------------------------------------------------
    # Public DFS API
    # ------------------------------------------------------------------

    def touch(self, filename: str):
        """
        Create an empty file. Raises FileExistsError if it already exists.
        """
        if self._load_metadata(filename) is not None:
            raise FileExistsError(f"File already exists: {filename!r}")

        meta = FileMetadata(filename=filename)
        self._commit_metadata(filename, meta, action="touch")
        log.info("touch(%r) done", filename)

    def append(self, filename: str, local_path: str):
        """
        Read a local file and append its contents to the DFS file as
        one or more pages.

        Each page is written directly to Chord (pages are immutable).
        The updated metadata is committed through Paxos.
        """
        meta = self._load_metadata(filename)
        if meta is None:
            raise FileNotFoundError(f"File not found: {filename!r}")

        if not os.path.exists(local_path):
            raise DFSError(f"Local file not found: {local_path!r}")

        with open(local_path, "rb") as fh:
            data = fh.read()

        if not data:
            log.warning("append: local file %r is empty, nothing to do", local_path)
            return

        # Slice the data into pages and store each one.
        replica_names = [n.name for n in self.ring.nodes[:self._num_replicas]]

        offset = 0
        while offset < len(data):
            chunk = data[offset: offset + PAGE_SIZE]
            pkey = _page_key(filename, meta.num_pages)

            # Write the raw page bytes to Chord.
            responsible = self.ring.get_node_for_key(pkey)
            responsible.local_put(pkey, chunk)
            log.debug("Stored page %d for %r at node %s (%d bytes)",
                      meta.num_pages, filename, responsible.name, len(chunk))

            meta.add_page(pkey, len(chunk), replica_names)
            offset += PAGE_SIZE

        self._commit_metadata(filename, meta, action="append")
        log.info("append(%r, %r): %d bytes → %d pages",
                 filename, local_path, len(data), meta.num_pages)

    def read(self, filename: str) -> bytes:
        """
        Reassemble and return the full file contents.
        """
        meta = self._load_metadata(filename)
        if meta is None:
            raise FileNotFoundError(f"File not found: {filename!r}")

        parts = []
        for pd in meta.pages:
            chunk = self.ring.get_node_for_key(pd.page_key).local_get(pd.page_key)
            if chunk is None:
                raise DFSError(
                    f"Missing page {pd.page_no} (key={pd.page_key}) for {filename!r}"
                )
            parts.append(chunk)

        return b"".join(parts)

    def head(self, filename: str, n: int = 10) -> str:
        """
        Return the first `n` lines of a file.
        """
        data = self.read(filename)
        lines = data.decode(errors="replace").splitlines()
        return "\n".join(lines[:n])

    def tail(self, filename: str, n: int = 10) -> str:
        """
        Return the last `n` lines of a file.
        """
        data = self.read(filename)
        lines = data.decode(errors="replace").splitlines()
        return "\n".join(lines[-n:])

    def delete_file(self, filename: str):
        """
        Remove a file. Deletes all pages from Chord, then removes metadata.
        """
        meta = self._load_metadata(filename)
        if meta is None:
            raise FileNotFoundError(f"File not found: {filename!r}")

        # Remove every page from Chord.
        for pd in meta.pages:
            self.ring.get_node_for_key(pd.page_key).local_delete(pd.page_key)

        # Remove the metadata entry.
        key = _metadata_key(filename)
        self.ring.get_node_for_key(key).local_delete(key)

        # Commit a tombstone through Paxos so replicas learn of the deletion.
        self._commit_metadata(filename, meta, action="delete")
        log.info("delete_file(%r) done", filename)

    def ls(self) -> list[str]:
        """
        List all files in the DFS.
        We scan all nodes for metadata keys.
        """
        filenames = []
        seen_keys: set[int] = set()

        for node in self.ring.nodes:
            for key in node.local_list_keys():
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                raw = node.local_get(key)
                if raw is None:
                    continue
                # Try to parse as metadata; skip non-metadata entries.
                try:
                    meta = FileMetadata.from_bytes(raw)
                    filenames.append(meta.filename)
                except Exception:
                    pass

        return sorted(filenames)

    def stat(self, filename: str) -> dict:
        """
        Return metadata about a file as a human-readable dict.
        """
        meta = self._load_metadata(filename)
        if meta is None:
            raise FileNotFoundError(f"File not found: {filename!r}")

        import time as _time
        return {
            "filename": meta.filename,
            "size_bytes": meta.size_bytes,
            "num_pages": meta.num_pages,
            "version": meta.version,
            "created_at": _time.strftime(
                "%Y-%m-%d %H:%M:%S", _time.localtime(meta.created_at)
            ),
            "pages": [
                {
                    "page_no": p.page_no,
                    "guid": p.guid,
                    "size": p.size,
                    "replicas": p.replicas,
                }
                for p in meta.pages
            ],
        }
