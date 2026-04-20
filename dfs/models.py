"""
dfs/models.py

Plain data classes for the two core DFS objects.

FileMetadata  — the "inode" of our DFS. Stored in the Chord ring
                under hash("metadata:" + filename).

PageDescriptor — describes where one chunk of file content lives.
                 The actual bytes are stored in Chord under page_key.

These classes deliberately stay simple: no business logic, just
structured data with JSON serialization. The DFS API layer owns all
the logic that operates on them.
"""

import json
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class PageDescriptor:
    """
    One page of a distributed file.

    page_no  — zero-based index within the file.
    guid     — stable identifier used when logging/debugging.
    page_key — the integer key under which raw bytes live in Chord.
    size     — byte count of this page's content.
    replicas — list of node names that hold a copy (for display only;
               actual replication is handled by the Paxos layer).
    """
    page_no: int
    guid: str
    page_key: int
    size: int = 0
    replicas: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "PageDescriptor":
        return cls(**d)


@dataclass
class FileMetadata:
    """
    Logical file descriptor — analogous to an inode.

    filename   — the user-visible name.
    size_bytes — total byte count across all pages.
    num_pages  — length of the pages list.
    pages      — ordered list of PageDescriptors.
    version    — incremented on every mutation so stale readers can
                 detect conflicts.
    created_at — unix timestamp of first touch().
    """
    filename: str
    size_bytes: int = 0
    num_pages: int = 0
    pages: list[PageDescriptor] = field(default_factory=list)
    version: int = 0
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        d = {
            "filename": self.filename,
            "size_bytes": self.size_bytes,
            "num_pages": self.num_pages,
            "pages": [p.to_dict() for p in self.pages],
            "version": self.version,
            "created_at": self.created_at,
        }
        return d

    def to_bytes(self) -> bytes:
        return json.dumps(self.to_dict(), indent=2).encode()

    @classmethod
    def from_bytes(cls, raw: bytes) -> "FileMetadata":
        d = json.loads(raw.decode())
        d["pages"] = [PageDescriptor.from_dict(p) for p in d.get("pages", [])]
        return cls(**d)

    def add_page(self, page_key: int, size: int, replica_names: list[str]):
        """Append a new page and update totals."""
        guid = str(uuid.uuid4())[:8]
        pd = PageDescriptor(
            page_no=self.num_pages,
            guid=guid,
            page_key=page_key,
            size=size,
            replicas=replica_names,
        )
        self.pages.append(pd)
        self.num_pages += 1
        self.size_bytes += size
        self.version += 1
        return pd

    def __repr__(self):
        return (f"FileMetadata(filename={self.filename!r}, "
                f"pages={self.num_pages}, bytes={self.size_bytes}, "
                f"v={self.version})")
