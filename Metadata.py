import json
import time
from dataclasses import dataclass, field, asdict
from typing import List
from config import METADATA_PREFIX
from chord.utils import chord_hash

class PageDescriptor:
    page_no: int
    guid: str
    replicas: List[str] = field(default_factory=list)
    def to_dict(self):
        return asdict(self)

    def from_dict(selfcls, d: dict):
        return cls(**d)

class FileMetadata:
    filename: str
    size_bytes: int = 0
    num_pages: int = 0
    pages: List[PageDescriptor] = field(default_factory=list)
    version: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def dht_key(self) -> str:
        return f"{METADATA_PREFIX}{self.filename}"
    def dht_key_id(self) -> int:
        return chord_hash(self.dht_key)
    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d)
    def from_json(cls,raw: str):
        d = json.loads(raw)
        d["pages"] = [PageDescriptor,from_dict (p) for p in d.get("pages", [])]
        return cls(**d)

    def add_page(self, guid: str, byte_count: int, replicas: List[str] = None):
        """Append a new page descriptor and update counters."""
        pd = PageDescriptor(
            page_no=self.num_pages,
            guid=guid,
            replicas=replicas or [],
        )
        self.pages.append(pd)
        self.num_pages += 1
        self.size_bytes += byte_count
        self.version += 1
        self.updated_at = time.time()

    def remove_all_pages(self):
        self.pages = []
        self.num_pages = 0
        self.size_bytes = 0
        self.version += 1
        self.updated_at = time.time()


def __repr__(self):
    return (f"FileMetadata('{self.filename}', "
            f"{self.num_pages} pages, {self.size_bytes} bytes, v{self.version})")