import json
import time
from dataclasses import dataclass, asdict

@dataclass
class AcceptMessage:
    """Sent by leader to all replica followers.

    Fields:
        operation   -- the DFS operation to commit, e.g.
                       {"type": "metadata_update", "metadata": <json>}
        ballot      -- monotonically increasing proposal number
        leader_id   -- node_id of the proposing leader
        filename    -- which file's metadata is being updated
        timestamp   -- wall clock for logging
    """
    operation: dict
    ballot: int
    leader_id: int
    filename: str
    timestamp: float = 0.0

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = time.time()

    def to_json(self) -> str:
        return json.dumps(asdict(self))


class LearnMessage:
    """Sent by follower back to leader as an acknowledgement.

    Fields:
        ballot      -- echoed from AcceptMessage
        follower_id -- node_id of the responding follower
        filename    -- which file
        accepted    -- True if follower accepted, False if it rejected
                       (e.g. it already saw a higher ballot)
        timestamp   -- wall clock
    """
    ballot: int
    follower_id: int
    filename: str
    accepted: bool = True
    timestamp: float = 0.0

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = time.time()

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str):
        return cls(**json.loads(raw)


class CommitMessage:
    """Sent by leader to all followers once majority is reached."""
    operation: dict
    ballot: int
    leader_id: int
    filename: str
    timestamp: float = 0.0

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = time.time()

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str):
        return cls(**json.loads(raw))