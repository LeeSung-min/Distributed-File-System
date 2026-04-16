# config.py — global constants for the DFS project

# ── Chord ──────────────────────────────────────────────────────────────────
M_BITS = 8                  # identifier ring size = 2^M_BITS (256 slots)
RING_SIZE = 2 ** M_BITS

STABILIZE_INTERVAL = 2.0    # seconds between stabilize() calls
FIX_FINGERS_INTERVAL = 3.0  # seconds between fix_fingers() calls
CHORD_TIMEOUT = 5.0         # socket timeout for inter-node RPC calls

# ── DFS ────────────────────────────────────────────────────────────────────
PAGE_SIZE_BYTES = 4096       # 4 KB pages
METADATA_PREFIX = "metadata:"
PAGE_SEPARATOR = ":"         # filename + PAGE_SEPARATOR + page_number

# ── Replication ────────────────────────────────────────────────────────────
REPLICATION_FACTOR = 3       # R = 3 replicas per metadata object

# ── Paxos ──────────────────────────────────────────────────────────────────
PAXOS_TIMEOUT = 3.0          # seconds to wait for LEARN responses
PAXOS_RETRY_LIMIT = 3        # max retransmit attempts before giving up

# ── Networking ─────────────────────────────────────────────────────────────
DEFAULT_HOST = "127.0.0.1"
BASE_PORT = 5000             # node 0 listens here; node i listens at BASE_PORT+i