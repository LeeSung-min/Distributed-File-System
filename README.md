# Distributed File System on Chord

A distributed file system (DFS) built on top of a Chord DHT, with Paxos-based
metadata replication and distributed sorting.

## Requirements

Python 3.10+. No external dependencies — standard library only.

## Project Structure

```
dfs/
├── chord/
│   ├── node.py       # Chord peer: finger table, successor routing, local KV store
│   └── ring.py       # Ring bootstrapper for N local nodes
├── dfs/
│   ├── models.py     # FileMetadata and PageDescriptor data classes
│   ├── api.py        # DFS API: touch, append, read, head, tail, delete, ls, stat
│   └── sort.py       # Distributed sort + verify_sorted
├── paxos/
│   └── replica.py    # Replica, PaxosLeader, ReplicationGroup
├── cli/
│   └── shell.py      # CLI dispatcher + interactive shell
├── tests/
│   └── test_dfs.py   # 34 integration tests
├── main.py           # Entry point
├── demo.py           # End-to-end demo with Paxos log output
└── sample_data.csv   # Sample input for sort demo
```

## Running

```bash
# Interactive shell
python main.py shell

# Single commands
python main.py touch myfile.txt
python main.py append myfile.txt ./local.txt
python main.py read myfile.txt
python main.py head myfile.txt 5
python main.py tail myfile.txt 5
python main.py ls
python main.py stat myfile.txt
python main.py delete myfile.txt
python main.py sort input.csv output.csv
python main.py verify output.csv

# Full end-to-end demo (generates Paxos logs)
python demo.py

# Tests
python tests/test_dfs.py
```

## Architecture

### Chord Layer (`chord/`)

Each `ChordNode` owns a slice of the SHA-1 ring [0, 2^160). Nodes maintain
a 160-entry finger table for O(log N) routing. `stabilize()` and
`fix_fingers()` run in background threads to keep the ring consistent
after joins.

Key derivation:
- `metadata_key = sha1_int("metadata:" + filename)`
- `page_key     = sha1_int(filename + ":" + page_no)`

### DFS Layer (`dfs/`)

Files are split into at most PAGE_SIZE (64 KB) chunks. Each chunk is a
`PageDescriptor` stored in Chord. A `FileMetadata` object tracks all page
descriptors and is stored in Chord under `metadata_key`. Every metadata
mutation goes through Paxos before being written.

### Paxos Layer (`paxos/`)

Each file has its own `ReplicationGroup` (1 leader + 3 replicas). Protocol:

1. Leader increments ballot and broadcasts `ACCEPT(op, ballot)`.
2. Each replica responds with `LEARN(op, ballot)` if ballot >= its highest.
3. Once a strict majority (≥ 2 of 3) respond, leader broadcasts `COMMIT`.
4. All replicas apply committed ops in ballot order.

Pages are written directly to Chord without Paxos because they are immutable
(append-only; pages are never modified after being written).

### Distributed Sort (`dfs/sort.py`)

1. **SCAN** - read all pages of input file, parse `key,value` records.
2. **ROUTE** - for each record, call `find_successor(sha1_int(key))` to
   determine the responsible Chord peer; insert into that peer's local
   sorted list via `bisect.insort`.
3. **ASSEMBLE** - collect all peer segments, final `sorted()` by raw key.
4. **WRITE** - store result as a new DFS file.

## Fault Model

- Crash-only failures assumed (no Byzantine behavior).
- Messages may be delayed, lost, duplicated, or reordered.
- Paxos tolerates up to 1 crash among 3 replicas.
- Chord ring self-heals via `stabilize()` after node joins.

## Topology

- 5 Chord peers (configurable in `main.py`)
- 3 Paxos replicas per file metadata group
