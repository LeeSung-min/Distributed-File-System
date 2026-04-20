"""
tests/test_dfs.py

Integration tests for the full DFS stack.

Run with:
    cd /path/to/dfs
    python -m pytest tests/ -v

Or directly:
    python tests/test_dfs.py
"""

import os
import sys
import tempfile
import time
import unittest

# Make sure the project root is on the path.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from chord.node import ChordNode, sha1_int, in_range
from chord.ring import ChordRing
from dfs.api import DFS, FileNotFoundError, FileExistsError
from dfs.sort import sort_file, verify_sorted
from paxos.replica import Replica, PaxosLeader, ReplicationGroup


# ---------------------------------------------------------------------------
# Chord unit tests
# ---------------------------------------------------------------------------

class TestChordBasics(unittest.TestCase):

    def test_sha1_int_deterministic(self):
        a = sha1_int("hello")
        b = sha1_int("hello")
        self.assertEqual(a, b)

    def test_sha1_int_range(self):
        v = sha1_int("any string")
        self.assertGreaterEqual(v, 0)
        self.assertLess(v, 2**160)

    def test_in_range_simple(self):
        self.assertTrue(in_range(5, 3, 9))
        self.assertFalse(in_range(2, 3, 9))
        self.assertFalse(in_range(9, 3, 9))

    def test_in_range_inclusive_hi(self):
        self.assertTrue(in_range(9, 3, 9, inclusive_hi=True))

    def test_in_range_wrap(self):
        # Arc that wraps past zero
        self.assertTrue(in_range(1, 10, 5))   # 1 < 5
        self.assertTrue(in_range(11, 10, 5))  # 11 > 10
        self.assertFalse(in_range(7, 10, 5))  # 5 < 7 < 10 — NOT in arc

    def test_single_node_ring(self):
        n = ChordNode(node_id=42, name="solo")
        n.create()
        self.assertIs(n.successor, n)
        # stabilize() runs in background and will set predecessor to self
        # almost immediately in a one-node ring — both None and self are valid.
        self.assertIn(n.predecessor, (None, n))
        n.stop()

    def test_local_store(self):
        n = ChordNode(node_id=100, name="storetest")
        n.create()
        n.local_put(999, b"hello")
        self.assertEqual(n.local_get(999), b"hello")
        self.assertTrue(n.local_delete(999))
        self.assertIsNone(n.local_get(999))
        n.stop()


class TestChordRing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ring = ChordRing(num_nodes=5)
        cls.ring.bootstrap(settle_time=2.0)

    @classmethod
    def tearDownClass(cls):
        cls.ring.shutdown()

    def test_ring_has_correct_node_count(self):
        self.assertEqual(len(self.ring.nodes), 5)

    def test_find_successor_returns_a_node(self):
        key = sha1_int("test-key")
        node = self.ring.nodes[0].find_successor(key)
        self.assertIsNotNone(node)

    def test_put_get_roundtrip(self):
        key = sha1_int("roundtrip-key")
        self.ring.nodes[0].put(key, b"value123")
        got = self.ring.nodes[0].get(key)
        self.assertEqual(got, b"value123")

    def test_delete(self):
        key = sha1_int("delete-key")
        self.ring.nodes[0].put(key, b"to-delete")
        self.ring.nodes[0].delete(key)
        self.assertIsNone(self.ring.nodes[0].get(key))

    def test_routing_is_deterministic(self):
        key = sha1_int("stable-key")
        n1 = self.ring.nodes[0].find_successor(key)
        n2 = self.ring.nodes[0].find_successor(key)
        self.assertEqual(n1.id, n2.id)


# ---------------------------------------------------------------------------
# Paxos unit tests
# ---------------------------------------------------------------------------

class TestPaxos(unittest.TestCase):

    def _make_group(self, num_replicas=3):
        committed = []
        group = ReplicationGroup(
            group_id=1,
            num_replicas=num_replicas,
            apply_fn=lambda op: committed.append(op),
        )
        return group, committed

    def test_single_commit_succeeds(self):
        group, log = self._make_group()
        ok = group.commit({"action": "test", "data": 42})
        self.assertTrue(ok)

    def test_committed_op_appears_in_log(self):
        group, _ = self._make_group()
        group.commit({"action": "touch", "filename": "x.txt"})
        paxos_log = group.get_committed_log()
        self.assertEqual(len(paxos_log), 1)
        ballot, op = paxos_log[0]
        self.assertEqual(op["action"], "touch")

    def test_multiple_commits_ordered(self):
        group, _ = self._make_group()
        for i in range(5):
            group.commit({"action": "op", "seq": i})
        paxos_log = group.get_committed_log()
        self.assertEqual(len(paxos_log), 5)
        ballots = [b for b, _ in paxos_log]
        self.assertEqual(ballots, sorted(ballots))

    def test_majority_with_3_replicas(self):
        # Majority = 2. With all 3 healthy the commit must succeed.
        group, _ = self._make_group(num_replicas=3)
        self.assertEqual(group.leader.majority, 2)
        self.assertTrue(group.commit({"action": "ping"}))

    def test_ballot_increases_monotonically(self):
        group, _ = self._make_group()
        group.commit({"op": 1})
        group.commit({"op": 2})
        log = group.get_committed_log()
        b1, b2 = log[0][0], log[1][0]
        self.assertLess(b1, b2)


# ---------------------------------------------------------------------------
# DFS integration tests
# ---------------------------------------------------------------------------

class TestDFSOperations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ring = ChordRing(num_nodes=5)
        cls.ring.bootstrap(settle_time=2.0)
        cls.dfs = DFS(ring=cls.ring, num_paxos_replicas=3)

        # Write some temp files for append tests.
        cls._tmpdir = tempfile.mkdtemp()

        cls.hello_path = os.path.join(cls._tmpdir, "hello.txt")
        with open(cls.hello_path, "w") as f:
            f.write("hello world\nline two\nline three\n")

        cls.csv_path = os.path.join(cls._tmpdir, "data.csv")
        with open(cls.csv_path, "w") as f:
            f.write("0050,eve\n0010,alice\n0090,carol\n0030,bob\n0070,dave\n")

        cls.big_path = os.path.join(cls._tmpdir, "big.txt")
        with open(cls.big_path, "wb") as f:
            # Write 3 pages worth of data.
            f.write(b"x" * (64 * 1024 * 3))

    @classmethod
    def tearDownClass(cls):
        cls.ring.shutdown()
        import shutil
        shutil.rmtree(cls._tmpdir, ignore_errors=True)

    def test_touch_creates_file(self):
        self.dfs.touch("_tc_test.txt")
        files = self.dfs.ls()
        self.assertIn("_tc_test.txt", files)

    def test_touch_duplicate_raises(self):
        self.dfs.touch("_dup_test.txt")
        with self.assertRaises(FileExistsError):
            self.dfs.touch("_dup_test.txt")

    def test_read_nonexistent_raises(self):
        with self.assertRaises(FileNotFoundError):
            self.dfs.read("nonexistent.txt")

    def test_append_and_read_roundtrip(self):
        self.dfs.touch("_ar_test.txt")
        self.dfs.append("_ar_test.txt", self.hello_path)
        data = self.dfs.read("_ar_test.txt")
        self.assertIn(b"hello world", data)
        self.assertIn(b"line two", data)

    def test_head(self):
        self.dfs.touch("_head_test.txt")
        self.dfs.append("_head_test.txt", self.hello_path)
        result = self.dfs.head("_head_test.txt", n=1)
        self.assertEqual(result.strip(), "hello world")

    def test_tail(self):
        self.dfs.touch("_tail_test.txt")
        self.dfs.append("_tail_test.txt", self.hello_path)
        result = self.dfs.tail("_tail_test.txt", n=1)
        self.assertEqual(result.strip(), "line three")

    def test_stat_returns_correct_fields(self):
        self.dfs.touch("_stat_test.txt")
        self.dfs.append("_stat_test.txt", self.hello_path)
        stat = self.dfs.stat("_stat_test.txt")
        self.assertEqual(stat["filename"], "_stat_test.txt")
        self.assertGreater(stat["size_bytes"], 0)
        self.assertGreater(stat["num_pages"], 0)
        self.assertIn("created_at", stat)
        self.assertIn("version", stat)

    def test_delete_removes_file(self):
        self.dfs.touch("_del_test.txt")
        self.dfs.delete_file("_del_test.txt")
        self.assertNotIn("_del_test.txt", self.dfs.ls())

    def test_delete_nonexistent_raises(self):
        with self.assertRaises(FileNotFoundError):
            self.dfs.delete_file("ghost_file.txt")

    def test_multi_page_file(self):
        self.dfs.touch("_big_test.bin")
        self.dfs.append("_big_test.bin", self.big_path)
        stat = self.dfs.stat("_big_test.bin")
        self.assertEqual(stat["num_pages"], 3)
        data = self.dfs.read("_big_test.bin")
        self.assertEqual(len(data), 64 * 1024 * 3)

    def test_version_increments_on_append(self):
        self.dfs.touch("_ver_test.txt")
        stat1 = self.dfs.stat("_ver_test.txt")
        self.dfs.append("_ver_test.txt", self.hello_path)
        stat2 = self.dfs.stat("_ver_test.txt")
        self.assertGreater(stat2["version"], stat1["version"])

    def test_ls_returns_all_files(self):
        self.dfs.touch("_ls_a.txt")
        self.dfs.touch("_ls_b.txt")
        files = self.dfs.ls()
        self.assertIn("_ls_a.txt", files)
        self.assertIn("_ls_b.txt", files)


# ---------------------------------------------------------------------------
# Distributed sort tests
# ---------------------------------------------------------------------------

class TestDistributedSort(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ring = ChordRing(num_nodes=5)
        cls.ring.bootstrap(settle_time=2.0)
        cls.dfs = DFS(ring=cls.ring, num_paxos_replicas=3)
        cls._tmpdir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        cls.ring.shutdown()
        import shutil
        shutil.rmtree(cls._tmpdir, ignore_errors=True)

    def _write_csv(self, name: str, records: list[tuple[str, str]]) -> str:
        path = os.path.join(self._tmpdir, name)
        with open(path, "w") as f:
            for k, v in records:
                f.write(f"{k},{v}\n")
        return path

    def test_sort_five_records(self):
        records = [("0050", "eve"), ("0010", "alice"), ("0090", "carol"),
                   ("0030", "bob"), ("0070", "dave")]
        path = self._write_csv("five.csv", records)
        self.dfs.touch("five_in.csv")
        self.dfs.append("five_in.csv", path)

        sort_file(self.dfs, "five_in.csv", "five_out.csv")
        self.assertTrue(verify_sorted(self.dfs, "five_out.csv"))

    def test_sort_already_sorted(self):
        records = [("0001", "a"), ("0002", "b"), ("0003", "c")]
        path = self._write_csv("asc.csv", records)
        self.dfs.touch("asc_in.csv")
        self.dfs.append("asc_in.csv", path)

        sort_file(self.dfs, "asc_in.csv", "asc_out.csv")
        self.assertTrue(verify_sorted(self.dfs, "asc_out.csv"))

    def test_sort_reverse_order(self):
        records = [("0090", "z"), ("0060", "m"), ("0030", "a"), ("0010", "aa")]
        path = self._write_csv("rev.csv", records)
        self.dfs.touch("rev_in.csv")
        self.dfs.append("rev_in.csv", path)

        sort_file(self.dfs, "rev_in.csv", "rev_out.csv")
        self.assertTrue(verify_sorted(self.dfs, "rev_out.csv"))

    def test_sort_preserves_all_records(self):
        records = [(f"{i:04d}", f"val{i}") for i in range(50, 0, -1)]
        path = self._write_csv("fifty.csv", records)
        self.dfs.touch("fifty_in.csv")
        self.dfs.append("fifty_in.csv", path)

        sort_file(self.dfs, "fifty_in.csv", "fifty_out.csv")
        result = self.dfs.read("fifty_out.csv").decode()
        out_records = [l for l in result.strip().splitlines() if l]
        self.assertEqual(len(out_records), 50)
        self.assertTrue(verify_sorted(self.dfs, "fifty_out.csv"))

    def test_verify_sorted_catches_bad_order(self):
        """verify_sorted should return False for a deliberately unsorted file."""
        path = self._write_csv("bad.csv", [("0090", "z"), ("0010", "a")])
        self.dfs.touch("bad_in.csv")
        self.dfs.append("bad_in.csv", path)
        # Don't sort it — just verify directly to confirm it fails.
        self.assertFalse(verify_sorted(self.dfs, "bad_in.csv"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
