import threading
import time
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client

from chord.finger_table import FingerTable
from chord.utils import chord_hash, in_range
from config import (M_BITS, RING_SIZE, STABILIZE_INTERVAL,
                    FIX_FINGERS_INTERVAL, CHORD_TIMEOUT, DEFAULT_HOST)


class ChordNode:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.node_id = chord_hash(f"{host}:{port}")
        self.finger_table = FingerTable(self.node_id)
        self.successor = (host, port)  # initially points to self
        self.predecessor = None
        self._store: dict = {}  # local key-value storage
        self._lock = threading.Lock()
        self._server = None
        self._server_thread = None
        self._running = False

    # ── Ring setup ────────────────────────────────────────────────────────

    def create(self):
        self.predecessor = None
        self.successor = (self.host, self.port)
        self.finger_table[0].node = self.successor

    def join(self, known_host: str, known_port: int):
        self.predecessor = None
        succ = self._rpc(known_host, known_port, "find_successor", self.node_id)
        self.successor = succ
        self.finger_table[0].node = succ

    def find_successor(self, key_id: int):
        raise NotImplementedError

    def closest_preceding_node(self, key_id: int):
        raise NotImplementedError

    def stabilize(self):
        raise NotImplementedError
    def notify(self, candidate_host: str, candidate_port: int):
        raise NotImplementedError
    def fix_fingers(self):
        raise NotImplementedError
    def check_predecessor(self):
        raise NotImplementedError
    def put(self, key: str, value: str) -> bool:
        raise NotImplementedError
    def get(self, key: str):
        raise NotImplementedError
    def delete(self, key: str) -> bool:
        raise NotImplementedError
    def put_local(self, key: str, value: str):
        with self._lock:
            self._store[key] = value

    def get_local(self, key: str):
        with self._lock:
            return self._store.get(key)

    def delete_local(self, key: str):
        with self._lock:
            self._store.pop(key, None)

    # ── RPC server ────────────────────────────────────────────────────────

    def start_server(self):
        self._server = SimpleXMLRPCServer(
            (self.host, self.port), logRequests=False, allow_none=True)
        self._server.register_instance(self)
        self._running = True
        self._server_thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._server_thread.start()
        threading.Thread(target=self._background_stabilize, daemon=True).start()
        threading.Thread(target=self._background_fix_fingers, daemon=True).start()
        print(f"[ChordNode] id={self.node_id} listening on {self.host}:{self.port}")

    def stop_server(self):
        self._running = False
        if self._server:
            self._server.shutdown()

    def _background_stabilize(self):
        while self._running:
            time.sleep(STABILIZE_INTERVAL)
            try:
                self.stabilize()
            except Exception as e:
                print(f"[stabilize error] {e}")

    def _background_fix_fingers(self):
        while self._running:
            time.sleep(FIX_FINGERS_INTERVAL)
            try:
                self.fix_fingers()
            except Exception as e:
                print(f"[fix_fingers error] {e}")

    # ── RPC helper ────────────────────────────────────────────────────────

    def _rpc(self, host: str, port: int, method: str, *args):
        proxy = xmlrpc.client.ServerProxy(
            f"http://{host}:{port}/", allow_none=True
        )
        return getattr(proxy, method)(*args)

    # ── Debug ─────────────────────────────────────────────────────────────

    def get_info(self) -> dict:

        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "successor": self.successor,
            "predecessor": self.predecessor,
            "finger_table": str(self.finger_table),
            "store_keys": list(self._store.keys()),
        }

    def __repr__(self):
        return f"ChordNode(id={self.node_id}, {self.host}:{self.port})"