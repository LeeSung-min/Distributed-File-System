import argparse
import logging
import sys

from chord.node import ChordNode
from replication.replica_manager import ReplicaManager
from dfs.file_ops import DFSOperations
from config import DEFAULT_HOST, BASE_PORT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/node.log"),
    ]
)


def launch_node(host, port, create, join_host, join_port):
    node = ChordNode(host, port)
    replica_mgr = ReplicaManager(node)
    dfs = DFSOperations(node, replica_mgr)

    if create:
        node.create()
        print(f"[main] Created new ring. Node ID = {node.node_id}")
    else:
        node.join(join_host, join_port)
        print(f"[main] Joined ring via {join_host}:{join_port}. Node ID = {node.node_id}")

    # Register DFS API methods on the XML-RPC server
    # so CLI client can call them
    node._server_before_start = lambda server: _register_dfs(server, dfs, replica_mgr)
    node.start_server()

    # Block forever
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop_server()


def _register_dfs(server, dfs: DFSOperations, replica_mgr: ReplicaManager):
    """Register DFS methods and Paxos RPC hooks on the XML-RPC server."""
    server.register_function(dfs.touch, "dfs_touch")
    server.register_function(dfs.append, "dfs_append")
    server.register_function(dfs.read, "dfs_read")
    server.register_function(dfs.head, "dfs_head")
    server.register_function(dfs.tail, "dfs_tail")
    server.register_function(dfs.delete_file, "dfs_delete")
    server.register_function(dfs.ls, "dfs_ls")
    server.register_function(dfs.stat, "dfs_stat")
    server.register_function(dfs.sort_file, "dfs_sort")
    # Paxos endpoints
    server.register_function(replica_mgr.paxos_accept, "paxos_accept")
    server.register_function(replica_mgr.paxos_commit, "paxos_commit")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start a DFS/Chord node")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=BASE_PORT)
    parser.add_argument("--create", action="store_true", help="create a new ring")
    parser.add_argument("--join-host", default=DEFAULT_HOST)
    parser.add_argument("--join-port", type=int, default=BASE_PORT)
    args = parser.parse_args()

    launch_node(args.host, args.port, args.create, args.join_host, args.join_port)