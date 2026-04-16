from dfs.metadata import FileMetadata, PageDescriptor
from dfs.page import split_into_pages, make_page_guid
from replication.replica_manager import ReplicaManager

class DFSOperations:
    def __init__(self, entry_node, replica_mgr: ReplicaManager):
        self.node = entry_node
        self.replica_mgr = replica_mgr

    def touch(self, filename: str) -> FileMetadata:
        raise NotImplementedError

    def append(self, filename: str, local_path: str) -> FileMetadata:
        raise NotImplementedError

    def read(self, filename: str) -> bytes:
        raise NotImplementedError

    def head(self, filename: str, n: int) -> str:
        raise NotImplementedError

    def tail(self, filename: str, n: int) -> str:
        raise NotImplementedError

    def delete_file(self, filename: str):
        raise NotImplementedError

    def ls(self) -> list:
        raise NotImplementedError

    def stat(self, filename: str) -> dict:
        raise NotImplementedError

    def sort_file(self, filename: str, output_filename: str):
        raise NotImplementedError

    def _get_metadata(self, filename: str) -> FileMetadata:
        from dfs.metadata import FileMetadata, METADATA_PREFIX
        key = f"{METADATA_PREFIX}{filename}"
        raw = self.node.get(key)
        if raw is None:
            raise FileNotFoundError(f"No DFS file named '{filename}'")
        return FileMetadata.from_json(raw)

    def _update_directory(self, filename: str, remove: bool = False):
        raise NotImplementedError
