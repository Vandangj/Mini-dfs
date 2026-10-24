"""
Storage Node Implementation (Storage Node 1, 2, 3)
Persists chunks (audio binary, datasets, text) to local directories
and exchanges replica chunks over the Replication Network.
"""

import os
import threading
import time
from typing import Dict, Any, Union
from minidfs.config import DATA_DIR, STORAGE_NODES_CONFIG

class LocalStorageNode:
    def __init__(self, node_id: str, base_data_dir: str = DATA_DIR):
        self.node_id = node_id
        self.cfg = STORAGE_NODES_CONFIG[node_id]
        self.storage_dir = os.path.join(base_data_dir, node_id.lower())
        os.makedirs(self.storage_dir, exist_ok=True)
        
        self.primary_chunk_id = f"Chunk_{self.cfg['primary_chunk']}"
        self.replica_chunk_id = f"Chunk_{self.cfg['replica_chunk']}"
        self._lock = threading.Lock()

        # Initialize default files on disk
        self._init_disk_storage()

    def _init_disk_storage(self):
        primary_file = os.path.join(self.storage_dir, f"{self.primary_chunk_id}.dat")
        replica_file = os.path.join(self.storage_dir, f"{self.replica_chunk_id}.dat")

        if not os.path.exists(primary_file):
            with open(primary_file, "wb") as f:
                f.write(f"Primary audio/data block for {self.primary_chunk_id} stored on {self.node_id}.\nInitialized at {time.strftime('%Y-%m-%d %H:%M:%S')}".encode('utf-8'))

        if not os.path.exists(replica_file):
            with open(replica_file, "wb") as f:
                f.write(f"Replica audio/data block for {self.replica_chunk_id} backed up on {self.node_id}.\nSynchronized over Replication Network.".encode('utf-8'))

    def write_local_chunk(self, chunk_id: str, content: Union[str, bytes], is_primary: bool = True):
        with self._lock:
            # Clean filename
            safe_cid = chunk_id.replace("/", "_").replace("\\", "_")
            filepath = os.path.join(self.storage_dir, f"{safe_cid}.dat")
            raw_bytes = content.encode('utf-8') if isinstance(content, str) else content
            with open(filepath, "wb") as f:
                f.write(raw_bytes)

    def read_local_chunk(self, chunk_id: str) -> bytes:
        with self._lock:
            safe_cid = chunk_id.replace("/", "_").replace("\\", "_")
            filepath = os.path.join(self.storage_dir, f"{safe_cid}.dat")
            if os.path.exists(filepath):
                with open(filepath, "rb") as f:
                    return f.read()
            return b""

    def get_disk_usage(self) -> Dict[str, Any]:
        with self._lock:
            total_bytes = 0
            file_list = []
            for fname in os.listdir(self.storage_dir):
                fpath = os.path.join(self.storage_dir, fname)
                if os.path.isfile(fpath):
                    sz = os.path.getsize(fpath)
                    total_bytes += sz
                    file_list.append({"file": fname, "size_bytes": sz})

            return {
                "node_id": self.node_id,
                "name": self.cfg["name"],
                "storage_directory": self.storage_dir,
                "total_bytes_on_disk": total_bytes,
                "files": file_list
            }
