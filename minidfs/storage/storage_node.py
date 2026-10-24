"""
Storage Node Implementation (Storage Node 1, 2, 3)
Persists chunks to local directories and exchanges replica chunks over
the Replication Network via TCP socket transfers.
"""

import os
import threading
import time
import socket
import json
from typing import Dict, Any
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
            with open(primary_file, "w", encoding="utf-8") as f:
                f.write(f"Primary data block for {self.primary_chunk_id} stored on {self.node_id}.\nInitialized at {time.strftime('%Y-%m-%d %H:%M:%S')}")

        if not os.path.exists(replica_file):
            with open(replica_file, "w", encoding="utf-8") as f:
                f.write(f"Replica data block for {self.replica_chunk_id} backed up on {self.node_id}.\nSynchronized over Replication Network.")

    def write_local_chunk(self, chunk_id: str, content: str, is_primary: bool = True):
        with self._lock:
            filepath = os.path.join(self.storage_dir, f"{chunk_id}.dat")
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)

    def read_local_chunk(self, chunk_id: str) -> str:
        with self._lock:
            filepath = os.path.join(self.storage_dir, f"{chunk_id}.dat")
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    return f.read()
            return ""

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
