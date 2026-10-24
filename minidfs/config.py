# MiniDFS Configuration
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8080

# Chunk configuration (in bytes)
DEFAULT_CHUNK_SIZE = 64 * 1024  # 64 KB for demo/simulation
HEARTBEAT_INTERVAL = 1.5         # Seconds between heartbeats
HEARTBEAT_TIMEOUT = 4.5          # Node marked dead after 4.5 seconds

# Storage Nodes configuration
STORAGE_NODES_CONFIG = {
    "SN1": {
        "id": "SN1",
        "name": "Storage Node 1",
        "rank": 1,
        "capacity_bytes": 10 * 1024 * 1024,
        "primary_chunk": "A",
        "replica_chunk": "B",
        "next_node": "SN2",
        "prev_node": "SN3",
        "color": "#38bdf8"
    },
    "SN2": {
        "id": "SN2",
        "name": "Storage Node 2",
        "rank": 2,
        "capacity_bytes": 10 * 1024 * 1024,
        "primary_chunk": "B",
        "replica_chunk": "C",
        "next_node": "SN3",
        "prev_node": "SN1",
        "color": "#a855f7"
    },
    "SN3": {
        "id": "SN3",
        "name": "Storage Node 3",
        "rank": 3,
        "capacity_bytes": 10 * 1024 * 1024,
        "primary_chunk": "C",
        "replica_chunk": "A",
        "next_node": "SN1",
        "prev_node": "SN2",
        "color": "#10b981"
    }
}
