"""
Concept 5: Data Consistency & Ring Replication Models
Formally defined Ring Replication Scheme:
  - Storage Node 1 (SN1): Primary Chunk A, Replica of Chunk B
  - Storage Node 2 (SN2): Primary Chunk B, Replica of Chunk C
  - Storage Node 3 (SN3): Primary Chunk C, Replica of Chunk A
Consistency Models supported:
  1. Synchronous Quorum (Strong Consistency: W=2, R=1 or W=1, R=2)
  2. Eventual Consistency (Async replication over Replication Network)
  3. Causal Consistency (Lamport timestamped update ordering)
"""

import threading
import time
from typing import Dict, Any, List, Optional
from minidfs.clocks.lamport_clock import LamportClock

class ChunkData:
    def __init__(self, chunk_id: str, content: str = "", version: int = 1, lamport_ts: int = 1):
        self.chunk_id = chunk_id
        self.content = content
        self.version = version
        self.lamport_ts = lamport_ts
        self.last_updated = time.time()
        self.checksum = hash(content)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "content": self.content,
            "version": self.version,
            "lamport_ts": self.lamport_ts,
            "last_updated": time.strftime("%H:%M:%S", time.localtime(self.last_updated)),
            "checksum": self.checksum,
            "size_bytes": len(self.content.encode('utf-8'))
        }

class RingReplicationManager:
    def __init__(self):
        # Ring mapping definition
        # Node -> (Primary Chunk ID, Replica Chunk ID)
        self.topology = {
            "SN1": {"primary": "Chunk_A", "replica": "Chunk_B", "replica_source": "SN2"},
            "SN2": {"primary": "Chunk_B", "replica": "Chunk_C", "replica_source": "SN3"},
            "SN3": {"primary": "Chunk_C", "replica": "Chunk_A", "replica_source": "SN1"}
        }

        # Where each chunk lives:
        # chunk_id -> {"primary_node": node, "replica_node": node}
        self.chunk_locations = {
            "Chunk_A": {"primary_node": "SN1", "replica_node": "SN3"},
            "Chunk_B": {"primary_node": "SN2", "replica_node": "SN1"},
            "Chunk_C": {"primary_node": "SN3", "replica_node": "SN2"}
        }

        # In-memory chunk stores per node:
        # node_id -> {"primary": ChunkData, "replica": ChunkData}
        self.storage = {
            "SN1": {
                "primary": ChunkData("Chunk_A", "Initial primary payload for Chunk A on SN1"),
                "replica": ChunkData("Chunk_B", "Initial replica payload of Chunk B on SN1")
            },
            "SN2": {
                "primary": ChunkData("Chunk_B", "Initial primary payload for Chunk B on SN2"),
                "replica": ChunkData("Chunk_C", "Initial replica payload of Chunk C on SN2")
            },
            "SN3": {
                "primary": ChunkData("Chunk_C", "Initial primary payload for Chunk C on SN3"),
                "replica": ChunkData("Chunk_A", "Initial replica payload of Chunk A on SN3")
            }
        }

        self.replication_log: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self.consistency_mode = "QUORUM_STRONG"  # or EVENTUAL or CAUSAL

    def set_consistency_mode(self, mode: str):
        with self._lock:
            if mode in ["QUORUM_STRONG", "EVENTUAL", "CAUSAL"]:
                self.consistency_mode = mode

    def write_chunk(self, chunk_id: str, new_content: str, lamport_clock: LamportClock, client_id: str = "Client-1") -> Dict[str, Any]:
        """
        Executes a write to a chunk under the selected consistency model.
        """
        with self._lock:
            if chunk_id not in self.chunk_locations:
                return {"error": f"Unknown chunk {chunk_id}"}

            info = self.chunk_locations[chunk_id]
            primary_node = info["primary_node"]
            replica_node = info["replica_node"]

            # Advance Lamport clock
            ts = lamport_clock.tick(f"Write {chunk_id} from {client_id}")

            primary_chunk = self.storage[primary_node]["primary"]
            new_version = primary_chunk.version + 1

            # Step 1: Write to Primary Node
            primary_chunk.content = new_content
            primary_chunk.version = new_version
            primary_chunk.lamport_ts = ts
            primary_chunk.last_updated = time.time()
            primary_chunk.checksum = hash(new_content)

            replication_entry = {
                "timestamp": time.strftime("%H:%M:%S"),
                "chunk_id": chunk_id,
                "version": new_version,
                "lamport_ts": ts,
                "primary_node": primary_node,
                "replica_node": replica_node,
                "consistency_mode": self.consistency_mode,
                "replicated": False
            }

            if self.consistency_mode == "QUORUM_STRONG":
                # Quorum W=2: Synchronously replicate to replica node before returning OK
                replica_chunk = self.storage[replica_node]["replica"]
                replica_chunk.content = new_content
                replica_chunk.version = new_version
                replica_chunk.lamport_ts = ts
                replica_chunk.last_updated = time.time()
                replica_chunk.checksum = hash(new_content)
                replication_entry["replicated"] = True
                replication_entry["details"] = f"Quorum achieved: Primary {primary_node} & Replica {replica_node} committed version {new_version} synchronously (W=2, N=2)"

            elif self.consistency_mode == "EVENTUAL":
                # Async eventual replication: Primary responds immediately, replica synced in background
                def async_replicate():
                    time.sleep(1.0)  # Simulate network propagation delay
                    with self._lock:
                        replica_chunk = self.storage[replica_node]["replica"]
                        replica_chunk.content = new_content
                        replica_chunk.version = new_version
                        replica_chunk.lamport_ts = ts
                        replica_chunk.last_updated = time.time()
                        replica_chunk.checksum = hash(new_content)
                        replication_entry["replicated"] = True
                
                threading.Thread(target=async_replicate, daemon=True).start()
                replication_entry["details"] = f"Eventual: Primary {primary_node} committed. Async replication enqueued to {replica_node} over Replication Network"

            elif self.consistency_mode == "CAUSAL":
                # Causal consistency: Replica verifies Lamport timestamp > current replica ts
                replica_chunk = self.storage[replica_node]["replica"]
                if ts >= replica_chunk.lamport_ts:
                    replica_chunk.content = new_content
                    replica_chunk.version = new_version
                    replica_chunk.lamport_ts = ts
                    replica_chunk.last_updated = time.time()
                    replica_chunk.checksum = hash(new_content)
                    replication_entry["replicated"] = True
                    replication_entry["details"] = f"Causal ordering preserved: TS {ts} > {replica_chunk.lamport_ts}. Replicated on {replica_node}"
                else:
                    replication_entry["details"] = f"Causal violation detected: Incoming TS {ts} <= {replica_chunk.lamport_ts}. Update rejected or queued"

            self.replication_log.append(replication_entry)
            if len(self.replication_log) > 30:
                self.replication_log.pop(0)

            return {
                "status": "SUCCESS",
                "chunk_id": chunk_id,
                "version": new_version,
                "lamport_ts": ts,
                "primary_node": primary_node,
                "replica_node": replica_node,
                "consistency_mode": self.consistency_mode,
                "quorum_achieved": replication_entry.get("replicated", False),
                "checksum": primary_chunk.checksum
            }

    def read_chunk(self, chunk_id: str, prefer_replica: bool = False) -> Dict[str, Any]:
        """
        Reads chunk according to consistency model and quorum rules.
        """
        with self._lock:
            if chunk_id not in self.chunk_locations:
                return {"error": f"Unknown chunk {chunk_id}"}

            info = self.chunk_locations[chunk_id]
            primary_node = info["primary_node"]
            replica_node = info["replica_node"]

            p_chunk = self.storage[primary_node]["primary"]
            r_chunk = self.storage[replica_node]["replica"]

            # Quorum read (R=1 or R=2): compare versions
            latest_chunk = p_chunk if p_chunk.version >= r_chunk.version else r_chunk
            is_consistent = (p_chunk.version == r_chunk.version and p_chunk.checksum == r_chunk.checksum)

            chosen_node = replica_node if (prefer_replica and is_consistent) else primary_node
            read_data = p_chunk if chosen_node == primary_node else r_chunk

            return {
                "chunk_id": chunk_id,
                "read_from": chosen_node,
                "is_replica": (chosen_node == replica_node),
                "content": read_data.content,
                "version": read_data.version,
                "lamport_ts": read_data.lamport_ts,
                "is_consistent": is_consistent,
                "primary_version": p_chunk.version,
                "replica_version": r_chunk.version
            }

    def get_topology_state(self) -> Dict[str, Any]:
        with self._lock:
            nodes_status = {}
            for nid, config in self.topology.items():
                p_chunk = self.storage[nid]["primary"].to_dict()
                r_chunk = self.storage[nid]["replica"].to_dict()
                nodes_status[nid] = {
                    "node_id": nid,
                    "primary_holding": p_chunk,
                    "replica_holding": r_chunk,
                    "expected_primary": config["primary"],
                    "expected_replica": config["replica"],
                    "replica_source": config["replica_source"]
                }

            # Overall consistency check across ring
            consistency_audit = []
            for cid, loc in self.chunk_locations.items():
                p = self.storage[loc["primary_node"]]["primary"]
                r = self.storage[loc["replica_node"]]["replica"]
                synced = (p.version == r.version and p.checksum == r.checksum)
                consistency_audit.append({
                    "chunk_id": cid,
                    "primary_node": loc["primary_node"],
                    "replica_node": loc["replica_node"],
                    "primary_version": p.version,
                    "replica_version": r.version,
                    "synchronized": synced
                })

            return {
                "consistency_mode": self.consistency_mode,
                "nodes": nodes_status,
                "audit": consistency_audit,
                "recent_replication_events": list(self.replication_log[-10:])
            }
