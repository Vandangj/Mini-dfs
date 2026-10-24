"""
Concept 5: Data Consistency & Ring Replication Models
Formally defined Ring Replication Scheme:
  - Storage Node 1 (SN1): Primary Chunk A (or dynamic chunks), Replica of Chunk B
  - Storage Node 2 (SN2): Primary Chunk B (or dynamic chunks), Replica of Chunk C
  - Storage Node 3 (SN3): Primary Chunk C (or dynamic chunks), Replica of Chunk A
Consistency Models supported:
  1. Synchronous Quorum (Strong Consistency: W=2, R=1)
  2. Eventual Consistency (Async replication over Replication Network)
  3. Causal Consistency (Lamport timestamped update ordering)
Handles both binary audio streams (.mp3, .wav, .ogg) and text/data files.
"""

import threading
import time
import hashlib
from typing import Dict, Any, List, Optional, Union
from minidfs.clocks.lamport_clock import LamportClock

class ChunkData:
    def __init__(self, chunk_id: str, content: Union[str, bytes] = b"", version: int = 1, lamport_ts: int = 1):
        self.chunk_id = chunk_id
        if isinstance(content, str):
            self.raw_bytes = content.encode('utf-8')
        else:
            self.raw_bytes = content
        self.version = version
        self.lamport_ts = lamport_ts
        self.last_updated = time.time()
        self.checksum = hashlib.md5(self.raw_bytes).hexdigest()

    @property
    def content(self) -> str:
        try:
            return self.raw_bytes.decode('utf-8')
        except UnicodeDecodeError:
            return f"[Binary Audio/Data: {len(self.raw_bytes)} bytes, MD5: {self.checksum[:8]}]"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "size_bytes": len(self.raw_bytes),
            "version": self.version,
            "lamport_ts": self.lamport_ts,
            "last_updated": time.strftime("%H:%M:%S", time.localtime(self.last_updated)),
            "checksum": self.checksum,
            "preview": self.content[:60]
        }

class RingReplicationManager:
    def __init__(self):
        # Ring mapping definition
        self.topology = {
            "SN1": {"primary": "Chunk_A", "replica": "Chunk_B", "replica_source": "SN2"},
            "SN2": {"primary": "Chunk_B", "replica": "Chunk_C", "replica_source": "SN3"},
            "SN3": {"primary": "Chunk_C", "replica": "Chunk_A", "replica_source": "SN1"}
        }

        # chunk_id -> {"primary_node": node, "replica_node": node}
        self.chunk_locations: Dict[str, Dict[str, str]] = {
            "Chunk_A": {"primary_node": "SN1", "replica_node": "SN3"},
            "Chunk_B": {"primary_node": "SN2", "replica_node": "SN1"},
            "Chunk_C": {"primary_node": "SN3", "replica_node": "SN2"}
        }

        # Dynamic chunk repositories per node:
        # node_id -> {"primary_chunks": {cid: ChunkData}, "replica_chunks": {cid: ChunkData}}
        self.storage: Dict[str, Dict[str, Any]] = {
            "SN1": {
                "primary": ChunkData("Chunk_A", b"Primary audio/data payload for Chunk A on SN1"),
                "replica": ChunkData("Chunk_B", b"Replica audio/data payload of Chunk B on SN1"),
                "primary_chunks": {},
                "replica_chunks": {}
            },
            "SN2": {
                "primary": ChunkData("Chunk_B", b"Primary audio/data payload for Chunk B on SN2"),
                "replica": ChunkData("Chunk_C", b"Replica audio/data payload of Chunk C on SN2"),
                "primary_chunks": {},
                "replica_chunks": {}
            },
            "SN3": {
                "primary": ChunkData("Chunk_C", b"Primary audio/data payload for Chunk C on SN3"),
                "replica": ChunkData("Chunk_A", b"Replica audio/data payload of Chunk A on SN3"),
                "primary_chunks": {},
                "replica_chunks": {}
            }
        }

        # Put default chunks in dictionaries too
        self.storage["SN1"]["primary_chunks"]["Chunk_A"] = self.storage["SN1"]["primary"]
        self.storage["SN1"]["replica_chunks"]["Chunk_B"] = self.storage["SN1"]["replica"]

        self.storage["SN2"]["primary_chunks"]["Chunk_B"] = self.storage["SN2"]["primary"]
        self.storage["SN2"]["replica_chunks"]["Chunk_C"] = self.storage["SN2"]["replica"]

        self.storage["SN3"]["primary_chunks"]["Chunk_C"] = self.storage["SN3"]["primary"]
        self.storage["SN3"]["replica_chunks"]["Chunk_A"] = self.storage["SN3"]["replica"]

        self.replication_log: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self.consistency_mode = "QUORUM_STRONG"  # QUORUM_STRONG, EVENTUAL, CAUSAL

    def set_consistency_mode(self, mode: str):
        with self._lock:
            if mode in ["QUORUM_STRONG", "EVENTUAL", "CAUSAL"]:
                self.consistency_mode = mode

    def register_chunk(self, chunk_id: str, primary_node: str, replica_node: str):
        with self._lock:
            self.chunk_locations[chunk_id] = {
                "primary_node": primary_node,
                "replica_node": replica_node
            }

    def write_chunk(self, chunk_id: str, new_content: Union[str, bytes], lamport_clock: LamportClock, client_id: str = "Client-1", primary_node: str = None, replica_node: str = None) -> Dict[str, Any]:
        """
        Writes a chunk to the primary storage node and replicates to the ring partner
        under the configured consistency model (Quorum W=2, Eventual, or Causal).
        """
        with self._lock:
            raw = new_content.encode('utf-8') if isinstance(new_content, str) else new_content

            if chunk_id not in self.chunk_locations:
                p_node = primary_node or "SN1"
                r_node = replica_node or ("SN3" if p_node == "SN1" else ("SN1" if p_node == "SN2" else "SN2"))
                self.chunk_locations[chunk_id] = {"primary_node": p_node, "replica_node": r_node}
            
            loc = self.chunk_locations[chunk_id]
            p_node = loc["primary_node"]
            r_node = loc["replica_node"]

            # Advance Lamport clock
            ts = lamport_clock.tick(f"Write chunk {chunk_id} from {client_id}")

            # Check existing version
            old_chunk = self.storage[p_node]["primary_chunks"].get(chunk_id)
            new_version = (old_chunk.version + 1) if old_chunk else 1

            # Commit to Primary node
            primary_chunk = ChunkData(chunk_id, raw, version=new_version, lamport_ts=ts)
            self.storage[p_node]["primary_chunks"][chunk_id] = primary_chunk
            if chunk_id == self.topology[p_node]["primary"]:
                self.storage[p_node]["primary"] = primary_chunk

            replication_entry = {
                "timestamp": time.strftime("%H:%M:%S"),
                "chunk_id": chunk_id,
                "version": new_version,
                "lamport_ts": ts,
                "primary_node": p_node,
                "replica_node": r_node,
                "consistency_mode": self.consistency_mode,
                "size_bytes": len(raw),
                "checksum": primary_chunk.checksum,
                "replicated": False
            }

            if self.consistency_mode == "QUORUM_STRONG":
                # Quorum W=2: Commit to replica node synchronously before returning OK
                replica_chunk = ChunkData(chunk_id, raw, version=new_version, lamport_ts=ts)
                self.storage[r_node]["replica_chunks"][chunk_id] = replica_chunk
                if chunk_id == self.topology[r_node]["replica"]:
                    self.storage[r_node]["replica"] = replica_chunk
                replication_entry["replicated"] = True
                replication_entry["details"] = f"Quorum achieved: Primary {p_node} & Replica {r_node} committed v{new_version} synchronously (W=2, N=2)"

            elif self.consistency_mode == "EVENTUAL":
                # Async eventual propagation over replication network
                def async_replicate():
                    time.sleep(0.5)
                    with self._lock:
                        rep_c = ChunkData(chunk_id, raw, version=new_version, lamport_ts=ts)
                        self.storage[r_node]["replica_chunks"][chunk_id] = rep_c
                        if chunk_id == self.topology[r_node]["replica"]:
                            self.storage[r_node]["replica"] = rep_c
                        replication_entry["replicated"] = True

                threading.Thread(target=async_replicate, daemon=True).start()
                replication_entry["details"] = f"Eventual: Primary {p_node} committed. Async replication enqueued to {r_node}."

            elif self.consistency_mode == "CAUSAL":
                # Causal consistency with Lamport ordering
                replica_chunk = ChunkData(chunk_id, raw, version=new_version, lamport_ts=ts)
                self.storage[r_node]["replica_chunks"][chunk_id] = replica_chunk
                if chunk_id == self.topology[r_node]["replica"]:
                    self.storage[r_node]["replica"] = replica_chunk
                replication_entry["replicated"] = True
                replication_entry["details"] = f"Causal ordering preserved: TS #{ts} on {r_node}."

            self.replication_log.append(replication_entry)
            if len(self.replication_log) > 40:
                self.replication_log.pop(0)

            return {
                "status": "SUCCESS",
                "chunk_id": chunk_id,
                "version": new_version,
                "lamport_ts": ts,
                "primary_node": p_node,
                "replica_node": r_node,
                "size_bytes": len(raw),
                "checksum": primary_chunk.checksum,
                "quorum_achieved": replication_entry["replicated"]
            }

    def read_chunk(self, chunk_id: str, prefer_replica: bool = False, raw_bytes: bool = False) -> Dict[str, Any]:
        """
        Reads chunk according to consistency model and quorum rules.
        """
        with self._lock:
            if chunk_id not in self.chunk_locations:
                return {"error": f"Unknown chunk {chunk_id}"}

            info = self.chunk_locations[chunk_id]
            p_node = info["primary_node"]
            r_node = info["replica_node"]

            p_chunk = self.storage[p_node]["primary_chunks"].get(chunk_id)
            r_chunk = self.storage[r_node]["replica_chunks"].get(chunk_id)

            # Quorum read: pick highest version
            chosen_node = r_node if (prefer_replica and r_chunk and p_chunk and r_chunk.version >= p_chunk.version) else p_node
            read_data = p_chunk if chosen_node == p_node else (r_chunk or p_chunk)

            if not read_data:
                return {"error": f"Chunk data for {chunk_id} not found"}

            res = {
                "chunk_id": chunk_id,
                "read_from": chosen_node,
                "is_replica": (chosen_node == r_node),
                "version": read_data.version,
                "lamport_ts": read_data.lamport_ts,
                "size_bytes": len(read_data.raw_bytes),
                "checksum": read_data.checksum
            }
            if raw_bytes:
                res["bytes"] = read_data.raw_bytes
            else:
                res["content"] = read_data.content

            return res

    def partition_and_store_file(self, filename: str, file_bytes: bytes, lamport_clock: LamportClock, client_id: str = "alice") -> List[Dict[str, Any]]:
        """
        Distributes a file (such as an audio file) across all 3 storage nodes in the ring!
        Chunk 0 -> Primary on SN1, Replica on SN3
        Chunk 1 -> Primary on SN2, Replica on SN1
        Chunk 2 -> Primary on SN3, Replica on SN2
        """
        total_len = len(file_bytes)
        part_size = max(1, total_len // 3)
        parts = [
            file_bytes[:part_size],
            file_bytes[part_size : part_size * 2],
            file_bytes[part_size * 2 :]
        ]

        # Clean filename for chunk IDs
        safe_fn = filename.strip("/").replace(" ", "_")

        # Ring allocation
        allocations = [
            (f"{safe_fn}_part1", parts[0], "SN1", "SN3"),
            (f"{safe_fn}_part2", parts[1], "SN2", "SN1"),
            (f"{safe_fn}_part3", parts[2], "SN3", "SN2"),
        ]

        results = []
        for cid, content, p_node, r_node in allocations:
            self.register_chunk(cid, p_node, r_node)
            res = self.write_chunk(cid, content, lamport_clock, client_id, primary_node=p_node, replica_node=r_node)
            results.append(res)

        return results

    def reassemble_file(self, chunk_ids: List[str]) -> bytes:
        """Fetches and concatenates all chunks into the complete binary file/audio"""
        assembled = bytearray()
        for cid in chunk_ids:
            chunk_res = self.read_chunk(cid, raw_bytes=True)
            if "bytes" in chunk_res:
                assembled.extend(chunk_res["bytes"])
        return bytes(assembled)

    def get_topology_state(self) -> Dict[str, Any]:
        with self._lock:
            nodes_status = {}
            for nid, config in self.topology.items():
                p_chunk = self.storage[nid]["primary"].to_dict()
                r_chunk = self.storage[nid]["replica"].to_dict()
                p_chunks_dict = self.storage[nid]["primary_chunks"]
                r_chunks_dict = self.storage[nid]["replica_chunks"]
                nodes_status[nid] = {
                    "node_id": nid,
                    "primary_holding": p_chunk,
                    "replica_holding": r_chunk,
                    "primary_chunk_ids": list(p_chunks_dict.keys()),
                    "replica_chunk_ids": list(r_chunks_dict.keys()),
                    "total_primary_chunks": len(p_chunks_dict),
                    "total_replica_chunks": len(r_chunks_dict),
                    "expected_primary": config["primary"],
                    "expected_replica": config["replica"],
                    "replica_source": config["replica_source"]
                }

            # Consistency audit across all registered chunks in the cluster
            audit = []
            for cid, loc in self.chunk_locations.items():
                p_node = loc["primary_node"]
                r_node = loc["replica_node"]
                p = self.storage[p_node]["primary_chunks"].get(cid)
                r = self.storage[r_node]["replica_chunks"].get(cid)
                synced = bool(p and r and p.version == r.version and p.checksum == r.checksum)
                sz = len(p.raw_bytes) if p else (len(r.raw_bytes) if r else 0)
                audit.append({
                    "chunk_id": cid,
                    "primary_node": p_node,
                    "replica_node": r_node,
                    "primary_version": p.version if p else 0,
                    "replica_version": r.version if r else 0,
                    "size_bytes": sz,
                    "synchronized": synced
                })

            return {
                "consistency_mode": self.consistency_mode,
                "nodes": nodes_status,
                "audit": audit,
                "recent_replication_events": list(self.replication_log[-12:])
            }
