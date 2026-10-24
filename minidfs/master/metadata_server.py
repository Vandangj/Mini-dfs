"""
Concept 1 & Core Architecture: Central Metadata Server (Master)
Owns:
  - File namespace (hierarchical path tree)
  - Per-file metadata (owner, permissions, size, timestamps)
  - Chunk-to-node location table
  - Access Control List (ACL) permissions checking
Protected by ReadWriteLock (Concept 2) and Lamport Clocks (Concept 3).
"""

import threading
import time
from typing import Dict, Any, List, Optional
from minidfs.multithreading.rwlock import ReadWriteLock
from minidfs.clocks.lamport_clock import LamportClock
from minidfs.load_balancer.balancer import LoadBalancer
from minidfs.consistency.ring_manager import RingReplicationManager

class FileMetadata:
    def __init__(self, path: str, owner: str = "alice", permissions: str = "rwxr-xr--", size_bytes: int = 0):
        self.path = path
        self.filename = path.strip("/").split("/")[-1] if "/" in path else path
        self.owner = owner
        self.permissions = permissions  # e.g., rwxr-xr--
        self.size_bytes = size_bytes
        self.created_at = time.time()
        self.modified_at = time.time()
        self.version = 1
        self.chunks: List[str] = []  # list of chunk_ids

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "filename": self.filename,
            "owner": self.owner,
            "permissions": self.permissions,
            "size_bytes": self.size_bytes,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.created_at)),
            "modified_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.modified_at)),
            "version": self.version,
            "chunks": list(self.chunks)
        }

class MetadataServer:
    def __init__(self, load_balancer: LoadBalancer, ring_manager: RingReplicationManager):
        self.load_balancer = load_balancer
        self.ring_manager = ring_manager
        self.rwlock = ReadWriteLock("MasterNamespaceRWLock")
        self.lamport_clock = LamportClock("Master-Coordinator")
        
        # In-memory namespace: path -> FileMetadata
        self.files: Dict[str, FileMetadata] = {}
        self.directories: set = {"/", "/user", "/user/alice", "/user/bob", "/system", "/analytics"}
        
        self.rpc_call_history: List[Dict[str, Any]] = []
        self._init_default_namespace()

    def _init_default_namespace(self):
        # Create default sample files mapping to the ring chunks
        f1 = FileMetadata("/user/alice/distributed_intro.txt", owner="alice", permissions="rwxr-xr--", size_bytes=1024)
        f1.chunks = ["Chunk_A"]
        self.files[f1.path] = f1

        f2 = FileMetadata("/user/bob/replication_analysis.log", owner="bob", permissions="rw-r--r--", size_bytes=2048)
        f2.chunks = ["Chunk_B"]
        self.files[f2.path] = f2

        f3 = FileMetadata("/analytics/matrix_dataset.dat", owner="admin", permissions="rwxrwxrwx", size_bytes=4096)
        f3.chunks = ["Chunk_C"]
        self.files[f3.path] = f3

    # ==================== RPC Method Handlers ====================

    def rpc_lookup_namespace(self, path: str, caller_user: str = "guest") -> Dict[str, Any]:
        """RPC: Namespace lookup for file or directory"""
        start_t = time.perf_counter()
        with self.rwlock.read_locked(caller=f"RPC-Lookup-{caller_user}"):
            ts = self.lamport_clock.tick(f"RPC lookup {path}")
            
            clean_path = path.rstrip("/") or "/"
            if clean_path in self.files:
                entry = self.files[clean_path].to_dict()
                elapsed = round((time.perf_counter() - start_t) * 1000, 3)
                self._record_rpc("lookup_namespace", caller_user, {"path": clean_path}, "FOUND_FILE", elapsed, ts)
                return {"status": "SUCCESS", "type": "FILE", "metadata": entry, "lamport_ts": ts}
            
            # Check directory
            matching_children = []
            for d in self.directories:
                if d != clean_path and d.startswith(clean_path):
                    matching_children.append({"path": d, "type": "DIRECTORY"})
            for fp, fmeta in self.files.items():
                if fp.startswith(clean_path):
                    matching_children.append({"path": fp, "type": "FILE", "size": fmeta.size_bytes})

            elapsed = round((time.perf_counter() - start_t) * 1000, 3)
            self._record_rpc("lookup_namespace", caller_user, {"path": clean_path}, "DIR_OR_NOT_FOUND", elapsed, ts)
            return {
                "status": "SUCCESS" if (clean_path in self.directories) else "NOT_FOUND",
                "type": "DIRECTORY" if (clean_path in self.directories) else "UNKNOWN",
                "path": clean_path,
                "children": matching_children,
                "lamport_ts": ts
            }

    def rpc_get_file_metadata(self, path: str, caller_user: str = "guest") -> Dict[str, Any]:
        """RPC: Retrieve detailed file metadata"""
        start_t = time.perf_counter()
        with self.rwlock.read_locked(caller=f"RPC-Meta-{caller_user}"):
            ts = self.lamport_clock.tick(f"RPC get_metadata {path}")
            clean_path = path.rstrip("/")
            if clean_path not in self.files:
                elapsed = round((time.perf_counter() - start_t) * 1000, 3)
                self._record_rpc("get_file_metadata", caller_user, {"path": clean_path}, "FILE_NOT_FOUND", elapsed, ts)
                return {"error": f"File '{clean_path}' not found in namespace"}

            meta = self.files[clean_path].to_dict()
            elapsed = round((time.perf_counter() - start_t) * 1000, 3)
            self._record_rpc("get_file_metadata", caller_user, {"path": clean_path}, "SUCCESS", elapsed, ts)
            return {"status": "SUCCESS", "metadata": meta, "lamport_ts": ts}

    def rpc_check_permissions(self, user: str, path: str, action: str = "READ") -> Dict[str, Any]:
        """
        RPC: Check ACL permissions for user on file.
        action can be READ, WRITE, or EXECUTE.
        """
        start_t = time.perf_counter()
        with self.rwlock.read_locked(caller=f"RPC-Perm-{user}"):
            ts = self.lamport_clock.tick(f"RPC check_perms {user} {action} {path}")
            clean_path = path.rstrip("/")
            if clean_path not in self.files:
                return {"allowed": False, "reason": "Path does not exist", "lamport_ts": ts}

            file_meta = self.files[clean_path]
            allowed = False
            perms = file_meta.permissions  # e.g., rwxr-xr--

            # Superuser admin always permitted
            if user == "admin" or user == "root":
                allowed = True
                reason = "Granted: Superuser privileges"
            elif user == file_meta.owner:
                # Owner permissions (indices 0..2)
                if action == "READ" and perms[0] == 'r':
                    allowed = True
                elif action == "WRITE" and perms[1] == 'w':
                    allowed = True
                elif action == "EXECUTE" and perms[2] == 'x':
                    allowed = True
                reason = f"Owner check ({user}): {'Permitted' if allowed else 'Denied'} for {action}"
            else:
                # Others permissions (indices 6..8)
                if action == "READ" and perms[6] == 'r':
                    allowed = True
                elif action == "WRITE" and perms[7] == 'w':
                    allowed = True
                elif action == "EXECUTE" and perms[8] == 'x':
                    allowed = True
                reason = f"World check ({user}): {'Permitted' if allowed else 'Denied'} for {action}"

            elapsed = round((time.perf_counter() - start_t) * 1000, 3)
            self._record_rpc("check_permissions", user, {"path": clean_path, "action": action}, f"ALLOWED={allowed}", elapsed, ts)
            return {
                "allowed": allowed,
                "user": user,
                "path": clean_path,
                "action": action,
                "permissions_str": perms,
                "reason": reason,
                "lamport_ts": ts
            }

    def rpc_get_chunk_locations(self, path: str, caller_user: str = "guest") -> Dict[str, Any]:
        """
        RPC: Directs client to the appropriate Storage Nodes holding chunks.
        Returns primary and replica nodes with load balancing routing suggestion!
        """
        start_t = time.perf_counter()
        with self.rwlock.read_locked(caller=f"RPC-Locations-{caller_user}"):
            ts = self.lamport_clock.tick(f"RPC get_chunk_locations {path}")
            clean_path = path.rstrip("/")
            if clean_path not in self.files:
                return {"error": f"File '{clean_path}' not found"}

            file_meta = self.files[clean_path]
            chunk_locations = []

            for cid in file_meta.chunks:
                if cid in self.ring_manager.chunk_locations:
                    loc = self.ring_manager.chunk_locations[cid]
                    p_node = loc["primary_node"]
                    r_node = loc["replica_node"]
                    # Get load-balanced read recommendation
                    routing = self.load_balancer.route_read_request(p_node, r_node)
                    chunk_locations.append({
                        "chunk_id": cid,
                        "primary_node": p_node,
                        "replica_node": r_node,
                        "recommended_node": routing.get("selected_node", p_node),
                        "routing_reason": routing.get("reason", "")
                    })

            elapsed = round((time.perf_counter() - start_t) * 1000, 3)
            self._record_rpc("get_chunk_locations", caller_user, {"path": clean_path}, f"CHUNKS={len(chunk_locations)}", elapsed, ts)
            return {
                "status": "SUCCESS",
                "path": clean_path,
                "chunks": chunk_locations,
                "lamport_ts": ts
            }

    def rpc_create_file(self, path: str, user: str, content: str, permissions: str = "rwxr-xr--") -> Dict[str, Any]:
        """RPC: Create a new file, place chunk via Load Balancer, and write to Ring"""
        start_t = time.perf_counter()
        with self.rwlock.write_locked(caller=f"RPC-CreateFile-{user}"):
            ts = self.lamport_clock.tick(f"RPC create_file {path} by {user}")
            clean_path = path.rstrip("/")
            
            # Map or allocate chunk ID
            chunk_options = ["Chunk_A", "Chunk_B", "Chunk_C"]
            # Balance allocation
            placement = self.load_balancer.select_placement_node("Chunk_Dyn", len(content.encode('utf-8')))
            assigned_node = placement.get("assigned_node", "SN1")
            
            # Select chunk based on assigned node's primary holding
            chunk_id = self.ring_manager.topology[assigned_node]["primary"]

            # Write content to Ring Storage
            write_res = self.ring_manager.write_chunk(chunk_id, content, self.lamport_clock, client_id=user)

            file_meta = FileMetadata(clean_path, owner=user, permissions=permissions, size_bytes=len(content.encode('utf-8')))
            file_meta.chunks = [chunk_id]
            file_meta.version = write_res.get("version", 1)
            self.files[clean_path] = file_meta

            elapsed = round((time.perf_counter() - start_t) * 1000, 3)
            self._record_rpc("create_file", user, {"path": clean_path, "chunk": chunk_id}, "CREATED", elapsed, ts)

            return {
                "status": "SUCCESS",
                "file": file_meta.to_dict(),
                "chunk_assigned": chunk_id,
                "placement_info": placement,
                "replication_info": write_res,
                "lamport_ts": ts
            }

    def rpc_read_file(self, path: str, user: str = "guest") -> Dict[str, Any]:
        """RPC: Read file content through Chunk routing & Quorum read"""
        start_t = time.perf_counter()
        with self.rwlock.read_locked(caller=f"RPC-ReadFile-{user}"):
            ts = self.lamport_clock.tick(f"RPC read_file {path} by {user}")
            clean_path = path.rstrip("/")
            if clean_path not in self.files:
                return {"error": f"File '{clean_path}' not found"}

            file_meta = self.files[clean_path]
            chunks_content = []
            for cid in file_meta.chunks:
                read_res = self.ring_manager.read_chunk(cid)
                chunks_content.append(read_res.get("content", ""))

            full_content = "\n".join(chunks_content)
            elapsed = round((time.perf_counter() - start_t) * 1000, 3)
            self._record_rpc("read_file", user, {"path": clean_path}, f"READ {len(full_content)} BYTES", elapsed, ts)
            return {
                "status": "SUCCESS",
                "path": clean_path,
                "content": full_content,
                "metadata": file_meta.to_dict(),
                "lamport_ts": ts
            }

    def rpc_delete_file(self, path: str, user: str = "guest") -> Dict[str, Any]:
        """RPC: Delete file from namespace"""
        start_t = time.perf_counter()
        with self.rwlock.write_locked(caller=f"RPC-DeleteFile-{user}"):
            ts = self.lamport_clock.tick(f"RPC delete_file {path} by {user}")
            clean_path = path.rstrip("/")
            if clean_path not in self.files:
                return {"error": f"File '{clean_path}' not found"}

            del self.files[clean_path]
            elapsed = round((time.perf_counter() - start_t) * 1000, 3)
            self._record_rpc("delete_file", user, {"path": clean_path}, "DELETED", elapsed, ts)
            return {"status": "SUCCESS", "deleted_path": clean_path, "lamport_ts": ts}

    def _record_rpc(self, method: str, caller: str, params: Dict[str, Any], result_summary: str, latency_ms: float, lamport_ts: int):
        entry = {
            "timestamp": time.strftime("%H:%M:%S"),
            "method": method,
            "caller": caller,
            "params": params,
            "result": result_summary,
            "latency_ms": latency_ms,
            "lamport_ts": lamport_ts
        }
        self.rpc_call_history.append(entry)
        if len(self.rpc_call_history) > 40:
            self.rpc_call_history.pop(0)

    def get_namespace_overview(self) -> Dict[str, Any]:
        with self.rwlock.read_locked(caller="Master-Overview"):
            return {
                "total_files": len(self.files),
                "directories": list(self.directories),
                "files": {path: meta.to_dict() for path, meta in self.files.items()},
                "recent_rpc_calls": list(self.rpc_call_history[-15:])
            }
