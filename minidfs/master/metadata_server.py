"""
Concept 1 & Core Architecture: Central Metadata Server (Master)
Owns:
  - File namespace (hierarchical path tree)
  - Per-file metadata (owner, permissions, size, timestamps, audio metadata)
  - Chunk-to-node location table
  - Access Control List (ACL) permissions checking
Supports arbitrary binary uploads, audio files (.mp3, .wav, .ogg), and distributed chunking.
Protected by ReadWriteLock (Concept 2) and Lamport Clocks (Concept 3).
"""

import threading
import time
import base64
import mimetypes
from typing import Dict, Any, List, Optional, Union
from minidfs.multithreading.rwlock import ReadWriteLock
from minidfs.clocks.lamport_clock import LamportClock
from minidfs.load_balancer.balancer import LoadBalancer
from minidfs.consistency.ring_manager import RingReplicationManager

class FileMetadata:
    def __init__(self, path: str, owner: str = "alice", permissions: str = "rwxr-xr--", size_bytes: int = 0, is_audio: bool = False, content_type: str = "text/plain"):
        self.path = path
        self.filename = path.strip("/").split("/")[-1] if "/" in path else path
        self.owner = owner
        self.permissions = permissions  # e.g., rwxr-xr--
        self.size_bytes = size_bytes
        self.is_audio = is_audio
        self.content_type = content_type
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
            "is_audio": self.is_audio,
            "content_type": self.content_type,
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
        self.directories: set = {"/", "/user", "/user/alice", "/user/bob", "/audio", "/analytics"}
        
        self.rpc_call_history: List[Dict[str, Any]] = []
        self._init_default_namespace()

    def _init_default_namespace(self):
        # 1. Text demo file
        f1 = FileMetadata("/user/alice/distributed_intro.txt", owner="alice", permissions="rwxr-xr--", size_bytes=1024, is_audio=False, content_type="text/plain")
        f1.chunks = ["Chunk_A"]
        self.files[f1.path] = f1

        # 2. System log demo file
        f2 = FileMetadata("/user/bob/replication_analysis.log", owner="bob", permissions="rw-r--r--", size_bytes=2048, is_audio=False, content_type="text/plain")
        f2.chunks = ["Chunk_B"]
        self.files[f2.path] = f2

        # 3. Audio demo file (generates a valid mini WAV audio header with a gentle 440Hz beep tone!)
        sample_audio_bytes = self._generate_sample_wav_bytes(duration_sec=2.0)
        f3_path = "/audio/welcome_chime.wav"
        f3 = FileMetadata(f3_path, owner="admin", permissions="rwxr-xr--", size_bytes=len(sample_audio_bytes), is_audio=True, content_type="audio/wav")
        # Partition audio across SN1, SN2, SN3 in the ring
        results = self.ring_manager.partition_and_store_file("welcome_chime.wav", sample_audio_bytes, self.lamport_clock, "admin")
        f3.chunks = [r["chunk_id"] for r in results]
        self.files[f3.path] = f3

    def _generate_sample_wav_bytes(self, duration_sec: float = 2.0, sample_rate: int = 8000) -> bytes:
        """Generates a small valid uncompressed PCM WAV tone for demonstration"""
        import struct
        import math
        num_samples = int(sample_rate * duration_sec)
        byte_rate = sample_rate * 1 * 2  # 1 channel, 16-bit
        block_align = 2
        data_chunk_size = num_samples * 2
        riff_chunk_size = 36 + data_chunk_size

        # WAV header
        header = struct.pack(
            '<4sI4s4sIHHIIHH4sI',
            b'RIFF', riff_chunk_size, b'WAVE',
            b'fmt ', 16, 1, 1, sample_rate, byte_rate, block_align, 16,
            b'data', data_chunk_size
        )

        # 440Hz Sine Wave tone
        samples = bytearray()
        for i in range(num_samples):
            t = float(i) / sample_rate
            # 440Hz (A4 note)
            amplitude = 12000
            val = int(amplitude * math.sin(2.0 * math.pi * 440.0 * t))
            samples.extend(struct.pack('<h', val))

        return header + bytes(samples)

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
            
            matching_children = []
            for d in self.directories:
                if d != clean_path and d.startswith(clean_path):
                    matching_children.append({"path": d, "type": "DIRECTORY"})
            for fp, fmeta in self.files.items():
                if fp.startswith(clean_path):
                    matching_children.append({"path": fp, "type": "FILE", "size": fmeta.size_bytes, "is_audio": fmeta.is_audio})

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
        """RPC: Check ACL permissions for user on file"""
        start_t = time.perf_counter()
        with self.rwlock.read_locked(caller=f"RPC-Perm-{user}"):
            ts = self.lamport_clock.tick(f"RPC check_perms {user} {action} {path}")
            clean_path = path.rstrip("/")
            if clean_path not in self.files:
                return {"allowed": False, "reason": "Path does not exist", "lamport_ts": ts}

            file_meta = self.files[clean_path]
            allowed = False
            perms = file_meta.permissions

            if user in ["admin", "root"]:
                allowed = True
                reason = "Granted: Superuser privileges"
            elif user == file_meta.owner:
                if action == "READ" and perms[0] == 'r': allowed = True
                elif action == "WRITE" and perms[1] == 'w': allowed = True
                elif action == "EXECUTE" and perms[2] == 'x': allowed = True
                reason = f"Owner check ({user}): {'Permitted' if allowed else 'Denied'} for {action}"
            else:
                if action == "READ" and perms[6] == 'r': allowed = True
                elif action == "WRITE" and perms[7] == 'w': allowed = True
                elif action == "EXECUTE" and perms[8] == 'x': allowed = True
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
        """RPC: Directs client to the appropriate Storage Nodes holding chunks"""
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

    def _resolve_path(self, path: str) -> Optional[str]:
        """Resolves path regardless of leading/trailing slash variants"""
        p = (path or "").strip().rstrip("/")
        if not p:
            return None
        if p in self.files:
            return p
        p_slash = "/" + p if not p.startswith("/") else p
        if p_slash in self.files:
            return p_slash
        p_noslash = p.lstrip("/")
        if p_noslash in self.files:
            return p_noslash
        return None

    def rpc_create_file(self, path: str, user: str, content: Union[str, bytes], permissions: str = "rwxr-xr--", content_type: str = None) -> Dict[str, Any]:
        """
        RPC: Create a new file or upload an audio file.
        Partitions the file into 3 distributed chunks across Storage Nodes 1, 2, 3 in the ring!
        """
        start_t = time.perf_counter()
        with self.rwlock.write_locked(caller=f"RPC-CreateFile-{user}"):
            clean_path = (path or "").strip()
            if not clean_path.startswith("/"):
                clean_path = "/" + clean_path
            clean_path = clean_path.rstrip("/") or "/"
            filename = clean_path.split("/")[-1] if "/" in clean_path else clean_path
            if not filename:
                filename = "upload.dat"

            # Register parent directory paths in namespace
            parts = [p for p in clean_path.split("/") if p]
            curr = ""
            for p in parts[:-1]:
                curr += "/" + p
                self.directories.add(curr)

            # Detect content type & audio format
            is_b64 = isinstance(content, str) and content.startswith("data:") and ";base64," in content
            if isinstance(content, str) and not is_b64:
                # Manual text payload fallback
                if not content_type or content_type == "audio/mpeg" or content_type == "application/octet-stream":
                    content_type = "text/plain"

            if content_type is None:
                content_type, _ = mimetypes.guess_type(filename)
                content_type = content_type or "application/octet-stream"

            is_audio = content_type.startswith("audio/") or filename.lower().endswith(('.mp3', '.wav', '.ogg', '.m4a', '.flac', '.aac'))

            # Convert base64 data URLs if passed from frontend FileReader
            if isinstance(content, str):
                if is_b64:
                    header_part, b64_part = content.split(";base64,", 1)
                    file_bytes = base64.b64decode(b64_part)
                    header_mime = header_part.replace("data:", "")
                    if header_mime:
                        content_type = header_mime
                        if content_type.startswith("audio/"):
                            is_audio = True
                else:
                    file_bytes = content.encode('utf-8')
            else:
                file_bytes = content or b""

            file_size = len(file_bytes)
            ts = self.lamport_clock.tick(f"RPC create_file {clean_path} ({file_size} bytes) by {user}")

            # Partition file into 3 distributed chunks across ring
            alloc_results = self.ring_manager.partition_and_store_file(filename, file_bytes, self.lamport_clock, user)
            chunk_ids = [res["chunk_id"] for res in alloc_results]

            file_meta = FileMetadata(
                path=clean_path,
                owner=user,
                permissions=permissions,
                size_bytes=file_size,
                is_audio=is_audio,
                content_type=content_type
            )
            file_meta.chunks = chunk_ids
            file_meta.version = 1
            self.files[clean_path] = file_meta

            elapsed = round((time.perf_counter() - start_t) * 1000, 3)
            self._record_rpc("create_file", user, {"path": clean_path, "size": file_size, "is_audio": is_audio}, f"CREATED ({file_size} B)", elapsed, ts)

            return {
                "status": "SUCCESS",
                "file": file_meta.to_dict(),
                "chunks_distributed": alloc_results,
                "is_audio": is_audio,
                "content_type": content_type,
                "lamport_ts": ts
            }

    def rpc_read_file(self, path: str, user: str = "guest") -> Dict[str, Any]:
        """RPC: Reassembles chunks from Storage Nodes and returns file/audio data"""
        start_t = time.perf_counter()
        with self.rwlock.read_locked(caller=f"RPC-ReadFile-{user}"):
            ts = self.lamport_clock.tick(f"RPC read_file {path} by {user}")
            clean_path = self._resolve_path(path)
            if not clean_path or clean_path not in self.files:
                return {"error": f"File '{path}' not found"}

            file_meta = self.files[clean_path]
            perm_check = self.rpc_check_permissions(user, clean_path, "READ")
            if not perm_check.get("allowed", False):
                return {"error": f"Access denied: {perm_check.get('reason')}"}

            # Reassemble binary bytes from ring chunks
            raw_data = self.ring_manager.reassemble_file(file_meta.chunks)
            elapsed = round((time.perf_counter() - start_t) * 1000, 3)

            # If audio or binary, encode as Base64 data URL for in-browser playback
            if file_meta.is_audio or file_meta.content_type.startswith("audio/"):
                b64 = base64.b64encode(raw_data).decode('utf-8')
                data_url = f"data:{file_meta.content_type};base64,{b64}"
                self._record_rpc("read_file", user, {"path": clean_path}, f"STREAMED AUDIO ({len(raw_data)} B)", elapsed, ts)
                return {
                    "status": "SUCCESS",
                    "path": clean_path,
                    "is_audio": True,
                    "content_type": file_meta.content_type,
                    "audio_data_url": data_url,
                    "metadata": file_meta.to_dict(),
                    "lamport_ts": ts
                }
            else:
                try:
                    text_str = raw_data.decode('utf-8')
                except UnicodeDecodeError:
                    text_str = f"[Binary data: {len(raw_data)} bytes]"

                self._record_rpc("read_file", user, {"path": clean_path}, f"READ {len(raw_data)} BYTES", elapsed, ts)
                return {
                    "status": "SUCCESS",
                    "path": clean_path,
                    "is_audio": False,
                    "content": text_str,
                    "metadata": file_meta.to_dict(),
                    "lamport_ts": ts
                }

    def get_file_raw_bytes(self, path: str) -> Optional[tuple[bytes, str, str]]:
        """Returns (raw_bytes, content_type, filename) for streaming/download"""
        with self.rwlock.read_locked(caller="Raw-Stream"):
            clean_path = self._resolve_path(path)
            if not clean_path or clean_path not in self.files:
                return None
            meta = self.files[clean_path]
            raw = self.ring_manager.reassemble_file(meta.chunks)
            return raw, meta.content_type, meta.filename

    def rpc_delete_file(self, path: str, user: str = "guest") -> Dict[str, Any]:
        """RPC: Delete file from namespace"""
        start_t = time.perf_counter()
        with self.rwlock.write_locked(caller=f"RPC-DeleteFile-{user}"):
            ts = self.lamport_clock.tick(f"RPC delete_file {path} by {user}")
            clean_path = self._resolve_path(path)
            if not clean_path or clean_path not in self.files:
                return {"error": f"File '{path}' not found"}

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
            total_bytes = sum(f.size_bytes for f in self.files.values())
            return {
                "total_files": len(self.files),
                "total_bytes": total_bytes,
                "directories": list(self.directories),
                "files": {path: meta.to_dict() for path, meta in self.files.items()},
                "recent_rpc_calls": list(self.rpc_call_history[-15:])
            }
