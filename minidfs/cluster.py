"""In-process MiniDFS cluster and distributed-systems demonstrations."""

from __future__ import annotations

import math
import re
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

from minidfs.config import STORAGE_NODES_CONFIG


class MiniDFSCluster:
    """Thread-safe metadata master, three storage nodes, and demo algorithms."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._compute_pool = ThreadPoolExecutor(max_workers=12, thread_name_prefix="minidfs")
        self._clock = 0
        self._events: list[dict[str, Any]] = []
        self._files: dict[str, dict[str, Any]] = {}
        self._nodes: dict[str, dict[str, Any]] = {}
        self._masters = [
            {"id": "M1", "priority": 1, "alive": True},
            {"id": "M2", "priority": 2, "alive": True},
            {"id": "M3", "priority": 3, "alive": True},
        ]
        self._leader = "M1"
        self._started = time.monotonic()
        self._monitor_stop = threading.Event()

        for node_id, config in STORAGE_NODES_CONFIG.items():
            self._nodes[node_id] = {
                **config,
                "status": "healthy",
                "last_heartbeat": time.monotonic(),
                "clock_offset_ms": 0,
                "chunks": {},
                "bytes_used": 0,
                "requests": 0,
                "active_requests": 0,
                "pool": ThreadPoolExecutor(max_workers=4, thread_name_prefix=node_id),
            }
        self._nodes["SN1"]["clock_offset_ms"] = -36
        self._nodes["SN2"]["clock_offset_ms"] = 18
        self._nodes["SN3"]["clock_offset_ms"] = 52

        self._write_file(
            "welcome.txt",
            "MiniDFS is ready. Upload a file, fail a storage node, and read it back "
            "to explore replication and recovery.\n",
            record_event=False,
        )
        self._event("Cluster initialized with three storage nodes and synchronous replication.")

        self._monitor = threading.Thread(target=self._heartbeat_monitor, daemon=True, name="heartbeat-monitor")
        self._monitor.start()

    def close(self) -> None:
        self._monitor_stop.set()
        self._compute_pool.shutdown(wait=False, cancel_futures=True)
        for node in self._nodes.values():
            node["pool"].shutdown(wait=False, cancel_futures=True)

    def _tick(self) -> tuple[int, str]:
        self._clock += 1
        return self._clock, datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    def _event(self, message: str, kind: str = "system") -> None:
        clock, physical_time = self._tick()
        self._events.insert(
            0,
            {"id": uuid.uuid4().hex[:8], "message": message, "kind": kind,
             "lamport": clock, "time": physical_time},
        )
        del self._events[100:]

    def _heartbeat_monitor(self) -> None:
        while not self._monitor_stop.wait(1.0):
            now = time.monotonic()
            with self._lock:
                for node in self._nodes.values():
                    if node["status"] == "healthy":
                        node["last_heartbeat"] = now
                    elif node["status"] == "suspected" and now - node["last_heartbeat"] > 1.5:
                        node["status"] = "failed"
                        self._event(f'{node["id"]} missed its heartbeat; marked failed.', "failure")
                        self._promote_replicas(node["id"])

    def _promote_replicas(self, failed_node: str) -> None:
        for file in self._files.values():
            for chunk in file["chunks"]:
                if chunk["primary"] == failed_node:
                    survivor = next(
                        (node_id for node_id in chunk["replicas"]
                         if self._nodes[node_id]["status"] == "healthy"),
                        None,
                    )
                    if survivor:
                        chunk["primary"] = survivor
                        self._nodes[survivor]["chunks"][chunk["id"]]["role"] = "primary"
                        self._event(f'{chunk["id"]} promoted on {survivor} after {failed_node} failure.', "recovery")

    def _available_nodes(self) -> list[dict[str, Any]]:
        return [node for node in self._nodes.values() if node["status"] == "healthy"]

    def _pick_primary(self, size: int, excluded: set[str] | None = None) -> dict[str, Any]:
        excluded = excluded or set()
        candidates = [node for node in self._available_nodes() if node["id"] not in excluded]
        if not candidates:
            raise RuntimeError("No healthy storage node is available.")
        return min(
            candidates,
            key=lambda node: (node["bytes_used"] + node["active_requests"] * 65536 + size)
            / max(1, node["capacity_bytes"]),
        )

    def _place_chunk(self, chunk_id: str, payload: bytes, version: int) -> dict[str, Any]:
        primary = self._pick_primary(len(payload))
        replica = self._pick_primary(len(payload), {primary["id"]})
        record = {"data": payload, "version": version, "role": "primary"}
        replica_record = {"data": payload, "version": version, "role": "replica"}

        def store(node: dict[str, Any], item: dict[str, Any]) -> None:
            node["active_requests"] += 1
            try:
                previous = node["chunks"].get(chunk_id)
                if previous:
                    node["bytes_used"] -= len(previous["data"])
                node["chunks"][chunk_id] = item
                node["bytes_used"] += len(payload)
                node["requests"] += 1
            finally:
                node["active_requests"] -= 1

        futures = [
            primary["pool"].submit(store, primary, record),
            replica["pool"].submit(store, replica, replica_record),
        ]
        for future in futures:
            future.result(timeout=5)
        return {"id": chunk_id, "size": len(payload), "version": version,
                "primary": primary["id"], "replicas": [replica["id"]]}

    def _write_file(self, name: str, content: str, record_event: bool = True) -> dict[str, Any]:
        clean_name = name.strip().replace("\\", "/").split("/")[-1]
        if not clean_name or clean_name in {".", ".."}:
            raise ValueError("Provide a valid file name.")
        payload = content.encode("utf-8")
        if len(payload) > 2_000_000:
            raise ValueError("Demo uploads are limited to 2 MB.")
        with self._lock:
            if not self._available_nodes():
                raise RuntimeError("No healthy storage nodes are available for writes.")
            previous = self._files.get(clean_name)
            version = (previous["version"] + 1) if previous else 1
            count = min(3, max(1, len(payload)))
            chunk_size = max(1, math.ceil(len(payload) / count))
            chunks = []
            file_id = previous["id"] if previous else uuid.uuid4().hex[:10]
            for index in range(count):
                start = index * chunk_size
                part = payload[start:start + chunk_size]
                chunk_id = f"{file_id}:{index}"
                chunks.append(self._place_chunk(chunk_id, part, version))
            self._files[clean_name] = {
                "id": file_id, "name": clean_name, "size": len(payload),
                "version": version, "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "chunks": chunks,
            }
            if record_event:
                self._event(f'Wrote {clean_name} v{version} to {len(chunks)} chunks with synchronous replicas.', "write")
            return self._public_file(self._files[clean_name])

    @staticmethod
    def _public_file(file: dict[str, Any]) -> dict[str, Any]:
        return {key: file[key] for key in ("id", "name", "size", "version", "updated_at", "chunks")}

    def _read_file(self, name: str) -> dict[str, Any]:
        with self._lock:
            file = self._files.get(name)
            if not file:
                raise ValueError(f"File not found: {name}")
            parts = []
            sources = []
            for chunk in file["chunks"]:
                candidates = [chunk["primary"], *chunk["replicas"]]
                source = next(
                    (node_id for node_id in candidates
                     if self._nodes[node_id]["status"] == "healthy"
                     and chunk["id"] in self._nodes[node_id]["chunks"]),
                    None,
                )
                if source is None:
                    raise RuntimeError(f"No healthy copy of chunk {chunk['id']} is available.")
                stored = self._nodes[source]["chunks"][chunk["id"]]
                if stored["version"] != chunk["version"]:
                    raise RuntimeError(f"Replica consistency check failed for chunk {chunk['id']}.")
                parts.append(stored["data"])
                sources.append(source)
                self._nodes[source]["requests"] += 1
            self._event(f'Read {name} from storage nodes {", ".join(sources)}.', "read")
            return {"name": name, "content": b"".join(parts).decode("utf-8"), "sources": sources,
                    "version": file["version"]}

    def _sync_clocks(self) -> dict[str, Any]:
        with self._lock:
            offsets = [node["clock_offset_ms"] for node in self._nodes.values()
                       if node["status"] == "healthy"]
            if not offsets:
                raise RuntimeError("Clock synchronization needs at least one healthy node.")
            average = round(sum(offsets) / len(offsets))
            for node in self._nodes.values():
                if node["status"] == "healthy":
                    node["clock_offset_ms"] = average
            self._event(f"Berkeley clock sync adjusted healthy nodes to {average:+d} ms.", "clock")
            return {"algorithm": "Berkeley physical clock synchronization",
                    "coordinator_offset_ms": average, "nodes": self._clock_state()}

    def _clock_state(self) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        return [{"node": node["id"], "logical": self._clock,
                 "physical": now.timestamp() * 1000 + node["clock_offset_ms"],
                 "offset_ms": node["clock_offset_ms"]}
                for node in self._nodes.values()]

    def _elect(self, algorithm: str) -> dict[str, Any]:
        with self._lock:
            active = [master for master in self._masters if master["alive"]]
            if not active:
                raise RuntimeError("No metadata-server candidates are available.")
            winner = max(active, key=lambda item: item["priority"])
            if algorithm.lower() == "ring":
                trace = [master["id"] for master in sorted(active, key=lambda item: item["priority"])]
                trace.append(winner["id"])
                label = "Ring"
            elif algorithm.lower() == "bully":
                trace = [f'{winner["id"]} (priority {winner["priority"]})']
                label = "Bully"
            else:
                raise ValueError("Election algorithm must be 'bully' or 'ring'.")
            self._leader = winner["id"]
            self._event(f"{label} election completed; {winner['id']} is the metadata leader.", "election")
            return {"algorithm": label, "leader": winner["id"], "trace": trace,
                    "candidates": self._masters}

    def _mapreduce(self) -> dict[str, Any]:
        with self._lock:
            names = list(self._files)
        contents = [self._read_file(name)["content"] for name in names]
        mapped = [Counter(re.findall(r"[A-Za-z0-9_'-]+", text.lower())) for text in contents]
        reduced: Counter[str] = Counter()
        for counts in mapped:
            reduced.update(counts)
        self._event(f"MapReduce word-count completed across {len(names)} file(s).", "compute")
        return {"job": "Word count", "files_processed": len(names),
                "words": [{"word": word, "count": count}
                          for word, count in reduced.most_common(12)]}

    def _collectives(self, message: str = "MiniDFS collective broadcast") -> dict[str, Any]:
        participants = [node["id"] for node in self._available_nodes()]
        if not participants:
            raise RuntimeError("MPI collectives need at least one healthy storage node.")
        broadcast = {node_id: message for node_id in participants}
        buckets = {node_id: [] for node_id in participants}
        for index, node_id in enumerate(participants):
            buckets[node_id].append(index + 1)
        gathered = [value for node_id in participants for value in buckets[node_id]]
        self._event(f"MPI-style broadcast/scatter/gather completed on {len(participants)} node(s).", "compute")
        return {"participants": participants, "broadcast": broadcast,
                "scatter": buckets, "gather": gathered}

    @staticmethod
    def _validate_matrix(value: Any, name: str) -> list[list[float]]:
        if not isinstance(value, list) or not value:
            raise ValueError(f"{name} must be a non-empty 2D array.")
        if any(not isinstance(row, list) or not row for row in value):
            raise ValueError(f"{name} must be a non-empty 2D array.")
        width = len(value[0])
        if any(len(row) != width for row in value):
            raise ValueError(f"{name} rows must have the same length.")
        try:
            return [[float(item) for item in row] for row in value]
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must contain only numbers.") from exc

    def _matrix(self, left: Any = None, right: Any = None) -> dict[str, Any]:
        left = self._validate_matrix(left or [[1, 2], [3, 4]], "A")
        right = self._validate_matrix(right or [[5, 6], [7, 8]], "B")
        if len(left[0]) != len(right):
            raise ValueError("A's column count must match B's row count.")
        participants = [node["id"] for node in self._available_nodes()]
        if not participants:
            raise RuntimeError("Matrix computation needs at least one healthy node.")

        def multiply_row(row: list[float]) -> list[float]:
            return [sum(row[k] * right[k][column] for k in range(len(right)))
                    for column in range(len(right[0]))]

        result = list(self._compute_pool.map(multiply_row, left))
        self._event(f"Parallel matrix multiplication completed on {len(participants)} storage worker(s).", "compute")
        return {"matrix": result, "workers": participants, "shape": [len(left), len(right[0])]}

    def _benchmark(self, requests: int = 48) -> dict[str, Any]:
        requests = max(8, min(int(requests), 200))
        with self._lock:
            name = next(iter(self._files), None)
        if name is None:
            raise RuntimeError("Create a file before running the concurrent-load test.")
        start = time.perf_counter()
        list(self._compute_pool.map(lambda _: self._read_file(name), range(requests)))
        elapsed = max(time.perf_counter() - start, 0.000001)
        self._event(f"Concurrent-load test completed: {requests} reads in {elapsed:.3f}s.", "compute")
        return {"requests": requests, "elapsed_seconds": round(elapsed, 4),
                "throughput_rps": round(requests / elapsed, 2)}

    def _fail_node(self, node_id: str) -> dict[str, Any]:
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                raise ValueError(f"Unknown storage node: {node_id}")
            if node["status"] == "failed":
                return {"node": node_id, "status": "failed"}
            node["status"] = "suspected"
            node["last_heartbeat"] = time.monotonic() - 2
            self._event(f"Heartbeat lost from {node_id}; failure detector is checking the node.", "failure")
            return {"node": node_id, "status": "suspected", "recovery_delay_seconds": 1.5}

    def _recover_node(self, node_id: str) -> dict[str, Any]:
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                raise ValueError(f"Unknown storage node: {node_id}")
            node["status"] = "healthy"
            node["last_heartbeat"] = time.monotonic()
            repaired = 0
            for file in self._files.values():
                for chunk in file["chunks"]:
                    if node_id in (chunk["primary"], *chunk["replicas"]):
                        source = next(
                            (candidate for candidate in (chunk["primary"], *chunk["replicas"])
                             if candidate != node_id and self._nodes[candidate]["status"] == "healthy"
                             and chunk["id"] in self._nodes[candidate]["chunks"]),
                            None,
                        )
                        if source:
                            data = self._nodes[source]["chunks"][chunk["id"]]["data"]
                            node["chunks"][chunk["id"]] = {
                                "data": data, "version": chunk["version"],
                                "role": "primary" if chunk["primary"] == node_id else "replica",
                            }
                            node["bytes_used"] += len(data)
                            repaired += 1
            self._event(f"{node_id} recovered; repaired {repaired} chunk replica(s).", "recovery")
            return {"node": node_id, "status": "healthy", "chunks_repaired": repaired}

    def state(self) -> dict[str, Any]:
        with self._lock:
            nodes = []
            for node in self._nodes.values():
                chunks = [
                    {"id": chunk_id, "role": item["role"], "version": item["version"],
                     "size": len(item["data"])}
                    for chunk_id, item in node["chunks"].items()
                ]
                nodes.append({
                    "id": node["id"], "name": node["name"], "status": node["status"],
                    "color": node["color"], "bytes_used": node["bytes_used"],
                    "capacity_bytes": node["capacity_bytes"], "chunks": chunks,
                    "requests": node["requests"], "active_requests": node["active_requests"],
                    "clock_offset_ms": node["clock_offset_ms"],
                })
            return {
                "name": "MiniDFS", "leader": self._leader, "uptime_seconds": round(time.monotonic() - self._started, 1),
                "files": [self._public_file(file) for file in self._files.values()],
                "nodes": nodes, "events": list(self._events), "lamport_clock": self._clock,
                "clocks": self._clock_state(), "masters": self._masters,
                "consistency": "Synchronous primary + one replica; version-checked reads",
            }

    def dispatch(self, method: str, params: dict[str, Any] | None = None) -> Any:
        params = params or {}
        methods = {
            "cluster.state": lambda: self.state(),
            "files.list": lambda: {"files": self.state()["files"]},
            "files.write": lambda: {"file": self._write_file(str(params.get("name", "")),
                                                               str(params.get("content", "")))},
            "files.read": lambda: self._read_file(str(params.get("name", ""))),
            "nodes.fail": lambda: self._fail_node(str(params.get("node", ""))),
            "nodes.recover": lambda: self._recover_node(str(params.get("node", ""))),
            "election.run": lambda: self._elect(str(params.get("algorithm", "bully"))),
            "clocks.sync": lambda: self._sync_clocks(),
            "analytics.mapreduce": lambda: self._mapreduce(),
            "compute.collectives": lambda: self._collectives(str(params.get("message", "MiniDFS collective broadcast"))),
            "compute.matrix": lambda: self._matrix(params.get("a"), params.get("b")),
            "benchmark.run": lambda: self._benchmark(params.get("requests", 48)),
        }
        action = methods.get(method)
        if action is None:
            raise ValueError(f"Unknown RPC method: {method}")
        return action()
