"""
Concept 1 & Concurrent Evaluation: MiniDFS RPC Client & Load Tester
Enables clients to invoke RPC methods and run concurrent load simulations.
Evaluates throughput (requests/sec) and average latency under concurrent client load.
"""

import threading
import time
from typing import Dict, Any, List

class MiniDFSClient:
    def __init__(self, rpc_dispatcher):
        self.dispatcher = rpc_dispatcher
        self.request_counter = 0

    def call(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        self.request_counter += 1
        req = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self.request_counter
        }
        return self.dispatcher.dispatch(req)

    def run_concurrent_load_test(self, num_threads: int = 10, requests_per_thread: int = 20) -> Dict[str, Any]:
        """
        Simulates concurrent client load hitting the Master Metadata Server.
        Evaluates throughput and latency with multithreaded request handling.
        """
        latencies = []
        errors = 0
        lock = threading.Lock()
        threads = []
        overall_start = time.perf_counter()

        def client_worker(thread_idx: int):
            nonlocal errors
            for i in range(requests_per_thread):
                t0 = time.perf_counter()
                try:
                    if i % 3 == 0:
                        # Namespace lookup
                        res = self.call("lookup_namespace", {"path": "/user/alice/distributed_intro.txt", "user": f"client_{thread_idx}"})
                    elif i % 3 == 1:
                        # Permission check
                        res = self.call("check_permissions", {"user": "alice", "path": "/user/alice/distributed_intro.txt", "action": "READ"})
                    else:
                        # Chunk locations lookup
                        res = self.call("get_chunk_locations", {"path": "/user/alice/distributed_intro.txt", "user": f"client_{thread_idx}"})

                    elapsed_ms = (time.perf_counter() - t0) * 1000.0
                    with lock:
                        latencies.append(elapsed_ms)
                except Exception:
                    with lock:
                        errors += 1

        for idx in range(num_threads):
            t = threading.Thread(target=client_worker, args=(idx + 1,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        total_time_sec = time.perf_counter() - overall_start
        total_requests = len(latencies)
        throughput_rps = round(total_requests / max(0.001, total_time_sec), 2)
        avg_latency_ms = round(sum(latencies) / max(1, total_requests), 2)
        min_lat = round(min(latencies), 2) if latencies else 0.0
        max_lat = round(max(latencies), 2) if latencies else 0.0

        return {
            "timestamp": time.strftime("%H:%M:%S"),
            "num_concurrent_clients": num_threads,
            "requests_per_client": requests_per_thread,
            "total_requests": total_requests,
            "total_time_seconds": round(total_time_sec, 3),
            "throughput_req_per_sec": throughput_rps,
            "avg_latency_ms": avg_latency_ms,
            "min_latency_ms": min_lat,
            "max_latency_ms": max_lat,
            "errors": errors
        }
