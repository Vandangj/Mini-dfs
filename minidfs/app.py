"""
MiniDFS Unified Application Server
Exposes REST & JSON-RPC APIs for all 10 Distributed Computing Concepts
and serves the rich interactive web dashboard.
Uses Python's ThreadingHTTPServer for concurrent multithreaded request handling.
"""

import os
import json
import time
import mimetypes
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn

from minidfs.config import SERVER_HOST, SERVER_PORT, BASE_DIR
from minidfs.multithreading.rwlock import ReadWriteLock
from minidfs.multithreading.thread_pool import MonitoredThreadPool
from minidfs.clocks.lamport_clock import LamportClock
from minidfs.clocks.berkeley_sync import BerkeleyTimeCoordinator
from minidfs.elections.bully_election import BullyElectionSystem
from minidfs.elections.ring_election import RingElectionSystem
from minidfs.consistency.ring_manager import RingReplicationManager
from minidfs.load_balancer.balancer import LoadBalancer
from minidfs.mapreduce.mapreduce_engine import MapReduceEngine
from minidfs.fault_tolerance.heartbeat_monitor import HeartbeatMonitor
from minidfs.mpi.mpi_collective import MPICommunicator
from minidfs.matrix_mult.parallel_matrix import ParallelMatrixMultiplier
from minidfs.master.metadata_server import MetadataServer
from minidfs.storage.storage_node import LocalStorageNode
from minidfs.rpc.rpc_server import RPCDispatcher
from minidfs.rpc.rpc_client import MiniDFSClient

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Multithreaded HTTP Server handling each request in a dedicated worker thread"""
    daemon_threads = True

class MiniDFSSystem:
    """Singleton container initializing and linking all 10 distributed modules"""
    def __init__(self):
        # Concept 2: Multithreading
        self.thread_pool = MonitoredThreadPool("CoreServerPool", num_workers=10)

        # Concept 5: Data Consistency & Ring Replication
        self.ring_manager = RingReplicationManager()

        # Concept 6: Load Balancer
        self.load_balancer = LoadBalancer()

        # Concept 8: Heartbeat Fault Tolerance & Recovery Monitor
        self.heartbeat_monitor = HeartbeatMonitor(self.ring_manager, self.load_balancer)

        # Concept 3: Physical & Logical Clock Synchronization
        self.berkeley_coordinator = BerkeleyTimeCoordinator()
        self.berkeley_coordinator.register_node("Master", drift_rate_ms=0.0)
        self.berkeley_coordinator.register_node("SN1", drift_rate_ms=3.2)
        self.berkeley_coordinator.register_node("SN2", drift_rate_ms=-4.1)
        self.berkeley_coordinator.register_node("SN3", drift_rate_ms=5.5)

        # Concept 4: Leader Elections
        self.bully_system = BullyElectionSystem()
        self.ring_election_system = RingElectionSystem()

        # Concept 7: MapReduce Distributed Analytics
        self.mapreduce_engine = MapReduceEngine(self.ring_manager)

        # Concept 9 & 10: MPI Collective & Parallel Matrix Multiplier
        self.mpi_communicator = MPICommunicator(size=4)
        self.matrix_multiplier = ParallelMatrixMultiplier(self.mpi_communicator)

        # Core Storage Nodes on Disk
        self.storage_nodes = {
            "SN1": LocalStorageNode("SN1"),
            "SN2": LocalStorageNode("SN2"),
            "SN3": LocalStorageNode("SN3")
        }

        # Concept 1: Metadata Server (Master) & RPC
        self.metadata_server = MetadataServer(self.load_balancer, self.ring_manager)
        self.rpc_dispatcher = RPCDispatcher(self.metadata_server)
        self.rpc_client = MiniDFSClient(self.rpc_dispatcher)

system = MiniDFSSystem()

class MiniDFSHTTPHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        # Suppress verbose standard logging to keep console clean
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path.startswith("/api/"):
            self._handle_api_get(path, query)
        else:
            self._serve_static_file(path)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        
        # Read body
        content_length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            body_json = json.loads(body_bytes.decode('utf-8'))
        except Exception:
            body_json = {}

        self._handle_api_post(path, body_json)

    def _send_json(self, data: Any, status_code: int = 200):
        resp = json.dumps(data, indent=2).encode('utf-8')
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(resp)))
        self.end_headers()
        self.wfile.write(resp)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _serve_static_file(self, req_path: str):
        web_root = os.path.join(BASE_DIR, "web")
        if req_path == "/" or req_path == "":
            req_path = "/index.html"
        
        clean_path = req_path.lstrip("/").replace("/", os.sep)
        full_path = os.path.join(web_root, clean_path)

        if not os.path.exists(full_path) or os.path.isdir(full_path):
            full_path = os.path.join(web_root, "index.html")

        if os.path.exists(full_path):
            content_type, _ = mimetypes.guess_type(full_path)
            content_type = content_type or "text/plain"
            try:
                with open(full_path, "rb") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
            except Exception as e:
                self.send_error(500, f"Error reading file: {e}")
        else:
            self.send_error(404, "Not Found")

    def _handle_api_get(self, path: str, query: Dict[str, Any]):
        try:
            if path == "/api/system/overview":
                data = {
                    "master": self.system.metadata_server.get_namespace_overview(),
                    "ring_topology": self.system.ring_manager.get_topology_state(),
                    "fault_tolerance": self.system.heartbeat_monitor.get_status(),
                    "load_balancer": self.system.load_balancer.get_status(),
                    "multithreading": self.system.thread_pool.get_telemetry(),
                    "rwlock": self.system.metadata_server.rwlock.get_stats(),
                    "lamport_clock": self.system.metadata_server.lamport_clock.get_status(),
                    "berkeley_clocks": self.system.berkeley_coordinator.get_all_clocks(),
                    "bully_election": self.system.bully_system.get_state(),
                    "ring_election": self.system.ring_election_system.get_state(),
                    "mpi_state": self.system.mpi_communicator.get_state(),
                    "mapreduce": self.system.mapreduce_engine.get_status()
                }
                self._send_json(data)

            elif path == "/api/multithreading/stats":
                data = {
                    "thread_pool": self.system.thread_pool.get_telemetry(),
                    "rwlock": self.system.metadata_server.rwlock.get_stats()
                }
                self._send_json(data)

            elif path == "/api/clocks/lamport":
                self._send_json(self.system.metadata_server.lamport_clock.get_status())

            elif path == "/api/clocks/berkeley":
                self._send_json(self.system.berkeley_coordinator.get_all_clocks())

            elif path == "/api/elections/bully":
                self._send_json(self.system.bully_system.get_state())

            elif path == "/api/elections/ring":
                self._send_json(self.system.ring_election_system.get_state())

            elif path == "/api/consistency/ring":
                self._send_json(self.system.ring_manager.get_topology_state())

            elif path == "/api/loadbalancer/status":
                self._send_json(self.system.load_balancer.get_status())

            elif path == "/api/mapreduce/history":
                self._send_json(self.system.mapreduce_engine.get_status())

            elif path == "/api/fault_tolerance/status":
                self._send_json(self.system.heartbeat_monitor.get_status())

            elif path == "/api/mpi/status":
                self._send_json(self.system.mpi_communicator.get_state())

            elif path == "/api/matrix/last_result":
                self._send_json(self.system.matrix_multiplier.get_status())

            elif path == "/api/storage/disk":
                disk_info = {nid: node.get_disk_usage() for nid, node in self.system.storage_nodes.items()}
                self._send_json(disk_info)

            else:
                self._send_json({"error": f"Endpoint GET {path} not found"}, 404)

        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_api_post(self, path: str, body: Dict[str, Any]):
        try:
            # 1. RPC
            if path == "/api/rpc":
                res = self.system.rpc_dispatcher.dispatch(body)
                self._send_json(res)

            elif path == "/api/rpc/benchmark":
                clients = body.get("num_clients", 10)
                reqs = body.get("requests_per_client", 20)
                res = self.system.rpc_client.run_concurrent_load_test(clients, reqs)
                self._send_json(res)

            # 3. Clocks
            elif path == "/api/clocks/tick":
                desc = body.get("description", "Manual UI Event")
                new_ts = self.system.metadata_server.lamport_clock.tick(desc)
                self._send_json({"lamport_ts": new_ts, "description": desc})

            elif path == "/api/clocks/berkeley/sync":
                res = self.system.berkeley_coordinator.run_berkeley_sync()
                self._send_json(res)

            elif path == "/api/clocks/cristian/sync":
                client_id = body.get("client_id", "SN1")
                res = self.system.berkeley_coordinator.run_cristian_sync(client_id)
                self._send_json(res)

            # 4. Elections
            elif path == "/api/elections/bully/trigger":
                initiator = body.get("initiator_id", 1)
                res = self.system.bully_system.trigger_election(int(initiator))
                self._send_json(res)

            elif path == "/api/elections/bully/toggle_node":
                nid = int(body.get("node_id"))
                alive = bool(body.get("is_alive", True))
                self.system.bully_system.set_node_status(nid, alive)
                self._send_json(self.system.bully_system.get_state())

            elif path == "/api/elections/ring/trigger":
                initiator = body.get("initiator_id", 1)
                res = self.system.ring_election_system.trigger_election(int(initiator))
                self._send_json(res)

            elif path == "/api/elections/ring/toggle_node":
                nid = int(body.get("node_id"))
                alive = bool(body.get("is_alive", True))
                self.system.ring_election_system.set_node_status(nid, alive)
                self._send_json(self.system.ring_election_system.get_state())

            # 5. Consistency & Replication
            elif path == "/api/consistency/set_mode":
                mode = body.get("mode", "QUORUM_STRONG")
                self.system.ring_manager.set_consistency_mode(mode)
                self._send_json({"status": "SUCCESS", "mode": mode})

            elif path == "/api/consistency/write":
                chunk_id = body.get("chunk_id", "Chunk_A")
                content = body.get("content", "Updated payload")
                user = body.get("user", "User-Client")
                res = self.system.ring_manager.write_chunk(chunk_id, content, self.system.metadata_server.lamport_clock, user)
                self._send_json(res)

            elif path == "/api/consistency/read":
                chunk_id = body.get("chunk_id", "Chunk_A")
                prefer_rep = body.get("prefer_replica", False)
                res = self.system.ring_manager.read_chunk(chunk_id, prefer_rep)
                self._send_json(res)

            # 6. Load Balancer
            elif path == "/api/loadbalancer/set_policy":
                pol = body.get("policy", "LEAST_LOADED")
                self.system.load_balancer.set_policy(pol)
                self._send_json({"status": "SUCCESS", "policy": pol})

            # 7. MapReduce
            elif path == "/api/mapreduce/run":
                job_type = body.get("job_type", "WordCount")
                custom_text = body.get("custom_text", None)
                if job_type == "LogAnalysis":
                    res = self.system.mapreduce_engine.run_log_analysis(custom_text)
                else:
                    res = self.system.mapreduce_engine.run_word_count(custom_text)
                self._send_json(res)

            # 8. Fault Tolerance & Heartbeat
            elif path == "/api/fault_tolerance/crash_node":
                node_id = body.get("node_id", "SN1")
                res = self.system.heartbeat_monitor.simulate_node_crash(node_id)
                self._send_json(res)

            elif path == "/api/fault_tolerance/revive_node":
                node_id = body.get("node_id", "SN1")
                res = self.system.heartbeat_monitor.simulate_node_revival(node_id)
                self._send_json(res)

            # 9. MPI Primitives
            elif path == "/api/mpi/bcast":
                root = int(body.get("root", 0))
                data = body.get("data", "Broadcast payload")
                res = self.system.mpi_communicator.bcast(root, data)
                self._send_json(res)

            elif path == "/api/mpi/scatter":
                root = int(body.get("root", 0))
                data_list = body.get("data_list", [10, 20, 30, 40, 50, 60, 70, 80])
                res = self.system.mpi_communicator.scatter(root, data_list)
                self._send_json(res)

            elif path == "/api/mpi/gather":
                root = int(body.get("root", 0))
                res = self.system.mpi_communicator.gather(root)
                self._send_json(res)

            elif path == "/api/mpi/reduce":
                root = int(body.get("root", 0))
                op = body.get("operator", "SUM")
                res = self.system.mpi_communicator.reduce(root, op)
                self._send_json(res)

            # 10. Matrix Multiplication
            elif path == "/api/matrix/multiply":
                dim = int(body.get("dimension", 8))
                res = self.system.matrix_multiplier.multiply_parallel_mpi(dim=dim)
                self._send_json(res)

            else:
                self._send_json({"error": f"Endpoint POST {path} not found"}, 404)

        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    @property
    def system(self) -> MiniDFSSystem:
        return system

def run_server(host: str = SERVER_HOST, port: int = SERVER_PORT):
    server_address = (host, port)
    httpd = ThreadedHTTPServer(server_address, MiniDFSHTTPHandler)
    print(f"=================================================================")
    print(f"  MiniDFS Distributed File System Server is Running!")
    print(f"  Access UI Dashboard at: http://{host}:{port}")
    print(f"  All 10 Distributed Systems concepts initialized and active.")
    print(f"=================================================================")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server gracefully...")
        httpd.server_close()

if __name__ == "__main__":
    run_server()
