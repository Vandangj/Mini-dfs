"""
Concept 3: Clock Synchronization
Part 2: Physical Clock Synchronization
Implements the Berkeley Algorithm (Gusella & Zatti, 1989) and Cristian's Algorithm (1989)
with simulated drift and fault-tolerant averaging.
"""

import threading
import time
import random
from typing import Dict, Any, List

class PhysicalClockNode:
    def __init__(self, node_id: str, drift_rate_ms_per_sec: float = 0.0):
        self.node_id = node_id
        self.drift_rate = drift_rate_ms_per_sec  # ms drift per real second
        self.offset_ms = 0.0
        self.base_real_time = time.time()
        self._lock = threading.Lock()
        self.sync_history: List[Dict[str, Any]] = []

    def get_physical_time(self) -> float:
        """Calculates current physical time incorporating drift + applied offset"""
        with self._lock:
            elapsed_sec = time.time() - self.base_real_time
            drift_sec = (elapsed_sec * self.drift_rate) / 1000.0
            return (time.time() + (self.offset_ms / 1000.0) + drift_sec)

    def apply_offset(self, adjustment_ms: float, algorithm: str = "Berkeley"):
        with self._lock:
            self.offset_ms += adjustment_ms
            entry = {
                "timestamp": time.strftime("%H:%M:%S"),
                "algorithm": algorithm,
                "adjustment_ms": round(adjustment_ms, 2),
                "new_offset_ms": round(self.offset_ms, 2)
            }
            self.sync_history.append(entry)
            if len(self.sync_history) > 20:
                self.sync_history.pop(0)

class BerkeleyTimeCoordinator:
    def __init__(self):
        self.nodes: Dict[str, PhysicalClockNode] = {}
        self._lock = threading.Lock()
        self.last_sync_results: Dict[str, Any] = {}

    def register_node(self, node_id: str, drift_rate_ms: float = 0.0) -> PhysicalClockNode:
        with self._lock:
            node = PhysicalClockNode(node_id, drift_rate_ms)
            self.nodes[node_id] = node
            return node

    def run_berkeley_sync(self, max_fault_tolerance_diff_ms: float = 5000.0) -> Dict[str, Any]:
        """
        Executes the Berkeley Clock Synchronization round:
        1. Master polls all nodes for their physical time.
        2. Estimates RTT and calculates clock differences relative to Master.
        3. Filters out outlier nodes (faulty clocks).
        4. Calculates the average difference.
        5. Computes individual offset adjustments and sends them to each node.
        """
        with self._lock:
            master_id = "Master"
            if master_id not in self.nodes:
                self.nodes[master_id] = PhysicalClockNode(master_id, drift_rate_ms_per_sec=0.0)
            
            master_time = self.nodes[master_id].get_physical_time()
            time_readings = {}
            rtt_simulated = {}

            # Step 1: Poll all nodes
            for nid, node in self.nodes.items():
                # Simulated network round-trip delay between 10ms and 50ms
                rtt = random.uniform(10.0, 50.0)
                rtt_simulated[nid] = rtt
                # Estimated remote time at response receipt
                node_time = node.get_physical_time() + (rtt / 2000.0)
                diff_ms = (node_time - master_time) * 1000.0
                time_readings[nid] = {
                    "reported_time": node_time,
                    "diff_to_master_ms": diff_ms,
                    "rtt_ms": rtt
                }

            # Step 2: Filter outliers (nodes that deviate beyond tolerance)
            valid_diffs = []
            for nid, data in time_readings.items():
                if abs(data["diff_to_master_ms"]) <= max_fault_tolerance_diff_ms:
                    valid_diffs.append(data["diff_to_master_ms"])

            # Step 3: Compute fault-tolerant average
            avg_diff_ms = sum(valid_diffs) / len(valid_diffs) if valid_diffs else 0.0

            # Step 4: Calculate adjustments: adjustment = avg_diff - node_diff
            adjustments = {}
            for nid, data in time_readings.items():
                node_diff = data["diff_to_master_ms"]
                adj_ms = avg_diff_ms - node_diff
                adjustments[nid] = round(adj_ms, 2)
                # Apply adjustment to node
                self.nodes[nid].apply_offset(adj_ms, algorithm="Berkeley")

            result = {
                "timestamp": time.strftime("%H:%M:%S"),
                "master_time_sec": round(master_time, 4),
                "nodes_polled": len(self.nodes),
                "average_diff_ms": round(avg_diff_ms, 2),
                "time_readings": time_readings,
                "adjustments_sent": adjustments
            }
            self.last_sync_results = result
            return result

    def run_cristian_sync(self, client_id: str, server_id: str = "Master") -> Dict[str, Any]:
        """
        Executes Cristian's Algorithm for a specific client:
        T_new = T_server + (RTT / 2)
        """
        with self._lock:
            if client_id not in self.nodes or server_id not in self.nodes:
                return {"error": "Invalid client or server ID"}

            client_node = self.nodes[client_id]
            server_node = self.nodes[server_id]

            # Request sent at T0
            t0 = client_node.get_physical_time()
            # Simulated network latency
            latency_one_way = random.uniform(0.015, 0.045)  # 15 to 45 ms
            # Server receives and reads clock at T_server
            t_server = server_node.get_physical_time()
            # Client receives response at T1
            t1 = t0 + (latency_one_way * 2)

            rtt = t1 - t0
            estimated_server_time = t_server + (rtt / 2.0)
            adjustment_sec = estimated_server_time - t1
            adjustment_ms = adjustment_sec * 1000.0

            client_node.apply_offset(adjustment_ms, algorithm="Cristian")

            return {
                "client_id": client_id,
                "server_id": server_id,
                "t0": round(t0, 4),
                "t1": round(t1, 4),
                "rtt_ms": round(rtt * 1000, 2),
                "t_server": round(t_server, 4),
                "adjustment_ms": round(adjustment_ms, 2),
                "algorithm": "Cristian"
            }

    def get_all_clocks(self) -> Dict[str, Any]:
        with self._lock:
            info = {}
            for nid, node in self.nodes.items():
                info[nid] = {
                    "physical_time_sec": round(node.get_physical_time(), 4),
                    "drift_rate_ms_sec": node.drift_rate,
                    "offset_ms": round(node.offset_ms, 2),
                    "recent_syncs": list(node.sync_history[-5:])
                }
            return {
                "nodes": info,
                "last_berkeley_sync": self.last_sync_results
            }
