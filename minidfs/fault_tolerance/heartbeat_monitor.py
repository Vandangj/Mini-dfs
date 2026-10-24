"""
Concept 8: Heartbeat-Driven Fault Tolerance & Primary-Backup Recovery
Monitors Storage Nodes (SN1, SN2, SN3) via periodic heartbeats.
Drives automatic failover, replica promotion, and ring healing upon revival.
"""

import threading
import time
from typing import Dict, Any, List

class HeartbeatMonitor:
    def __init__(self, ring_manager, load_balancer, timeout_sec: float = 4.5):
        self.ring_manager = ring_manager
        self.load_balancer = load_balancer
        self.timeout_sec = timeout_sec
        self._lock = threading.Lock()
        self.stopped = False

        # Heartbeat timestamps
        now = time.time()
        self.heartbeats: Dict[str, float] = {
            "SN1": now,
            "SN2": now,
            "SN3": now
        }
        self.node_status: Dict[str, str] = {
            "SN1": "HEALTHY",
            "SN2": "HEALTHY",
            "SN3": "HEALTHY"
        }
        self.promotions: Dict[str, Dict[str, Any]] = {}
        self.recovery_log: List[Dict[str, Any]] = []

        # Background monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()

    def record_heartbeat(self, node_id: str):
        with self._lock:
            if node_id in self.heartbeats and self.node_status[node_id] != "CRASHED_MANUAL":
                self.heartbeats[node_id] = time.time()
                if self.node_status[node_id] != "HEALTHY":
                    self.node_status[node_id] = "HEALTHY"
                    self.load_balancer.update_node_metrics(node_id, is_alive=True)

    def simulate_node_crash(self, node_id: str) -> Dict[str, Any]:
        """Manually kills a storage node to simulate sudden failure"""
        with self._lock:
            if node_id not in self.node_status:
                return {"error": f"Invalid node {node_id}"}

            self.node_status[node_id] = "CRASHED_MANUAL"
            self.load_balancer.update_node_metrics(node_id, is_alive=False)
            fail_time = time.time()
            
            # Immediately trigger failover and replica promotion
            promotion_result = self._handle_node_failure(node_id, fail_time)
            return {
                "action": "CRASH_SIMULATED",
                "node_id": node_id,
                "status": "CRASHED",
                "failover": promotion_result
            }

    def simulate_node_revival(self, node_id: str) -> Dict[str, Any]:
        """Revives a crashed node and triggers ring re-synchronization"""
        with self._lock:
            if node_id not in self.node_status:
                return {"error": f"Invalid node {node_id}"}

            start_sync = time.time()
            self.node_status[node_id] = "HEALTHY"
            self.heartbeats[node_id] = time.time()
            self.load_balancer.update_node_metrics(node_id, is_alive=True)

            # Demote previously promoted replica if applicable
            promoted_chunk = None
            if node_id in self.promotions:
                promoted_chunk = self.promotions.pop(node_id)

            # Re-synchronize data from the neighbor over Replication Network
            sync_detail = self._resynchronize_node(node_id)
            sync_time_ms = round((time.time() - start_sync) * 1000 + 2.5, 2)

            entry = {
                "timestamp": time.strftime("%H:%M:%S"),
                "event": "NODE_REVIVED_AND_HEALED",
                "node_id": node_id,
                "sync_time_ms": sync_time_ms,
                "details": f"Node {node_id} revived. Synchronized chunks over Replication Network in {sync_time_ms} ms."
            }
            self.recovery_log.append(entry)

            return {
                "action": "REVIVED",
                "node_id": node_id,
                "status": "HEALTHY",
                "sync_time_ms": sync_time_ms,
                "restored_promotions": promoted_chunk,
                "sync_details": sync_detail
            }

    def _handle_node_failure(self, dead_node: str, fail_time: float) -> Dict[str, Any]:
        """
        Drives replica promotion:
        - SN1 holds Chunk A (Primary). Replica is on SN3. If SN1 dies, SN3 promotes Chunk A replica to Primary!
        - SN2 holds Chunk B (Primary). Replica is on SN1. If SN2 dies, SN1 promotes Chunk B replica to Primary!
        - SN3 holds Chunk C (Primary). Replica is on SN2. If SN3 dies, SN2 promotes Chunk C replica to Primary!
        """
        primary_chunk_map = {
            "SN1": ("Chunk_A", "SN3"),
            "SN2": ("Chunk_B", "SN1"),
            "SN3": ("Chunk_C", "SN2")
        }

        chunk_id, replica_node = primary_chunk_map[dead_node]
        promotion_entry = {
            "dead_node": dead_node,
            "lost_primary_chunk": chunk_id,
            "promoted_replica_node": replica_node,
            "promoted_at": time.strftime("%H:%M:%S"),
            "failover_latency_ms": round((time.time() - fail_time) * 1000 + 1.5, 2)
        }
        self.promotions[dead_node] = promotion_entry

        log_entry = {
            "timestamp": time.strftime("%H:%M:%S"),
            "event": "FAILOVER_REPLICA_PROMOTED",
            "node_id": dead_node,
            "details": f"Node {dead_node} failure detected! Promoted replica of {chunk_id} on {replica_node} to ACTIVE PRIMARY."
        }
        self.recovery_log.append(log_entry)
        if len(self.recovery_log) > 25:
            self.recovery_log.pop(0)

        return promotion_entry

    def _resynchronize_node(self, node_id: str) -> str:
        with self.ring_manager._lock:
            # Re-sync primary and replica chunk versions
            cfg = self.ring_manager.topology[node_id]
            rep_source = cfg["replica_source"]
            # Copy latest source primary to our replica
            source_primary = self.ring_manager.storage[rep_source]["primary"]
            our_replica = self.ring_manager.storage[node_id]["replica"]
            our_replica.content = source_primary.content
            our_replica.version = source_primary.version
            our_replica.lamport_ts = source_primary.lamport_ts
            our_replica.checksum = source_primary.checksum
            return f"Synchronized {our_replica.chunk_id} from {rep_source} (Version {our_replica.version})"

    def _monitor_loop(self):
        while not self.stopped:
            time.sleep(1.0)
            now = time.time()
            with self._lock:
                for nid in list(self.heartbeats.keys()):
                    if self.node_status[nid] == "HEALTHY":
                        elapsed = now - self.heartbeats[nid]
                        if elapsed > self.timeout_sec:
                            self.node_status[nid] = "DEAD_TIMEOUT"
                            self.load_balancer.update_node_metrics(nid, is_alive=False)
                            self._handle_node_failure(nid, now)

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            nodes_info = {}
            for nid, last_hb in self.heartbeats.items():
                time_since_hb = round(now - last_hb, 1) if self.node_status[nid] != "CRASHED_MANUAL" else 999.0
                nodes_info[nid] = {
                    "node_id": nid,
                    "status": self.node_status[nid],
                    "seconds_since_last_heartbeat": time_since_hb,
                    "is_alive": self.node_status[nid] == "HEALTHY"
                }

            return {
                "nodes": nodes_info,
                "timeout_threshold_sec": self.timeout_sec,
                "active_promotions": self.promotions,
                "recovery_log": list(self.recovery_log[-10:])
            }
