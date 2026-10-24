"""
Concept 6: Load Balancing Algorithm
Implements load-aware chunk placement and client request routing across Storage Nodes.
Policies:
  - LEAST_LOADED (Dynamic composite score based on connections + storage)
  - WEIGHTED_CAPACITY (Available storage capacity aware)
  - ROUND_ROBIN (Standard cyclic balancing)
"""

import threading
import time
from typing import Dict, Any, List

class NodeLoadMetrics:
    def __init__(self, node_id: str, capacity_bytes: int = 10 * 1024 * 1024):
        self.node_id = node_id
        self.capacity_bytes = capacity_bytes
        self.used_bytes = 1024 * 50  # base footprint
        self.active_connections = 0
        self.total_requests_served = 0
        self.cpu_usage_pct = 5.0
        self.is_alive = True

    def get_composite_load_score(self) -> float:
        """
        Composite Load Score (0.0 to 100.0):
        40% Storage Utilization + 35% Active Connections + 25% CPU
        """
        if not self.is_alive:
            return float('inf')
        storage_pct = (self.used_bytes / self.capacity_bytes) * 100.0
        conn_score = min(100.0, self.active_connections * 10.0)
        score = (0.40 * storage_pct) + (0.35 * conn_score) + (0.25 * self.cpu_usage_pct)
        return round(score, 2)

class LoadBalancer:
    def __init__(self):
        self.nodes: Dict[str, NodeLoadMetrics] = {
            "SN1": NodeLoadMetrics("SN1"),
            "SN2": NodeLoadMetrics("SN2"),
            "SN3": NodeLoadMetrics("SN3")
        }
        self.policy = "LEAST_LOADED"  # LEAST_LOADED, ROUND_ROBIN, WEIGHTED_CAPACITY
        self.rr_index = 0
        self.node_keys = ["SN1", "SN2", "SN3"]
        self.placement_history: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def set_policy(self, policy: str):
        with self._lock:
            if policy in ["LEAST_LOADED", "ROUND_ROBIN", "WEIGHTED_CAPACITY"]:
                self.policy = policy

    def update_node_metrics(self, node_id: str, delta_bytes: int = 0, delta_conns: int = 0, cpu_pct: float = None, is_alive: bool = None):
        with self._lock:
            if node_id in self.nodes:
                node = self.nodes[node_id]
                if is_alive is not None:
                    node.is_alive = is_alive
                node.used_bytes = max(1024, node.used_bytes + delta_bytes)
                node.active_connections = max(0, node.active_connections + delta_conns)
                if cpu_pct is not None:
                    node.cpu_usage_pct = max(1.0, min(100.0, cpu_pct))
                if delta_conns > 0:
                    node.total_requests_served += delta_conns

    def select_placement_node(self, chunk_id: str, chunk_size_bytes: int = 4096) -> Dict[str, Any]:
        """
        Selects target primary node for chunk placement based on current load policy.
        """
        with self._lock:
            alive_nodes = [nid for nid in self.node_keys if self.nodes[nid].is_alive]
            if not alive_nodes:
                return {"error": "No healthy storage nodes available"}

            chosen_node = None
            reason = ""

            if self.policy == "ROUND_ROBIN":
                chosen_node = alive_nodes[self.rr_index % len(alive_nodes)]
                self.rr_index = (self.rr_index + 1) % len(alive_nodes)
                reason = f"Cyclic round-robin assignment (step {self.rr_index})"

            elif self.policy == "WEIGHTED_CAPACITY":
                # Pick node with maximum free bytes
                chosen_node = max(alive_nodes, key=lambda n: self.nodes[n].capacity_bytes - self.nodes[n].used_bytes)
                free_kb = (self.nodes[chosen_node].capacity_bytes - self.nodes[chosen_node].used_bytes) // 1024
                reason = f"Maximum available capacity ({free_kb} KB free)"

            else:  # LEAST_LOADED default
                # Pick node with minimum composite load score
                chosen_node = min(alive_nodes, key=lambda n: self.nodes[n].get_composite_load_score())
                min_score = self.nodes[chosen_node].get_composite_load_score()
                reason = f"Lowest composite load score ({min_score})"

            # Update metrics on chosen node
            self.nodes[chosen_node].used_bytes += chunk_size_bytes
            self.nodes[chosen_node].total_requests_served += 1

            record = {
                "timestamp": time.strftime("%H:%M:%S"),
                "chunk_id": chunk_id,
                "chosen_node": chosen_node,
                "policy_used": self.policy,
                "reason": reason,
                "node_scores": {nid: self.nodes[nid].get_composite_load_score() for nid in alive_nodes}
            }
            self.placement_history.append(record)
            if len(self.placement_history) > 30:
                self.placement_history.pop(0)

            return {
                "chunk_id": chunk_id,
                "assigned_node": chosen_node,
                "policy": self.policy,
                "reason": reason,
                "composite_load": self.nodes[chosen_node].get_composite_load_score()
            }

    def route_read_request(self, primary_node: str, replica_node: str) -> Dict[str, Any]:
        """
        Load-aware read routing: Chooses between primary or replica based on least load!
        """
        with self._lock:
            p_alive = self.nodes[primary_node].is_alive
            r_alive = self.nodes[replica_node].is_alive

            if not p_alive and not r_alive:
                return {"error": "Both primary and replica nodes are offline!"}
            if not p_alive:
                return {"selected_node": replica_node, "reason": f"Primary {primary_node} offline, routed to Replica"}
            if not r_alive:
                return {"selected_node": primary_node, "reason": f"Replica {replica_node} offline, routed to Primary"}

            p_load = self.nodes[primary_node].get_composite_load_score()
            r_load = self.nodes[replica_node].get_composite_load_score()

            if p_load <= r_load:
                selected = primary_node
                diff = round(r_load - p_load, 2)
                reason = f"Primary load ({p_load}) <= Replica load ({r_load}) [Advantage: -{diff}]"
            else:
                selected = replica_node
                diff = round(p_load - r_load, 2)
                reason = f"Replica load ({r_load}) < Primary load ({p_load}) [Load redirected, saved: -{diff}]"

            self.nodes[selected].total_requests_served += 1
            return {
                "selected_node": selected,
                "is_replica": (selected == replica_node),
                "primary_load": p_load,
                "replica_load": r_load,
                "reason": reason
            }

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "active_policy": self.policy,
                "nodes": {
                    nid: {
                        "node_id": nid,
                        "is_alive": n.is_alive,
                        "used_bytes": n.used_bytes,
                        "capacity_bytes": n.capacity_bytes,
                        "used_pct": round((n.used_bytes / n.capacity_bytes) * 100, 1),
                        "active_connections": n.active_connections,
                        "cpu_pct": n.cpu_usage_pct,
                        "total_requests": n.total_requests_served,
                        "load_score": n.get_composite_load_score()
                    }
                    for nid, n in self.nodes.items()
                },
                "recent_placements": list(self.placement_history[-10:])
            }
