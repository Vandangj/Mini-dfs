"""
Concept 4: Leader Election Algorithms
Part 2: Token-Ring Election Algorithm (Chang & Roberts / LeLann)
Nodes are arranged in a logical unidirectional ring.
"""

import threading
import time
from typing import Dict, Any, List

class RingElectionSystem:
    def __init__(self):
        # Ring topology: 1 -> 2 -> 3 -> 4 -> 5 -> 1
        self.ring_order = [1, 2, 3, 4, 5]
        self.nodes = {
            1: {"id": 1, "name": "Node-1", "is_alive": True},
            2: {"id": 2, "name": "Node-2", "is_alive": True},
            3: {"id": 3, "name": "Node-3", "is_alive": True},
            4: {"id": 4, "name": "Node-4", "is_alive": True},
            5: {"id": 5, "name": "Node-5", "is_alive": True}
        }
        self.current_leader = 5
        self.message_trace: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def set_node_status(self, node_id: int, is_alive: bool):
        with self._lock:
            if node_id in self.nodes:
                self.nodes[node_id]["is_alive"] = is_alive

    def _get_next_live_neighbor(self, current_id: int) -> int:
        idx = self.ring_order.index(current_id)
        n = len(self.ring_order)
        for offset in range(1, n):
            candidate = self.ring_order[(idx + offset) % n]
            if self.nodes[candidate]["is_alive"]:
                return candidate
        return None

    def trigger_election(self, initiator_id: int) -> Dict[str, Any]:
        with self._lock:
            self.message_trace.clear()
            if initiator_id not in self.nodes or not self.nodes[initiator_id]["is_alive"]:
                return {"error": f"Initiator {initiator_id} is dead or invalid"}

            self._record_trace("RING_START", initiator_id, None, f"Node {initiator_id} initiates Ring Election")

            # Active candidate list
            active_list = [initiator_id]
            curr = initiator_id

            while True:
                next_node = self._get_next_live_neighbor(curr)
                if next_node is None:
                    # All other nodes dead, curr is leader
                    break

                self._record_trace("FORWARD_ELECTION", curr, next_node, f"Node {curr} forwards election token {active_list} to Node {next_node}")
                
                if next_node == initiator_id:
                    # Full loop completed!
                    self._record_trace("LOOP_COMPLETE", next_node, None, f"Token returned to initiator {initiator_id}. Active candidates: {active_list}")
                    break

                if next_node not in active_list:
                    active_list.append(next_node)
                curr = next_node

            # The leader is the node with the maximum ID in active_list
            new_leader = max(active_list)
            self._record_trace("LEADER_CHOSEN", new_leader, None, f"Node {new_leader} has highest ID among active nodes. Elected as COORDINATOR!")

            # Circulate COORDINATOR message around the ring
            curr = new_leader
            while True:
                next_node = self._get_next_live_neighbor(curr)
                if next_node is None or next_node == new_leader:
                    break
                self._record_trace("COORDINATOR_TOKEN", curr, next_node, f"Node {curr} informs Node {next_node} that Node {new_leader} is the new COORDINATOR")
                curr = next_node

            self.current_leader = new_leader
            return {
                "algorithm": "Ring",
                "initiator_id": initiator_id,
                "elected_leader": new_leader,
                "active_ring": active_list,
                "message_trace": list(self.message_trace)
            }

    def _record_trace(self, msg_type: str, sender: int, receiver: int, desc: str):
        entry = {
            "timestamp": time.strftime("%H:%M:%S"),
            "msg_type": msg_type,
            "sender": sender,
            "receiver": receiver,
            "description": desc
        }
        self.message_trace.append(entry)

    def get_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "current_leader": self.current_leader,
                "ring_order": self.ring_order,
                "nodes": self.nodes,
                "last_trace": list(self.message_trace[-20:])
            }
