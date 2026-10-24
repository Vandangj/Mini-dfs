"""
Concept 4: Leader Election Algorithms
Part 1: Garcia-Molina's Bully Algorithm (1982)
Used to elect a new Master/Metadata Server coordinator among candidate nodes.
"""

import threading
import time
from typing import Dict, Any, List

class BullyNode:
    def __init__(self, node_id: int, name: str, is_alive: bool = True):
        self.node_id = node_id
        self.name = name
        self.is_alive = is_alive
        self.current_coordinator: int = None

class BullyElectionSystem:
    def __init__(self):
        self.nodes: Dict[int, BullyNode] = {}
        self._lock = threading.Lock()
        self.message_trace: List[Dict[str, Any]] = []
        self.current_leader_id: int = None

        # Setup default candidate nodes (Metadata Server replicas)
        default_candidates = [
            (1, "Master-Replica-1"),
            (2, "Master-Replica-2"),
            (3, "Master-Replica-3"),
            (4, "Master-Standby"),
            (5, "Master-Primary")
        ]
        for nid, name in default_candidates:
            self.nodes[nid] = BullyNode(nid, name, is_alive=True)
        self.current_leader_id = 5

    def set_node_status(self, node_id: int, is_alive: bool):
        with self._lock:
            if node_id in self.nodes:
                self.nodes[node_id].is_alive = is_alive
                self._record_trace("STATUS_CHANGE", node_id, None, f"Node {node_id} ({self.nodes[node_id].name}) set to {'ALIVE' if is_alive else 'CRASHED'}")

    def trigger_election(self, initiator_id: int) -> Dict[str, Any]:
        """
        Executes Bully Election starting from initiator_id:
        1. Initiator sends ELECTION to all higher-ID nodes.
        2. Higher live nodes respond with OK / ANSWER and start their own election.
        3. If no higher node responds, initiator declares itself COORDINATOR.
        """
        with self._lock:
            self.message_trace.clear()
            
            if initiator_id not in self.nodes or not self.nodes[initiator_id].is_alive:
                return {"error": f"Initiator {initiator_id} is either invalid or crashed"}

            self._record_trace("ELECTION_START", initiator_id, None, f"Node {initiator_id} noticed coordinator failure and starts Bully Election")

            # Recursive / iterative bully resolution
            new_leader = self._bully_round(initiator_id)
            self.current_leader_id = new_leader

            for nid, node in self.nodes.items():
                if node.is_alive:
                    node.current_coordinator = new_leader

            return {
                "algorithm": "Bully",
                "initiator_id": initiator_id,
                "elected_leader_id": new_leader,
                "elected_leader_name": self.nodes[new_leader].name if new_leader else "None",
                "message_trace": list(self.message_trace)
            }

    def _bully_round(self, candidate_id: int) -> int:
        higher_nodes = [nid for nid in sorted(self.nodes.keys()) if nid > candidate_id]
        
        answered_higher_nodes = []
        for h_id in higher_nodes:
            self._record_trace("ELECTION_MSG", candidate_id, h_id, f"Node {candidate_id} sends ELECTION to higher node {h_id}")
            if self.nodes[h_id].is_alive:
                self._record_trace("ANSWER_OK", h_id, candidate_id, f"Node {h_id} is alive and responds OK to node {candidate_id}")
                answered_higher_nodes.append(h_id)
            else:
                self._record_trace("TIMEOUT_NO_ANSWER", candidate_id, h_id, f"No answer from crashed node {h_id} (Timeout)")

        if not answered_higher_nodes:
            # Candidate wins!
            self._record_trace("VICTORY", candidate_id, None, f"Node {candidate_id} received no OKs from higher nodes. It becomes COORDINATOR!")
            for other_id in self.nodes.keys():
                if other_id != candidate_id and self.nodes[other_id].is_alive:
                    self._record_trace("COORDINATOR_BROADCAST", candidate_id, other_id, f"Node {candidate_id} broadcasts COORDINATOR message to Node {other_id}")
            return candidate_id
        else:
            # The highest live node among responders initiates election
            highest_responder = max(answered_higher_nodes)
            return self._bully_round(highest_responder)

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
                "current_leader": self.current_leader_id,
                "nodes": {
                    nid: {
                        "id": nid,
                        "name": n.name,
                        "is_alive": n.is_alive,
                        "coordinator": n.current_coordinator
                    }
                    for nid, n in self.nodes.items()
                },
                "last_trace": list(self.message_trace[-20:])
            }
