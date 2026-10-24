"""
Concept 3: Clock Synchronization
Part 1: Lamport Logical Clocks (Leslie Lamport, 1978)
Maintains causal ordering of events across distributed nodes.
"""

import threading
import time
from typing import Dict, Any, List

class LamportClock:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.clock = 0
        self._lock = threading.Lock()
        self.event_log: List[Dict[str, Any]] = []

    def tick(self, event_description: str = "Local Event") -> int:
        """Rule 1: Increment on internal event"""
        with self._lock:
            self.clock += 1
            current = self.clock
            self._log_event("LOCAL", event_description, current, None)
            return current

    def send_event(self, target_node: str, message_type: str = "MSG") -> int:
        """Rule 2: Increment on sending a message and attach timestamp"""
        with self._lock:
            self.clock += 1
            current = self.clock
            self._log_event("SEND", f"Sent {message_type} to {target_node}", current, None)
            return current

    def receive_event(self, sender_node: str, received_timestamp: int, message_type: str = "MSG") -> int:
        """Rule 3: Update on receiving: clock = max(clock, received_timestamp) + 1"""
        with self._lock:
            old_clock = self.clock
            self.clock = max(self.clock, received_timestamp) + 1
            current = self.clock
            self._log_event(
                "RECEIVE",
                f"Received {message_type} from {sender_node} (was {old_clock}, msg was {received_timestamp})",
                current,
                received_timestamp
            )
            return current

    def _log_event(self, event_type: str, details: str, current_ts: int, incoming_ts: int = None):
        entry = {
            "node_id": self.node_id,
            "type": event_type,
            "description": details,
            "timestamp": current_ts,
            "incoming_ts": incoming_ts,
            "wall_time": time.strftime("%H:%M:%S")
        }
        self.event_log.append(entry)
        if len(self.event_log) > 60:
            self.event_log.pop(0)

    def get_time(self) -> int:
        with self._lock:
            return self.clock

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "node_id": self.node_id,
                "logical_time": self.clock,
                "events_count": len(self.event_log),
                "recent_events": list(self.event_log[-10:])
            }
