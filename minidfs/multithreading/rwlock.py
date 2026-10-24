"""
Concept 2: Multithreading in Distributed Systems
Reader-Writer Lock (RWLock) implementation with telemetry
Allows concurrent readers OR exclusive writer access with fairness.
"""

import threading
import time
from typing import Dict, Any

class ReadWriteLock:
    def __init__(self, name: str = "NamespaceRWLock"):
        self.name = name
        self._lock = threading.Lock()
        self._readers_ok = threading.Condition(self._lock)
        self._writers_ok = threading.Condition(self._lock)
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False
        
        # Telemetry stats
        self.total_read_locks = 0
        self.total_write_locks = 0
        self.lock_history = []

    def acquire_read(self, caller: str = "Unknown"):
        with self._lock:
            while self._writer_active or self._writers_waiting > 0:
                self._readers_ok.wait()
            self._readers += 1
            self.total_read_locks += 1
            self._record_event("READ_ACQUIRED", caller, self._readers, self._writer_active)

    def release_read(self, caller: str = "Unknown"):
        with self._lock:
            self._readers -= 1
            self._record_event("READ_RELEASED", caller, self._readers, self._writer_active)
            if self._readers == 0:
                self._writers_ok.notify()

    def acquire_write(self, caller: str = "Unknown"):
        with self._lock:
            self._writers_waiting += 1
            while self._readers > 0 or self._writer_active:
                self._writers_ok.wait()
            self._writers_waiting -= 1
            self._writer_active = True
            self.total_write_locks += 1
            self._record_event("WRITE_ACQUIRED", caller, self._readers, self._writer_active)

    def release_write(self, caller: str = "Unknown"):
        with self._lock:
            self._writer_active = False
            self._record_event("WRITE_RELEASED", caller, self._readers, self._writer_active)
            # Prioritize waiting writers, else wake up readers
            if self._writers_waiting > 0:
                self._writers_ok.notify()
            else:
                self._readers_ok.notify_all()

    def _record_event(self, action: str, caller: str, readers: int, writer: bool):
        entry = {
            "timestamp": time.time(),
            "time_str": time.strftime("%H:%M:%S"),
            "action": action,
            "caller": caller,
            "active_readers": readers,
            "writer_active": writer,
            "writers_waiting": self._writers_waiting
        }
        self.lock_history.append(entry)
        if len(self.lock_history) > 50:
            self.lock_history.pop(0)

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "name": self.name,
                "active_readers": self._readers,
                "writer_active": self._writer_active,
                "writers_waiting": self._writers_waiting,
                "total_read_locks": self.total_read_locks,
                "total_write_locks": self.total_write_locks,
                "recent_history": list(self.lock_history[-10:])
            }

    # Context managers for pythonic usage
    class ReadContext:
        def __init__(self, rwlock, caller):
            self.rwlock = rwlock
            self.caller = caller

        def __enter__(self):
            self.rwlock.acquire_read(self.caller)
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.rwlock.release_read(self.caller)

    class WriteContext:
        def __init__(self, rwlock, caller):
            self.rwlock = rwlock
            self.caller = caller

        def __enter__(self):
            self.rwlock.acquire_write(self.caller)
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.rwlock.release_write(self.caller)

    def read_locked(self, caller: str = "Reader"):
        return self.ReadContext(self, caller)

    def write_locked(self, caller: str = "Writer"):
        return self.WriteContext(self, caller)
