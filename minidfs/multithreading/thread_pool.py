"""
Concept 2: Multithreading in Distributed Systems
Thread Pool Manager with live telemetry monitoring for Master and Storage Nodes
"""

import threading
import queue
import time
from typing import Callable, Any, Dict, List

class WorkerThread(threading.Thread):
    def __init__(self, worker_id: int, task_queue: queue.Queue, pool):
        super().__init__(name=f"PoolWorker-{worker_id}", daemon=True)
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.pool = pool
        self.is_busy = False
        self.current_task_name = "Idle"
        self.tasks_processed = 0

    def run(self):
        while not self.pool.stopped:
            try:
                task_item = self.task_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if task_item is None:
                self.task_queue.task_done()
                break

            func, args, kwargs, future_dict = task_item
            self.is_busy = True
            self.current_task_name = getattr(func, "__name__", str(func))
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                future_dict["result"] = result
                future_dict["status"] = "SUCCESS"
            except Exception as e:
                future_dict["error"] = str(e)
                future_dict["status"] = "FAILED"
            finally:
                elapsed = time.time() - start_time
                future_dict["elapsed"] = elapsed
                future_dict["event"].set()
                self.tasks_processed += 1
                self.is_busy = False
                self.current_task_name = "Idle"
                self.task_queue.task_done()
                self.pool.record_task_done(elapsed)

class MonitoredThreadPool:
    def __init__(self, name: str = "MiniDFSThreadPool", num_workers: int = 8):
        self.name = name
        self.num_workers = num_workers
        self.task_queue = queue.Queue()
        self.workers: List[WorkerThread] = []
        self.stopped = False
        self._lock = threading.Lock()
        
        self.total_submitted = 0
        self.total_completed = 0
        self.total_execution_time = 0.0

        for i in range(num_workers):
            w = WorkerThread(i + 1, self.task_queue, self)
            w.start()
            self.workers.append(w)

    def submit(self, func: Callable, *args, **kwargs) -> Dict[str, Any]:
        with self._lock:
            self.total_submitted += 1
        
        future = {
            "result": None,
            "error": None,
            "status": "PENDING",
            "elapsed": 0.0,
            "event": threading.Event()
        }
        self.task_queue.put((func, args, kwargs, future))
        return future

    def record_task_done(self, elapsed: float):
        with self._lock:
            self.total_completed += 1
            self.total_execution_time += elapsed

    def shutdown(self):
        self.stopped = True
        for _ in self.workers:
            self.task_queue.put(None)
        for w in self.workers:
            w.join(timeout=1.0)

    def get_telemetry(self) -> Dict[str, Any]:
        with self._lock:
            active_count = sum(1 for w in self.workers if w.is_busy)
            avg_time = (self.total_execution_time / self.total_completed) if self.total_completed > 0 else 0.0
            
            workers_info = [
                {
                    "worker_id": w.worker_id,
                    "name": w.name,
                    "busy": w.is_busy,
                    "current_task": w.current_task_name,
                    "tasks_processed": w.tasks_processed
                }
                for w in self.workers
            ]

            return {
                "pool_name": self.name,
                "total_workers": self.num_workers,
                "busy_workers": active_count,
                "idle_workers": self.num_workers - active_count,
                "queue_depth": self.task_queue.qsize(),
                "total_submitted": self.total_submitted,
                "total_completed": self.total_completed,
                "avg_execution_ms": round(avg_time * 1000, 2),
                "workers": workers_info
            }
