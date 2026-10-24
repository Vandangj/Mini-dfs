"""
Concept 9: MPI Collective Communication Primitives
Implements standard MPI collective operations:
  - MPI_Bcast: One-to-all broadcast of identical data
  - MPI_Scatter: One-to-all distribution of partitioned data
  - MPI_Gather: All-to-one aggregation of distributed results
  - MPI_Reduce: All-to-one parallel reduction (SUM, MAX, MIN, PROD)
"""

import threading
import time
from typing import Dict, Any, List

class MPIProcess:
    def __init__(self, rank: int, name: str):
        self.rank = rank
        self.name = name
        self.local_buffer: Any = None
        self.received_messages: List[Dict[str, Any]] = []

class MPICommunicator:
    def __init__(self, size: int = 4):
        self.size = size
        self.ranks: Dict[int, MPIProcess] = {
            0: MPIProcess(0, "Root (Master)"),
            1: MPIProcess(1, "Worker 1 (SN1)"),
            2: MPIProcess(2, "Worker 2 (SN2)"),
            3: MPIProcess(3, "Worker 3 (SN3)")
        }
        self.comm_history: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def bcast(self, root: int, data: Any) -> Dict[str, Any]:
        """MPI_Bcast: Broadcast identical data from root to all processes"""
        with self._lock:
            start_t = time.time()
            transfers = []
            
            for rank, proc in self.ranks.items():
                proc.local_buffer = data
                if rank != root:
                    transfers.append({
                        "from_rank": root,
                        "to_rank": rank,
                        "data_preview": str(data)[:40],
                        "bytes": len(str(data).encode('utf-8'))
                    })

            elapsed_ms = round((time.time() - start_t) * 1000 + 0.8, 2)
            record = {
                "operation": "MPI_Bcast",
                "root_rank": root,
                "timestamp": time.strftime("%H:%M:%S"),
                "elapsed_ms": elapsed_ms,
                "transfers": transfers,
                "verification": {r: proc.local_buffer for r, proc in self.ranks.items()}
            }
            self.comm_history.append(record)
            return record

    def scatter(self, root: int, data_list: List[Any]) -> Dict[str, Any]:
        """MPI_Scatter: Splits data_list into equal segments and sends one to each rank"""
        with self._lock:
            start_t = time.time()
            num_proc = self.size
            
            # Pad or partition data_list
            chunk_size = max(1, len(data_list) // num_proc)
            partitions = []
            for i in range(num_proc):
                part = data_list[i * chunk_size : (i + 1) * chunk_size] if i < num_proc - 1 else data_list[i * chunk_size :]
                partitions.append(part)

            transfers = []
            for rank, proc in self.ranks.items():
                assigned_chunk = partitions[rank] if rank < len(partitions) else []
                proc.local_buffer = assigned_chunk
                transfers.append({
                    "from_rank": root,
                    "to_rank": rank,
                    "partition_index": rank,
                    "items": assigned_chunk
                })

            elapsed_ms = round((time.time() - start_t) * 1000 + 1.1, 2)
            record = {
                "operation": "MPI_Scatter",
                "root_rank": root,
                "total_items": len(data_list),
                "timestamp": time.strftime("%H:%M:%S"),
                "elapsed_ms": elapsed_ms,
                "transfers": transfers
            }
            self.comm_history.append(record)
            return record

    def gather(self, root: int) -> Dict[str, Any]:
        """MPI_Gather: Collects local_buffers from all ranks and combines them at root"""
        with self._lock:
            start_t = time.time()
            gathered_result = []
            transfers = []

            for rank in sorted(self.ranks.keys()):
                proc = self.ranks[rank]
                item = proc.local_buffer
                gathered_result.append(item)
                if rank != root:
                    transfers.append({
                        "from_rank": rank,
                        "to_rank": root,
                        "data": item
                    })

            self.ranks[root].local_buffer = gathered_result
            elapsed_ms = round((time.time() - start_t) * 1000 + 0.9, 2)
            record = {
                "operation": "MPI_Gather",
                "root_rank": root,
                "timestamp": time.strftime("%H:%M:%S"),
                "elapsed_ms": elapsed_ms,
                "transfers": transfers,
                "gathered_result": gathered_result
            }
            self.comm_history.append(record)
            return record

    def reduce(self, root: int, operator: str = "SUM") -> Dict[str, Any]:
        """MPI_Reduce: Combines local_buffers using reduction operator into root"""
        with self._lock:
            start_t = time.time()
            values = []
            for rank, proc in self.ranks.items():
                val = proc.local_buffer
                if isinstance(val, (int, float)):
                    values.append(val)
                elif isinstance(val, list) and all(isinstance(x, (int, float)) for x in val):
                    values.append(sum(val))
                else:
                    values.append(1)

            if operator == "SUM":
                reduced_val = sum(values)
            elif operator == "MAX":
                reduced_val = max(values)
            elif operator == "MIN":
                reduced_val = min(values)
            elif operator == "PROD":
                reduced_val = 1
                for v in values:
                    reduced_val *= v
            else:
                reduced_val = sum(values)

            self.ranks[root].local_buffer = reduced_val
            elapsed_ms = round((time.time() - start_t) * 1000 + 0.7, 2)
            record = {
                "operation": "MPI_Reduce",
                "operator": operator,
                "root_rank": root,
                "contributing_values": values,
                "reduced_result": reduced_val,
                "elapsed_ms": elapsed_ms,
                "timestamp": time.strftime("%H:%M:%S")
            }
            self.comm_history.append(record)
            return record

    def get_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "communicator_size": self.size,
                "ranks": {
                    r: {
                        "rank": r,
                        "name": p.name,
                        "buffer_preview": str(p.local_buffer)[:50]
                    }
                    for r, p in self.ranks.items()
                },
                "recent_operations": list(self.comm_history[-10:])
            }
