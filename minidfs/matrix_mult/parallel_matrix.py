"""
Concept 10: Parallel Matrix Multiplication using MPI
Uses MPI Collective primitives (Scatter, Broadcast, Gather) to multiply
two matrices across distributed worker ranks (Storage Nodes).
Compares sequential vs parallel performance and calculates speedup.
"""

import threading
import time
import random
from typing import Dict, Any, List, Tuple

class ParallelMatrixMultiplier:
    def __init__(self, mpi_comm):
        self.mpi_comm = mpi_comm
        self.last_run_result: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def generate_random_matrix(self, rows: int, cols: int, min_val: int = 1, max_val: int = 9) -> List[List[int]]:
        return [[random.randint(min_val, max_val) for _ in range(cols)] for _ in range(rows)]

    def multiply_sequential(self, A: List[List[int]], B: List[List[int]]) -> Tuple[List[List[int]], float]:
        rows_A = len(A)
        cols_A = len(A[0])
        rows_B = len(B)
        cols_B = len(B[0])

        if cols_A != rows_B:
            raise ValueError(f"Incompatible dimensions: {cols_A} != {rows_B}")

        start_t = time.perf_counter()
        C = [[0] * cols_B for _ in range(rows_A)]
        for i in range(rows_A):
            for k in range(cols_A):
                for j in range(cols_B):
                    C[i][j] += A[i][k] * B[k][j]
        elapsed_ms = (time.perf_counter() - start_t) * 1000.0
        return C, round(elapsed_ms, 3)

    def multiply_parallel_mpi(self, A: List[List[int]] = None, B: List[List[int]] = None, dim: int = 8) -> Dict[str, Any]:
        """
        Parallel Matrix Multiplication via MPI Collective:
          1. Root broadcasts Matrix B to all ranks (MPI_Bcast)
          2. Root scatters row blocks of Matrix A across ranks (MPI_Scatter)
          3. Ranks compute local dot-products in parallel
          4. Root gathers computed row blocks (MPI_Gather)
        """
        with self._lock:
            if A is None or B is None:
                A = self.generate_random_matrix(dim, dim)
                B = self.generate_random_matrix(dim, dim)

            rows_A = len(A)
            cols_A = len(A[0])
            rows_B = len(B)
            cols_B = len(B[0])

            if cols_A != rows_B:
                return {"error": "Dimension mismatch between Matrix A and B"}

            # Step 0: Sequential baseline for benchmark and correctness verification
            seq_result, seq_time_ms = self.multiply_sequential(A, B)

            par_start_t = time.perf_counter()

            # Step 1: MPI_Bcast - Broadcast Matrix B to all ranks
            bcast_event = self.mpi_comm.bcast(root=0, data=B)

            # Step 2: MPI_Scatter - Partition rows of Matrix A and scatter across ranks
            scatter_event = self.mpi_comm.scatter(root=0, data_list=A)

            # Step 3: Parallel Local Computation per Rank
            worker_computations = []
            threads = []
            local_results = [None] * self.mpi_comm.size

            def compute_worker_rows(rank, row_slice):
                t0 = time.perf_counter()
                computed_rows = []
                for row in row_slice:
                    c_row = [0] * cols_B
                    for k in range(cols_A):
                        for j in range(cols_B):
                            c_row[j] += row[k] * B[k][j]
                    computed_rows.append(c_row)
                local_results[rank] = computed_rows
                w_time = (time.perf_counter() - t0) * 1000.0
                worker_computations.append({
                    "rank": rank,
                    "rows_computed": len(row_slice),
                    "time_ms": round(w_time, 3)
                })

            for rank, proc in self.mpi_comm.ranks.items():
                row_slice = proc.local_buffer or []
                t = threading.Thread(target=compute_worker_rows, args=(rank, row_slice))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            # Assign local results to rank buffers
            for rank in range(self.mpi_comm.size):
                self.mpi_comm.ranks[rank].local_buffer = local_results[rank]

            # Step 4: MPI_Gather - Gather computed rows back at Root
            gather_event = self.mpi_comm.gather(root=0)

            # Flatten gathered rows into complete Matrix C
            par_matrix = []
            for sublist in self.mpi_comm.ranks[0].local_buffer:
                if sublist:
                    par_matrix.extend(sublist)

            par_time_ms = round((time.perf_counter() - par_start_t) * 1000.0, 3)

            # Verification: Check if parallel output matches sequential
            is_correct = (par_matrix == seq_result)
            speedup = round(seq_time_ms / max(0.01, par_time_ms), 2) if seq_time_ms > 0 else 1.0
            efficiency = round(speedup / self.mpi_comm.size, 2)

            result = {
                "timestamp": time.strftime("%H:%M:%S"),
                "dimensions": f"{rows_A}x{cols_A} * {rows_B}x{cols_B}",
                "num_ranks": self.mpi_comm.size,
                "sequential_time_ms": seq_time_ms,
                "parallel_time_ms": par_time_ms,
                "speedup": speedup,
                "efficiency": efficiency,
                "verified_correct": is_correct,
                "worker_computations": worker_computations,
                "matrix_A_preview": A[:4],
                "matrix_B_preview": B[:4],
                "matrix_C_preview": par_matrix[:4],
                "mpi_events": {
                    "bcast": bcast_event["elapsed_ms"],
                    "scatter": scatter_event["elapsed_ms"],
                    "gather": gather_event["elapsed_ms"]
                }
            }
            self.last_run_result = result
            return result

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return self.last_run_result
