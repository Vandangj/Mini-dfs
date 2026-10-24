import unittest

from minidfs.cluster import MiniDFSCluster


class MiniDFSClusterTests(unittest.TestCase):
    def setUp(self):
        self.cluster = MiniDFSCluster()

    def tearDown(self):
        self.cluster.close()

    def test_write_read_and_replicate_chunks(self):
        result = self.cluster.dispatch(
            "files.write", {"name": "hello.txt", "content": "distributed systems"}
        )["file"]
        self.assertEqual(result["version"], 1)
        self.assertEqual(len(result["chunks"]), 3)
        for chunk in result["chunks"]:
            self.assertNotEqual(chunk["primary"], chunk["replicas"][0])
        self.assertEqual(
            self.cluster.dispatch("files.read", {"name": "hello.txt"})["content"],
            "distributed systems",
        )

    def test_failed_primary_is_promoted_and_file_remains_readable(self):
        metadata = self.cluster.dispatch(
            "files.write", {"name": "resilient.txt", "content": "replica survives"}
        )["file"]
        failed_node = metadata["chunks"][0]["primary"]
        self.cluster.dispatch("nodes.fail", {"node": failed_node})
        state = self.cluster.state()
        with self.cluster._lock:
            self.cluster._nodes[failed_node]["status"] = "failed"
            self.cluster._promote_replicas(failed_node)
        result = self.cluster.dispatch("files.read", {"name": "resilient.txt"})
        self.assertEqual(result["content"], "replica survives")
        self.assertNotIn(failed_node, result["sources"])

    def test_election_and_clock_algorithms(self):
        self.assertEqual(self.cluster.dispatch("election.run", {"algorithm": "bully"})["leader"], "M3")
        ring = self.cluster.dispatch("election.run", {"algorithm": "ring"})
        self.assertEqual(ring["leader"], "M3")
        synchronized = self.cluster.dispatch("clocks.sync")
        offsets = {node["offset_ms"] for node in synchronized["nodes"]}
        self.assertEqual(len(offsets), 1)

    def test_mapreduce_collectives_and_parallel_matrix(self):
        self.cluster.dispatch("files.write", {"name": "words.txt", "content": "map reduce map"})
        counts = self.cluster.dispatch("analytics.mapreduce")
        self.assertEqual(counts["words"][0], {"word": "map", "count": 2})
        collective = self.cluster.dispatch("compute.collectives")
        self.assertEqual(collective["gather"], [1, 2, 3])
        matrix = self.cluster.dispatch("compute.matrix")
        self.assertEqual(matrix["matrix"], [[19.0, 22.0], [43.0, 50.0]])

    def test_rejects_incompatible_matrix_dimensions(self):
        with self.assertRaisesRegex(ValueError, "column count"):
            self.cluster.dispatch("compute.matrix", {"a": [[1, 2]], "b": [[1, 2]]})

    def test_unknown_rpc_method_returns_clear_error(self):
        with self.assertRaisesRegex(ValueError, "Unknown RPC method"):
            self.cluster.dispatch("missing.method")


if __name__ == "__main__":
    unittest.main()
