"""Unit tests for the bench coordinator.

No live AWS calls — every boto3 client is hand-mocked. The tests pin
the pure logic (peer discovery shape, ring-wrap arithmetic, latency
percentiles, run-report rendering) so a future change can't silently
drift the bench-control protocol.
"""

from __future__ import annotations

import json
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Add the parent dir to sys.path so `import coordinator` resolves.
# The bench/ tree is intentionally outside `backend/app/` — it's a
# standalone tool, not part of the AO Python package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import coordinator  # noqa: E402


class TestDiscoverRingPeers(unittest.TestCase):
    """Peer discovery from a mocked EC2 describe_instances response."""

    def _make_response(self, *, instances: list[dict]) -> dict:
        return {
            "Reservations": [
                {"Instances": [self._make_instance(**i) for i in instances]}
            ]
        }

    def _make_instance(
        self,
        *,
        instance_id: str,
        node_id: int | str | None,
        private_ip: str,
        az: str,
        state: str = "running",
        role: str | None = "ring",
    ) -> dict:
        tags = []
        tags.append({"Key": "Project", "Value": coordinator.PROJECT_TAG})
        if role is not None:
            tags.append({"Key": "Role", "Value": role})
        if node_id is not None:
            tags.append({"Key": "NodeId", "Value": str(node_id)})
        return {
            "InstanceId": instance_id,
            "PrivateIpAddress": private_ip,
            "Placement": {"AvailabilityZone": az},
            "Tags": tags,
            "State": {"Name": state},
        }

    def test_three_peers_discovered_and_sorted(self):
        mock_ec2 = MagicMock()
        mock_ec2.describe_instances.return_value = self._make_response(
            instances=[
                {"instance_id": "i-aaa", "node_id": 2, "private_ip": "10.50.1.10", "az": "us-east-1a"},
                {"instance_id": "i-bbb", "node_id": 0, "private_ip": "10.50.1.20", "az": "us-east-1a"},
                {"instance_id": "i-ccc", "node_id": 1, "private_ip": "10.50.1.30", "az": "us-east-1a"},
            ]
        )
        peers = coordinator.discover_ring_peers(
            region="us-east-1", expected_size=3, ec2_client=mock_ec2
        )
        self.assertEqual([p.node_id for p in peers], [0, 1, 2])
        self.assertEqual(peers[0].instance_id, "i-bbb")
        self.assertEqual(peers[1].private_ip, "10.50.1.30")

    def test_partial_ring_raises(self):
        mock_ec2 = MagicMock()
        mock_ec2.describe_instances.return_value = self._make_response(
            instances=[
                {"instance_id": "i-aaa", "node_id": 0, "private_ip": "10.50.1.10", "az": "us-east-1a"},
                {"instance_id": "i-bbb", "node_id": 1, "private_ip": "10.50.1.20", "az": "us-east-1a"},
            ]
        )
        with self.assertRaisesRegex(RuntimeError, "discovered 2 ring peers"):
            coordinator.discover_ring_peers(
                region="us-east-1", expected_size=3, ec2_client=mock_ec2
            )

    def test_filter_query_shape(self):
        """The EC2 filter must be exactly Project=trains-bench, Role=ring,
        state=running. A regression here would silently match the wrong
        instances (e.g., the coordinator itself, or a stopped peer)."""
        mock_ec2 = MagicMock()
        mock_ec2.describe_instances.return_value = self._make_response(instances=[])
        with self.assertRaises(RuntimeError):
            coordinator.discover_ring_peers(
                region="us-east-1", expected_size=1, ec2_client=mock_ec2
            )
        call = mock_ec2.describe_instances.call_args
        filters = {f["Name"]: f["Values"] for f in call.kwargs["Filters"]}
        self.assertEqual(filters["tag:Project"], [coordinator.PROJECT_TAG])
        self.assertEqual(filters["tag:Role"], ["ring"])
        self.assertEqual(filters["instance-state-name"], ["running"])

    def test_missing_node_id_tag_skipped(self):
        """An instance missing the NodeId tag is skipped (logged warning)
        rather than crashing — the bench should still run if the operator
        manually tagged a coordinator with Role=ring by mistake."""
        mock_ec2 = MagicMock()
        mock_ec2.describe_instances.return_value = self._make_response(
            instances=[
                {"instance_id": "i-no-id", "node_id": None, "private_ip": "10.50.1.99", "az": "us-east-1a"},
                {"instance_id": "i-aaa", "node_id": 0, "private_ip": "10.50.1.10", "az": "us-east-1a"},
            ]
        )
        # expected_size=1 because the no-id instance is skipped
        peers = coordinator.discover_ring_peers(
            region="us-east-1", expected_size=1, ec2_client=mock_ec2
        )
        self.assertEqual(len(peers), 1)
        self.assertEqual(peers[0].instance_id, "i-aaa")


class TestRingWrapArithmetic(unittest.TestCase):
    """Successor wiring in build_node_runner_environment must wrap correctly."""

    def _peers(self, n: int) -> list[coordinator.RingPeer]:
        return [
            coordinator.RingPeer(
                node_id=i,
                instance_id=f"i-{i:03d}",
                private_ip=f"10.50.1.{10 + i}",
                az="us-east-1a",
            )
            for i in range(n)
        ]

    def _config(self) -> coordinator.RunConfig:
        return coordinator.RunConfig(
            duration_seconds=30.0,
            message_count=1000,
            payload_size=1024,
            ring_size=3,
            results_bucket="bench-bucket",
            run_id="20260522T200000Z",
            region="us-east-1",
            az_spread="single",
        )

    def test_successor_wraps_at_ring_end(self):
        peers = self._peers(3)
        env = coordinator.build_node_runner_environment(
            peer=peers[2],
            peers=peers,
            config=self._config(),
            peer_fingerprints="abc,def,ghi",
        )
        # node 2's successor is node 0 (wrap-around)
        self.assertEqual(env["SUCCESSOR_ADDR"], "10.50.1.10:7777")
        self.assertEqual(env["NODE_ID"], "2")
        self.assertEqual(env["RING_SIZE"], "3")
        self.assertEqual(env["ISSUE_INITIAL"], "false")

    def test_node_zero_is_issuer(self):
        peers = self._peers(3)
        env = coordinator.build_node_runner_environment(
            peer=peers[0],
            peers=peers,
            config=self._config(),
            peer_fingerprints="abc,def,ghi",
        )
        self.assertEqual(env["ISSUE_INITIAL"], "true")
        # node 0's successor is node 1
        self.assertEqual(env["SUCCESSOR_ADDR"], "10.50.1.11:7777")

    def test_listen_addr_uses_ring_port(self):
        peers = self._peers(3)
        env = coordinator.build_node_runner_environment(
            peer=peers[1],
            peers=peers,
            config=self._config(),
            peer_fingerprints="abc",
        )
        self.assertEqual(env["LISTEN_ADDR"], f"0.0.0.0:{coordinator.RING_PORT}")

    def test_results_bucket_and_run_id_propagated(self):
        peers = self._peers(3)
        env = coordinator.build_node_runner_environment(
            peer=peers[0],
            peers=peers,
            config=self._config(),
            peer_fingerprints="abc",
        )
        self.assertEqual(env["RESULTS_BUCKET"], "bench-bucket")
        self.assertEqual(env["RUN_ID"], "20260522T200000Z")


class TestShellQuote(unittest.TestCase):
    def test_simple_value_quoted(self):
        self.assertEqual(coordinator._shell_quote("10.50.1.10:7777"), "'10.50.1.10:7777'")

    def test_embedded_single_quote(self):
        self.assertEqual(coordinator._shell_quote("a'b"), "'a'\\''b'")


class TestBuildRunCommandParameters(unittest.TestCase):
    def test_exports_prefix_the_script(self):
        env = {"FOO": "bar", "BAZ": "qux"}
        params = coordinator.build_run_command_parameters(
            script_path="/opt/bench/node_runner.sh", env=env
        )
        self.assertEqual(len(params["commands"]), 3)
        # Order is dict-iteration order (insertion order in Python 3.7+)
        self.assertEqual(params["commands"][0], "export FOO='bar'")
        self.assertEqual(params["commands"][1], "export BAZ='qux'")
        self.assertEqual(params["commands"][2], "bash /opt/bench/node_runner.sh")


class TestLatencyPercentiles(unittest.TestCase):
    """Percentile arithmetic — small inputs that we can hand-check."""

    def test_empty_returns_none_tuple(self):
        p50, p95, p99, p999, n = coordinator.compute_latency_percentiles(
            send_log=[], deliveries=[]
        )
        self.assertIsNone(p50)
        self.assertIsNone(p95)
        self.assertIsNone(p99)
        self.assertIsNone(p999)
        self.assertEqual(n, 0)

    def test_no_overlap_returns_none_tuple(self):
        send_log = [{"seq": 0, "send_ns": 1000}]
        deliveries = [{"seq": 99, "recv_ns": 2000}]
        p50, p95, p99, p999, n = coordinator.compute_latency_percentiles(
            send_log=send_log, deliveries=deliveries
        )
        self.assertIsNone(p50)
        self.assertEqual(n, 0)

    def test_three_sample_latencies(self):
        """Send: 0@1000ns, 1@2000ns, 2@3000ns. Recv: 0@2000ns, 1@5000ns,
        2@4000ns. Latencies (ms): 1ms / 3ms / 1ms. Sorted: 1, 1, 3.
        p50 = middle = 1. p95 = top end (interpolated)."""
        send_log = [
            {"seq": 0, "send_ns": 1_000_000},
            {"seq": 1, "send_ns": 2_000_000},
            {"seq": 2, "send_ns": 3_000_000},
        ]
        deliveries = [
            {"seq": 0, "recv_ns": 2_000_000},  # 1 ms
            {"seq": 1, "recv_ns": 5_000_000},  # 3 ms
            {"seq": 2, "recv_ns": 4_000_000},  # 1 ms
        ]
        p50, p95, p99, p999, n = coordinator.compute_latency_percentiles(
            send_log=send_log, deliveries=deliveries
        )
        self.assertAlmostEqual(p50, 1.0, places=3)
        # p95/p99 of [1, 1, 3] interpolated:
        #   k = (3-1)*0.95 = 1.9, f=1, c=2, v = 1.0 + (3.0-1.0)*0.9 = 2.8
        self.assertAlmostEqual(p95, 2.8, places=3)
        self.assertAlmostEqual(p99, 2.96, places=3)
        self.assertEqual(n, 3)

    def test_warmup_discards_first_N(self):
        """With warmup_count=2, the first 2 samples (sorted by seq)
        are dropped before percentiles. Pins the warm-up behaviour."""
        send_log = [
            {"seq": 0, "send_ns": 0},
            {"seq": 1, "send_ns": 0},
            {"seq": 2, "send_ns": 0},
            {"seq": 3, "send_ns": 0},
            {"seq": 4, "send_ns": 0},
        ]
        # Cold-flight latencies on seq 0-1 are huge (100ms, 50ms);
        # steady-state on seq 2-4 is 1-2ms.
        deliveries = [
            {"seq": 0, "recv_ns": 100_000_000},  # 100 ms (cold)
            {"seq": 1, "recv_ns": 50_000_000},   # 50 ms (cold)
            {"seq": 2, "recv_ns": 1_000_000},    # 1 ms
            {"seq": 3, "recv_ns": 2_000_000},    # 2 ms
            {"seq": 4, "recv_ns": 1_500_000},    # 1.5 ms
        ]
        p50, p95, p99, p999, n = coordinator.compute_latency_percentiles(
            send_log=send_log, deliveries=deliveries, warmup_count=2,
        )
        self.assertEqual(n, 3)
        self.assertAlmostEqual(p50, 1.5, places=3)  # median of 1, 1.5, 2


class TestAggregateRun(unittest.TestCase):
    """End-to-end pure aggregator: peers + send_log + delivery_logs → report."""

    def _config(self) -> coordinator.RunConfig:
        return coordinator.RunConfig(
            duration_seconds=30.0,
            message_count=3,
            payload_size=64,
            ring_size=3,
            results_bucket="bench-bucket",
            run_id="20260522T200000Z",
            region="us-east-1",
            az_spread="single",
        )

    def _peers(self) -> list[coordinator.RingPeer]:
        return [
            coordinator.RingPeer(
                node_id=i,
                instance_id=f"i-{i:03d}",
                private_ip=f"10.50.1.{10 + i}",
                az="us-east-1a",
            )
            for i in range(3)
        ]

    def test_success_path_all_three_nodes_deliver_all(self):
        report = coordinator.aggregate_run(
            config=self._config(),
            peers=self._peers(),
            issuer_send_log=[
                {"seq": 0, "send_ns": 1_000_000},
                {"seq": 1, "send_ns": 2_000_000},
                {"seq": 2, "send_ns": 3_000_000},
            ],
            per_node_deliveries={
                0: [
                    {"seq": 0, "recv_ns": 1_500_000},
                    {"seq": 1, "recv_ns": 2_500_000},
                    {"seq": 2, "recv_ns": 3_500_000},
                ],
                1: [
                    {"seq": 0, "recv_ns": 1_600_000},
                    {"seq": 1, "recv_ns": 2_600_000},
                    {"seq": 2, "recv_ns": 3_600_000},
                ],
                2: [
                    {"seq": 0, "recv_ns": 1_700_000},
                    {"seq": 1, "recv_ns": 2_700_000},
                    {"seq": 2, "recv_ns": 3_700_000},
                ],
            },
            iperf3_results={"0-1": 950.0, "1-2": 945.0, "2-0": 952.0},
            started_at_ns=1_000_000_000,
            ended_at_ns=31_000_000_000,
        )
        self.assertTrue(report.success)
        self.assertIsNone(report.failure_reason)
        self.assertEqual(report.messages_sent, 3)
        self.assertEqual(report.deliveries_per_node, {0: 3, 1: 3, 2: 3})
        # Latency is computed from node 0's deliveries (first sorted node):
        # latencies = [0.5, 0.5, 0.5] ms → all percentiles == 0.5
        self.assertAlmostEqual(report.latency_p50_ms, 0.5, places=3)
        self.assertAlmostEqual(report.latency_p95_ms, 0.5, places=3)

    def test_partial_delivery_fails_with_specific_reason(self):
        report = coordinator.aggregate_run(
            config=self._config(),
            peers=self._peers(),
            issuer_send_log=[
                {"seq": 0, "send_ns": 1_000_000},
                {"seq": 1, "send_ns": 2_000_000},
                {"seq": 2, "send_ns": 3_000_000},
            ],
            per_node_deliveries={
                0: [
                    {"seq": 0, "recv_ns": 1_500_000},
                    {"seq": 1, "recv_ns": 2_500_000},
                    {"seq": 2, "recv_ns": 3_500_000},
                ],
                1: [
                    {"seq": 0, "recv_ns": 1_600_000},
                    {"seq": 1, "recv_ns": 2_600_000},
                ],  # missing seq=2
                2: [],  # missing all
            },
            iperf3_results={},
            started_at_ns=1_000_000_000,
            ended_at_ns=31_000_000_000,
        )
        self.assertFalse(report.success)
        self.assertIn("UTO completeness", report.failure_reason)
        self.assertEqual(report.deliveries_per_node, {0: 3, 1: 2, 2: 0})

    def test_zero_messages_sent_fails(self):
        report = coordinator.aggregate_run(
            config=self._config(),
            peers=self._peers(),
            issuer_send_log=[],
            per_node_deliveries={0: [], 1: [], 2: []},
            iperf3_results={},
            started_at_ns=1_000_000_000,
            ended_at_ns=2_000_000_000,
        )
        self.assertFalse(report.success)
        self.assertIn("issuer sent 0 messages", report.failure_reason)


class TestRunReportMarkdown(unittest.TestCase):
    def _minimal_report(self, *, success: bool, failure_reason: str | None = None):
        return coordinator.RunReport(
            config=coordinator.RunConfig(
                duration_seconds=30.0,
                message_count=1000,
                payload_size=1024,
                ring_size=3,
                results_bucket="bench-bucket",
                run_id="20260522T200000Z",
                region="us-east-1",
                az_spread="single",
                warmup_count=100,
            ),
            peers=[
                coordinator.RingPeer(
                    node_id=0, instance_id="i-aaa", private_ip="10.50.1.10",
                    az="us-east-1a",
                ),
            ],
            issuer_node_id=0,
            messages_sent=1000,
            deliveries_per_node={0: 1000},
            latency_p50_ms=0.45,
            latency_p95_ms=1.23,
            latency_p99_ms=2.34,
            latency_p999_ms=4.56,
            latency_sample_count=900,
            iperf3_throughput_mbps={},
            sockperf_pairwise_us={"0-to-1": {"p50_us": 25.0, "p99_us": 60.0, "observation_count": 100000}},
            success=success,
            failure_reason=failure_reason,
            started_at_ns=1_000_000_000,
            ended_at_ns=31_000_000_000,
        )

    def test_markdown_success_marker_present(self):
        md = self._minimal_report(success=True).to_markdown()
        self.assertIn("✅ success", md)
        self.assertIn("0.450 ms", md)
        self.assertIn("Ring size: 3", md)
        self.assertIn("🚂", md)  # issuer marker
        self.assertIn("25.0", md)  # sockperf p50 µs
        self.assertIn("warm-up discarded first 100", md)

    def test_markdown_failure_includes_reason(self):
        md = self._minimal_report(
            success=False, failure_reason="ring agent died"
        ).to_markdown()
        self.assertIn("❌ failed", md)
        self.assertIn("ring agent died", md)


class TestRunReportRoundtrip(unittest.TestCase):
    def test_json_roundtrip(self):
        report = coordinator.RunReport(
            config=coordinator.RunConfig(
                duration_seconds=1.0,
                message_count=10,
                payload_size=64,
                ring_size=3,
                results_bucket="b",
                run_id="r",
                region="us-east-1",
                az_spread="single",
                warmup_count=0,
            ),
            peers=[],
            issuer_node_id=0,
            messages_sent=10,
            deliveries_per_node={0: 10, 1: 10, 2: 10},
            latency_p50_ms=1.0,
            latency_p95_ms=2.0,
            latency_p99_ms=3.0,
            latency_p999_ms=4.0,
            latency_sample_count=10,
            iperf3_throughput_mbps={"0-1": 100.0},
            sockperf_pairwise_us=None,
            success=True,
            failure_reason=None,
            started_at_ns=time.time_ns(),
            ended_at_ns=time.time_ns() + 1_000_000_000,
        )
        d = report.to_dict()
        # Round-trips through JSON without errors
        json.dumps(d)
        self.assertEqual(d["config"]["ring_size"], 3)
        self.assertIn("duration_wall_s", d)
        self.assertAlmostEqual(d["duration_wall_s"], 1.0, places=2)


if __name__ == "__main__":
    unittest.main()
