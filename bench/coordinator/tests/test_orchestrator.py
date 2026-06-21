"""Unit tests for bench/coordinator/orchestrator.py.

All boto3 clients are hand-mocked. The tests pin:
- SSMOrchestrator dry-run semantics (no boto3 ever constructed).
- Live-mode behaviour (send_command + wait_for_command loops).
- Each step of the run pipeline (identities, start, wait, issuer,
  collect, aggregate) — what SSM calls get issued, in what order,
  with what shape.
- End-to-end dry-run smoke (`TestRunDryRun`) runs the full `run()`
  function with synthetic peers and asserts the SSM call log shape.

No live AWS calls. No real wall-clock sleeps (`time.sleep` is
injected and stubbed).
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import coordinator  # noqa: E402
import orchestrator  # noqa: E402


# ── SSMOrchestrator dry-run ─────────────────────────────────────────


class TestSSMOrchestratorDryRun(unittest.TestCase):
    """In dry-run mode, no real boto3 ssm client is created and no AWS
    calls happen. Every send_command + wait_for_command is logged for
    inspection."""

    def test_send_command_records_kwargs(self):
        ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
        cmd_id = ssm.send_command(
            instance_ids=["i-aaa"],
            parameters={"commands": ["echo hello"]},
            comment="test",
        )
        self.assertTrue(cmd_id.startswith("dry-run-"))
        self.assertEqual(len(ssm.dry_run_log), 1)
        entry = ssm.dry_run_log[0]
        self.assertEqual(entry["call"], "send_command")
        self.assertEqual(entry["kwargs"]["InstanceIds"], ["i-aaa"])
        self.assertEqual(entry["kwargs"]["DocumentName"], orchestrator.SSM_DOCUMENT)
        self.assertEqual(entry["kwargs"]["Parameters"], {"commands": ["echo hello"]})
        self.assertEqual(entry["command_id"], cmd_id)

    def test_dry_run_does_not_construct_boto3_client(self):
        # The internal _ssm attribute must be None — proves no
        # boto3.client("ssm") was called.
        ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
        self.assertIsNone(ssm._ssm)

    def test_wait_for_command_returns_synthetic_success(self):
        ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
        inv = ssm.wait_for_command(command_id="dry-run-aaa", instance_id="i-aaa")
        self.assertEqual(inv["Status"], "Success")
        self.assertEqual(inv["ResponseCode"], 0)
        # The wait call is logged too (for the smoke-test transcript).
        wait_entries = [e for e in ssm.dry_run_log if e["call"] == "wait_for_command"]
        self.assertEqual(len(wait_entries), 1)

    def test_dry_run_command_ids_are_unique(self):
        ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
        ids = set()
        for _ in range(20):
            ids.add(ssm.send_command(
                instance_ids=["i-x"], parameters={"commands": ["true"]}, comment="x",
            ))
        self.assertEqual(len(ids), 20, "dry-run command ids should be unique")


# ── SSMOrchestrator live mode (with injected mock client) ───────────


class TestSSMOrchestratorLive(unittest.TestCase):
    def test_send_command_calls_boto3_with_expected_shape(self):
        mock_ssm = MagicMock()
        mock_ssm.send_command.return_value = {"Command": {"CommandId": "real-aaa"}}
        ssm = orchestrator.SSMOrchestrator(
            region="us-east-1", dry_run=False, ssm_client=mock_ssm
        )
        cmd_id = ssm.send_command(
            instance_ids=["i-aaa"],
            parameters={"commands": ["echo hi"]},
            comment="live test",
            timeout_seconds=120,
        )
        self.assertEqual(cmd_id, "real-aaa")
        mock_ssm.send_command.assert_called_once_with(
            InstanceIds=["i-aaa"],
            DocumentName=orchestrator.SSM_DOCUMENT,
            Parameters={"commands": ["echo hi"]},
            Comment="live test",
            TimeoutSeconds=120,
        )

    def test_wait_for_command_polls_until_success(self):
        mock_ssm = MagicMock()
        # Two "InProgress" responses, then "Success"
        mock_ssm.get_command_invocation.side_effect = [
            {"Status": "InProgress"},
            {"Status": "InProgress"},
            {"Status": "Success", "ResponseCode": 0, "StandardOutputContent": "done"},
        ]
        sleeps: list[float] = []
        times = iter([0.0, 1.0, 2.0, 3.0, 4.0])
        ssm = orchestrator.SSMOrchestrator(
            region="us-east-1", dry_run=False, ssm_client=mock_ssm,
        )
        inv = ssm.wait_for_command(
            command_id="real-aaa", instance_id="i-aaa", max_wait_s=60,
            poll_interval_s=1.0,
            sleep_fn=sleeps.append,
            now_fn=lambda: next(times),
        )
        self.assertEqual(inv["Status"], "Success")
        self.assertEqual(mock_ssm.get_command_invocation.call_count, 3)
        # Slept twice (between three polls)
        self.assertEqual(sleeps, [1.0, 1.0])

    @unittest.skipIf(
        orchestrator.botocore is None,
        "botocore not installed on this host (install boto3 to run)",
    )
    def test_wait_for_command_handles_initial_invocation_does_not_exist(self):
        """SSM can return InvocationDoesNotExist for ~1-2s after SendCommand
        before the invocation is registered. The waiter must retry."""
        from botocore.exceptions import ClientError
        mock_ssm = MagicMock()
        not_found = ClientError(
            error_response={"Error": {"Code": "InvocationDoesNotExist", "Message": "x"}},
            operation_name="GetCommandInvocation",
        )
        mock_ssm.get_command_invocation.side_effect = [
            not_found,
            {"Status": "Success", "ResponseCode": 0},
        ]
        ssm = orchestrator.SSMOrchestrator(
            region="us-east-1", dry_run=False, ssm_client=mock_ssm,
        )
        sleeps: list[float] = []
        times = iter([0.0, 1.0, 2.0, 3.0])
        inv = ssm.wait_for_command(
            command_id="real-aaa", instance_id="i-aaa", max_wait_s=60,
            poll_interval_s=1.0,
            sleep_fn=sleeps.append,
            now_fn=lambda: next(times),
        )
        self.assertEqual(inv["Status"], "Success")
        self.assertEqual(len(sleeps), 1)

    def test_wait_for_command_raises_on_timeout(self):
        mock_ssm = MagicMock()
        mock_ssm.get_command_invocation.return_value = {"Status": "InProgress"}
        ssm = orchestrator.SSMOrchestrator(
            region="us-east-1", dry_run=False, ssm_client=mock_ssm,
        )
        # Time advances faster than deadline allows
        times = iter([0.0, 100.0, 200.0])
        with self.assertRaisesRegex(TimeoutError, "did not terminate"):
            ssm.wait_for_command(
                command_id="real-aaa", instance_id="i-aaa", max_wait_s=10,
                poll_interval_s=1.0,
                sleep_fn=lambda _s: None,
                now_fn=lambda: next(times),
            )


# ── S3Helper ─────────────────────────────────────────────────────────


class TestS3HelperDryRun(unittest.TestCase):
    def test_put_object_in_dry_run_records_call(self):
        s3 = orchestrator.S3Helper(region="us-east-1", dry_run=True)
        s3.put_object(bucket="b", key="k.json", body=b'{"x":1}')
        self.assertEqual(len(s3.dry_run_log), 1)
        entry = s3.dry_run_log[0]
        self.assertEqual(entry["call"], "put_object")
        self.assertEqual(entry["bucket"], "b")
        self.assertEqual(entry["key"], "k.json")
        self.assertEqual(entry["size"], 7)

    def test_get_object_in_dry_run_returns_synthetic_empty_json(self):
        s3 = orchestrator.S3Helper(region="us-east-1", dry_run=True)
        body = s3.get_object(bucket="b", key="k.json")
        self.assertEqual(body, b"{}")
        # Parseable as empty JSON object — so downstream `.get(...)` works.
        self.assertEqual(json.loads(body), {})


class TestS3HelperLive(unittest.TestCase):
    def test_get_object_unwraps_body_stream(self):
        # boto3 returns Body as a streaming object with a .read() method.
        import io as io_module
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {"Body": io_module.BytesIO(b'{"a":1}')}
        helper = orchestrator.S3Helper(
            region="us-east-1", dry_run=False, s3_client=mock_s3,
        )
        body = helper.get_object(bucket="b", key="k.json")
        self.assertEqual(body, b'{"a":1}')

    def test_put_object_passes_body_through(self):
        mock_s3 = MagicMock()
        helper = orchestrator.S3Helper(
            region="us-east-1", dry_run=False, s3_client=mock_s3,
        )
        helper.put_object(bucket="b", key="k.json", body=b'{"a":1}')
        mock_s3.put_object.assert_called_once_with(Bucket="b", Key="k.json", Body=b'{"a":1}')


# ── Fingerprint parsing ──────────────────────────────────────────────


class TestParseFingerprint(unittest.TestCase):
    def test_parses_keygen_fingerprint_line(self):
        # `trains keygen` prints two lines; we parse `fingerprint: <hex>`
        # (SPKI fingerprint), NOT sha256sum of the identity file
        # (the old test asserted sha256sum — wrong, see 2026-05-23 bench).
        stdout = (
            "identity:    /tmp/identity-0.json\n"
            "fingerprint: abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789\n"
        )
        fp = orchestrator._parse_fingerprint(stdout, node_id=0, dry_run=False)
        self.assertEqual(fp, "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")

    def test_dry_run_fallback_stable_per_node(self):
        # Empty stdout — dry-run should fabricate a stable fingerprint
        fp_0 = orchestrator._parse_fingerprint("", node_id=0, dry_run=True)
        fp_1 = orchestrator._parse_fingerprint("", node_id=1, dry_run=True)
        self.assertEqual(len(fp_0), 64)
        self.assertEqual(len(fp_1), 64)
        self.assertNotEqual(fp_0, fp_1)
        # Stable: re-parsing same node_id gives the same value
        self.assertEqual(fp_0, orchestrator._parse_fingerprint("garbage", node_id=0, dry_run=True))

    def test_live_mode_raises_on_unparseable(self):
        with self.assertRaisesRegex(RuntimeError, "could not parse"):
            orchestrator._parse_fingerprint("not a fingerprint", node_id=0, dry_run=False)


# ── generate_identities ─────────────────────────────────────────────


class TestGenerateIdentitiesDryRun(unittest.TestCase):
    def test_dry_run_emits_one_command_per_node(self):
        ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
        s3 = orchestrator.S3Helper(region="us-east-1", dry_run=True)
        fingerprints = orchestrator.generate_identities(
            coordinator_instance_id="i-coord",
            ring_size=3,
            ssm=ssm,
            s3=s3,
            results_bucket="bench-bucket",
        )
        self.assertEqual(set(fingerprints.keys()), {0, 1, 2})
        send_calls = [e for e in ssm.dry_run_log if e["call"] == "send_command"]
        self.assertEqual(len(send_calls), 3)
        # Every command targets the coordinator
        for call in send_calls:
            self.assertEqual(call["kwargs"]["InstanceIds"], ["i-coord"])
            # The command body includes the keygen + s3 cp.
            # sha256sum was REMOVED 2026-05-23 — was producing the
            # wrong hash (file content, not SPKI). We now parse the
            # `fingerprint: <hex>` line trains-cli prints itself.
            cmd_text = "\n".join(call["kwargs"]["Parameters"]["commands"])
            self.assertIn("trains keygen", cmd_text)
            self.assertIn("aws s3 cp", cmd_text)
            self.assertNotIn("sha256sum", cmd_text)


# ── start_ring_nodes ────────────────────────────────────────────────


class TestStartRingNodes(unittest.TestCase):
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

    def test_one_command_per_peer(self):
        peers = self._peers(3)
        fingerprints = {0: "aa" * 32, 1: "bb" * 32, 2: "cc" * 32}
        ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
        s3 = orchestrator.S3Helper(region="us-east-1", dry_run=True)
        cmd_ids = orchestrator.start_ring_nodes(
            peers=peers, config=self._config(),
            fingerprints=fingerprints, ssm=ssm, s3=s3,
        )
        self.assertEqual(set(cmd_ids.keys()), {0, 1, 2})
        self.assertEqual(len(ssm.dry_run_log), 3)
        # Each call targets the corresponding peer instance
        for call in ssm.dry_run_log:
            instance = call["kwargs"]["InstanceIds"][0]
            self.assertTrue(instance.startswith("i-"))

    def test_peer_fingerprints_excludes_self(self):
        peers = self._peers(3)
        fingerprints = {0: "aa" * 32, 1: "bb" * 32, 2: "cc" * 32}
        ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
        s3 = orchestrator.S3Helper(region="us-east-1", dry_run=True)
        orchestrator.start_ring_nodes(
            peers=peers, config=self._config(),
            fingerprints=fingerprints, ssm=ssm, s3=s3,
        )
        # Walk each call, find the PEER_FINGERPRINTS export, parse the value.
        # Node 0's PEER_FINGERPRINTS must include 1's + 2's fingerprints,
        # not its own.
        for call_idx, call in enumerate(ssm.dry_run_log):
            commands = call["kwargs"]["Parameters"]["commands"]
            peer_fp_line = next(c for c in commands if c.startswith("export PEER_FINGERPRINTS="))
            # Extract the quoted value
            value = peer_fp_line.split("=", 1)[1].strip("'")
            own_fp = fingerprints[call_idx]
            self.assertNotIn(own_fp, value, f"node-{call_idx} should not pin its own fp")
            for other_id, other_fp in fingerprints.items():
                if other_id != call_idx:
                    self.assertIn(other_fp, value,
                                  f"node-{call_idx} must pin node-{other_id}'s fp")

    def test_issue_initial_set_on_first_NUM_TRAINS_nodes(self):
        """ISSUE_INITIAL must be true on first NUM_TRAINS=2 nodes,
        not just node 0. trains-core's AllPriorDelivered invariant
        blocks delivery if any issuer's clock stays at 0. Pinned
        2026-05-23 after a multi-hour debugging session where only
        node 0 issued and 0/0/0 deliveries resulted."""
        peers = self._peers(3)
        fingerprints = {0: "aa" * 32, 1: "bb" * 32, 2: "cc" * 32}
        ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
        s3 = orchestrator.S3Helper(region="us-east-1", dry_run=True)
        orchestrator.start_ring_nodes(
            peers=peers, config=self._config(),
            fingerprints=fingerprints, ssm=ssm, s3=s3,
        )
        # Find each peer's start command and check ISSUE_INITIAL value
        for i, call in enumerate(ssm.dry_run_log):
            commands = call["kwargs"]["Parameters"]["commands"]
            issue_line = next(c for c in commands if c.startswith("export ISSUE_INITIAL="))
            # NUM_TRAINS=2 → nodes 0 AND 1 issue.
            expected = "'true'" if i < 2 else "'false'"
            self.assertIn(expected, issue_line, f"node-{i}: {issue_line}")


# ── wait_for_ring_formation ─────────────────────────────────────────


class TestWaitForRingFormation(unittest.TestCase):
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

    def test_all_success_returns_quietly(self):
        peers = self._peers(3)
        ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
        start_cmds = {0: "c0", 1: "c1", 2: "c2"}
        # dry-run returns Success automatically
        orchestrator.wait_for_ring_formation(
            peers=peers, start_command_ids=start_cmds, ssm=ssm,
        )

    def test_failed_node_raises_with_stderr(self):
        peers = self._peers(3)
        mock_ssm = MagicMock()
        mock_ssm.get_command_invocation.side_effect = [
            {"Status": "Success", "ResponseCode": 0,
             "StandardOutputContent": "", "StandardErrorContent": ""},
            {"Status": "Failed", "ResponseCode": 4,
             "StandardOutputContent": "",
             "StandardErrorContent": "FATAL: trains-cli exited within 3s"},
            # Third never called because we raise on the second
        ]
        ssm = orchestrator.SSMOrchestrator(
            region="us-east-1", dry_run=False, ssm_client=mock_ssm,
        )
        # Stub the time helpers so wait_for_command exits quickly
        ssm.wait_for_command = lambda command_id, instance_id, max_wait_s=60: (  # type: ignore
            mock_ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
        )
        start_cmds = {0: "c0", 1: "c1", 2: "c2"}
        with self.assertRaisesRegex(RuntimeError, "node-1.*failed to start"):
            orchestrator.wait_for_ring_formation(
                peers=peers, start_command_ids=start_cmds, ssm=ssm,
            )


# ── trigger_issuer_workload + collect_results ───────────────────────


class TestTriggerIssuerWorkloadDryRun(unittest.TestCase):
    def test_issuer_workload_is_now_a_noop(self):
        """As of 2026-05-23 v2, broadcasts happen INLINE in
        start_ring_nodes (Python producer piped to trains-cli stdin
        in one bash subshell). trigger_issuer_workload is a stub that
        only exists to preserve orchestrator.run()'s call signature."""
        issuer = coordinator.RingPeer(
            node_id=0, instance_id="i-issuer", private_ip="10.50.1.10", az="us-east-1a",
        )
        config = coordinator.RunConfig(
            duration_seconds=30.0, message_count=1000, payload_size=1024,
            ring_size=3, results_bucket="bench-bucket",
            run_id="20260522T200000Z", region="us-east-1",
            az_spread="single", warmup_count=0,
        )
        ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
        orchestrator.trigger_issuer_workload(issuer=issuer, config=config, ssm=ssm)
        # ONE harmless `echo` command, signalling the no-op nature.
        self.assertEqual(len(ssm.dry_run_log), 1)
        call = ssm.dry_run_log[0]
        cmd_text = "\n".join(call["kwargs"]["Parameters"]["commands"])
        self.assertIn("broadcasts-are-inline", cmd_text)


class TestCollectResultsDryRun(unittest.TestCase):
    def _peers(self, n: int) -> list[coordinator.RingPeer]:
        return [
            coordinator.RingPeer(
                node_id=i, instance_id=f"i-{i:03d}",
                private_ip=f"10.50.1.{10 + i}", az="us-east-1a",
            )
            for i in range(n)
        ]

    def test_one_command_per_peer_with_kill_parse_sockperf_upload(self):
        """collect_results: kill trains-cli, parse stdout for
        DELIVER lines, run sockperf ping-pong to successor, upload
        everything to S3. (iperf3 was the wrong tool; sockperf gives
        per-message percentile latency.)"""
        peers = self._peers(3)
        config = coordinator.RunConfig(
            duration_seconds=30.0, message_count=1000, payload_size=1024,
            ring_size=3, results_bucket="bench-bucket",
            run_id="20260522T200000Z", region="us-east-1",
            az_spread="single", warmup_count=0,
        )
        ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
        cmd_ids = orchestrator.collect_results(peers=peers, config=config, ssm=ssm)
        self.assertEqual(set(cmd_ids.keys()), {0, 1, 2})
        self.assertEqual(len(ssm.dry_run_log), 3)
        for i, call in enumerate(ssm.dry_run_log):
            self.assertEqual(call["kwargs"]["InstanceIds"], [f"i-{i:03d}"])
            cmd_text = "\n".join(call["kwargs"]["Parameters"]["commands"])
            self.assertIn("kill -INT", cmd_text)
            self.assertIn("sockperf pp", cmd_text)
            self.assertNotIn("iperf3 -c", cmd_text)  # explicitly removed
            self.assertIn("aws s3 cp", cmd_text)
            next_node = (i + 1) % 3
            self.assertIn(f"to-{next_node}", cmd_text)


# ── aggregate_from_s3 ───────────────────────────────────────────────


class TestAggregateFromS3(unittest.TestCase):
    def _config(self) -> coordinator.RunConfig:
        return coordinator.RunConfig(
            duration_seconds=30.0, message_count=3, payload_size=64,
            ring_size=3, results_bucket="bench-bucket",
            run_id="20260522T200000Z", region="us-east-1", az_spread="single",
        )

    def _peers(self) -> list[coordinator.RingPeer]:
        return [
            coordinator.RingPeer(
                node_id=i, instance_id=f"i-{i:03d}",
                private_ip=f"10.50.1.{10 + i}", az="us-east-1a",
            )
            for i in range(3)
        ]

    def test_dry_run_yields_zero_messages_failure(self):
        # In dry-run, every S3 fetch returns synthetic empty JSON, so
        # the aggregator sees 0 messages sent + 0 deliveries → failure
        # with the "issuer sent 0 messages" reason.
        s3 = orchestrator.S3Helper(region="us-east-1", dry_run=True)
        report = orchestrator.aggregate_from_s3(
            peers=self._peers(), config=self._config(), s3=s3,
            started_at_ns=1_000_000_000, ended_at_ns=2_000_000_000,
        )
        self.assertFalse(report.success)
        self.assertIn("0 messages", report.failure_reason)
        self.assertEqual(report.messages_sent, 0)

    def test_live_mode_joins_real_data(self):
        """With the 2026-05-23-v2 design, issuer_send_log is derived
        from node-0's deliveries (no separate issuer-send-log.json).
        sockperf replaces iperf3."""
        mock_s3 = MagicMock()
        import io as io_module
        SOCKPERF_TEXT = (
            "sockperf: starting\n"
            "sockperf: Total 100 observations; each percentile contains 100 observations\n"
            "sockperf: ---> percentile 99.000 =   60.123\n"
            "sockperf: ---> percentile 50.000 =   25.456\n"
            "sockperf: avg-lat= 27.5 (std-dev=4.2)\n"
        )
        def s3_response(*, Bucket, Key):
            if "deliveries.json" in Key:
                node_id = int(Key.split("node-")[1].split("-")[0])
                payload = {
                    "deliveries": [
                        # Each delivery embeds send_ns (extracted from
                        # the payload bytes by parse_results).
                        {"seq": 0, "send_ns": 1_000_000, "recv_ns": 1_500_000, "node_id": node_id},
                        {"seq": 1, "send_ns": 2_000_000, "recv_ns": 2_500_000, "node_id": node_id},
                        {"seq": 2, "send_ns": 3_000_000, "recv_ns": 3_500_000, "node_id": node_id},
                    ]
                }
                return {"Body": io_module.BytesIO(json.dumps(payload).encode())}
            elif "sockperf" in Key:
                return {"Body": io_module.BytesIO(SOCKPERF_TEXT.encode())}
            else:
                return {"Body": io_module.BytesIO(b"{}")}
        mock_s3.get_object.side_effect = s3_response

        s3 = orchestrator.S3Helper(
            region="us-east-1", dry_run=False, s3_client=mock_s3,
        )
        report = orchestrator.aggregate_from_s3(
            peers=self._peers(), config=self._config(), s3=s3,
            started_at_ns=1_000_000_000, ended_at_ns=2_000_000_000,
        )
        self.assertTrue(report.success)
        self.assertEqual(report.messages_sent, 3)
        self.assertEqual(report.deliveries_per_node, {0: 3, 1: 3, 2: 3})
        # sockperf parsed for each ring pair
        self.assertIsNotNone(report.sockperf_pairwise_us)
        self.assertEqual(
            set(report.sockperf_pairwise_us.keys()),
            {"0-to-1", "1-to-2", "2-to-0"},
        )
        for pair_data in report.sockperf_pairwise_us.values():
            self.assertAlmostEqual(pair_data["p50_us"], 25.456, places=2)
            self.assertAlmostEqual(pair_data["p99_us"], 60.123, places=2)


# ── End-to-end dry-run smoke ────────────────────────────────────────


class TestRunDryRun(unittest.TestCase):
    """Run the top-level `orchestrator.run()` end-to-end in dry-run.

    Asserts that every step issues the expected SSM call shape and
    the function completes without crashing on synthetic peers. Also
    proves the report is written to disk.
    """

    def test_full_dry_run_pipeline(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
            s3 = orchestrator.S3Helper(region="us-east-1", dry_run=True)
            report = orchestrator.run(
                region="us-east-1",
                ring_size=3,
                results_bucket="bench-bucket",
                coordinator_instance_id=None,  # let dry-run fabricate
                duration_seconds=30.0,
                message_count=1000,
                payload_size=1024,
                az_spread="single",
                dry_run=True,
                output_dir=Path(tmp),
                ssm_orchestrator=ssm,
                s3_helper=s3,
            )
            # The full pipeline ran; report exists; success is False
            # because dry-run synthesises empty deliveries.
            self.assertFalse(report.success)
            self.assertIn("0 messages", report.failure_reason)
            # Local files written
            files = list(Path(tmp).glob("run-*.md"))
            self.assertEqual(len(files), 1)
            # Report content sanity
            md = files[0].read_text()
            self.assertIn("TRAINS-bench run report", md)
            self.assertIn("Ring size: 3", md)
            self.assertIn("AZ spread: single", md)

    def test_dry_run_issues_expected_ssm_calls(self):
        """Phase-by-phase count of SSM calls in a 3-node dry run:
        - 3 identity-gens (one per node) → 3
        - 3 start-ring-nodes              → 3
        - (wait_for_ring_formation makes wait_for_command calls but not send_command)
        - 1 issuer trigger                → 1
        - 3 collect_results               → 3
        Total send_command calls = 10
        """
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
            s3 = orchestrator.S3Helper(region="us-east-1", dry_run=True)
            orchestrator.run(
                region="us-east-1",
                ring_size=3,
                results_bucket="bench-bucket",
                coordinator_instance_id=None,
                duration_seconds=30.0,
                message_count=1000,
                payload_size=1024,
                az_spread="single",
                dry_run=True,
                output_dir=Path(tmp),
                ssm_orchestrator=ssm,
                s3_helper=s3,
            )
            send_calls = [e for e in ssm.dry_run_log if e["call"] == "send_command"]
            # Expected SSM calls per the orchestrator.run() pipeline:
            #   3   identity generation (one per ring node, on coordinator)
            #   3   ring-node start (one per node)
            #   3   pre-bench NIC snapshot (one per node) — added 2026-05-23
            #   1   no-op issuer workload (legacy stub)
            #   3   post-bench NIC snapshot (one per node) — added 2026-05-23
            #   3   collect-results (one per node)
            # Total: 16
            self.assertEqual(
                len(send_calls), 3 + 3 + 3 + 1 + 3 + 3,
                "expected 16 send_command calls (added 6 for pre+post NIC snapshots)",
            )

    def test_dry_run_uploads_report_to_s3(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            ssm = orchestrator.SSMOrchestrator(region="us-east-1", dry_run=True)
            s3 = orchestrator.S3Helper(region="us-east-1", dry_run=True)
            orchestrator.run(
                region="us-east-1",
                ring_size=3,
                results_bucket="bench-bucket",
                coordinator_instance_id=None,
                duration_seconds=30.0,
                message_count=1000,
                payload_size=1024,
                az_spread="single",
                dry_run=True,
                output_dir=Path(tmp),
                ssm_orchestrator=ssm,
                s3_helper=s3,
            )
            put_calls = [e for e in s3.dry_run_log if e["call"] == "put_object"]
            # Two report uploads (md + json) at the end of run()
            md_uploads = [c for c in put_calls if c["key"].endswith(".md")]
            json_uploads = [c for c in put_calls if c["key"].endswith(".json")]
            self.assertEqual(len(md_uploads), 1)
            self.assertEqual(len(json_uploads), 1)


# ── shell-quote stays in sync with coordinator's ────────────────────


class TestShellQuoteSync(unittest.TestCase):
    """Pin that orchestrator._shell_quote and coordinator._shell_quote
    behave identically. Keeps drift from creeping in via either side."""

    def test_simple_and_quoted_values_match(self):
        for value in [
            "10.50.1.10:7777",
            "0:bench:00000042:1779472993000000000:abc",
            "a'b",
            "",
            "with spaces",
        ]:
            self.assertEqual(
                orchestrator._shell_quote(value),
                coordinator._shell_quote(value),
                f"drift on input {value!r}",
            )


if __name__ == "__main__":
    unittest.main()
