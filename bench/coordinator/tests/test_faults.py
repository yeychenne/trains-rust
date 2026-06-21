"""Unit tests for bench/coordinator/faults.py.

No AWS, no wall-clock. A fake SSM records send_command calls; a fake
EC2 records stop/start; sleep_fn records the timed-window schedule.
"""

from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import faults  # noqa: E402


def _peer(node_id: int):
    return types.SimpleNamespace(
        node_id=node_id,
        instance_id=f"i-{node_id:04d}",
        private_ip=f"10.50.1.{10 + node_id}",
    )


def _peers(n: int):
    return [_peer(i) for i in range(n)]


class _FakeSSM:
    def __init__(self):
        self.calls: list[dict] = []

    def send_command(self, *, instance_ids, parameters, comment, timeout_seconds=None):
        self.calls.append({
            "instance_ids": instance_ids,
            "script": parameters["commands"][0],
            "comment": comment,
        })
        return f"cmd-{len(self.calls)}"


class _FakeEC2:
    def __init__(self):
        self.stopped: list[str] = []
        self.started: list[str] = []

    def stop_instances(self, *, InstanceIds):
        self.stopped.extend(InstanceIds)

    def start_instances(self, *, InstanceIds):
        self.started.extend(InstanceIds)


class TestFaultSpec(unittest.TestCase):
    def test_from_dict_none(self):
        self.assertIsNone(faults.FaultSpec.from_dict(None))
        self.assertIsNone(faults.FaultSpec.from_dict({}))

    def test_from_dict_full(self):
        spec = faults.FaultSpec.from_dict({
            "type": "netem-loss", "target_node": 4,
            "inject_at_s": 5, "duration_s": 20, "loss_pct": 5,
        })
        self.assertEqual(spec.type, "netem-loss")
        self.assertEqual(spec.target_node, 4)
        self.assertEqual(spec.loss_pct, 5)
        self.assertFalse(spec.is_permanent)

    def test_is_permanent(self):
        spec = faults.FaultSpec.from_dict({
            "type": "fis-kill", "target_node": 0,
            "inject_at_s": 8, "duration_s": 0,
        })
        self.assertTrue(spec.is_permanent)


class TestCommandBuilders(unittest.TestCase):
    def test_netem_loss_apply(self):
        spec = faults.FaultSpec("netem-loss", 0, 0, 5, loss_pct=5)
        sh = faults._netem_apply_sh(spec, None)
        self.assertIn("tc qdisc replace", sh)
        self.assertIn("netem loss 5%", sh)
        self.assertIn("ip -4 route show default", sh)  # iface discovery

    def test_netem_latency_apply(self):
        spec = faults.FaultSpec("netem-latency", 0, 0, 5, latency_ms=100)
        sh = faults._netem_apply_sh(spec, None)
        self.assertIn("netem delay 100ms", sh)

    def test_partition_apply_uses_iptables_to_peer(self):
        spec = faults.FaultSpec("netem-partition", 0, 0, 5, partition_peer=1)
        sh = faults._netem_apply_sh(spec, "10.50.1.11")
        self.assertIn("iptables -A OUTPUT -d 10.50.1.11 -j DROP", sh)

    def test_netem_clear(self):
        spec = faults.FaultSpec("netem-loss", 0, 0, 5, loss_pct=5)
        self.assertIn("tc qdisc del", faults._netem_clear_sh(spec, None))

    def test_fis_kill_sh(self):
        sh = faults._fis_kill_sh()
        self.assertIn("kill -9", sh)
        self.assertIn("trains node --id", sh)

    def test_fis_kill_redis_sh_targets_the_proxy(self):
        sh = faults._fis_kill_sh(faults._REDIS_PIDFILE, faults._REDIS_PGREP)
        self.assertIn("kill -9", sh)
        self.assertIn("trains-valkey --id", sh)
        self.assertIn("/tmp/trains-valkey.pid", sh)
        self.assertNotIn("trains node --id", sh)

    def test_fis_kill_redis_injects_kill_on_target(self):
        ssm = _FakeSSM()
        spec = faults.FaultSpec("fis-kill-redis", 2, 8, 0)
        faults.inject_fault(spec, peers=_peers(9), ssm=ssm, log_fn=lambda *_: None)
        self.assertIn("trains-valkey --id", ssm.calls[0]["script"])
        self.assertEqual(ssm.calls[0]["instance_ids"], ["i-0002"])

    def test_fis_kill_redis_is_permanent(self):
        # clear_fault must accept fis-kill-redis as a permanent (no-op) fault.
        faults.clear_fault(
            faults.FaultSpec("fis-kill-redis", 2, 8, 0),
            peers=_peers(9),
            ssm=_FakeSSM(),
            log_fn=lambda *_: None,
        )


class TestInjectClear(unittest.TestCase):
    def test_netem_loss_injects_on_target(self):
        ssm = _FakeSSM()
        spec = faults.FaultSpec("netem-loss", 2, 5, 20, loss_pct=5)
        faults.inject_fault(spec, peers=_peers(9), ssm=ssm, log_fn=lambda *_: None)
        self.assertEqual(len(ssm.calls), 1)
        self.assertEqual(ssm.calls[0]["instance_ids"], ["i-0002"])
        self.assertIn("netem loss 5%", ssm.calls[0]["script"])

    def test_partition_injects_both_directions(self):
        ssm = _FakeSSM()
        spec = faults.FaultSpec("netem-partition", 4, 5, 20, partition_peer=5)
        faults.inject_fault(spec, peers=_peers(9), ssm=ssm, log_fn=lambda *_: None)
        self.assertEqual(len(ssm.calls), 2)
        targets = {c["instance_ids"][0] for c in ssm.calls}
        self.assertEqual(targets, {"i-0004", "i-0005"})

    def test_fis_kill_injects_kill(self):
        ssm = _FakeSSM()
        spec = faults.FaultSpec("fis-kill", 3, 8, 0)
        faults.inject_fault(spec, peers=_peers(9), ssm=ssm, log_fn=lambda *_: None)
        self.assertIn("kill -9", ssm.calls[0]["script"])
        self.assertEqual(ssm.calls[0]["instance_ids"], ["i-0003"])

    def test_fis_stop_start_uses_ec2(self):
        ssm = _FakeSSM()
        ec2 = _FakeEC2()
        spec = faults.FaultSpec("fis-stop-start", 1, 8, 30)
        faults.inject_fault(spec, peers=_peers(9), ssm=ssm, ec2=ec2, log_fn=lambda *_: None)
        self.assertEqual(ec2.stopped, ["i-0001"])
        faults.clear_fault(spec, peers=_peers(9), ssm=ssm, ec2=ec2, log_fn=lambda *_: None)
        self.assertEqual(ec2.started, ["i-0001"])


class TestRunFaultWindow(unittest.TestCase):
    def _record_sleep(self):
        slept: list[float] = []
        return slept, lambda s: slept.append(s)

    def test_transient_timeline_and_clear(self):
        ssm = _FakeSSM()
        slept, sleep_fn = self._record_sleep()
        spec = faults.FaultSpec("netem-loss", 4, 5, 10, loss_pct=5)
        events = faults.run_fault_window(
            spec, total_duration_s=30, peers=_peers(9), ssm=ssm,
            sleep_fn=sleep_fn, log_fn=lambda *_: None,
        )
        # pre=5, hold=10, remainder=15
        self.assertEqual(slept, [5, 10, 15])
        self.assertEqual(events["injected_at_s"], 5)
        self.assertEqual(events["cleared_at_s"], 15)
        self.assertEqual(events["window_end_s"], 30)
        # one inject + one clear
        self.assertEqual(len(ssm.calls), 2)

    def test_permanent_no_clear(self):
        ssm = _FakeSSM()
        slept, sleep_fn = self._record_sleep()
        spec = faults.FaultSpec("fis-kill", 4, 8, 0)
        events = faults.run_fault_window(
            spec, total_duration_s=30, peers=_peers(9), ssm=ssm,
            sleep_fn=sleep_fn, log_fn=lambda *_: None,
        )
        # pre=8, hold=22 (rest), remainder=0 (not appended)
        self.assertEqual(slept, [8, 22])
        self.assertIsNone(events["cleared_at_s"])
        self.assertEqual(len(ssm.calls), 1)  # inject only

    def test_inject_at_beyond_window_clamps(self):
        ssm = _FakeSSM()
        slept, sleep_fn = self._record_sleep()
        spec = faults.FaultSpec("netem-loss", 4, 100, 10, loss_pct=5)
        faults.run_fault_window(
            spec, total_duration_s=30, peers=_peers(9), ssm=ssm,
            sleep_fn=sleep_fn, log_fn=lambda *_: None,
        )
        # pre clamps to 30; hold/remainder are 0
        self.assertEqual(slept, [30])


if __name__ == "__main__":
    unittest.main()
