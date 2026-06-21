"""faults.py — Phase G mid-bench fault injection.

Drives the OPTIONAL `[fault]` config section (see _parse_config.py +
bench/RESEARCH-ROADMAP.md § Phase G). The orchestrator calls
`run_fault_window()` in place of the plain in-bench settle sleep when a
fault is configured; with no fault, the orchestrator just sleeps as
before and this module is never touched.

Two fault families:
  - **netem-* / partition** — applied on the target ring node's NIC via
    SSM RunCommand. Loss/latency use `tc qdisc ... netem`; a partition
    uses bidirectional `iptables ... -j DROP` between two nodes (cleaner
    than netem for per-peer drops).
  - **fis-* ** — instance-level. `fis-kill` SIGKILLs trains-cli (SSM);
    `fis-stop-start` stops then starts the EC2 instance (EC2 control
    plane).

Design for testability:
  - Remote shell is built by pure `_*_sh()` helpers (unit-tested on the
    string).
  - `inject_fault` / `clear_fault` route every AWS call through the
    injected SSMOrchestrator (dry-run aware) and an optional ec2 client.
  - `run_fault_window` takes an injected `sleep_fn` so timing is tested
    without real wall-clock.
"""

from __future__ import annotations

import dataclasses
import time
from typing import Any, Callable

# trains-cli runs as `trains node --id N ...` with its PID in this file
# (see orchestrator.collect_results). Both are used so a kill works
# whether or not the pidfile is present.
_TRAINS_PIDFILE = "/tmp/trains-cli.pid"
_TRAINS_PGREP = "trains node --id"

# The TRAINS-replicated Redis proxy (PR-RD-4) runs as `trains-valkey --id N ...`
# with its PID here; `fis-kill-redis` targets it instead of trains-cli.
_REDIS_PIDFILE = "/tmp/trains-valkey.pid"
_REDIS_PGREP = "trains-valkey --id"

# Discover the primary NIC the same way orchestrator's NIC snapshot does.
_IFACE_SH = (
    'IFACE=$(ip -4 route show default 2>/dev/null | awk \'{print $5; exit}\'); '
    '[ -z "$IFACE" ] && IFACE=$(ls /sys/class/net/ 2>/dev/null | grep -v "^lo$" | head -1)'
)

# AL2023 doesn't ship `tc` (iproute-tc) or `iptables` by default. Best-effort
# install so a netem/partition fault doesn't silently no-op. `|| true` so a
# no-NAT node degrades to a visible "tc: command not found" rather than
# aborting the SSM command early.
_ENSURE_TC = "command -v tc >/dev/null 2>&1 || sudo dnf install -y -q iproute-tc 2>/dev/null || true; "
_ENSURE_IPTABLES = "command -v iptables >/dev/null 2>&1 || sudo dnf install -y -q iptables-nft 2>/dev/null || true; "


@dataclasses.dataclass
class FaultSpec:
    type: str
    target_node: int
    inject_at_s: int
    duration_s: int
    loss_pct: int | None = None
    latency_ms: int | None = None
    partition_peer: int | None = None

    @classmethod
    def from_dict(cls, fault: dict | None) -> "FaultSpec | None":
        if not fault:
            return None
        return cls(
            type=fault["type"],
            target_node=int(fault["target_node"]),
            inject_at_s=int(fault["inject_at_s"]),
            duration_s=int(fault["duration_s"]),
            loss_pct=_opt_int(fault.get("loss_pct")),
            latency_ms=_opt_int(fault.get("latency_ms")),
            partition_peer=_opt_int(fault.get("partition_peer")),
        )

    @property
    def is_permanent(self) -> bool:
        """duration_s == 0 means the fault holds for the rest of the run."""
        return self.duration_s == 0


def _opt_int(v: Any) -> int | None:
    return int(v) if v is not None else None


# ── Pure remote-command builders ───────────────────────────────────

def _netem_apply_sh(spec: FaultSpec, peer_ip: str | None) -> str:
    """Shell to APPLY a netem/partition fault on the target node."""
    if spec.type == "netem-loss":
        return (
            f"{_ENSURE_TC}{_IFACE_SH}; "
            f"sudo tc qdisc replace dev \"$IFACE\" root netem loss {spec.loss_pct}% "
            f"&& echo applied-netem-loss-{spec.loss_pct}pct-on-$IFACE"
        )
    if spec.type == "netem-latency":
        return (
            f"{_ENSURE_TC}{_IFACE_SH}; "
            f"sudo tc qdisc replace dev \"$IFACE\" root netem delay {spec.latency_ms}ms "
            f"&& echo applied-netem-delay-{spec.latency_ms}ms-on-$IFACE"
        )
    if spec.type == "netem-partition":
        # Drop egress toward the peer IP (the matching peer-side rule is
        # issued separately for a symmetric cut).
        return (
            f"{_ENSURE_IPTABLES}"
            f"sudo iptables -A OUTPUT -d {peer_ip} -j DROP "
            f"&& echo applied-partition-to-{peer_ip}"
        )
    raise ValueError(f"_netem_apply_sh: not a netem fault: {spec.type}")


def _netem_clear_sh(spec: FaultSpec, peer_ip: str | None) -> str:
    """Shell to CLEAR a netem/partition fault on the target node."""
    if spec.type in ("netem-loss", "netem-latency"):
        return (
            f"{_IFACE_SH}; "
            f"sudo tc qdisc del dev \"$IFACE\" root 2>/dev/null; echo cleared-netem-on-$IFACE"
        )
    if spec.type == "netem-partition":
        return (
            f"sudo iptables -D OUTPUT -d {peer_ip} -j DROP 2>/dev/null; "
            f"echo cleared-partition-to-{peer_ip}"
        )
    raise ValueError(f"_netem_clear_sh: not a netem fault: {spec.type}")


def _fis_kill_sh(pidfile: str = _TRAINS_PIDFILE, pgrep: str = _TRAINS_PGREP) -> str:
    """Shell to SIGKILL the target process on the node (trains-cli by default,
    or the trains-valkey proxy when given the redis pidfile/pgrep)."""
    return (
        f"sudo kill -9 $(cat {pidfile}) 2>/dev/null; "
        f"sudo pkill -9 -f '{pgrep}' 2>/dev/null; "
        f"echo killed-process"
    )


# ── Inject / clear (route AWS calls through ssm/ec2) ───────────────

def inject_fault(
    spec: FaultSpec,
    *,
    peers: list,
    ssm: Any,
    ec2: Any = None,
    log_fn: Callable[[str], None] = print,
) -> None:
    target = _peer_by_node(peers, spec.target_node)

    if spec.type in ("netem-loss", "netem-latency"):
        log_fn(f"[fault] injecting {spec.type} on node-{spec.target_node}")
        ssm.send_command(
            instance_ids=[target.instance_id],
            parameters={"commands": [_netem_apply_sh(spec, None)]},
            comment=f"trains-bench: fault inject {spec.type} node-{spec.target_node}",
            timeout_seconds=30,
        )
    elif spec.type == "netem-partition":
        peer = _peer_by_node(peers, spec.partition_peer)
        log_fn(
            f"[fault] partitioning node-{spec.target_node} <-> "
            f"node-{spec.partition_peer} (bidirectional)"
        )
        # Symmetric cut: drop in both directions.
        ssm.send_command(
            instance_ids=[target.instance_id],
            parameters={"commands": [_netem_apply_sh(spec, peer.private_ip)]},
            comment=f"trains-bench: fault partition node-{spec.target_node}->peer",
            timeout_seconds=30,
        )
        ssm.send_command(
            instance_ids=[peer.instance_id],
            parameters={"commands": [
                f"{_ENSURE_IPTABLES}"
                f"sudo iptables -A OUTPUT -d {target.private_ip} -j DROP "
                f"&& echo applied-partition-to-{target.private_ip}"
            ]},
            comment=f"trains-bench: fault partition node-{spec.partition_peer}->target",
            timeout_seconds=30,
        )
    elif spec.type == "fis-kill":
        log_fn(f"[fault] SIGKILL trains-cli on node-{spec.target_node}")
        ssm.send_command(
            instance_ids=[target.instance_id],
            parameters={"commands": [_fis_kill_sh()]},
            comment=f"trains-bench: fault fis-kill node-{spec.target_node}",
            timeout_seconds=30,
        )
    elif spec.type == "fis-kill-redis":
        log_fn(f"[fault] SIGKILL trains-valkey proxy on node-{spec.target_node}")
        ssm.send_command(
            instance_ids=[target.instance_id],
            parameters={"commands": [_fis_kill_sh(_REDIS_PIDFILE, _REDIS_PGREP)]},
            comment=f"trains-bench: fault fis-kill-redis node-{spec.target_node}",
            timeout_seconds=30,
        )
    elif spec.type == "fis-stop-start":
        log_fn(f"[fault] EC2 stop node-{spec.target_node} ({target.instance_id})")
        if ec2 is not None:
            ec2.stop_instances(InstanceIds=[target.instance_id])
        else:
            log_fn("[fault] (dry-run / no ec2 client — stop skipped)")
    else:
        raise ValueError(f"inject_fault: unknown fault type {spec.type!r}")


def clear_fault(
    spec: FaultSpec,
    *,
    peers: list,
    ssm: Any,
    ec2: Any = None,
    log_fn: Callable[[str], None] = print,
) -> None:
    target = _peer_by_node(peers, spec.target_node)

    if spec.type in ("netem-loss", "netem-latency"):
        log_fn(f"[fault] clearing {spec.type} on node-{spec.target_node}")
        ssm.send_command(
            instance_ids=[target.instance_id],
            parameters={"commands": [_netem_clear_sh(spec, None)]},
            comment=f"trains-bench: fault clear node-{spec.target_node}",
            timeout_seconds=30,
        )
    elif spec.type == "netem-partition":
        peer = _peer_by_node(peers, spec.partition_peer)
        log_fn(f"[fault] healing partition node-{spec.target_node} <-> node-{spec.partition_peer}")
        ssm.send_command(
            instance_ids=[target.instance_id],
            parameters={"commands": [_netem_clear_sh(spec, peer.private_ip)]},
            comment=f"trains-bench: fault heal node-{spec.target_node}",
            timeout_seconds=30,
        )
        ssm.send_command(
            instance_ids=[peer.instance_id],
            parameters={"commands": [
                f"sudo iptables -D OUTPUT -d {target.private_ip} -j DROP 2>/dev/null; "
                f"echo cleared-partition-to-{target.private_ip}"
            ]},
            comment=f"trains-bench: fault heal node-{spec.partition_peer}",
            timeout_seconds=30,
        )
    elif spec.type == "fis-stop-start":
        log_fn(f"[fault] EC2 start node-{spec.target_node} ({target.instance_id})")
        if ec2 is not None:
            ec2.start_instances(InstanceIds=[target.instance_id])
        else:
            log_fn("[fault] (dry-run / no ec2 client — start skipped)")
    elif spec.type in ("fis-kill", "fis-kill-redis"):
        # Permanent by design — nothing to clear.
        log_fn(f"[fault] {spec.type} is permanent; no clear")
    else:
        raise ValueError(f"clear_fault: unknown fault type {spec.type!r}")


# ── Timed window orchestration ─────────────────────────────────────

def run_fault_window(
    spec: FaultSpec,
    *,
    total_duration_s: float,
    peers: list,
    ssm: Any,
    ec2: Any = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    log_fn: Callable[[str], None] = print,
) -> dict:
    """Replace the plain in-bench settle sleep with a timed fault.

    Timeline within the `total_duration_s` settle window:
      [0, inject_at_s)            normal traffic
      inject_at_s                 inject fault
      [inject_at_s, +hold)        fault active
      inject_at_s+hold            clear fault (unless permanent)
      [..., total_duration_s)     recovery / remainder

    Returns a small dict of wall-clock-relative event markers (seconds
    from settle start) for the run report.
    """
    events: dict = {"type": spec.type, "target_node": spec.target_node}

    pre = max(0.0, min(float(spec.inject_at_s), total_duration_s))
    if pre:
        sleep_fn(pre)
    elapsed = pre

    inject_fault(spec, peers=peers, ssm=ssm, ec2=ec2, log_fn=log_fn)
    events["injected_at_s"] = round(elapsed, 3)

    if spec.is_permanent:
        hold = max(0.0, total_duration_s - elapsed)
    else:
        hold = min(float(spec.duration_s), max(0.0, total_duration_s - elapsed))
    if hold:
        sleep_fn(hold)
    elapsed += hold

    if not spec.is_permanent:
        clear_fault(spec, peers=peers, ssm=ssm, ec2=ec2, log_fn=log_fn)
        events["cleared_at_s"] = round(elapsed, 3)
    else:
        events["cleared_at_s"] = None

    remainder = max(0.0, total_duration_s - elapsed)
    if remainder:
        sleep_fn(remainder)
    events["window_end_s"] = round(total_duration_s, 3)
    return events


def _peer_by_node(peers: list, node_id: int):
    for p in peers:
        if p.node_id == node_id:
            return p
    raise ValueError(f"no ring peer with node_id={node_id}")
