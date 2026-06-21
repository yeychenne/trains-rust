#!/usr/bin/env python3
"""coordinator.py — TRAINS-bench orchestrator.

Runs on the coordinator EC2 instance (or on the operator's host, with
AWS creds). Discovers ring peers by EC2 tag, drives the bench end to
end via SSM RunCommand, and aggregates per-node results into a single
run report.

Sub-commands:

    coordinator.py preflight
        Check identity, region, bucket, instance count. Read-only.

    coordinator.py run [--ring-size N] [--duration 30s] ...
        Execute one bench run. Writes results to S3 and a local file.

    coordinator.py report --run-id <ts>
        Re-fetch S3 results for a past run and re-emit the markdown
        report locally (useful for re-rendering after editing the
        aggregator).

    coordinator.py teardown-check
        Verify no instances tagged Project=trains-bench are running.
        Read-only — does NOT call cdk destroy (operator's job).

The bench-control protocol is documented in `bench/ARCHITECTURE.md`
section "Bench-control protocol". All AWS calls are scoped via the
coordinator's IAM instance profile (or the operator's local creds
when run from the host).
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import statistics
import sys
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

# boto3 is optional at import time so unit tests can mock the client
# without requiring the SDK to be installed in the test venv. Live
# `run` / `preflight` paths fail loudly if boto3 isn't present.
try:
    import boto3  # type: ignore[import-untyped]
    import botocore  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover — handled at runtime
    boto3 = None  # type: ignore[assignment]
    botocore = None  # type: ignore[assignment]


log = logging.getLogger("trains-bench.coordinator")

# ── Constants ───────────────────────────────────────────────────────

PROJECT_TAG = "trains-bench"
"""Value of the `Project` tag stamped onto every bench resource. Used
to discover ring peers via `ec2:DescribeInstances`."""

RING_PORT = 7777
"""Default port for the TRAINS ring TLS/QUIC transport. Each node
listens on 0.0.0.0:RING_PORT and connects to its successor at
<peer-ip>:RING_PORT."""

DEFAULT_REGION = "us-east-1"
"""Region for the first bench run. Override with --region or the
AWS_REGION env var."""

DEFAULT_RESULTS_PREFIX = "results"
"""S3 key prefix under which per-run subdirectories live:
s3://<bucket>/<prefix>/run-<ts>/{node-N-stderr.log,node-N-deliveries.json,...}"""

SSM_DOCUMENT = "AWS-RunShellScript"
"""SSM document used for all RunCommand invocations. We do NOT define
custom documents — keeps the IAM surface small."""


# ── Data classes ────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class RingPeer:
    """One ring node, as discovered via EC2 tag query."""

    node_id: int
    instance_id: str
    private_ip: str
    az: str


@dataclasses.dataclass(frozen=True)
class RunConfig:
    """Inputs to one bench run."""

    duration_seconds: float
    message_count: int
    payload_size: int
    ring_size: int  # expected ring size; aborts if discovered != this
    results_bucket: str
    run_id: str  # opaque; used in S3 keys and log dirs
    region: str
    az_spread: str  # "single" or "three" — for the run report only
    # Warm-up: discard the first N samples from latency percentiles to
    # exclude cold-flight cost (TLS handshake, TCP slow-start, initial
    # train formation). Default 0 = include everything (back-compat with
    # earlier small-N runs). For N≥10k, set warmup_count to ~10 % of N.
    warmup_count: int = 0
    # Delivery mode the ring nodes run in: "uto" (strict, default — the
    # benchmark path) or "to" (TotalOrder = crash-masking). With "to" the
    # nodes also receive the full ring topology (--peer-addr) so they can
    # reconfigure past a crashed node — required to MASK a fis-kill (PR-R4).
    delivery_mode: str = "uto"

    def s3_results_prefix(self) -> str:
        return f"{DEFAULT_RESULTS_PREFIX}/run-{self.run_id}"


@dataclasses.dataclass
class RunReport:
    """Aggregated output of one bench run. Serialised to JSON + Markdown."""

    config: RunConfig
    peers: list[RingPeer]
    issuer_node_id: int
    messages_sent: int
    deliveries_per_node: dict[int, int]
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_p99_ms: float | None
    latency_p999_ms: float | None  # 99.9th — only meaningful when N≥1k after warmup
    latency_sample_count: int  # how many deliveries fed into the percentiles
    iperf3_throughput_mbps: dict[int, float]  # adjacent-peer pairs (legacy field)
    sockperf_pairwise_us: dict[str, dict] | None  # NEW: per-pair sockperf latency
    success: bool
    failure_reason: str | None
    started_at_ns: int
    ended_at_ns: int
    # Per-node NIC counter deltas (post-bench − pre-bench). Captured via
    # SSM around steps 4-5. Format:
    #   {0: {"interface": "ens5",
    #        "rx_bytes": 12345678,  "tx_bytes": 12345678,
    #        "rx_packets": 1000,    "tx_packets": 1000,
    #        "ena_srd_tx_bytes": 0, "ena_srd_rx_bytes": 0,
    #        "duration_s": 30.0}}
    # None if the snapshots couldn't be captured (graceful degradation).
    nic_deltas_per_node: dict[int, dict] | None = None

    def to_dict(self) -> dict:
        d = dataclasses.asdict(self)
        # `config` is a frozen dataclass — keep keys flat for JSON.
        d["config"] = dataclasses.asdict(self.config)
        d["duration_wall_s"] = (self.ended_at_ns - self.started_at_ns) / 1e9
        return d

    def to_markdown(self) -> str:
        lines: list[str] = []
        lines.append(f"# TRAINS-bench run report — {self.config.run_id}")
        lines.append("")
        lines.append(f"**Status:** {'✅ success' if self.success else '❌ failed'}")
        if self.failure_reason:
            lines.append(f"**Failure reason:** {self.failure_reason}")
        lines.append("")
        lines.append("## Configuration")
        lines.append("")
        lines.append(f"- Ring size: {self.config.ring_size}")
        lines.append(f"- AZ spread: {self.config.az_spread}")
        lines.append(f"- Region: {self.config.region}")
        lines.append(f"- Duration target: {self.config.duration_seconds:.1f}s")
        lines.append(f"- Message count target: {self.config.message_count}")
        lines.append(f"- Payload size: {self.config.payload_size} bytes")
        lines.append("")
        lines.append("## Ring peers")
        lines.append("")
        lines.append("| Node | Instance | AZ | Private IP |")
        lines.append("|---|---|---|---|")
        for p in sorted(self.peers, key=lambda x: x.node_id):
            issuer = " 🚂" if p.node_id == self.issuer_node_id else ""
            lines.append(
                f"| {p.node_id}{issuer} | `{p.instance_id}` | {p.az} | `{p.private_ip}` |"
            )
        lines.append("")
        lines.append("## Results")
        lines.append("")
        lines.append(f"- Messages sent by issuer: **{self.messages_sent}**")
        lines.append(
            f"- Wall-clock duration: **{(self.ended_at_ns - self.started_at_ns) / 1e9:.2f}s**"
        )
        lines.append("")
        lines.append("### Delivery completeness per node")
        lines.append("")
        lines.append("| Node | Delivered | % of sent |")
        lines.append("|---|---|---|")
        for node_id in sorted(self.deliveries_per_node.keys()):
            n = self.deliveries_per_node[node_id]
            pct = (n / self.messages_sent * 100.0) if self.messages_sent else 0.0
            lines.append(f"| {node_id} | {n} | {pct:.1f}% |")
        lines.append("")
        lines.append("### Latency (TRAINS round-trip, steady-state)")
        lines.append("")
        if self.latency_p50_ms is not None:
            lines.append(
                f"- Sample count after warm-up: **{self.latency_sample_count}**"
                f" (warm-up discarded first {self.config.warmup_count})"
            )
            lines.append(f"- p50:   **{self.latency_p50_ms:.3f} ms**")
            lines.append(f"- p95:   **{self.latency_p95_ms:.3f} ms**")
            lines.append(f"- p99:   **{self.latency_p99_ms:.3f} ms**")
            if self.latency_p999_ms is not None:
                lines.append(f"- p99.9: **{self.latency_p999_ms:.3f} ms**")
        else:
            lines.append("- _no successful deliveries — latency unavailable_")
        lines.append("")
        lines.append("### Measured wire bandwidth (NIC counter delta)")
        lines.append("")
        if self.nic_deltas_per_node:
            lines.append(
                "Per-node bytes-on-wire during the bench, measured by "
                "diffing `/sys/class/net/<iface>/statistics/` snapshots "
                "before ring formation (post-listener-settle, no broadcast "
                "traffic yet) and after the in-bench settle (all broadcasts "
                "flushed). Validates the Phase D 2-lap model with measured "
                "ground truth."
            )
            lines.append("")
            lines.append(
                "| Node | Iface | Δ s | TX (MB) | RX (MB) | TX Mbps | "
                "RX Mbps | TX pkts | SRD TX pkts | SRD pkt share |"
            )
            lines.append("|---|---|---|---|---|---|---|---|---|---|")
            for node_id in sorted(self.nic_deltas_per_node.keys()):
                d = self.nic_deltas_per_node[node_id]
                if not d:
                    lines.append(f"| {node_id} | _missing_ | — | — | — | — | — | — | — | — |")
                    continue
                dur = d.get("duration_s") or 0.0
                tx_b = d.get("tx_bytes") or 0
                rx_b = d.get("rx_bytes") or 0
                tx_p = d.get("tx_packets") or 0
                srd_p = d.get("ena_srd_tx_pkts") or 0
                tx_mb = tx_b / 1e6
                rx_mb = rx_b / 1e6
                tx_mbps = (tx_b * 8 / (dur * 1e6)) if dur > 0 else 0.0
                rx_mbps = (rx_b * 8 / (dur * 1e6)) if dur > 0 else 0.0
                # ethtool exposes ena_srd_tx_pkts but NOT ena_srd_tx_bytes —
                # report packet share as the SRD-usage proxy. ~100% means
                # essentially all TX went via SRD.
                srd_share = (srd_p / tx_p * 100.0) if tx_p > 0 else 0.0
                lines.append(
                    f"| {node_id} | `{d.get('interface', '?')}` | "
                    f"{dur:.1f} | {tx_mb:.1f} | {rx_mb:.1f} | "
                    f"{tx_mbps:.1f} | {rx_mbps:.1f} | "
                    f"{tx_p} | {srd_p} | {srd_share:.1f}% |"
                )
        else:
            lines.append("- _no NIC counters captured this run_")
        lines.append("")
        lines.append("### sockperf baseline (pairwise adjacent peers)")
        lines.append("")
        if self.sockperf_pairwise_us:
            lines.append("| Peer pair | p50 (µs) | p99 (µs) | observation |")
            lines.append("|---|---|---|---|")
            for pair, m in sorted(self.sockperf_pairwise_us.items()):
                p50 = m.get("p50_us")
                p99 = m.get("p99_us")
                obs = m.get("observation_count", "?")
                lines.append(
                    f"| {pair} | {p50:.1f} | {p99:.1f} | {obs} |"
                    if p50 is not None
                    else f"| {pair} | _missing_ | _missing_ | _missing_ |"
                )
        elif self.iperf3_throughput_mbps:
            lines.append("| Peer pair | iperf3 Throughput (Mbps, legacy) |")
            lines.append("|---|---|")
            for pair, mbps in sorted(self.iperf3_throughput_mbps.items()):
                lines.append(f"| {pair} | {mbps:.0f} |")
        else:
            lines.append("- _no baseline captured_")
        lines.append("")
        return "\n".join(lines)


# ── EC2 / SSM clients ───────────────────────────────────────────────


def _require_boto3() -> None:
    if boto3 is None:
        raise SystemExit(
            "FATAL: boto3 not installed. `pip install boto3` (or use the "
            "celery-frontend container which has it pre-installed)."
        )


def discover_ring_peers(
    *, region: str, expected_size: int, ec2_client: Any = None
) -> list[RingPeer]:
    """Discover ring peers via EC2 tag query.

    Returns peers sorted by node_id (asc). Raises if the discovered
    count doesn't match `expected_size` — a partial ring is never a
    valid bench target (TRAINS requires all N peers to form the ring).
    """
    if ec2_client is None:
        _require_boto3()
        ec2_client = boto3.client("ec2", region_name=region)

    response = ec2_client.describe_instances(
        Filters=[
            {"Name": "tag:Project", "Values": [PROJECT_TAG]},
            {"Name": "tag:Role", "Values": ["ring"]},
            {"Name": "instance-state-name", "Values": ["running"]},
        ]
    )
    peers: list[RingPeer] = []
    for reservation in response.get("Reservations", []):
        for instance in reservation.get("Instances", []):
            node_id_tag = _get_tag(instance, "NodeId")
            if node_id_tag is None:
                log.warning(
                    "instance %s tagged Role=ring but missing NodeId tag; skipping",
                    instance.get("InstanceId"),
                )
                continue
            try:
                node_id = int(node_id_tag)
            except ValueError:
                log.warning(
                    "instance %s has non-integer NodeId=%s; skipping",
                    instance.get("InstanceId"),
                    node_id_tag,
                )
                continue
            peers.append(
                RingPeer(
                    node_id=node_id,
                    instance_id=instance["InstanceId"],
                    private_ip=instance["PrivateIpAddress"],
                    az=instance["Placement"]["AvailabilityZone"],
                )
            )

    peers.sort(key=lambda p: p.node_id)
    if len(peers) != expected_size:
        raise RuntimeError(
            f"discovered {len(peers)} ring peers (NodeId={[p.node_id for p in peers]}); "
            f"expected exactly {expected_size}. Refusing to run bench against a partial ring."
        )

    return peers


def _get_tag(instance: dict, key: str) -> str | None:
    """Read a tag value from an EC2 describe-instances response."""
    for tag in instance.get("Tags", []):
        if tag.get("Key") == key:
            return tag.get("Value")
    return None


def build_node_runner_environment(
    *, peer: RingPeer, peers: list[RingPeer], config: RunConfig, peer_fingerprints: str
) -> dict[str, str]:
    """Build the env-var block passed to node_runner.sh on one peer.

    The successor wiring is ring-shaped: node 0 → node 1 → ... → node
    N-1 → node 0. Wrapping happens via `% len(peers)` arithmetic.
    """
    successor = peers[(peer.node_id + 1) % len(peers)]
    # ISSUE_INITIAL must be set on the FIRST `NUM_TRAINS` nodes
    # (per trains-core's two-lap UTO protocol — AllPriorDelivered
    # blocks delivery until every issuer's clock advances). The
    # default NUM_TRAINS is 2 (compiled into trains-core via build.rs
    # TRAINS_NUM_TRAINS env, default 2). So nodes 0 AND 1 must
    # issue. Without this, payloads from issuer 0 NEVER deliver
    # because issuer 1's clock stays at 0 forever. Discovered
    # 2026-05-23 after multiple FIFO/pipe rewrites; the bug was in
    # the protocol setup all along, not the stdin pipe.
    NUM_TRAINS = 2
    # Reconfiguration (TotalOrder mode): give every node the full ring
    # topology as `--peer-addr <id>=<ip:port>` flags so it can retarget past
    # a crashed successor. Empty in UTO mode (reconfiguration disabled).
    peer_addrs = (
        " ".join(
            f"--peer-addr {p.node_id}={p.private_ip}:{RING_PORT}" for p in peers
        )
        if config.delivery_mode == "to"
        else ""
    )
    return {
        "NODE_ID": str(peer.node_id),
        "RING_SIZE": str(len(peers)),
        "LISTEN_ADDR": f"0.0.0.0:{RING_PORT}",
        "SUCCESSOR_ADDR": f"{successor.private_ip}:{RING_PORT}",
        "PEER_FINGERPRINTS": peer_fingerprints,
        "DELIVERY_MODE": config.delivery_mode,
        "PEER_ADDRS": peer_addrs,
        "ISSUE_INITIAL": "true" if peer.node_id < NUM_TRAINS else "false",
        # IS_BROADCASTER selects WHICH node runs the Python broadcast
        # producer (writes payloads to trains-cli stdin). Distinct from
        # ISSUE_INITIAL (which is about train-slot ownership at the
        # protocol layer). For bench v0.1 we broadcast from node 0 only
        # so seq numbers correlate trivially to issuer 0.
        "IS_BROADCASTER": "true" if peer.node_id == 0 else "false",
        "RESULTS_BUCKET": config.results_bucket,
        "RUN_ID": config.run_id,
        # Per-bench knobs needed by the inline issuer producer:
        "MESSAGE_COUNT": str(config.message_count),
        "PAYLOAD_SIZE": str(config.payload_size),
        "BENCH_DURATION_S": str(int(config.duration_seconds)),
    }


def build_run_command_parameters(
    *, script_path: str, env: dict[str, str]
) -> dict[str, list[str]]:
    """Translate an env-var block into the SSM RunCommand Parameters shape.

    SSM's RunShellScript document takes `commands` as a list of lines.
    We prefix the env-var exports so the script picks them up. This
    keeps the env contract explicit instead of relying on instance
    UserData to set them globally.
    """
    exports = [f"export {k}={_shell_quote(v)}" for k, v in env.items()]
    return {"commands": exports + [f"bash {script_path}"]}


def _shell_quote(value: str) -> str:
    """Minimal shell-quote for SSM RunCommand env-var exports.

    We use single quotes and escape embedded single quotes the standard
    POSIX way. The values we pass are IP addresses, integers, and
    SHA-256 hex — none of which need rich quoting in practice, but
    quoting is the right reflex.
    """
    return "'" + value.replace("'", "'\\''") + "'"


def compute_latency_percentiles(
    *,
    send_log: list[dict],
    deliveries: Iterable[dict],
    warmup_count: int = 0,
) -> tuple[float | None, float | None, float | None, float | None, int]:
    """Compute p50/p95/p99/p99.9 latencies (ms) over the steady-state window.

    Joins the issuer's send-log to one node's delivery-log by `seq`.
    Sorts deliveries by seq, then discards the first `warmup_count`
    (cold-flight cost — TLS handshake, TCP slow-start, initial train
    formation). Steady-state numbers are computed over the remaining.

    Returns (p50, p95, p99, p999, sample_count_after_warmup).
    Returns (None, None, None, None, 0) if too few overlapping seqs.

    Why warm-up matters: with N=100 we get one observation at p99
    which is statistically meaningless. Standard practice (Java
    steady-state, distributed-systems literature): discard first N
    until p99 changes < 5 % across two consecutive windows. We
    approximate via a fixed `warmup_count`.
    """
    send_by_seq = {entry["seq"]: entry["send_ns"] for entry in send_log}
    # Sort by seq so warm-up discards the EARLIEST samples (cold-flight),
    # not arbitrary ones.
    paired: list[tuple[int, float]] = []
    for delivery in deliveries:
        send_ns = send_by_seq.get(delivery["seq"])
        if send_ns is None:
            continue
        latency_ms = (delivery["recv_ns"] - send_ns) / 1e6
        paired.append((delivery["seq"], latency_ms))

    if not paired:
        return (None, None, None, None, 0)

    paired.sort(key=lambda x: x[0])  # sort by seq
    if warmup_count > 0:
        paired = paired[warmup_count:]
    if not paired:
        return (None, None, None, None, 0)

    latencies_ms = sorted(lat for _, lat in paired)
    return (
        _percentile(latencies_ms, 50),
        _percentile(latencies_ms, 95),
        _percentile(latencies_ms, 99),
        _percentile(latencies_ms, 99.9),
        len(latencies_ms),
    )


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Compute a percentile from a pre-sorted list (linear interpolation).

    Uses the same convention as numpy.percentile(..., interpolation='linear').
    For tiny N this is more honest than `statistics.quantiles` which
    inserts edge effects.
    """
    if not sorted_values:
        raise ValueError("cannot compute percentile of empty list")
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[f]
    return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)


# ── Run orchestration ───────────────────────────────────────────────


def run_preflight(*, region: str, results_bucket: str, expected_ring_size: int) -> dict:
    """Read-only pre-flight: identity, bucket, ring discovery.

    Returns a dict suitable for printing or asserting in tests. Raises
    on any failure with a precise error message — no fabrication.
    """
    _require_boto3()
    sts = boto3.client("sts", region_name=region)
    identity = sts.get_caller_identity()

    s3 = boto3.client("s3", region_name=region)
    try:
        s3.head_bucket(Bucket=results_bucket)
        bucket_ok = True
        bucket_error = None
    except botocore.exceptions.ClientError as exc:
        bucket_ok = False
        bucket_error = str(exc)

    peers = discover_ring_peers(region=region, expected_size=expected_ring_size)

    return {
        "account": identity["Account"],
        "arn": identity["Arn"],
        "region": region,
        "bucket": results_bucket,
        "bucket_ok": bucket_ok,
        "bucket_error": bucket_error,
        "ring_peers": [dataclasses.asdict(p) for p in peers],
    }


def aggregate_run(
    *,
    config: RunConfig,
    peers: list[RingPeer],
    issuer_send_log: list[dict],
    per_node_deliveries: dict[int, list[dict]],
    iperf3_results: dict[str, float],
    started_at_ns: int,
    ended_at_ns: int,
    sockperf_pairwise_us: dict[str, dict] | None = None,
    nic_deltas_per_node: dict[int, dict] | None = None,
) -> RunReport:
    """Build the RunReport from collected raw data. Pure function — no AWS."""
    issuer_node_id = 0
    deliveries_per_node = {
        node_id: len(deliveries) for node_id, deliveries in per_node_deliveries.items()
    }
    # Use ANY node's deliveries for latency — TRAINS UTO means every
    # node delivers in the same order. Pick the first one with data.
    latency_p50, latency_p95, latency_p99, latency_p999, sample_count = (
        None, None, None, None, 0,
    )
    for node_id in sorted(per_node_deliveries.keys()):
        deliveries = per_node_deliveries[node_id]
        if deliveries:
            (
                latency_p50, latency_p95, latency_p99, latency_p999, sample_count,
            ) = compute_latency_percentiles(
                send_log=issuer_send_log,
                deliveries=deliveries,
                warmup_count=config.warmup_count,
            )
            break

    messages_sent = len(issuer_send_log)
    success = (
        messages_sent > 0
        and all(d == messages_sent for d in deliveries_per_node.values())
        and latency_p50 is not None
    )
    failure_reason = None
    if messages_sent == 0:
        failure_reason = "issuer sent 0 messages"
    elif any(d != messages_sent for d in deliveries_per_node.values()):
        failure_reason = "delivery count mismatch — TRAINS UTO completeness violated"
    elif latency_p50 is None:
        failure_reason = "no latency samples computed"

    return RunReport(
        config=config,
        peers=peers,
        issuer_node_id=issuer_node_id,
        messages_sent=messages_sent,
        deliveries_per_node=deliveries_per_node,
        latency_p50_ms=latency_p50,
        latency_p95_ms=latency_p95,
        latency_p99_ms=latency_p99,
        latency_p999_ms=latency_p999,
        latency_sample_count=sample_count,
        iperf3_throughput_mbps=iperf3_results,
        sockperf_pairwise_us=sockperf_pairwise_us,
        success=success,
        failure_reason=failure_reason,
        started_at_ns=started_at_ns,
        ended_at_ns=ended_at_ns,
        nic_deltas_per_node=nic_deltas_per_node,
    )


# ── CLI ─────────────────────────────────────────────────────────────


def cmd_preflight(args: argparse.Namespace) -> int:
    result = run_preflight(
        region=args.region,
        results_bucket=args.results_bucket,
        expected_ring_size=args.ring_size,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["bucket_ok"] else 1


def cmd_run(args: argparse.Namespace) -> int:
    # Lazy import: orchestrator pulls in boto3 + heavy SSM shapes; the
    # rest of this CLI (preflight, teardown-check) doesn't need them.
    from orchestrator import run as run_bench  # type: ignore[import-not-found]

    try:
        duration_s = _parse_duration(args.duration)
    except ValueError as exc:
        print(f"FATAL: invalid --duration: {exc}", file=sys.stderr)
        return 1

    report = run_bench(
        region=args.region,
        ring_size=args.ring_size,
        results_bucket=args.results_bucket,
        coordinator_instance_id=args.coordinator_instance_id,
        duration_seconds=duration_s,
        message_count=args.message_count,
        payload_size=args.payload_size,
        az_spread=args.az_spread,
        warmup_count=args.warmup_count,
        delivery_mode=args.delivery_mode,
        dry_run=args.dry_run,
        output_dir=Path(args.output_dir),
        fault=_fault_from_args(args),
    )
    return 0 if report.success else 1


def _fault_from_args(args: argparse.Namespace) -> dict | None:
    """Assemble a Phase G [fault] dict from --fault-* CLI args, or None.

    Mirrors the [fault] schema in scripts/_parse_config.py. reproduce.sh
    passes these only when the config has a [fault] section.
    """
    if not getattr(args, "fault_type", None):
        return None
    fault: dict = {
        "type": args.fault_type,
        "target_node": args.fault_target_node,
        "inject_at_s": args.fault_inject_at_s,
        "duration_s": args.fault_duration_s,
    }
    if args.fault_loss_pct is not None:
        fault["loss_pct"] = args.fault_loss_pct
    if args.fault_latency_ms is not None:
        fault["latency_ms"] = args.fault_latency_ms
    if args.fault_partition_peer is not None:
        fault["partition_peer"] = args.fault_partition_peer
    return fault


def _parse_duration(s: str) -> float:
    """Parse a duration string like '30s', '5m', '500ms' into seconds.
    Mirrors load-gen/issuer_workload.py's parser so the CLI accepts
    the same shapes as the on-instance driver."""
    s = s.strip().lower()
    if s.endswith("ms"):
        return float(s[:-2]) / 1000.0
    if s.endswith("s"):
        return float(s[:-1])
    if s.endswith("m"):
        return float(s[:-1]) * 60.0
    return float(s)


def cmd_teardown_check(args: argparse.Namespace) -> int:
    _require_boto3()
    ec2 = boto3.client("ec2", region_name=args.region)
    response = ec2.describe_instances(
        Filters=[
            {"Name": "tag:Project", "Values": [PROJECT_TAG]},
            {"Name": "instance-state-name", "Values": ["running", "pending", "stopping"]},
        ]
    )
    remaining: list[str] = []
    for reservation in response.get("Reservations", []):
        for instance in reservation.get("Instances", []):
            remaining.append(instance["InstanceId"])
    if remaining:
        print(
            f"❌ {len(remaining)} instances still tagged Project={PROJECT_TAG}:",
            file=sys.stderr,
        )
        for iid in remaining:
            print(f"  - {iid}", file=sys.stderr)
        return 1
    print(f"✅ no instances tagged Project={PROJECT_TAG} — teardown clean")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    raise SystemExit(
        "FATAL: `report` sub-command is not yet implemented.\n"
        "Phase-3 will add re-render-from-S3 once the first run produces "
        "raw artefacts. See bench/README.md §'Phase-3 enablement'."
    )


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--region", default=os.environ.get("AWS_REGION", DEFAULT_REGION),
        help=f"AWS region (default: {DEFAULT_REGION} or $AWS_REGION)",
    )
    p.add_argument(
        "--ring-size", type=int, default=3,
        help="Expected ring size (default: 3)",
    )
    p.add_argument(
        "--results-bucket", default=os.environ.get("TRAINS_BENCH_RESULTS_BUCKET"),
        help="S3 bucket name from CDK stack output `ResultsBucket`",
    )
    p.add_argument("--verbose", action="store_true", help="Enable DEBUG logging")

    sub = p.add_subparsers(dest="cmd", required=True)

    sub_preflight = sub.add_parser("preflight", help="Read-only pre-flight check")
    sub_preflight.set_defaults(func=cmd_preflight)

    sub_run = sub.add_parser("run", help="Execute one bench run")
    sub_run.add_argument("--duration", default="30s",
                         help="Bench duration, e.g. 30s, 5m (default 30s)")
    sub_run.add_argument("--message-count", type=int, default=10000,
                         help="Total broadcasts (default 10000; N<1000 = "
                              "no statistical signal at p99)")
    sub_run.add_argument("--payload-size", type=int, default=256,
                         help="Per-message size in bytes (default 256)")
    sub_run.add_argument(
        "--warmup-count", type=int, default=1000,
        help="Discard first N samples from percentiles (cold-flight: "
             "TLS handshake + TCP slow-start + initial train formation). "
             "Default 1000 (~10%% of N=10000). Set 0 for raw / debug runs.",
    )
    sub_run.add_argument(
        "--delivery-mode", choices=("uto", "to"), default="uto",
        help="Ring delivery mode: uto (strict, default) or to (crash-masking; "
             "nodes get the full ring topology and reconfigure past a crashed "
             "node — required to MASK a fis-kill, PR-R4)",
    )
    sub_run.add_argument(
        "--coordinator-instance-id", default=os.environ.get("TRAINS_BENCH_COORDINATOR"),
        help="EC2 instance id of the coordinator (from CDK stack output `CoordinatorInstanceId`)",
    )
    sub_run.add_argument(
        "--az-spread", choices=("single", "three"), default="single",
        help="For the run-report only; must match the deployed stack's azSpread",
    )
    sub_run.add_argument(
        "--output-dir", default="bench/results",
        help="Local directory for the run report (default: bench/results)",
    )
    # Default to DRY-RUN — live runs require explicit --no-dry-run.
    # This is the safety-default: a fat-fingered `coordinator.py run`
    # against a real CDK stack does NOT spend money or mutate AWS.
    dry_run_group = sub_run.add_mutually_exclusive_group()
    dry_run_group.add_argument(
        "--dry-run", dest="dry_run", action="store_true", default=True,
        help="Skip every SSM/S3 call; print what would be issued (DEFAULT)",
    )
    dry_run_group.add_argument(
        "--no-dry-run", dest="dry_run", action="store_false",
        help="Live mode — issues real SSM SendCommand + S3 puts. Bench spends ~$0 (SSM only).",
    )
    # Phase G chaos fault injection (all optional; passed by reproduce.sh
    # only when the config has a [fault] section). See faults.py.
    sub_run.add_argument(
        "--fault-type", default=None,
        choices=("netem-loss", "netem-latency", "netem-partition",
                 "fis-kill", "fis-stop-start"),
        help="Phase G: inject a fault mid-bench (default: none)",
    )
    sub_run.add_argument("--fault-target-node", type=int, default=0,
                         help="Ring node index to apply the fault to")
    sub_run.add_argument("--fault-inject-at-s", type=int, default=0,
                         help="Seconds into the bench window to inject")
    sub_run.add_argument("--fault-duration-s", type=int, default=0,
                         help="Fault duration (0 = rest of run)")
    sub_run.add_argument("--fault-loss-pct", type=int, default=None,
                         help="netem-loss: packet loss percent")
    sub_run.add_argument("--fault-latency-ms", type=int, default=None,
                         help="netem-latency: added latency in ms")
    sub_run.add_argument("--fault-partition-peer", type=int, default=None,
                         help="netem-partition: peer node index to cut off")
    sub_run.set_defaults(func=cmd_run)

    sub_teardown = sub.add_parser(
        "teardown-check", help="Verify no Project=trains-bench instances running"
    )
    sub_teardown.set_defaults(func=cmd_teardown_check)

    sub_report = sub.add_parser("report", help="Re-render a past run report (Phase 3)")
    sub_report.add_argument("--run-id", required=True)
    sub_report.set_defaults(func=cmd_report)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    if args.cmd in ("preflight",) and not args.results_bucket:
        parser.error(
            "--results-bucket is required for `preflight` "
            "(or set TRAINS_BENCH_RESULTS_BUCKET env var)"
        )
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
