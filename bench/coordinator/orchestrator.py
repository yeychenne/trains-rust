"""orchestrator.py — SSM-driven bench execution.

Layered on top of `coordinator.py` (pure helpers + CLI). This module
holds the side-effecting AWS code: SSM SendCommand, polling, S3 round-
trips. Every AWS call routes through `SSMOrchestrator` so `--dry-run`
mode can be implemented in one place.

Design contract:
- `--dry-run` mode NEVER calls `boto3.client("ssm").send_command` etc.
  It captures the would-be calls in `SSMOrchestrator.dry_run_log` for
  inspection.
- `--no-dry-run` (live) mode requires `boto3` to be installed; the
  module errors loudly if it isn't.
- Pure aggregation is delegated back to `coordinator.aggregate_run`
  (already tested in `test_coordinator.py`); this module only adds
  the SSM/S3 fetch glue.
"""

from __future__ import annotations

import base64
import dataclasses
import io
import json
import logging
import secrets
import time
import threading
from collections.abc import Iterable
from pathlib import Path
from typing import Any

try:
    import boto3  # type: ignore[import-untyped]
    import botocore  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]
    botocore = None  # type: ignore[assignment]

from coordinator import (  # type: ignore[import-not-found]
    RING_PORT,
    PROJECT_TAG,
    RingPeer,
    RunConfig,
    RunReport,
    aggregate_run,
    build_node_runner_environment,
    build_run_command_parameters,
    discover_ring_peers,
)
import faults  # type: ignore[import-not-found]

log = logging.getLogger("trains-bench.orchestrator")

# ── Constants ───────────────────────────────────────────────────────

SSM_DOCUMENT = "AWS-RunShellScript"
"""SSM document used for all RunCommand invocations."""

DEFAULT_BENCH_SCRIPT_DIR = "/opt/trains-bench"
"""On-instance directory where the load-gen scripts are dropped by
the coordinator's UserData (in production deploys). For dry-run we
just reference the path string — no filesystem access."""

DEFAULT_SSM_TIMEOUT_S = 600
"""Per-command SSM timeout. 600 s is generous for a 30 s bench."""

DEFAULT_POLL_INTERVAL_S = 2.0
"""How often to call ssm:GetCommandInvocation while waiting."""

DEFAULT_MAX_WAIT_S = 300
"""Default cap on `wait_for_command`. Caller can override per-call."""


# ── SSM orchestrator (dry-run-aware boto3 shim) ─────────────────────


class SSMOrchestrator:
    """Thin wrapper over boto3 ssm with `--dry-run` semantics.

    Constructed once per `coordinator.py run` invocation. Every SSM
    call in the orchestrator routes through this class so the dry-run
    safety net is centralised — there is no path where a `send_command`
    can happen behind its back.

    In dry-run mode:
    - `send_command` returns a fabricated id (`dry-run-<hex>`) and
      records the call shape in `dry_run_log` for inspection.
    - `wait_for_command` returns a synthetic Success after one tick.
    - No `boto3.client("ssm")` is ever constructed.
    """

    def __init__(
        self,
        *,
        region: str,
        dry_run: bool,
        ssm_client: Any = None,
    ) -> None:
        self.region = region
        self.dry_run = dry_run
        self.dry_run_log: list[dict] = []
        if ssm_client is not None:
            self._ssm = ssm_client
        elif dry_run:
            self._ssm = None
        else:
            _require_boto3()
            self._ssm = boto3.client("ssm", region_name=region)

    def send_command(
        self,
        *,
        instance_ids: list[str],
        parameters: dict[str, list[str]],
        comment: str,
        timeout_seconds: int = DEFAULT_SSM_TIMEOUT_S,
    ) -> str:
        """Issue ssm:SendCommand. Returns command_id."""
        kwargs: dict[str, Any] = {
            "InstanceIds": instance_ids,
            "DocumentName": SSM_DOCUMENT,
            "Parameters": parameters,
            "Comment": comment,
            "TimeoutSeconds": timeout_seconds,
        }
        if self.dry_run:
            fake_id = f"dry-run-{secrets.token_hex(4)}"
            self.dry_run_log.append(
                {"call": "send_command", "kwargs": kwargs, "command_id": fake_id}
            )
            log.info(
                "DRY RUN ssm:SendCommand %s → command_id=%s comment=%r",
                instance_ids, fake_id, comment,
            )
            return fake_id
        response = self._ssm.send_command(**kwargs)
        command_id = response["Command"]["CommandId"]
        log.info(
            "ssm:SendCommand %s → command_id=%s comment=%r",
            instance_ids, command_id, comment,
        )
        return command_id

    def wait_for_command(
        self,
        *,
        command_id: str,
        instance_id: str,
        max_wait_s: int = DEFAULT_MAX_WAIT_S,
        poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
        sleep_fn: Any = time.sleep,
        now_fn: Any = time.monotonic,
    ) -> dict[str, Any]:
        """Poll ssm:GetCommandInvocation until terminal.

        Returns the final invocation dict. Raises TimeoutError if the
        command does not reach a terminal status within max_wait_s.

        In dry-run mode, returns a synthetic Success immediately.

        `sleep_fn` and `now_fn` are injected for deterministic tests
        (no real wall-clock waiting).
        """
        if self.dry_run:
            self.dry_run_log.append(
                {"call": "wait_for_command", "command_id": command_id, "instance_id": instance_id}
            )
            return {
                "Status": "Success",
                "ResponseCode": 0,
                "StandardOutputContent": "[dry-run synthetic success]",
                "StandardErrorContent": "",
            }

        deadline = now_fn() + max_wait_s
        while now_fn() < deadline:
            try:
                invocation = self._ssm.get_command_invocation(
                    CommandId=command_id, InstanceId=instance_id
                )
            except botocore.exceptions.ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code == "InvocationDoesNotExist":
                    sleep_fn(poll_interval_s)
                    continue
                raise
            status = invocation.get("Status", "")
            if status in ("Success", "Failed", "Cancelled", "TimedOut"):
                return invocation
            sleep_fn(poll_interval_s)

        raise TimeoutError(
            f"SSM command {command_id} on {instance_id} did not terminate within {max_wait_s}s"
        )


def _require_boto3() -> None:
    if boto3 is None:
        raise SystemExit(
            "FATAL: boto3 not installed. `pip install boto3` (or use the "
            "celery-frontend container which has it pre-installed)."
        )


# ── S3 helper (small, dry-run-aware) ────────────────────────────────


class S3Helper:
    """boto3 s3 wrapper with dry-run support."""

    def __init__(
        self,
        *,
        region: str,
        dry_run: bool,
        s3_client: Any = None,
    ) -> None:
        self.region = region
        self.dry_run = dry_run
        self.dry_run_log: list[dict] = []
        if s3_client is not None:
            self._s3 = s3_client
        elif dry_run:
            self._s3 = None
        else:
            _require_boto3()
            self._s3 = boto3.client("s3", region_name=region)

    def put_object(self, *, bucket: str, key: str, body: bytes) -> None:
        if self.dry_run:
            self.dry_run_log.append(
                {"call": "put_object", "bucket": bucket, "key": key, "size": len(body)}
            )
            log.info("DRY RUN s3:PutObject s3://%s/%s (%d B)", bucket, key, len(body))
            return
        self._s3.put_object(Bucket=bucket, Key=key, Body=body)

    def get_object(self, *, bucket: str, key: str) -> bytes:
        """Download an S3 object. In dry-run, returns a synthetic empty JSON."""
        if self.dry_run:
            self.dry_run_log.append({"call": "get_object", "bucket": bucket, "key": key})
            log.info("DRY RUN s3:GetObject s3://%s/%s → synthetic empty", bucket, key)
            return b"{}"
        response = self._s3.get_object(Bucket=bucket, Key=key)
        body = response["Body"]
        if hasattr(body, "read"):
            return body.read()
        return bytes(body)


# ── Run orchestration (the actual work) ─────────────────────────────


@dataclasses.dataclass
class RunArtefacts:
    """Mutable accumulator for one run's collected raw data."""

    peer_fingerprints: dict[int, str] = dataclasses.field(default_factory=dict)
    issuer_send_log: list[dict] = dataclasses.field(default_factory=list)
    per_node_deliveries: dict[int, list[dict]] = dataclasses.field(default_factory=dict)
    iperf3_results: dict[str, float] = dataclasses.field(default_factory=dict)
    failures: list[str] = dataclasses.field(default_factory=list)


def generate_identities(
    *,
    coordinator_instance_id: str,
    ring_size: int,
    ssm: SSMOrchestrator,
    s3: S3Helper,
    results_bucket: str,
) -> dict[int, str]:
    """Generate one TLS identity per ring node on the coordinator.

    Calls `trains keygen --out identity-N.json` once per node via SSM,
    parses the printed SHA-256 fingerprint, uploads identity-N.json to
    s3://<bucket>/identities/identity-N.json.

    Returns {node_id: fingerprint}.

    In dry-run mode, returns synthetic fingerprints derived from node_id
    so downstream wiring still produces a valid PEER_FINGERPRINTS env var.
    """
    fingerprints: dict[int, str] = {}
    for node_id in range(ring_size):
        local_path = f"/tmp/identity-{node_id}.json"
        # `trains keygen` prints two lines:
        #   identity:    /tmp/identity-N.json
        #   fingerprint: <64-hex SPKI fingerprint>
        # The fingerprint is the SHA-256 of the public key's SPKI, NOT
        # of the identity file. We DO NOT run sha256sum here (it returns
        # the file's content hash, which is completely different and
        # caused every TLS handshake to fail on 2026-05-23 first run).
        commands = [
            f"/opt/trains-src/target/release/trains keygen --out {local_path}",
            f"aws s3 cp {local_path} s3://{results_bucket}/identities/identity-{node_id}.json",
        ]
        cmd_id = ssm.send_command(
            instance_ids=[coordinator_instance_id],
            parameters={"commands": commands},
            comment=f"trains-bench: generate identity-{node_id}",
        )
        invocation = ssm.wait_for_command(
            command_id=cmd_id, instance_id=coordinator_instance_id
        )
        if invocation.get("Status") != "Success":
            raise RuntimeError(
                f"identity generation failed for node {node_id}: "
                f"{invocation.get('StandardErrorContent', '')[:500]}"
            )
        fingerprint = _parse_fingerprint(
            invocation.get("StandardOutputContent", ""), node_id=node_id, dry_run=ssm.dry_run
        )
        fingerprints[node_id] = fingerprint
    return fingerprints


def _parse_fingerprint(stdout: str, *, node_id: int, dry_run: bool) -> str:
    """Extract the SPKI fingerprint from `trains keygen` stdout.

    Format: a line `fingerprint: <64-hex>` printed by the trains-cli
    binary. The SPKI fingerprint is the SHA-256 of the X.509 public-key
    SubjectPublicKeyInfo block — this is the value trains-cli pins for
    peer TLS validation. (Do NOT use sha256sum of the identity file:
    that's a file-content hash, completely different, and caused every
    TLS handshake to fail on 2026-05-23 first live deploy.)
    """
    for line in stdout.splitlines():
        line = line.strip()
        # Expected format: "fingerprint: <hex>" (with any amount of whitespace)
        if line.lower().startswith("fingerprint:"):
            value = line.split(":", 1)[1].strip()
            if len(value) == 64 and all(c in "0123456789abcdef" for c in value.lower()):
                return value.lower()
    if dry_run:
        # Stable, parseable fingerprint for dry-run smoke tests
        return f"{node_id:02d}" * 32
    raise RuntimeError(
        f"could not parse 'fingerprint: <hex>' line from keygen stdout: {stdout!r}"
    )


def start_ring_nodes(
    *,
    peers: list[RingPeer],
    config: RunConfig,
    fingerprints: dict[int, str],
    ssm: SSMOrchestrator,
    s3: S3Helper,
) -> dict[int, str]:
    """Start trains-cli on every ring node in parallel via SSM.

    Returns {node_id: command_id} so the caller can poll completion.
    Each command does the work of `load-gen/node_runner.sh` inline so
    we don't depend on the script being pre-deployed on the host (the
    CDK UserData copies it but we don't want to fail if it's stale).

    The peer-fingerprints env var is the comma-joined sequence of all
    fingerprints EXCEPT this node's own (trains-cli pins its peers,
    not itself).
    """
    command_ids: dict[int, str] = {}
    for peer in peers:
        peer_fps = ",".join(
            fingerprints[p.node_id] for p in peers if p.node_id != peer.node_id
        )
        env = build_node_runner_environment(
            peer=peer, peers=peers, config=config, peer_fingerprints=peer_fps
        )
        # Inline the script body so we don't depend on a pre-deployed file.
        # See bench/load-gen/node_runner.sh for the canonical version.
        commands = (
            [f"export {k}={_shell_quote(v)}" for k, v in env.items()]
            + [_inline_node_runner_body()]
        )
        cmd_id = ssm.send_command(
            instance_ids=[peer.instance_id],
            parameters={"commands": commands},
            comment=f"trains-bench: start node-{peer.node_id}",
        )
        command_ids[peer.node_id] = cmd_id
    return command_ids


def wait_for_ring_formation(
    *,
    peers: list[RingPeer],
    start_command_ids: dict[int, str],
    ssm: SSMOrchestrator,
    max_wait_s: int = 60,
) -> None:
    """Wait for every ring node's start command to reach Success.

    A node's start command exits within ~5s (node_runner.sh launches
    trains-cli in background then exits). Failure here means the
    `kill -0 $PID` liveness check inside node_runner.sh tripped — the
    trains-cli process died within 3 s of launch.
    """
    for peer in peers:
        cmd_id = start_command_ids[peer.node_id]
        invocation = ssm.wait_for_command(
            command_id=cmd_id,
            instance_id=peer.instance_id,
            max_wait_s=max_wait_s,
        )
        status = invocation.get("Status", "")
        if status != "Success":
            stderr = invocation.get("StandardErrorContent", "")[:500]
            raise RuntimeError(
                f"node-{peer.node_id} ({peer.instance_id}) failed to start: "
                f"status={status}, stderr={stderr!r}"
            )


def trigger_issuer_workload(
    *,
    issuer: RingPeer,
    config: RunConfig,
    ssm: SSMOrchestrator,
) -> str:
    """No-op: the new node_runner design (2026-05-23 v2) embeds the
    Python broadcast producer directly in the trains-cli pipeline as
    part of start_ring_nodes. This function is kept as a stub to
    preserve the orchestrator.run() call signature; it returns a
    synthetic command_id that wait_for_command resolves instantly in
    dry-run mode and is a recognized fast-success path in live mode."""
    # In dry-run, we return a fake id and the wait completes instantly.
    # In live, we DON'T send any SSM command — the issuer's broadcasts
    # are happening inline in its node_runner script via Python pipe.
    cmd_id = ssm.send_command(
        instance_ids=[issuer.instance_id],
        parameters={"commands": ["echo issuer-broadcasts-are-inline-in-node-runner"]},
        comment="trains-bench: issuer (no-op, broadcasts run inline)",
        timeout_seconds=30,
    )
    return cmd_id


# Shell snippet executed via SSM on each ring node to snapshot the
# primary NIC's byte/packet counters + ENA SRD counters. Writes a single
# JSON blob to /tmp/<phase>-<node>.json then uploads to S3. Designed to
# be cheap (< 1s on the node) and bulletproof — any subcommand failure
# falls back to null fields, never breaks the bench.
_NIC_SNAPSHOT_SH = r"""
set +e
PHASE='__PHASE__'
NODE_ID='__NODE_ID__'
BUCKET='__BUCKET__'
S3_PREFIX='__S3_PREFIX__'
OUT="/tmp/nic-${PHASE}-${NODE_ID}.json"
NOW_NS=$(date +%s%N)

# Identify primary interface — the one carrying the default route.
IFACE=$(ip -4 route show default 2>/dev/null | awk '{print $5; exit}')
[ -z "$IFACE" ] && IFACE=$(ls /sys/class/net/ 2>/dev/null | grep -v '^lo$' | head -1)

read_stat() {
    cat "/sys/class/net/$IFACE/statistics/$1" 2>/dev/null || echo "null"
}

# Pull ENA SRD counters from ethtool if available. These are 0 on
# instances without SRD enabled; non-zero means the traffic actually
# went over ENA Express's reliable-datagram path.
ena_stat() {
    ethtool -S "$IFACE" 2>/dev/null | awk -v key="$1" '$1 == key":" {print $2; found=1} END{if(!found) print "null"}'
}

cat > "$OUT" <<JSON
{
  "phase":            "$PHASE",
  "node_id":          $NODE_ID,
  "captured_at_ns":   $NOW_NS,
  "interface":        "$IFACE",
  "rx_bytes":         $(read_stat rx_bytes),
  "tx_bytes":         $(read_stat tx_bytes),
  "rx_packets":       $(read_stat rx_packets),
  "tx_packets":       $(read_stat tx_packets),
  "ena_srd_tx_pkts":  $(ena_stat ena_srd_tx_pkts),
  "ena_srd_tx_bytes": $(ena_stat ena_srd_tx_bytes),
  "ena_srd_rx_pkts":  $(ena_stat ena_srd_rx_pkts),
  "ena_srd_rx_bytes": $(ena_stat ena_srd_rx_bytes)
}
JSON
aws s3 cp "$OUT" "s3://${BUCKET}/${S3_PREFIX}/nic-${PHASE}-node-${NODE_ID}.json" 2>&1 | tail -1
"""


def capture_nic_snapshots(
    *,
    peers: list[RingPeer],
    config: RunConfig,
    phase: str,  # "pre" or "post"
    ssm: SSMOrchestrator,
) -> dict[int, str]:
    """Fan out NIC counter snapshots to every ring node in parallel.

    Best-effort: failures here log a warning and continue. The bench
    must NOT fail because NIC capture is opportunistic.

    Returns a {node_id: command_id} map for the caller to wait on (so
    pre-snapshot is fully written before traffic starts).
    """
    if phase not in ("pre", "post"):
        raise ValueError(f"phase must be 'pre' or 'post', got {phase!r}")
    out: dict[int, str] = {}
    for peer in peers:
        try:
            script = (
                _NIC_SNAPSHOT_SH
                .replace("__PHASE__", phase)
                .replace("__NODE_ID__", str(peer.node_id))
                .replace("__BUCKET__", config.results_bucket)
                .replace("__S3_PREFIX__", config.s3_results_prefix())
            )
            cmd_id = ssm.send_command(
                instance_ids=[peer.instance_id],
                parameters={"commands": [script]},
                comment=f"trains-bench: nic-snapshot-{phase} node-{peer.node_id}",
                timeout_seconds=30,
            )
            out[peer.node_id] = cmd_id
        except Exception as exc:  # noqa: BLE001
            log.warning("nic-snapshot-%s send failed for node %d: %s", phase, peer.node_id, exc)
    return out


def compute_nic_deltas(
    *,
    peers: list[RingPeer],
    config: RunConfig,
    s3: S3Helper,
) -> dict[int, dict] | None:
    """Fetch pre + post NIC snapshots from S3 and compute deltas.

    Returns {node_id: delta_dict} or None if no usable data was
    captured. Per-node missing data → None entry but other nodes still
    aggregate normally.
    """
    s3_prefix = config.s3_results_prefix()
    result: dict[int, dict] = {}
    for peer in peers:
        pre_key = f"{s3_prefix}/nic-pre-node-{peer.node_id}.json"
        post_key = f"{s3_prefix}/nic-post-node-{peer.node_id}.json"
        # `path=None` returns the whole JSON object — the snapshot's
        # fields are flat (no wrapper key). Earlier `path="nic-pre"` was
        # a misread of the helper's contract; it would search for a
        # top-level key NAMED "nic-pre" inside the JSON and return None
        # when not found. Observed 2026-05-23 c7i-ring-6: all 12
        # snapshots present in S3, all 12 reported "missing" because of
        # this path lookup.
        pre = _safe_fetch_json(
            s3=s3, bucket=config.results_bucket, key=pre_key, path=None, default=None,
        )
        post = _safe_fetch_json(
            s3=s3, bucket=config.results_bucket, key=post_key, path=None, default=None,
        )
        if not (pre and post):
            log.warning("nic snapshots missing for node %d (pre=%s post=%s)",
                        peer.node_id, bool(pre), bool(post))
            continue

        def _diff(field: str) -> int | None:
            a, b = pre.get(field), post.get(field)
            if a is None or b is None:
                return None
            try:
                return int(b) - int(a)
            except (TypeError, ValueError):
                return None

        captured_dur_ns = (post.get("captured_at_ns") or 0) - (pre.get("captured_at_ns") or 0)
        result[peer.node_id] = {
            "interface":        post.get("interface", pre.get("interface")),
            "duration_s":       captured_dur_ns / 1e9 if captured_dur_ns > 0 else None,
            "rx_bytes":         _diff("rx_bytes"),
            "tx_bytes":         _diff("tx_bytes"),
            "rx_packets":       _diff("rx_packets"),
            "tx_packets":       _diff("tx_packets"),
            "ena_srd_tx_pkts":  _diff("ena_srd_tx_pkts"),
            "ena_srd_tx_bytes": _diff("ena_srd_tx_bytes"),
            "ena_srd_rx_pkts":  _diff("ena_srd_rx_pkts"),
            "ena_srd_rx_bytes": _diff("ena_srd_rx_bytes"),
        }
    return result if result else None


def collect_results(
    *,
    peers: list[RingPeer],
    config: RunConfig,
    ssm: SSMOrchestrator,
) -> dict[int, str]:
    """For each ring node: kill trains-cli, parse stderr, run iperf3, upload.

    Returns {node_id: command_id} so the caller polls + collects.
    """
    log_dir = f"/var/log/trains-bench/{config.run_id}"
    s3_prefix = config.s3_results_prefix()
    command_ids: dict[int, str] = {}

    for i, peer in enumerate(peers):
        next_peer = peers[(i + 1) % len(peers)]
        commands = [
            # 1. Stop trains-cli + its producer (python/sleep) cleanly.
            #    Kill trains-cli first; the producer pipe breaks; the
            #    bash wrapper exits. pkill catches any straggler in the
            #    new session created by setsid in node_runner.
            "kill -INT $(cat /tmp/trains-cli.pid) 2>/dev/null || true",
            "sudo pkill -INT -f 'trains node --id' 2>/dev/null || true",
            "sleep 2",  # let stdout/stderr flush
            # 2. Parse STDOUT for DELIVER lines.
            #    trains-cli node.rs:dispatch() prints delivered payloads
            #    with println! (=> stdout). eprintln! events go to stderr
            #    (boot/error msgs only). Reading stderr finds zero
            #    matches — bench burn 2026-05-23 attempt #3.
            _inline_parse_results(
                stderr_log=f"{log_dir}/trains-cli.stdout.log",
                node_id=peer.node_id,
                output=f"/tmp/node-{peer.node_id}-deliveries.json",
            ),
            # 3. sockperf ping-pong (3 s, 64-byte msgs) to adjacent
            #    successor — the proper baseline for percentile
            #    latency comparison. Replaces iperf3 (wrong tool —
            #    iperf3 measures bulk bandwidth, not per-msg latency
            #    distribution). Each node measures its outbound hop
            #    so we get N pairwise samples per run.
            (
                f"timeout 8s sockperf pp -i {next_peer.private_ip} -p 11111 "
                f"--msg-size 64 -t 3 --full-rtt --full-log "
                f"/tmp/sockperf-{peer.node_id}-to-{next_peer.node_id}.full.csv "
                f"> /tmp/sockperf-{peer.node_id}-to-{next_peer.node_id}.txt 2>&1 || "
                f"echo 'sockperf failed' "
                f"> /tmp/sockperf-{peer.node_id}-to-{next_peer.node_id}.txt"
            ),
            # 4. Upload everything to S3 under the per-run prefix
            (
                f"aws s3 cp /tmp/node-{peer.node_id}-deliveries.json "
                f"s3://{config.results_bucket}/{s3_prefix}/node-{peer.node_id}-deliveries.json"
            ),
            (
                f"aws s3 cp /tmp/sockperf-{peer.node_id}-to-{next_peer.node_id}.txt "
                f"s3://{config.results_bucket}/{s3_prefix}/sockperf-{peer.node_id}-to-{next_peer.node_id}.txt "
                "|| true"
            ),
            # 5. Tear down sockperf server (free port for next run).
            "kill -TERM $(cat /tmp/sockperf-sr.pid) 2>/dev/null || true",
            # Upload BOTH stdout and stderr for forensics.
            (
                f"aws s3 cp {log_dir}/trains-cli.stdout.log "
                f"s3://{config.results_bucket}/{s3_prefix}/node-{peer.node_id}-stdout.log "
                "|| true"
            ),
            (
                f"aws s3 cp {log_dir}/trains-cli.stderr.log "
                f"s3://{config.results_bucket}/{s3_prefix}/node-{peer.node_id}-stderr.log "
                "|| true"
            ),
        ]
        cmd_id = ssm.send_command(
            instance_ids=[peer.instance_id],
            parameters={"commands": commands},
            comment=f"trains-bench: collect node-{peer.node_id}",
        )
        command_ids[peer.node_id] = cmd_id
    return command_ids


def aggregate_from_s3(
    *,
    peers: list[RingPeer],
    config: RunConfig,
    s3: S3Helper,
    started_at_ns: int,
    ended_at_ns: int,
) -> RunReport:
    """Download per-node JSON artefacts from S3 and aggregate.

    Wraps `coordinator.aggregate_run` (already tested) with the S3 fetch
    glue. In dry-run mode, the synthetic empty payloads roll up into a
    "0 messages sent" report with failure_reason set — that's the right
    shape for a dry-run smoke (proves the pipeline completes end-to-end
    without crashing on missing data).
    """
    s3_prefix = config.s3_results_prefix()

    # Fetch per-node deliveries first — we'll derive issuer_send_log
    # from node-0's own deliveries (each payload embeds send_ns).
    per_node_deliveries: dict[int, list[dict]] = {}
    for peer in peers:
        deliveries = _safe_fetch_json(
            s3=s3,
            bucket=config.results_bucket,
            key=f"{s3_prefix}/node-{peer.node_id}-deliveries.json",
            path="deliveries",
            default=[],
        )
        per_node_deliveries[peer.node_id] = deliveries

    # Derive issuer_send_log from node-0's deliveries. With the
    # 2026-05-23-v2 design (no separate issuer-send-log SSM
    # producer), the issuer's broadcasts are recovered from the
    # delivery records themselves — every payload embeds the
    # original send_ns, so node-0's DELIVERs ARE the send history.
    issuer_deliveries = per_node_deliveries.get(0, [])
    issuer_send_log = [
        {"seq": d["seq"], "send_ns": d["send_ns"]}
        for d in issuer_deliveries
    ]

    # Fetch sockperf results — parse the text report from each pair.
    sockperf_pairwise_us: dict[str, dict] = {}
    iperf3_results: dict[str, float] = {}  # legacy, kept for back-compat
    for i, peer in enumerate(peers):
        next_peer = peers[(i + 1) % len(peers)]
        pair = f"{peer.node_id}-to-{next_peer.node_id}"
        # sockperf writes a text report; we fetch as bytes and parse.
        try:
            body = s3.get_object(
                bucket=config.results_bucket,
                key=f"{s3_prefix}/sockperf-{pair}.txt",
            )
            parsed = _parse_sockperf_text(body.decode("utf-8", errors="replace"))
            if parsed:
                sockperf_pairwise_us[pair] = parsed
        except Exception as exc:  # noqa: BLE001
            log.warning("sockperf result fetch failed for %s: %s", pair, exc)

    # NIC counter deltas (captured pre + post bench). Best-effort:
    # None if snapshots missing — RunReport.to_markdown handles that.
    nic_deltas = None
    try:
        nic_deltas = compute_nic_deltas(peers=peers, config=config, s3=s3)
    except Exception as exc:  # noqa: BLE001
        log.warning("nic-delta aggregation failed: %s", exc)

    return aggregate_run(
        config=config,
        peers=peers,
        issuer_send_log=issuer_send_log,
        per_node_deliveries=per_node_deliveries,
        iperf3_results=iperf3_results,
        sockperf_pairwise_us=sockperf_pairwise_us if sockperf_pairwise_us else None,
        started_at_ns=started_at_ns,
        ended_at_ns=ended_at_ns,
        nic_deltas_per_node=nic_deltas,
    )


def _parse_sockperf_text(text: str) -> dict | None:
    """Parse sockperf ping-pong text output.

    Looking for lines like:
      sockperf: ====> avg-lat= 5.123 (std-dev=1.234)
      sockperf: ---> <MAX> observation =   12.345
      sockperf: ---> percentile 99.000 =    8.456
      sockperf: ---> percentile 50.000 =    4.789
      sockperf: Total <N> observations
    Returns a dict with p50_us, p99_us, avg_us, observation_count.
    Returns None if no valid stats parsed.
    """
    import re
    out: dict = {}
    p50 = re.search(r"percentile 50\.000\s*=\s*([\d.]+)", text)
    p99 = re.search(r"percentile 99\.000\s*=\s*([\d.]+)", text)
    avg = re.search(r"avg-lat=\s*([\d.]+)", text)
    obs = re.search(r"Total\s+(\d+)\s+observations", text)
    if p50:
        out["p50_us"] = float(p50.group(1))
    if p99:
        out["p99_us"] = float(p99.group(1))
    if avg:
        out["avg_us"] = float(avg.group(1))
    if obs:
        out["observation_count"] = int(obs.group(1))
    return out or None


def _safe_fetch_json(
    *,
    s3: S3Helper,
    bucket: str,
    key: str,
    path: str | None,
    default: Any,
) -> Any:
    """Fetch + parse a JSON blob from S3. On any failure return `default`.

    If `path` is given, returns that key from the parsed object; otherwise
    returns the whole object.
    """
    try:
        body = s3.get_object(bucket=bucket, key=key)
        obj = json.loads(body)
    except Exception as exc:  # noqa: BLE001 — we genuinely want to swallow
        log.warning("failed to fetch/parse s3://%s/%s: %s", bucket, key, exc)
        return default
    if path is None:
        return obj
    return obj.get(path, default)


def _extract_iperf3_mbps(raw: dict) -> float | None:
    """Extract sender Mbps from iperf3 JSON output.

    iperf3 -J emits a nested structure; the sender's bits_per_second
    lives at `end.sum_sent.bits_per_second`.
    """
    try:
        bps = raw["end"]["sum_sent"]["bits_per_second"]
        return bps / 1_000_000.0
    except (KeyError, TypeError):
        return None


# ── Inlined script bodies ───────────────────────────────────────────
#
# These mirror the canonical scripts under bench/load-gen/ but are
# inlined into SSM RunCommand payloads so we don't depend on the
# scripts being pre-deployed on the instances. The canonical files
# stay as the readable / testable source of truth for the script
# logic; this inline form is the wire-format version.

# Issuer broadcast producer — runs on node 0, pipes broadcasts to
# trains-cli's stdin within the same bash subshell. Reads bench knobs
# from env (MESSAGE_COUNT, PAYLOAD_SIZE, BENCH_DURATION_S). After
# sending all messages it sleeps to keep stdin open until killed.
# We base64-encode this at module-load time so it can be embedded in
# the SSM RunCommand without quoting issues.
_ISSUER_PRODUCER_PY = '''\
import os, sys, time
N = int(os.environ["MESSAGE_COUNT"])
SZ = int(os.environ["PAYLOAD_SIZE"])
D = int(os.environ["BENCH_DURATION_S"])
R = max(1.0, N / max(1.0, D))
# Use absolute-deadline timing instead of incremental sleep.
# time.sleep(1/R) has ~50us jitter on Linux, which compounds at high
# rates: at 5000 msg/s (interval 200us), the jitter exceeds the
# interval and rate drifts substantially. Absolute deadlines + a
# short busy-wait under 200us keep rate within ~1%.
PAD = "p" * max(0, SZ - 30)
sys.stderr.write(f"[issuer-producer] starting N={N} SZ={SZ} D={D} rate~={R:.1f}Hz\\n")
sys.stderr.flush()
t0 = time.monotonic()
interval = 1.0 / R
busy_threshold = 0.001  # 1ms: use sleep above, busy-wait below
for i in range(N):
    deadline = t0 + (i * interval)
    now = time.monotonic()
    delta = deadline - now
    if delta > busy_threshold:
        time.sleep(delta - busy_threshold)
    # Busy-wait the last sub-ms for accurate timing.
    while time.monotonic() < deadline:
        pass
    line = f"0:bench:{i:08d}:{time.time_ns()}:" + PAD
    print(line, flush=True)
elapsed = time.monotonic() - t0
achieved_rate = N / elapsed if elapsed > 0 else 0
sys.stderr.write(
    f"[issuer-producer] sent {N} broadcasts in {elapsed:.3f}s "
    f"(achieved={achieved_rate:.0f}Hz target={R:.0f}Hz); holding stdin\\n"
)
sys.stderr.flush()
time.sleep(86400)
'''
_ISSUER_PRODUCER_B64 = base64.b64encode(_ISSUER_PRODUCER_PY.encode()).decode()

# Stdout timestamp prefixer — reads trains-cli's stdout line-by-line
# and prepends a wall-clock nanosecond timestamp. This gives the
# parse_results helper a real recv_ns per DELIVER (vs the parse-time
# approximation we had before, which conflated send-to-parse time
# with actual delivery latency).
# Format: `<recv_ns_int> <original_line>`
_TS_PREFIX_PY = '''\
import sys, time
for line in sys.stdin:
    sys.stdout.write(f"{time.time_ns()} {line}")
    sys.stdout.flush()
'''
_TS_PREFIX_B64 = base64.b64encode(_TS_PREFIX_PY.encode()).decode()


def _inline_node_runner_body() -> str:
    """The body of node_runner.sh, minus the env-var declarations
    (which are exported separately above the script body).

    Substitutes the issuer-producer Python script (base64-encoded) into
    the script body so the producer file lands on the instance with
    zero quoting issues."""
    # We intentionally keep this as a single string so the SSM
    # command stays under the 4KB-per-command limit. The script
    # body itself is small (~1.5 KB).
    body = """
set -euo pipefail
LOG_DIR="/var/log/trains-bench/${RUN_ID}"
mkdir -p "$LOG_DIR" /opt/trains-bench
BINARY="/opt/trains-bench/trains"
if [[ ! -x "$BINARY" ]]; then
    aws s3 cp "s3://${RESULTS_BUCKET}/bin/trains" "$BINARY" --quiet
    chmod +x "$BINARY"
fi
IDENTITY="/opt/trains-bench/identity-${NODE_ID}.json"
if [[ ! -f "$IDENTITY" ]]; then
    aws s3 cp "s3://${RESULTS_BUCKET}/identities/identity-${NODE_ID}.json" "$IDENTITY" --quiet
fi
ISSUE_FLAG=""
if [[ "$ISSUE_INITIAL" == "true" ]]; then
    ISSUE_FLAG="--issue-initial"
fi
STDERR_LOG="${LOG_DIR}/trains-cli.stderr.log"
STDOUT_LOG="${LOG_DIR}/trains-cli.stdout.log"

# Start a sockperf server in the background so the collect step
# can pp-bench us. Port 11111 (sockperf default). Logs to file so
# stderr doesn't mix with trains-cli. The `|| true` lets the bench
# proceed even if sockperf isn't installed (UserData build can
# fail; we shouldn't crash the whole bench).
if command -v sockperf > /dev/null 2>&1; then
    nohup sockperf sr -i 0.0.0.0 -p 11111 \
        > "${LOG_DIR}/sockperf-sr.log" 2>&1 < /dev/null &
    echo "$!" > /tmp/sockperf-sr.pid
fi

# Design (2026-05-23 final): pipe a producer directly to trains-cli's
# stdin within a single bash subshell. No FIFO, no sleep-infinity
# writer, no separate issuer_workload SSM round-trip. All process
# state stays inside one nohup'd bash, fully owned by the SSM
# RunCommand worker.
#
# Why FIFO designs failed:
# - `< FIFO`: bash deadlocks at open() until a writer appears.
# - `<> FIFO`: tokio stdin doesn't see Python writes (proven by
#   "stdin closed; exiting" symptom even with held writers).
# - `cat FIFO | trains-cli` + sleep-infinity-writer: SSM agent
#   appears to kill the sleep-infinity-writer at script exit even
#   with nohup; cat sees EOF + trains-cli REPL terminates.
#
# Pipe-from-stdin design:
# - The producer (python on issuer, sleep on non-issuer) runs in the
#   SAME bash subshell as trains-cli, in a single pipeline.
# - trains-cli's stdin is the producer's stdout — a plain pipe
#   tokio handles correctly.
# - Producer holds the pipe write end open for the bench duration
#   (issuer: writes broadcasts + sleeps; non-issuer: just sleeps).
# - When collect_results SIGINTs trains-cli, the broken pipe takes
#   down the producer too.
# Decode the Python producer + stdout timestamp prefixer (base64 avoids
# all SSM/bash quoting hell).
echo "__PRODUCER_B64__" | base64 -d > /tmp/issuer-producer.py
echo "__TS_PREFIX_B64__" | base64 -d > /tmp/ts-prefix.py
PRODUCER=""
if [[ "$IS_BROADCASTER" == "true" ]]; then
    # Broadcaster: python writes broadcasts spread over the bench
    # duration, then sleeps. Reads N/SZ/D from env (already exported).
    # IS_BROADCASTER is distinct from ISSUE_INITIAL — only one node
    # broadcasts (so seq numbers trivially correlate), but NUM_TRAINS
    # nodes issue initial trains (protocol requirement).
    PRODUCER="python3 -u /tmp/issuer-producer.py"
else
    # Non-broadcaster: nothing to write, just keep the pipe open until killed.
    PRODUCER="sleep 86400"
fi

# Launch as a fully-detached subshell. setsid creates a new session
# so the SSM agent's process-group cleanup at script exit doesn't
# reach us. nohup ignores SIGHUP. </dev/null disconnects any
# inherited tty.
#
# Pipeline (inside bash -c so we can insert the timestamper):
#   PRODUCER → trains-cli (stderr → STDERR_LOG) → ts-prefixer → STDOUT_LOG
# The ts-prefixer prepends a real wall-clock timestamp to every line
# trains-cli writes to stdout, giving parse_results accurate recv_ns
# per DELIVER. Without it, the latency report is meaningless (uses
# parse-time as recv_ns, which is many seconds late).
RUST_LOG="trains_core=debug,trains_net=debug,info" \
    setsid nohup bash -c "$PRODUCER | $BINARY node \
        --id $NODE_ID \
        --listen $LISTEN_ADDR \
        --successor $SUCCESSOR_ADDR \
        --identity $IDENTITY \
        --peer-fp $PEER_FINGERPRINTS \
        --delivery-mode ${DELIVERY_MODE:-uto} \
        ${PEER_ADDRS:-} \
        $ISSUE_FLAG \
        2> $STDERR_LOG | python3 -u /tmp/ts-prefix.py > $STDOUT_LOG" \
        > /dev/null 2>&1 </dev/null &
SETSID_PID=$!
# Wait a moment for the bash subshell to spawn the pipeline.
sleep 3
# Find the real trains-cli PID by name (not the wrapper).
TRAINS_REAL_PID=$(pgrep -f "$BINARY node" | head -1)
if [[ -z "$TRAINS_REAL_PID" ]]; then
    echo "FATAL: trains-cli not running after 3s (setsid pid=$SETSID_PID)" >&2
    tail -20 "$STDERR_LOG" >&2 || true
    exit 4
fi
echo "$TRAINS_REAL_PID" > /tmp/trains-cli.pid
echo "node-${NODE_ID} ready, pid=$TRAINS_REAL_PID"
""".strip()
    return (
        body
        .replace("__PRODUCER_B64__", _ISSUER_PRODUCER_B64)
        .replace("__TS_PREFIX_B64__", _TS_PREFIX_B64)
    )


def _inline_issuer_workload(
    *,
    duration_s: float,
    message_count: int,
    payload_size: int,
    output_path: str,
) -> str:
    """Inline Python that drives the load. Mirrors issuer_workload.py."""
    return f"""python3 - <<'PYEOF'
import json, os, secrets, time, sys
from pathlib import Path
pipe = Path("/tmp/trains-cli.stdin")
output = Path({output_path!r})
duration_s = {duration_s!r}
message_count = {message_count!r}
payload_size = {payload_size!r}
start_ns = time.time_ns()
deadline_ns = start_ns + int(duration_s * 1e9)
sent_log = []
with pipe.open("w", buffering=1) as f:
    for seq in range(message_count):
        if time.time_ns() >= deadline_ns:
            break
        prefix = f"0:bench:{{seq:08d}}:{{time.time_ns()}}:"
        pad_len = max(0, payload_size - len(prefix) - 1)
        pad = secrets.token_hex(pad_len // 2 + 1)[:pad_len]
        line = prefix + pad
        send_ns = time.time_ns()
        try:
            f.write(line + "\\n")
            f.flush()
        except (BrokenPipeError, OSError) as exc:
            print(f"FATAL: write failed at seq={{seq}}: {{exc}}", file=sys.stderr)
            sys.exit(3)
        sent_log.append({{"seq": seq, "send_ns": send_ns, "bytes": len(line) + 1}})
elapsed_ns = time.time_ns() - start_ns
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps({{
    "run_start_ns": start_ns,
    "run_end_ns": start_ns + elapsed_ns,
    "elapsed_ns": elapsed_ns,
    "messages_sent": len(sent_log),
    "messages_target": message_count,
    "duration_target_s": duration_s,
    "payload_size": payload_size,
    "send_log": sent_log,
}}))
print(f"sent {{len(sent_log)}} messages in {{elapsed_ns/1e9:.3f}}s")
PYEOF"""


def _inline_parse_results(*, stderr_log: str, node_id: int, output: str) -> str:
    """Inline Python that parses trains-cli's STDOUT for DELIVER lines.

    Note: the `stderr_log` parameter name is historical — we now read
    the STDOUT log (where println! DELIVER lines actually go). Each
    stdout line carries an `<recv_ns> <original_line>` prefix added by
    the ts-prefix helper in the runtime pipeline, so we recover real
    per-message delivery timestamps (not parse-time approximations).
    """
    return f"""python3 - <<'PYEOF'
import json, re, time
from pathlib import Path
stdout_path = Path({stderr_log!r})
output = Path({output!r})
node_id = {node_id!r}
# Each line: "<recv_ns_int> <original trains-cli stdout line>"
LINE_RE = re.compile(r"^(\\d{{16,20}}) (.+)$")
PAYLOAD_RE = re.compile(r"0:bench:(?P<seq>\\d{{1,8}}):(?P<send_ns>\\d{{16,20}}):")
deliveries = []
seen = set()
fallback_recv_ns = time.time_ns()  # only used if ts-prefix wasn't applied
if stdout_path.exists():
    for line in stdout_path.open("r", encoding="utf-8", errors="replace"):
        m_line = LINE_RE.match(line)
        if m_line:
            real_recv_ns = int(m_line.group(1))
            rest = m_line.group(2)
        else:
            real_recv_ns = fallback_recv_ns
            rest = line
        m_pay = PAYLOAD_RE.search(rest)
        if not m_pay:
            continue
        seq = int(m_pay.group("seq"))
        if seq in seen:
            continue
        seen.add(seq)
        deliveries.append({{
            "seq": seq,
            "send_ns": int(m_pay.group("send_ns")),
            "recv_ns": real_recv_ns,
            "node_id": node_id,
            "size": len(rest),
        }})
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps({{
    "node_id": node_id,
    "delivery_count": len(deliveries),
    "deliveries": deliveries,
}}))
print(f"parsed {{len(deliveries)}} deliveries for node {{node_id}}")
PYEOF"""


def _shell_quote(value: str) -> str:
    """Same as coordinator._shell_quote — duplicated here to keep this
    module importable from a fresh Python without bench.coordinator on
    sys.path. The two are kept in sync by `test_orchestrator.py`."""
    return "'" + value.replace("'", "'\\''") + "'"


# ── Top-level run() entry — wired from coordinator.cmd_run ──────────


def run(
    *,
    region: str,
    ring_size: int,
    results_bucket: str,
    coordinator_instance_id: str | None,
    duration_seconds: float,
    message_count: int,
    payload_size: int,
    az_spread: str,
    warmup_count: int = 0,
    delivery_mode: str = "uto",
    dry_run: bool,
    output_dir: Path,
    fault: dict | None = None,
    ssm_orchestrator: SSMOrchestrator | None = None,
    s3_helper: S3Helper | None = None,
    ec2_client: Any = None,
) -> RunReport:
    """Top-level bench-run orchestrator.

    Args:
        region: AWS region (us-east-1).
        ring_size: expected ring size. Aborts if discovered != this.
        results_bucket: S3 bucket name from CDK stack output.
        coordinator_instance_id: EC2 instance id of the coordinator,
            needed for `trains keygen`. In dry-run mode, may be None
            (a placeholder is used).
        duration_seconds, message_count, payload_size: workload params.
        az_spread: "single" or "three" — for the run report only.
        dry_run: if True, every AWS call is mocked at the SSM/S3 helper
            layer; this function still goes through all the orchestration
            steps end-to-end so the wiring is exercised.
        output_dir: local directory to write the run report.
        ssm_orchestrator / s3_helper / ec2_client: dependency-inject for
            tests; live runs construct them from boto3.

    Returns the RunReport (also written to disk + uploaded to S3).
    """
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    config = RunConfig(
        duration_seconds=duration_seconds,
        message_count=message_count,
        payload_size=payload_size,
        ring_size=ring_size,
        results_bucket=results_bucket,
        run_id=run_id,
        region=region,
        az_spread=az_spread,
        warmup_count=warmup_count,
        delivery_mode=delivery_mode,
    )
    log.info("=" * 60)
    log.info("trains-bench run %s — %s", run_id, "DRY RUN" if dry_run else "LIVE")
    log.info("ring=%d az=%s duration=%.1fs", ring_size, az_spread, duration_seconds)
    log.info("=" * 60)

    if ssm_orchestrator is None:
        ssm_orchestrator = SSMOrchestrator(region=region, dry_run=dry_run)
    if s3_helper is None:
        s3_helper = S3Helper(region=region, dry_run=dry_run)

    started_at_ns = time.time_ns()

    # Step 1: discover ring peers
    log.info("[1/7] discovering ring peers")
    if dry_run and ec2_client is None:
        # Synthetic peers — bypass the live EC2 discovery in dry-run mode.
        peers = [
            RingPeer(
                node_id=i,
                instance_id=f"i-dryrun{i:04d}",
                private_ip=f"10.50.1.{10 + i}",
                az="us-east-1a",
            )
            for i in range(ring_size)
        ]
        log.info("  dry-run synthetic peers: %s", [p.instance_id for p in peers])
    else:
        peers = discover_ring_peers(
            region=region, expected_size=ring_size, ec2_client=ec2_client
        )
    log.info("  found %d peers: %s", len(peers), [(p.node_id, p.instance_id) for p in peers])

    coordinator_id = coordinator_instance_id or ("i-dryrun-coord" if dry_run else None)
    if not coordinator_id:
        raise SystemExit(
            "FATAL: --coordinator-instance-id is required for live runs"
        )

    # Step 2: generate per-node TLS identities
    log.info("[2/7] generating %d TLS identities on coordinator %s", ring_size, coordinator_id)
    fingerprints = generate_identities(
        coordinator_instance_id=coordinator_id,
        ring_size=ring_size,
        ssm=ssm_orchestrator,
        s3=s3_helper,
        results_bucket=results_bucket,
    )
    log.info("  fingerprints: %s", {k: v[:8] + "..." for k, v in fingerprints.items()})

    # Step 3: start ring nodes (parallel SSM)
    log.info("[3/7] starting %d ring nodes", len(peers))
    start_cmds = start_ring_nodes(
        peers=peers, config=config, fingerprints=fingerprints,
        ssm=ssm_orchestrator, s3=s3_helper,
    )

    # Phase G: launch the fault injector CONCURRENTLY with active broadcast.
    # The producer (node-0) begins broadcasting during start_ring_nodes, so
    # anchoring the fault HERE makes inject_at_s land mid-circulation. The
    # earlier design ran the fault in the post-broadcast settle (step 5),
    # where it fired AFTER delivery had already completed — verified inert
    # (e.g. fis-kill ran 6 s after node-4 finished delivering). Runs on its
    # own SSM/EC2 clients (boto3 clients are not safe to share across
    # threads); joined before collect.
    fault_spec = faults.FaultSpec.from_dict(fault)
    fault_thread: threading.Thread | None = None
    fault_events: dict = {}
    if fault_spec is not None:
        fault_ssm = SSMOrchestrator(region=region, dry_run=dry_run)
        fault_ec2 = None
        if fault_spec.type == "fis-stop-start" and not dry_run and boto3 is not None:
            fault_ec2 = ec2_client or boto3.client("ec2", region_name=region)
        fault_total = float(fault_spec.inject_at_s + (fault_spec.duration_s or 0) + 2)

        def _fault_runner() -> None:
            try:
                fault_events.update(faults.run_fault_window(
                    fault_spec, total_duration_s=fault_total, peers=peers,
                    ssm=fault_ssm, ec2=fault_ec2, sleep_fn=time.sleep,
                    log_fn=lambda m: log.info("%s", m),
                ))
            except Exception as exc:  # noqa: BLE001
                log.warning("fault thread error: %s", exc)

        fault_thread = threading.Thread(target=_fault_runner, name="fault", daemon=True)
        fault_thread.start()
        log.info(
            "[fault] launched concurrent %s on node-%d (inject +%ds, hold %ds) at broadcast start",
            fault_spec.type, fault_spec.target_node, fault_spec.inject_at_s, fault_spec.duration_s,
        )

    # Step 4: wait for ring formation
    log.info("[4/7] waiting for ring formation (max 60s)")
    wait_for_ring_formation(
        peers=peers, start_command_ids=start_cmds, ssm=ssm_orchestrator,
    )
    # The start command exits once the script reaches "node ready",
    # but `RingTransport::spawn()` only returns AFTER the listen socket
    # is bound. The TCP listener and the outgoing connector spawn into
    # background tokio tasks though, and the OUTGOING connections retry
    # with backoff. If we trigger the issuer immediately, the issuer's
    # first connect attempt to its successor may race the successor's
    # listener bind → ConnectionRefused → trains-net's retry backoff
    # delays ring formation. A 15s grace gives the retry loop time to
    # converge. Observed 2026-05-23 attempt #4: issuer printed only
    # "connect failed; will retry" once, then ring never formed.
    grace_s = 15.0
    log.info("  ring start commands complete; sleeping %.0fs for listeners + retries to settle", grace_s)
    time.sleep(grace_s)

    # Step 4b: pre-bench NIC counter snapshot (instrumentation, best-effort)
    # Captures byte/packet counters BEFORE broadcast traffic starts.
    # Pairs with the post-snapshot after the in-bench settle to compute
    # actual bytes-on-wire — validates Phase D's 2-lap model with
    # measured ground truth and exposes the SRD share of TX traffic.
    log.info("[4b/7] capturing pre-bench NIC counters on %d nodes", len(peers))
    try:
        pre_cmds = capture_nic_snapshots(
            peers=peers, config=config, phase="pre", ssm=ssm_orchestrator,
        )
        for peer in peers:
            cmd_id = pre_cmds.get(peer.node_id)
            if cmd_id:
                ssm_orchestrator.wait_for_command(
                    command_id=cmd_id, instance_id=peer.instance_id, max_wait_s=30,
                )
    except Exception as exc:  # noqa: BLE001
        log.warning("pre-bench NIC capture skipped: %s", exc)

    # Step 5: trigger issuer workload (synchronous)
    issuer = peers[0]
    log.info("[5/7] triggering issuer workload on node-%d (%s)", issuer.node_id, issuer.instance_id)
    issuer_cmd = trigger_issuer_workload(
        issuer=issuer, config=config, ssm=ssm_orchestrator,
    )
    issuer_inv = ssm_orchestrator.wait_for_command(
        command_id=issuer_cmd, instance_id=issuer.instance_id,
        max_wait_s=int(duration_seconds) + 90,
    )
    if issuer_inv.get("Status") != "Success":
        log.error("issuer workload failed: %s", issuer_inv.get("StandardErrorContent", "")[:500])

    # The issuer_workload Python script writes all N messages to the
    # FIFO in milliseconds (kernel pipe buffer absorbs them), then
    # exits. We must wait for trains-cli to actually drain the FIFO
    # and circulate broadcasts through the ring before killing it.
    # Observed 2026-05-23: without this sleep, all nodes reported 0
    # deliveries because trains-cli was SIGINT'd immediately after
    # the issuer python script returned (~10 ms after sending 1000
    # messages).
    log.info(
        "[5/7] issuer-workload returned; sleeping %.0fs for trains-cli to "
        "process broadcasts before collect",
        duration_seconds,
    )
    time.sleep(duration_seconds)

    # Step 5b: post-bench NIC counter snapshot — paired with step 4b.
    # Diff is computed in aggregate_from_s3 below.
    log.info("[5b/7] capturing post-bench NIC counters on %d nodes", len(peers))
    try:
        post_cmds = capture_nic_snapshots(
            peers=peers, config=config, phase="post", ssm=ssm_orchestrator,
        )
        for peer in peers:
            cmd_id = post_cmds.get(peer.node_id)
            if cmd_id:
                ssm_orchestrator.wait_for_command(
                    command_id=cmd_id, instance_id=peer.instance_id, max_wait_s=30,
                )
    except Exception as exc:  # noqa: BLE001
        log.warning("post-bench NIC capture skipped: %s", exc)

    # Ensure the concurrent fault window finished (inject + clear) before we
    # collect — otherwise a transient fault could still be active at collect.
    if fault_thread is not None:
        join_budget = float(fault_spec.inject_at_s + (fault_spec.duration_s or 0) + 30)
        fault_thread.join(timeout=join_budget)
        if fault_thread.is_alive():
            log.warning("[fault] thread still active at collect; proceeding anyway")
        else:
            log.info("[fault] window complete: %s", fault_events)

    # Step 6: collect results (kill trains-cli, parse, iperf3, upload)
    log.info("[6/7] collecting results from %d nodes", len(peers))
    collect_cmds = collect_results(
        peers=peers, config=config, ssm=ssm_orchestrator,
    )
    for peer in peers:
        ssm_orchestrator.wait_for_command(
            command_id=collect_cmds[peer.node_id], instance_id=peer.instance_id,
        )

    # Step 7: aggregate from S3 + write report
    log.info("[7/7] aggregating from S3 + writing report")
    ended_at_ns = time.time_ns()
    report = aggregate_from_s3(
        peers=peers, config=config, s3=s3_helper,
        started_at_ns=started_at_ns, ended_at_ns=ended_at_ns,
    )

    # Write local + upload to S3
    output_dir.mkdir(parents=True, exist_ok=True)
    md_path = output_dir / f"run-{run_id}.md"
    json_path = output_dir / f"run-{run_id}.json"
    md_path.write_text(report.to_markdown())
    json_path.write_text(json.dumps(report.to_dict(), indent=2, default=str))
    log.info("  local report: %s", md_path)

    s3_helper.put_object(
        bucket=results_bucket,
        key=f"{config.s3_results_prefix()}/run-{run_id}.md",
        body=md_path.read_bytes(),
    )
    s3_helper.put_object(
        bucket=results_bucket,
        key=f"{config.s3_results_prefix()}/run-{run_id}.json",
        body=json_path.read_bytes(),
    )

    log.info("=" * 60)
    log.info("run %s complete — %s", run_id, "✅ success" if report.success else "❌ failed")
    if report.failure_reason:
        log.info("failure_reason: %s", report.failure_reason)
    log.info("=" * 60)
    return report
