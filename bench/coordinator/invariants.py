"""invariants.py — Phase G chaos invariant checker.

Pure functions over the per-node delivery records the orchestrator
collects (`per_node_deliveries: dict[node_id, list[{"seq", "send_ns", ...}]]`).
No AWS, no I/O — fully unit-testable with synthetic data.

What IS computable from delivery logs alone:
  - **UTO completeness**: every (alive) node delivered every broadcast seq.
  - **Total order**: the delivery ORDER of seqs is identical across all
    nodes, restricted to the seqs they have in common (and no node
    delivers the same seq twice).
  - **No phantom delivery**: no node delivered a seq that was never
    broadcast.

What is NOT computable from delivery logs (reported as `not_measured`,
never silently "passed"):
  - **Liveness recovery time** — needs per-delivery *receive* timestamps
    plus the fault-clear wall-clock, neither of which the delivery
    records currently carry.
  - **Bounded queue depth** — needs trains-cli outbox/pending gauges
    sampled during the run.

See bench/RESEARCH-ROADMAP.md § Phase G for the invariant definitions.
"""

from __future__ import annotations

import dataclasses


@dataclasses.dataclass
class InvariantResult:
    """One invariant's verdict.

    passed:
        True  — invariant held
        False — invariant violated (see detail)
        None  — not measurable from available data (not a pass!)
    """

    name: str
    passed: bool | None
    detail: str


def _seq_lists(per_node_deliveries: dict[int, list[dict]]) -> dict[int, list[int]]:
    return {
        node: [d["seq"] for d in dels]
        for node, dels in per_node_deliveries.items()
    }


def check_uto_completeness(
    per_node_deliveries: dict[int, list[dict]],
    *,
    broadcast_seqs: set[int],
    alive_nodes: set[int] | None = None,
) -> InvariantResult:
    """Every alive node must have delivered every broadcast seq.

    `alive_nodes` lets a scenario exclude a deliberately-killed node from
    the completeness requirement (its absence is expected, not a bug).
    Defaults to every node present in the delivery map.
    """
    seqs = _seq_lists(per_node_deliveries)
    alive = alive_nodes if alive_nodes is not None else set(seqs)

    if not broadcast_seqs:
        return InvariantResult(
            "uto_completeness", None,
            "no broadcast seqs known — cannot judge completeness",
        )

    shortfalls: list[str] = []
    for node in sorted(alive):
        delivered = set(seqs.get(node, []))
        missing = broadcast_seqs - delivered
        if missing:
            shortfalls.append(f"node {node} missing {len(missing)}/{len(broadcast_seqs)}")

    if shortfalls:
        return InvariantResult(
            "uto_completeness", False,
            "; ".join(shortfalls),
        )
    return InvariantResult(
        "uto_completeness", True,
        f"all {len(alive)} alive node(s) delivered every one of "
        f"{len(broadcast_seqs)} broadcasts",
    )


def check_total_order(
    per_node_deliveries: dict[int, list[dict]],
) -> InvariantResult:
    """Deliveries must be consistently ordered across all nodes.

    Implementation: (1) no node may deliver the same seq twice; (2) for
    every pair of nodes, projecting both delivery sequences onto their
    common seq set must yield identical lists. That is exactly the
    condition for the per-node sequences to be extensions of one global
    total order.
    """
    seqs = _seq_lists(per_node_deliveries)

    dup_nodes = [n for n, lst in seqs.items() if len(lst) != len(set(lst))]
    if dup_nodes:
        return InvariantResult(
            "total_order", False,
            f"duplicate deliveries on node(s): {sorted(dup_nodes)}",
        )

    nodes = sorted(seqs)
    for i in range(len(nodes)):
        for j in range(i + 1, len(nodes)):
            a, b = seqs[nodes[i]], seqs[nodes[j]]
            common = set(a) & set(b)
            a_proj = [s for s in a if s in common]
            b_proj = [s for s in b if s in common]
            if a_proj != b_proj:
                return InvariantResult(
                    "total_order", False,
                    f"order mismatch between node {nodes[i]} and node {nodes[j]}",
                )
    return InvariantResult(
        "total_order", True,
        f"delivery order consistent across {len(nodes)} node(s)",
    )


def check_no_phantom(
    per_node_deliveries: dict[int, list[dict]],
    *,
    broadcast_seqs: set[int],
) -> InvariantResult:
    """No node may deliver a seq that was never broadcast."""
    if not broadcast_seqs:
        return InvariantResult(
            "no_phantom", None,
            "no broadcast seqs known — cannot judge phantoms",
        )
    seqs = _seq_lists(per_node_deliveries)
    phantoms: list[str] = []
    for node in sorted(seqs):
        extra = set(seqs[node]) - broadcast_seqs
        if extra:
            sample = sorted(extra)[:5]
            phantoms.append(f"node {node}: {len(extra)} phantom(s) e.g. {sample}")
    if phantoms:
        return InvariantResult("no_phantom", False, "; ".join(phantoms))
    return InvariantResult(
        "no_phantom", True,
        "no node delivered an un-broadcast seq",
    )


def max_progress_stall(
    per_node_deliveries: dict[int, list[dict]],
    *,
    alive_nodes: set[int] | None = None,
) -> dict[int, float]:
    """Largest inter-delivery gap (progress stall) per node, in ms, from
    `recv_ns`.

    This is the recovery / MTTR signal: under a ring-breaking fault,
    delivery progress stalls; the largest gap ≈ the disruption window. A
    fully-*masked* fault leaves the max gap at the normal inter-delivery
    spacing (a few ms); a disruption-then-recovery shows a multi-second
    gap that then closes. Returns {node: stall_ms}; empty if the records
    carry no `recv_ns`.
    """
    nodes = alive_nodes if alive_nodes is not None else set(per_node_deliveries)
    out: dict[int, float] = {}
    for node in sorted(nodes):
        recv = sorted(
            r["recv_ns"] for r in per_node_deliveries.get(node, []) if "recv_ns" in r
        )
        if len(recv) >= 2:
            out[node] = max(recv[i + 1] - recv[i] for i in range(len(recv) - 1)) / 1e6
    return out


def check_all(
    per_node_deliveries: dict[int, list[dict]],
    *,
    broadcast_seqs: set[int] | None = None,
    alive_nodes: set[int] | None = None,
) -> dict[str, InvariantResult]:
    """Run every invariant. Returns name -> InvariantResult.

    `broadcast_seqs` defaults to the union of all delivered seqs — a
    weaker ground truth (it can't catch a phantom delivered by *every*
    node, nor an undelivered-by-all broadcast), so pass the issuer's
    send-log seq set when available for a strict check.
    """
    if broadcast_seqs is None:
        broadcast_seqs = set()
        for dels in per_node_deliveries.values():
            broadcast_seqs |= {d["seq"] for d in dels}

    results = {
        "uto_completeness": check_uto_completeness(
            per_node_deliveries, broadcast_seqs=broadcast_seqs, alive_nodes=alive_nodes,
        ),
        "total_order": check_total_order(per_node_deliveries),
        "no_phantom": check_no_phantom(
            per_node_deliveries, broadcast_seqs=broadcast_seqs,
        ),
        "bounded_queue_depth": InvariantResult(
            "bounded_queue_depth", None,
            "needs trains-cli outbox/pending gauges sampled during run",
        ),
    }

    # Recovery / MTTR signal from recv_ns (measured when timestamps present).
    stalls = max_progress_stall(per_node_deliveries, alive_nodes=alive_nodes)
    if stalls:
        worst_node = max(stalls, key=lambda n: stalls[n])
        worst = stalls[worst_node]
        results["liveness_recovery"] = InvariantResult(
            "liveness_recovery", True,
            f"max delivery-progress stall {worst:.0f} ms (node {worst_node}); "
            f"alive nodes resumed and completed"
            if results["uto_completeness"].passed
            else f"max delivery-progress stall {worst:.0f} ms (node {worst_node}); "
            f"did NOT reach completeness (no full recovery in window)",
        )
    else:
        results["liveness_recovery"] = InvariantResult(
            "liveness_recovery", None,
            "no recv_ns timestamps in delivery records",
        )

    return results
