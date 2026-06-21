#!/usr/bin/env python3
"""TRAINS-bench CDK app entry point.

Synthesises three stacks:
- TrainsBenchResults — S3 bucket for the binary + per-node logs.
- TrainsBenchNetwork — VPC, subnets, security group, placement group.
- TrainsBenchCompute — EC2 ring nodes + coordinator + IAM.

Context variables (set via `cdk deploy -c key=value` or cdk.json):
    ringSize          int     1..15. Number of ring nodes (excludes coordinator). Default 3.
    azSpread          str     "single" or "three". Placement-group + subnet choice. Default "single".
    instanceType      str     t4g.micro / t4g.small / c7g.large / c7g.4xlarge. Default t4g.micro.
    budgetOverride    bool    true to allow ringSize > 5 or instance > c7g.large. Default false.

Budget guardrail (matches `bench/SPEC.md` §Budget guardrail):
    Without budgetOverride=true, ringSize must be ≤ 5 AND instanceType
    must be in {t4g.micro, t4g.small, t4g.medium, c7g.large}. Synth
    aborts with a clear message otherwise.
"""

from __future__ import annotations

import os
import sys

import aws_cdk as cdk

from stacks.compute_stack import TrainsBenchComputeStack
from stacks.network_stack import TrainsBenchNetworkStack
from stacks.results_stack import TrainsBenchResultsStack

# ── Read context with explicit defaults so synth from any directory works ──

app = cdk.App()


def _ctx_str(key: str, default: str) -> str:
    v = app.node.try_get_context(key)
    return default if v is None else str(v)


def _ctx_int(key: str, default: int) -> int:
    v = app.node.try_get_context(key)
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"FATAL: context {key}={v!r} is not an integer: {exc}")


def _ctx_bool(key: str, default: bool) -> bool:
    v = app.node.try_get_context(key)
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    return str(v).lower() in ("true", "1", "yes", "on")


ring_size = _ctx_int("ringSize", 3)
az_spread = _ctx_str("azSpread", "single")
instance_type = _ctx_str("instanceType", "t4g.micro")
# ringInstanceType lets the ring use a beefier network-optimised
# instance (c7gn.8xlarge etc) while the coordinator stays cheap.
# Default = same as instanceType (uniform deploy).
ring_instance_type = _ctx_str("ringInstanceType", instance_type)
enable_ena_express = _ctx_bool("enableEnaExpress", False)
# Compile-time tunable for trains-net's MAX_FRAME_LEN. Default 16 MB.
# Set higher (32–256 MB) on big-NIC bandwidth-saturation runs so the
# per-train batch ceiling isn't a bottleneck. Drives the
# TRAINS_MAX_FRAME_LEN_MB env var in the coordinator's build step.
trains_max_frame_len_mb = _ctx_int("trainsMaxFrameLenMb", 16)
budget_override = _ctx_bool("budgetOverride", False)


# ── Validation ─────────────────────────────────────────────────────

if ring_size < 1 or ring_size > 15:
    raise SystemExit(f"FATAL: ringSize={ring_size} out of range [1, 15]")

if az_spread not in ("single", "three"):
    raise SystemExit(
        f"FATAL: azSpread={az_spread!r} must be 'single' or 'three'"
    )

# Coordinator instance: cheap-by-default. The ring is what matters.
# When the ring is Intel x86_64 (c7i, c6i, m7i…), the coordinator must
# also be x86_64 so its UserData-built binary runs on the ring nodes.
# Adding c7i.large + c6i.large + m7i.large to the allow-list — they're
# cheap enough ($0.085-0.10/hr) that they don't need budgetOverride.
ALLOWED_COORDINATOR_INSTANCES_NO_OVERRIDE = {
    "t4g.micro", "t4g.small", "t4g.medium", "c7g.large", "c7gn.medium",
    "c7i.large", "c6i.large", "m7i.large", "m6i.large",
}
# Ring instance: allow larger network-optimised options without
# override IF enableEnaExpress=true (the whole point is to test SRD,
# and SRD needs big instances). Otherwise stay small.
ALLOWED_RING_INSTANCES_NO_OVERRIDE = ALLOWED_COORDINATOR_INSTANCES_NO_OVERRIDE
ALLOWED_RING_INSTANCES_FOR_ENA_EXPRESS = {
    # The c7gn family is the cheapest path to ENA Express (Graviton,
    # arm64 — reuses our existing binary). Source for supported types:
    # `aws ec2 describe-instance-types --filters
    #     Name=network-info.ena-srd-supported,Values=true`
    "c7gn.8xlarge", "c7gn.16xlarge", "c7gn.metal",
    "c7i.16xlarge", "c7i.24xlarge", "c7i.metal-24xl", "c7i.metal-48xl",
    "c7g.16xlarge", "c7gd.16xlarge",
    "m7i.16xlarge", "m7i.24xlarge", "m7i.48xlarge", "m7i.metal-24xl",
    "m7g.16xlarge",
}
ALLOWED_RING_INSTANCES_WITH_OVERRIDE = (
    ALLOWED_RING_INSTANCES_NO_OVERRIDE
    | ALLOWED_RING_INSTANCES_FOR_ENA_EXPRESS
    | {"c7g.xlarge", "c7g.2xlarge", "c7g.4xlarge"}
)

if not budget_override:
    if ring_size > 5:
        raise SystemExit(
            f"FATAL: ringSize={ring_size} > 5 requires -c budgetOverride=true.\n"
            f"First-bench guardrail (bench/SPEC.md §Budget guardrail)."
        )
    if instance_type not in ALLOWED_COORDINATOR_INSTANCES_NO_OVERRIDE:
        raise SystemExit(
            f"FATAL: coordinator instanceType={instance_type!r} requires -c budgetOverride=true.\n"
            f"Allowed without override: {sorted(ALLOWED_COORDINATOR_INSTANCES_NO_OVERRIDE)}."
        )
    # Ring nodes: allow ENA-Express-required instances IFF
    # enableEnaExpress=true. Otherwise must be small.
    allowed_ring = (
        ALLOWED_RING_INSTANCES_FOR_ENA_EXPRESS | ALLOWED_RING_INSTANCES_NO_OVERRIDE
        if enable_ena_express
        else ALLOWED_RING_INSTANCES_NO_OVERRIDE
    )
    if ring_instance_type not in allowed_ring:
        raise SystemExit(
            f"FATAL: ringInstanceType={ring_instance_type!r} not allowed.\n"
            f"With enableEnaExpress={enable_ena_express}, allowed: "
            f"{sorted(allowed_ring)}.\n"
            f"Pass -c budgetOverride=true to widen the whitelist."
        )
else:
    if instance_type not in ALLOWED_RING_INSTANCES_WITH_OVERRIDE:
        raise SystemExit(
            f"FATAL: coordinator instanceType={instance_type!r} not allowed even with override.\n"
            f"Allowed: {sorted(ALLOWED_RING_INSTANCES_WITH_OVERRIDE)}."
        )
    if ring_instance_type not in ALLOWED_RING_INSTANCES_WITH_OVERRIDE:
        raise SystemExit(
            f"FATAL: ringInstanceType={ring_instance_type!r} not allowed even with override.\n"
            f"Allowed: {sorted(ALLOWED_RING_INSTANCES_WITH_OVERRIDE)}."
        )

# ── Environment ────────────────────────────────────────────────────

# Account/region come from the operator's local config — explicit None
# lets `cdk synth` succeed without AWS credentials (writes to cdk.out
# with a placeholder env). `cdk deploy` requires them to be set.
env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
)

# ── Stacks ─────────────────────────────────────────────────────────

results = TrainsBenchResultsStack(
    app, "TrainsBenchResults",
    env=env,
    description="S3 bucket for trains-bench binary + per-node logs (lifecycle 90d)",
)

network = TrainsBenchNetworkStack(
    app, "TrainsBenchNetwork",
    az_spread=az_spread,
    env=env,
    description=f"VPC + placement group ({az_spread}-AZ) for trains-bench ring",
)

compute = TrainsBenchComputeStack(
    app, "TrainsBenchCompute",
    vpc=network.vpc,
    security_group=network.security_group,
    placement_group_name=network.placement_group.placement_group_name,
    ring_size=ring_size,
    instance_type_str=instance_type,
    ring_instance_type_str=ring_instance_type,
    enable_ena_express=enable_ena_express,
    trains_max_frame_len_mb=trains_max_frame_len_mb,
    results_bucket=results.bucket,
    az_spread=az_spread,
    env=env,
    description=(
        f"Ring of {ring_size} × {ring_instance_type} + coordinator "
        f"({az_spread}-AZ placement); budget_override={budget_override}"
    ),
)
compute.add_dependency(network)
compute.add_dependency(results)

# Stack tags for cost attribution + tear-down verification
for stack in (results, network, compute):
    cdk.Tags.of(stack).add("Project", "trains-bench")
    cdk.Tags.of(stack).add("BenchRingSize", str(ring_size))
    cdk.Tags.of(stack).add("BenchAzSpread", az_spread)

app.synth()
