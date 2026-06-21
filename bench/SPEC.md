# TRAINS-bench — Specification

**Status:** v0.1 (Phase-2 authoring).
**Owner:** Operator.
**System under test:** `yeychenne/trains-rust` — Rust workspace
implementing the TRAINS consensus protocol (Simatic et al., CFIP 2015)
with TLA+-verified ring TLS/QUIC transport.

## Goal

Measure the **network-level behaviour** of a TRAINS ring deployed on
EC2 — round-trip latency and sustained throughput — under controlled
ring sizes and AZ topologies, as a function of:

| Variable | Values |
|---|---|
| Ring size | 3, 5, 10, 15 (operator-staged; first run = 3) |
| AZ spread | single-AZ (Cluster placement) OR 3-AZ (Spread placement) |
| Instance type | `t4g.micro` for ring 3; allow `c7g.large` for ring 15 |
| Payload size | 1 KiB (fixed; not a sweep variable in v0.1) |

## Acceptance criteria (per-run)

A bench run is **successful** if all of the following hold:

| ID | Criterion | Source |
|---|---|---|
| AC-1 | All N ring nodes report `node-ready` within 60 s of `cdk deploy` | SSM Run Command timeout |
| AC-2 | The issuer node successfully broadcasts ≥ 1000 messages in 30 s | issuer log |
| AC-3 | Every ring node delivers every broadcast (UTO completeness) | per-node delivery log |
| AC-4 | Per-message round-trip latency JSON lands in S3 from every node | S3 object count == N |
| AC-5 | iperf3 baseline TCP throughput recorded (sanity check vs TRAINS) | per-node iperf3 log |
| AC-6 | Teardown clean — `aws ec2 describe-instances --filters tag:Project=trains-bench` returns 0 running instances within 5 min of `cdk destroy` | post-teardown query |

## Expected values (first single-AZ ring-3 run)

These are **hypotheses** to be confirmed or refuted by the first run.
The bench reports actuals; the operator updates this section after run 1.

| Metric | Hypothesis (single-AZ, ring 3, t4g.micro) | Hypothesis (3-AZ, ring 3, t4g.micro) |
|---|---|---|
| TRAINS RTT p50 | < 2 ms | 4–10 ms (3-5× single-AZ) |
| TRAINS RTT p99 | < 5 ms | 10–25 ms |
| iperf3 throughput | ≥ 1 Gbps (t4g.micro burst) | ≥ 1 Gbps |
| TRAINS broadcasts/sec sustained | ≥ 1000 (1 KiB messages) | ≥ 500 |
| Single-run wall-clock | < 5 min | < 5 min |
| Single-run spend (3 nodes + coord) | < $0.10 | < $0.15 |

## Out-of-scope (v0.1)

- Multi-region (us-east-1 only).
- Payload-size sweep (fixed at 1 KiB).
- Protocol correctness re-verification (TLC + Kani harnesses live
  upstream in the TRAINS repo; this bench is a network-behaviour
  measurement, not a correctness check).
- Encryption-overhead isolation (TLS is always on; no plaintext
  comparison).
- Sustained > 5 min runs (the bench is sized for fast iteration).

## Budget guardrail

The first run is bounded by:

- **Ring size ≤ 5** — `cdk deploy -c ringSize=N` rejects N > 5 without
  an explicit `-c budgetOverride=true` flag.
- **Instance type ≤ c7g.large** — same enforcement.
- **$5 absolute ceiling per run** — the coordinator queries
  `aws ce get-cost-and-usage` before each run and aborts if today's
  spend exceeds the daily cap.

Larger configurations (ring 10, ring 15, c7g.4xlarge, etc.) require an
operator decision and an explicit `-c budgetOverride=true` flag.

## Definition of Done

- [ ] Bench harness authored (Python coordinator + per-node driver).
- [ ] CDK app authored, `cdk synth` succeeds in single-AZ / ring 3.
- [ ] Coordinator unit tests pass (mocked boto3, no live AWS).
- [ ] Operator runs `cdk deploy -c ringSize=3 -c azSpread=single` once
      and captures the first run report at `bench/results/run-<ts>.json`.
- [ ] Teardown verified clean (AC-6).
- [ ] First single-AZ result recorded in the "Expected values" section
      above as actuals.

## References

- TRAINS source: `yeychenne/trains-rust`
- Protocol paper: Simatic et al., *Trains: a Fast Real-Time Consensus
  Protocol*, CFIP 2015 (IEEE 7293477).
- AWS placement-group docs:
  https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html
- Graviton (T4G / C7G) network performance:
  https://docs.aws.amazon.com/ec2/latest/instancetypes/gp.html
