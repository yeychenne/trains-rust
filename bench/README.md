# TRAINS-bench — operator runbook

> Network-level benchmark for the TRAINS consensus protocol on EC2.
> Authored directly in Claude Code (no AO topology) for fast iteration.
> System under test: [`yeychenne/trains-rust`](https://github.com/yeychenne/trains-rust).

## What this is

A self-contained set of:

- **Spec + architecture docs** — [`SPEC.md`](SPEC.md), [`ARCHITECTURE.md`](ARCHITECTURE.md).
- **Python coordinator** — [`coordinator/coordinator.py`](coordinator/coordinator.py): EC2 peer discovery + SSM-driven bench trigger + result aggregation. 20 unit tests, no live AWS.
- **Per-node bench drivers** — [`load-gen/`](load-gen/): bash + Python scripts that run on each ring EC2 via SSM RunCommand.
- **AWS CDK app** — [`infrastructure/`](infrastructure/): three stacks (Results / Network / Compute) with hard budget guardrails on `ringSize` and instance type.

Total source: ~1.5K lines. Synthesises to ~25 AWS resources for a ring-3 single-AZ deploy.

## Prerequisites

| Tool | Where it's needed | How to get it |
|---|---|---|
| Python 3.10+ | host (coordinator tests, ad-hoc commands) | macOS native; `python3 --version` |
| boto3 | host (live coordinator runs) | `pip install -r coordinator/requirements.txt` |
| AWS CLI v2 | host (deploy + SSM Session) | `brew install awscli`; `aws --version` |
| AWS credentials | host (must be able to `cdk deploy`) | `aws configure` or `~/.aws/credentials` + `AWS_PROFILE` |
| Node 22+ / cdk 2.x | host OR `celery-frontend` container | `npm i -g aws-cdk` on host, OR `podman exec agentorchestrator-celery-frontend-1 cdk --version` (pre-installed) |

The AO `celery-frontend` worker container has cdk + node + aws CLI pre-installed (CLAUDE.md rule #9 ecosystem). You can run all CDK commands there if your host doesn't have them.

## Quick start (Phase-2: synth only, no AWS spend)

```bash
# 1. From repo root — install CDK deps in the container (one-shot):
podman exec agentorchestrator-celery-frontend-1 \
    bash -c "cd /repo/bench/infrastructure && pip install -r requirements.txt"

# 2. Run cdk synth with default context (ringSize=3, single-AZ, t4g.micro):
podman exec -e JSII_SILENCE_WARNING_DEPRECATED_NODE_VERSION=1 \
    agentorchestrator-celery-frontend-1 \
    bash -c "cd /repo/bench/infrastructure && cdk synth -c ringSize=3 -c azSpread=single"

# 3. Verify the budget guardrail (this MUST fail):
podman exec -e JSII_SILENCE_WARNING_DEPRECATED_NODE_VERSION=1 \
    agentorchestrator-celery-frontend-1 \
    bash -c "cd /repo/bench/infrastructure && cdk synth -c ringSize=10 2>&1 | grep FATAL"
# expected: FATAL: ringSize=10 > 5 requires -c budgetOverride=true.

# 4. Run the coordinator unit tests on host (no boto3 needed for these):
python3 -m unittest bench.coordinator.tests.test_coordinator -v
# expected: 20 tests, all OK.
```

No AWS calls happen in any of the steps above. Synth writes to `bench/infrastructure/cdk.out/` only.

## Phase-3: live deploy (operator-authorised, ~$0.50 per run)

Phase 3 lives behind explicit operator action because it spends real
money on EC2 + EBS + S3. Walk-through:

### 1. AWS credentials + region

```bash
export AWS_PROFILE=workshop-profile   # or your IAM identity
export AWS_REGION=us-east-1            # only region tested in Phase 2
aws sts get-caller-identity            # must return your account/arn cleanly
```

If the ARN includes `:user/root` or contains `prod`, STOP — bench against
a sandbox account, not production.

### 2. CDK bootstrap (one-shot per account+region)

```bash
cdk bootstrap aws://<account>/us-east-1
# CDK creates the CDKToolkit stack: ~2 min, ~$0 (just an S3 bucket + IAM role).
```

If already done, this is a no-op.

### 3. Deploy

```bash
cd bench/infrastructure
cdk deploy --all -c ringSize=3 -c azSpread=single
# Approx 5-8 min. Resources: ~25.
# - 1 × VPC + 1 × subnet + 1 × SG + 1 × placement group
# - 4 × interface VPC endpoints (SSM, SSM Messages, EC2 Messages, S3)
# - 1 × S3 bucket (results)
# - 4 × t4g.micro EC2 (1 coordinator + 3 ring nodes)
# - 2 × IAM role + 1 × IAM policy + 2 × InstanceProfile
```

Capture the stack outputs:

```bash
export TRAINS_BENCH_RESULTS_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name TrainsBenchResults \
    --query 'Stacks[0].Outputs[?OutputKey==`ResultsBucketName`].OutputValue' \
    --output text)
export TRAINS_BENCH_COORDINATOR=$(aws cloudformation describe-stacks \
    --stack-name TrainsBenchCompute \
    --query 'Stacks[0].Outputs[?OutputKey==`CoordinatorInstanceId`].OutputValue' \
    --output text)
echo "Results bucket: $TRAINS_BENCH_RESULTS_BUCKET"
echo "Coordinator: $TRAINS_BENCH_COORDINATOR"
```

### 4. Wait for the coordinator's UserData to finish

The coordinator's first boot installs Rust + clones the TRAINS source +
builds `trains-cli` + uploads the binary to S3. Takes 3–5 min. Check:

```bash
aws ssm start-session --target $TRAINS_BENCH_COORDINATOR
# On the coordinator:
ls /var/lib/trains-bench-coordinator-ready    # exists when done
sudo tail -100 /var/log/cloud-init-output.log # if it doesn't exist, look here
exit
```

If the coordinator-ready marker doesn't appear within 10 min, pull the
cloud-init log to S3 for forensics and post-mortem.

### 5. Pre-flight via coordinator.py

```bash
cd bench/coordinator
pip install -r requirements.txt
python3 coordinator.py preflight \
    --region us-east-1 \
    --ring-size 3 \
    --results-bucket $TRAINS_BENCH_RESULTS_BUCKET
# expected: JSON output with bucket_ok=true and 3 ring_peers listed.
```

### 6. Trigger the bench

> ⚠️ The `run` sub-command is **not yet wired** in this PR. Phase-2
> ships the orchestrator skeleton + 20 unit tests + the CDK app.
> Wiring SSM RunCommand calls end-to-end is the next step.

Until then, the bench is triggerable manually by SSH-ing (via SSM) to
the coordinator and running the load-gen scripts directly. See
`ARCHITECTURE.md` § "Bench-control protocol" for the per-step
commands the future `run` sub-command will issue.

### 7. Teardown

```bash
cd bench/infrastructure
cdk destroy --all      # asks for confirmation; ~3 min

# Verify clean teardown:
cd ../coordinator
python3 coordinator.py teardown-check --region us-east-1
# expected: "✅ no instances tagged Project=trains-bench — teardown clean"
```

If any instances remain, list them and decide whether to delete by hand
or re-run `cdk destroy` (CloudFormation sometimes leaves orphans after
network-attached failures).

## Budget guardrails

The CDK app rejects two classes of dangerous deploys without an
explicit `-c budgetOverride=true` flag:

| Variable | Default | Without override | With override |
|---|---|---|---|
| `ringSize` | 3 | max 5 | max 15 (validated by the CDK app range check) |
| `instanceType` | t4g.micro | t4g.{micro,small,medium} + c7g.large | adds c7g.{xlarge,2xlarge,4xlarge} |

A first-bench-on-T4G run (single-AZ, ring 3) is roughly:

| Item | $ |
|---|---|
| 4 × t4g.micro (5 min on-demand) | $0.01 |
| EBS gp3 8 GiB × 4 (1 h pro-rated) | $0.005 |
| S3 (a few MB stored + traffic) | $0.001 |
| Cost Explorer + Describe APIs | $0 |
| **Total per deploy + run + destroy** | **≈ $0.02** |

The $5 absolute ceiling exists for safety, not budget — it accommodates
multiple iterations within a comfortable margin.

## Directory layout

```
bench/
├── README.md                       — this file
├── SPEC.md                         — what to measure + acceptance criteria
├── ARCHITECTURE.md                 — AWS shape + bench-control protocol
├── coordinator/
│   ├── coordinator.py              — orchestrator (CLI: preflight / run / teardown-check / report)
│   ├── requirements.txt            — boto3
│   └── tests/
│       └── test_coordinator.py     — 20 unit tests, no live AWS
├── load-gen/
│   ├── node_runner.sh              — per-ring-node bootstrap (invoked via SSM)
│   ├── issuer_workload.py          — broadcast generator (run on issuer)
│   └── parse_results.py            — stderr → per-message latency JSON
└── infrastructure/
    ├── cdk.json                    — CDK app config + context defaults
    ├── app.py                      — CDK app entry, guardrail validation
    ├── requirements.txt            — aws-cdk-lib + constructs
    └── stacks/
        ├── __init__.py
        ├── results_stack.py        — S3 bucket (versioned, lifecycle 90d)
        ├── network_stack.py        — VPC + subnets + SG + placement group
        └── compute_stack.py        — EC2 ring + coordinator + IAM
```

## Why "Claude Code direct" instead of an AO topology

The first plan was to build a 5-node AO topology (PO → TA →
BenchmarkAgent → DE-Prep → DE-Apply) where AO would author this same
bench rig as a test of AO's bench-authoring capability. That plan is
preserved on the [`claude/feat-ec2-benchmark-phase2-topology`](https://github.com/yeychenne/AgentOrchestrator/tree/claude/feat-ec2-benchmark-phase2-topology)
branch (commit 8f6f267) for a future "AO authors a Rust+CDK project"
test sprint.

For the bench itself, AO authoring would have added 4-8 h of agent
runtime and ~$3-5 in Bedrock spend per iteration, with non-trivial
risk of authoring subtly wrong CDK or harness code (see
[`mistral-failure-investigation-2026-05-22.md`](../aidlc-docs/operations/mistral-failure-investigation-2026-05-22.md)
for why agent-authored code drifts). Direct authoring in Claude Code
is faster, cheaper, and tested-as-you-go.

The AO topology remains a future test artefact, not a dependency of
this bench.

## Phase-3 work-remaining checklist

- [ ] Wire `coordinator.py run` to actually call SSM RunCommand (vs the
      current "FATAL: not yet wired" stub). 4-8 h of work; mostly
      mocking + retry logic.
- [ ] Add a CloudWatch Logs alarm on `cdk-deploy.log` for
      operator notification on bench failures.
- [ ] Add a smoke-only mode (`coordinator.py run --smoke`) that
      verifies the ring forms + delivers 1 message, ~5s wall-clock.
- [ ] First live deploy on 3 × t4g.micro single-AZ — capture the actuals
      into [`SPEC.md`](SPEC.md) §"Expected values".
- [ ] Decision: grow to ring 5 / switch to 3-AZ / stop here.

## References

- TRAINS protocol paper: Simatic et al., *Trains: a Fast Real-Time
  Consensus Protocol*, CFIP 2015 (IEEE 7293477).
- Upstream Rust workspace:
  [yeychenne/trains-rust](https://github.com/yeychenne/trains-rust).
- AWS placement-group docs:
  https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html
- CLAUDE.md rule #9 — IaC is CDK only (no Terraform).
- AO Phase-2 AO-topology variant (preserved):
  branch `claude/feat-ec2-benchmark-phase2-topology` at commit `8f6f267`.
