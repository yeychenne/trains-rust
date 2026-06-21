# trains-rust

**TRAINS** is a uniform total-order broadcast protocol: a group of machines
agree on one order for a stream of operations and apply them identically, so the
group behaves like a single machine that doesn't lose state when nodes fail.
Unlike Paxos and Raft there is no leader — the right to order travels a logical
ring as a circulating "train," so every node does equal work. This is a Rust
implementation, formally verified in TLA+, Apalache (inductive invariants), and
Ivy, and extended with online node rejoin.

It also has a thirty-year history: invented for power-plant control supervision
at Cegelec/Alcatel in the early 1990s — advised by Flaviu Cristian, patented,
now public domain — and revived academically by Michel Simatic. See
[History](#history) below.

Companion repo: **[trains-valkey](https://github.com/yeychenne/trains-valkey)**
— uses this protocol to give Valkey/Redis the durability its native
Sentinel HA story sacrifices.

## What's here

| Crate | What it provides |
|---|---|
| `trains-core` | The protocol kernel — a sync, I/O-free state machine implementing uniform total-order broadcast over a token ring. |
| `trains-net` | TLS ring transport (rustls + the ring crypto backend; no OpenSSL). SPKI fingerprint pinning on both sides. |
| `trains-recovery` | Failure detector (◇S; clock-gap hints + successor-unreachable signal) and view-change state machine — Schneider-style Gather / Compute / Install tokens for **exclude**, and the symmetric **re-admit** tokens that return a recovered node to the ring (v3). |
| `trains-cli` | Production CLI for running a node. |
| `trains-ao` | JSON-envelope adapter for embedding `trains-core` inside an Agent Orchestrator topology. Transport-agnostic. |
| `verification/` | TLA+ specification, Apalache inductive-invariant proofs, Ivy specification, and a Rust differential-testing reference implementation. |
| `benches/` | Standalone protocol-level benchmarks (release-mode bin tests; not criterion). |

## Properties

- **Uniform total order.** Every alive node delivers the same set of messages in the same order, even when some senders crash mid-ring.
- **Crash masking.** A confirmed permanent crash triggers a distributed view change; the ring re-forms around the survivors with no operator intervention.
- **Node re-integration (rejoin + re-admission).** A failed-then-restarted node rejoins: first as a passive replica that catches up from a survivor's snapshot + contiguous delivered-effect tail (v2), then — via a *virtually-synchronous re-admit view change* (the mirror of exclude) — back to a full **acking** member, restoring N-redundancy (v3). See [`docs/WHITEPAPER-rejoin-and-readmission-2026-06-16.md`](docs/WHITEPAPER-rejoin-and-readmission-2026-06-16.md) and [`docs/ADR-001-rejoin-virtual-synchrony-2026-06-15.md`](docs/ADR-001-rejoin-virtual-synchrony-2026-06-15.md).
- **Formally verified core.** TLA+ + Apalache inductive invariants prove safety + liveness of the protocol kernel — including the `ReAdmit` action, TLC-checked at 6.28 M states. See `VERIFICATION_REPORT.md`.

## Build + test

```bash
cargo test --workspace
```

Expected: ~120-130 tests pass, 0 failed.

## Run a node

```bash
cargo build --release -p trains-cli
./target/release/trains node \
    --id 0 --listen 0.0.0.0:7000 --successor <next-ip>:7000 \
    --identity /path/to/id0.json --peer-fp <fp0,fp1,fp2> \
    --delivery-mode to --issue-initial
```

For an end-to-end bench on EC2, see `bench/README.md`.

## Paper + blog

The original TRAINS paper + blog live under `docs/`:

- [`docs/paper.md`](docs/paper.md) — protocol paper
- [`docs/blog.md`](docs/blog.md) — engineering blog
- [`docs/blog-simatic.md`](docs/blog-simatic.md) — Siemens / industrial-automation framing
- [`docs/diagrams.md`](docs/diagrams.md) — protocol diagrams
- [`docs/paper-benchmarks.md`](docs/paper-benchmarks.md) — benchmark paper: throughput, scaling, and fault behaviour from laptop to Tailscale to EC2
- [`docs/lineage-and-train-protocol.md`](docs/lineage-and-train-protocol.md) — lineage + code-oriented train explanation + Raft/Paxos mapping
- [`docs/DRAFT-proof-to-production-survey.md`](docs/DRAFT-proof-to-production-survey.md) — *draft*: implementation size + proof-to-production correspondence across open-source Raft/Paxos/gossip

The trains-valkey application paper (which uses TRAINS to give Valkey/Redis
loss-free failover) lives in the [trains-valkey](https://github.com/yeychenne/trains-valkey)
repo.

## Verification

```bash
# TLA+ model check
cd verification/tla && tlc TRAINS.tla -workers auto

# Apalache inductive proof (see APALACHE-INDUCTIVE-PLAN.md)
cd verification/tla && apalache-mc check ...

# Ivy spec
cd verification/ivy && ivy_check trains.ivy

# Differential random testing
cargo test -p drt
```

Full report: [`VERIFICATION_REPORT.md`](VERIFICATION_REPORT.md).

## License

MIT — see [`LICENSE`](LICENSE).

## History

TRAINS began in the late-1980s European RACE programme as fault-tolerant
telecom middleware, on advice from **Flaviu Cristian** — whose atomic-broadcast
[Cristian et al. 1995] and processor-group-membership [Cristian 1991] papers are
its foundation, and whose advice gave the project the circulating-train idea
itself. It was industrialised at **Cegelec / Alcatel-Alsthom** in the
early 1990s to keep power-plant control supervisors (the Cegelec P3200) running
through hardware failure without losing state, and patented — *Method of
broadcasting data by means of a data train*
([US 5,483,520 A](https://patents.google.com/patent/US5483520A/en), Eychenne &
Simatic, Cegelec, 1996), plus a replicated-object layer (US 5,488,723 A). Both
patents are now **expired and in the public domain**. (Tested under network
partition, it halted rather than diverge — the CP corner of the CAP theorem,
eight years before CAP had a name.)

Michel Simatic, a co-inventor, revived it academically (CNAM thesis 2012;
TRAINS, CFIP/NOTERE 2015; BBOBB, DSN 2016) with an open-source implementation.
This repository is a from-the-source Rust reimplementation, formally verified
and extended with online rejoin/re-admission.

See [`docs/lineage-and-train-protocol.md`](docs/lineage-and-train-protocol.md)
for the full lineage, a code-oriented explanation of the train mechanism, and a
mapping to Raft/Paxos abstractions.
