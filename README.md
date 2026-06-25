# trains-rust

**TRAINS is a control-plane total-order broadcast primitive** — the same family
as etcd and ZooKeeper, not a data-plane replication engine. A small group of
nodes agree on one order for a stream of small, order-critical messages (config,
membership, leases, locks, coordination events) and apply them identically, so
the group behaves like a single consistent machine. It is **consistency-first**:
under a partition it halts rather than diverge (the CP corner of CAP) — the
correct stance for a control plane, where split-brain is catastrophic. Unlike
Paxos and Raft there is no leader: the right to order travels a logical ring as
a circulating "train," so every node does equal work.

What makes this implementation unusual is not speed — it is **how little code it
is and how thoroughly it is checked**: a **~2,360-line protocol kernel**
checked seven independent ways (TLA+/TLC, Apalache symbolic checking, Kani/CBMC
bounded model checking of the Rust, PropTest fuzzing with crash injection,
differential random testing against a reference impl, runtime trace
validation, and Ivy parameterised verification at unbounded N for the abstract
delivery semantics), with the spec, the reference implementation, and the tests
all in this repo. To our knowledge it is the smallest and most-verified
total-order-broadcast core in open source, and the only ring-based one that
ships a machine-checked spec.

| | trains-rust kernel | etcd raft | typical Raft impls | larger frameworks |
|---|---|---|---|---|
| Core LOC | **~2,360** | ~6,150 | ~11k | up to ~50k |
| In-repo formal verification | **7 methods** | TLA+ + traces | none | none |

It also has a thirty-year control-plane pedigree: invented for **power-plant
control supervision** at Cegelec/Alcatel in the early 1990s — advised by Flaviu
Cristian, patented, now public domain — and revived academically by Michel
Simatic. The modern incarnation is the `trains-ao` adapter (ordered control
events for an agent-orchestrator control plane). See [History](#history) below.

> **Not** for bulk data-plane throughput. TRAINS optimises small-message,
> totally-ordered, consistency-first coordination on small clusters — see
> [`docs/paper-benchmarks.md`](docs/paper-benchmarks.md) for the honest envelope.

Companion repo: **[trains-valkey](https://github.com/yeychenne/trains-valkey)**
— control-plane-grade HA: it uses this protocol to give Valkey/Redis the
loss-free failover its native Sentinel story sacrifices.

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
- [`docs/blog-simatic.md`](docs/blog-simatic.md) — Michel Simatic's academic continuation + industrial-automation framing
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
