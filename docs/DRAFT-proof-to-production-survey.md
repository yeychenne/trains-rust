# DRAFT — How big are open-source consensus implementations, and is the proof in the box?

**Status: DRAFT (2026-06-22).** A short comparative survey. Two questions:
(1) how many lines of code is a production implementation of a consensus or
group-communication protocol, and (2) when the protocol has a *formal proof*,
does that proof ship with — and correspond to — the open-source code you would
actually deploy, or does it live in a separate research artifact?

This is a draft: the line counts are a single-tool, raw measurement (see
*Method*), the "formal artifact" column reflects what is in each repository as
of mid-2026, and the correspondence discussion is deliberately careful because —
as anyone who has tried it knows — **correlating a proof to the exact deployed
code is the hard part**, and most projects do not close that gap.

---

## 1. The numbers

Raw line counts of the **core implementation** (non-test source) and of any
**formal artifact co-located in the same repository**, measured with `wc -l` on
shallow clones, 2026-06-22.

| Implementation | Family | Lang | Core impl LOC | Formal artifact **in the same repo** |
|---|---|---|---:|---|
| [etcd-io/raft](https://github.com/etcd-io/raft) | Raft | Go | 6,153¹ | **Yes — TLA+ spec (1,523 LOC) + model-based trace validation** |
| [hashicorp/raft](https://github.com/hashicorp/raft) | Raft | Go | 10,776 | No |
| [tikv/raft-rs](https://github.com/tikv/raft-rs) | Raft | Rust | 11,006 | No |
| [datafuselabs/openraft](https://github.com/datafuselabs/openraft) | Raft | Rust | 50,537 | No |
| [hashicorp/memberlist](https://github.com/hashicorp/memberlist) | Gossip (SWIM) | Go | 6,556 | No |
| [Tencent/phxpaxos](https://github.com/Tencent/phxpaxos) | Paxos | C++ | 22,324 | No |
| **trains-rust (core kernel)** | TRAINS ring TOB | Rust | **2,360** | **Yes — TLA+ (930) + Ivy (54) + DRT/reference (486), co-located** |
| **trains-rust (full impl)**² | TRAINS ring TOB | Rust | ~7,003 | same |

¹ etcd-io/raft root package, non-test; ~10,051 including its `confchange`,
`quorum`, `tracker` subpackages. ² trains-rust core kernel + net + recovery +
cli + ao.

**First-order observations.**

- Production consensus is **6k–50k lines** of code you must trust. Raft
  implementations cluster around 6k–11k (etcd, hashicorp, raft-rs); a full
  *framework* like openraft is 50k. A production Paxos (phxpaxos, used in
  WeChat) is ~22k of C++. Gossip/SWIM (memberlist) is ~6.5k.
- The TRAINS **protocol kernel is ~2.4k LOC** — the small end — because the
  ring design pushes complexity onto the topology rather than a leader-election
  + log-reconciliation state machine. That small surface is *why* exhaustive
  model checking and a line-comparable reference implementation are tractable.

### 1.1 The broader landscape: who ships a spec with the code

Widening beyond consensus *libraries* to the production systems that actually
run total-order broadcast, replication, and group communication, the
formal-methods picture sorts into a few bands (LOC measured where cheap; "—" =
large, not measured for this draft):

| System | Family | Lang | Formal artifact **in-repo** | Notes / source |
|---|---|---|---|---|
| [etcd-io/raft](https://github.com/etcd-io/raft) | Raft | Go | **TLA+ + trace validation** | spec checks *running* traces |
| [mongodb/mongo](https://github.com/mongodb/mongo) | Raft-like replication | C++ | **TLA+** (`src/mongo/tla_plus/`) | replication + sharding/reconfig specs |
| [apache/zookeeper](https://github.com/apache/zookeeper) | ZAB atomic broadcast | Java | **TLA+** (`zookeeper-specifications/`) | `Zab.tla` + a system spec |
| [corosync/corosync](https://github.com/corosync/corosync) | **Totem ring TOB** + membership | C | none | ~15.8k LOC totem; Totem's proofs are in 1990s papers, not the repo |
| [simatic/TrainsProtocol](https://github.com/simatic/TrainsProtocol) | **TRAINS ring TOB** | C | none | ~5.1k LOC; the canonical academic TRAINS |
| **trains-rust** | **ring TOB** | Rust | **TLA+ + Ivy + DRT** | 2.4k-LOC kernel; this repo |
| [apache/kafka](https://github.com/apache/kafka) (KRaft) | Raft variant | Java/Scala | not found in-repo | external KRaft TLA+ models exist |
| [tigerbeetle](https://github.com/tigerbeetle/tigerbeetle) | Viewstamped Replication | Zig | none | no published mechanical proof; heavy deterministic-simulation testing |
| [redis](https://github.com/redis/redis) Sentinel | HA / failover | C | **none** | ~5.5k LOC `sentinel.c`; Jepsen measured acked-write loss (below) |

Three things stand out.

1. **A small but real cohort ships a formal spec *in the same repository* as the
   code — etcd-raft, MongoDB, ZooKeeper.** etcd goes further and ties the spec to
   the *running* code via trace validation. This is the standard worth holding up.
2. **In the ring-based total-order-broadcast family specifically — TRAINS's own
   family — none of the prior production implementations ship a formal spec.**
   Corosync's Totem (~15.8k LOC) and Simatic's TrainsProtocol (~5.1k LOC) are
   battle-tested but unspecified in-repo; the Totem protocol's correctness lives
   in 1990s papers. To our knowledge `trains-rust` is the first ring-TOB
   implementation to co-locate a machine-checked spec, a reference
   implementation, and differential testing.
3. **At the other end, the system most people run for Redis HA — Sentinel — has
   no formal model at all,** and its safety has been characterised *empirically*
   as **losing** acknowledged writes: Jepsen's "Call me maybe: Redis" recorded
   1,126 of 1,998 acknowledged writes discarded in a single partition — by
   design (asynchronous replication), not as a bug
   ([Jepsen](https://aphyr.com/posts/283-jepsen-redis);
   [Redis docs](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)).
   This is the gap `trains-valkey` targets.

---

## 2. Is the proof in the box?

There is a spectrum between "the algorithm has been proven somewhere" and "this
repository's code is proven."

**(a) Proof in a separate research artifact (the common case).** The strongest
proofs of these protocols exist, but as *distinct* verified codebases, not the
production libraries:

- **Verdi Raft** ([uwplse/verdi-raft](https://github.com/uwplse/verdi-raft),
  Wilcox et al., PLDI 2015) — Raft verified in Coq, extracted to OCaml, runnable
  as a key-value store (`vard`) "along the lines of etcd." It is a real verified
  implementation — but it is **not** the Go code in etcd or hashicorp/raft that
  the world actually runs.
- **IronFleet / IronRSL** ([microsoft/Ironclad](https://github.com/microsoft/Ironclad),
  SOSP 2015) — a Paxos-based replicated state machine verified in Dafny with Z3,
  via TLA-style refinement. Again a research artifact, not phxpaxos.

So for Raft and Paxos the honest statement is: *the protocol family has
machine-checked proofs; the open-source library you deploy is, with one
exception below, not the proven artifact.* The proof and the production code are
two different programs that are believed to implement the same algorithm.

**(b) Formal model co-located with production code (rare, but a real cohort).**
A handful of production systems ship a TLA+ spec *in the same repository* as the
code: **MongoDB** (`src/mongo/tla_plus/` — replication and sharding/reconfig),
**ZooKeeper** (`zookeeper-specifications/` — a ZAB spec and a system spec), and
**etcd-raft**. [etcd-io/raft](https://github.com/etcd-io/raft) goes one step
further: alongside a TLA+ spec of *its own* algorithm — "including the
distinctive behaviors like membership reconfiguration that differentiate it from
the classic Raft algorithm" — it ships **model-based trace validation**
(`Traceetcdraft.tla`): the running Go implementation emits a trace, and TLC
checks that trace against the spec. None of these make the production code a
Coq-extracted proof, but they are real, maintained specs living with the code —
and etcd's trace validation is a runtime-checked correspondence between spec and
production code that almost nobody else has.

**(c) trains-rust.** This repository co-locates the TLA+/Ivy spec, a
line-comparable **reference implementation**, and a **differential random
testing** harness that feeds identical inputs to the production kernel and the
reference and asserts identical output, plus a runtime **trace validator** that
re-checks the spec invariants on live traces (the same idea as etcd's
`Traceetcdraft`). With a 2.4k-LOC kernel the spec↔code distance is short enough
that this is maintainable. It is not a refinement proof (the gap in §3 remains),
but proof, reference, and production code live and evolve together.

---

## 3. Why the correspondence is the hard part (and a metric worth having)

Even where a proof exists, three gaps separate "proven" from "what runs":

1. **Language gap.** Verdi proves Coq, extracts OCaml; IronFleet proves Dafny.
   The deployed etcd is Go. A proof about the extracted/refined program is not
   automatically a proof about an independent reimplementation.
2. **Model gap.** A TLA+ spec (etcd's, the Raft dissertation's, TRAINS's)
   models the *algorithm*. It abstracts away the wire format, the scheduler,
   the memory model — exactly where real bugs also live. TLC/Apalache check the
   model; they do not check the binary.
3. **Drift gap.** Production code changes faster than specs. Without an
   automated link (trace validation, DRT), a spec proven once silently
   decorrelates from the code over time.

This suggests a metric the field does not routinely report and that this draft
proposes collecting: **for each production protocol implementation, (i) core
LOC, (ii) whether a formal model lives in the same repository, and (iii)
whether there is an *automated, maintained* link from the running code to that
model** (trace validation, differential testing, or extraction). On that third
axis the population is small: etcd-raft (trace validation) and trains-rust
(DRT + trace validation) are the in-sample examples; most others score "model
elsewhere, link manual or none."

---

## 4. Threats to validity / to-confirm

- **Raw line counts.** `wc -l` counts comments and blanks; it is not SLOC. A
  follow-up should re-measure with `tokei`/`scc` and report code-only lines.
  Treat every number here as order-of-magnitude.
- **"Core" is a judgment call.** Each repo draws the algorithm/library boundary
  differently (etcd splits subpackages; openraft is a framework with runtimes,
  stores, examples). The table uses the primary source directory and excludes
  tests; reasonable people would draw some lines differently.
- **Snapshot.** Measured on shallow clones on 2026-06-22/23; upstreams move.
  LOC for the §1.1 systems (MongoDB, ZooKeeper, Kafka, TigerBeetle) were not
  measured (large, multi-purpose repos); only the directly-comparable cores
  (Sentinel `sentinel.c` ~5.5k, Corosync totem ~15.8k, simatic/TrainsProtocol
  ~5.1k) were counted.
- **In-repo TLA+ claims** for MongoDB (`src/mongo/tla_plus/`) and ZooKeeper
  (`zookeeper-specifications/`) are from GitHub code search (file paths
  confirmed). **Kafka KRaft** has published TLA+ models, but none were located
  in the apache/kafka repo by this search — treat "not found in-repo" as exactly
  that. **TigerBeetle** ships no TLA+ but is known for extensive
  deterministic-simulation testing.
- **Verification claims** for Verdi and IronFleet are from their papers/repos
  and are well established; the etcd TLA+/trace-validation claim is from the
  spec and README in `etcd-io/raft/tla/` (inspected directly). The "no in-repo
  formal artifact" entries mean *none found in the repository*, not that no
  external proof of that algorithm exists.

---

## 5. Takeaway (draft)

Production consensus and broadcast is 5k–50k lines of trusted code. A small
cohort ships a formal spec in the same repo — etcd-raft, MongoDB, ZooKeeper —
and etcd alone ties that spec to the running code with trace validation. Most
proofs, though, live in separate research artifacts (Verdi, IronFleet) or in
papers whose correspondence to the deployed binary is informal; and the system
most people run for Redis HA, Sentinel, has no formal model and is empirically
known to lose acknowledged writes.

Two findings are worth carrying out of this draft. First, the interesting,
rarely-reported metric is not "is the algorithm proven" but "is there a
*maintained, automated link* from the code that runs to the model that was
checked" — on that axis the population is tiny (etcd-raft, trains-rust). Second,
in the ring-based total-order-broadcast family specifically — Totem/Corosync,
Simatic's TrainsProtocol, and TRAINS — `trains-rust` appears to be the **first**
to co-locate a machine-checked spec, a reference implementation, and
differential testing. A 2.4k-LOC kernel is what makes that maintainable: the
proof travels with the code.

*Sources: repositories linked inline; Défago, Schiper & Urbán, "Total Order
Broadcast … Taxonomy and Survey" (ACM CS 2004); Wilcox et al., "Verdi" (PLDI
2015); Hawblitzel et al., "IronFleet" (SOSP 2015); Kingsbury, "Call me maybe:
Redis" (Jepsen, 2013); etcd-io/raft, mongodb/mongo, apache/zookeeper specs
(paths confirmed by code search).*
