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

**(b) Formal model co-located with production code (rare, exemplary).**
[etcd-io/raft](https://github.com/etcd-io/raft) ships, in-repo, a TLA+ spec of
*its own* algorithm — "including the distinctive behaviors like membership
reconfiguration that differentiate it from the classic Raft algorithm" — plus
**model-based trace validation** (`Traceetcdraft.tla`): the running Go
implementation emits a trace, and TLC checks that trace against the spec. This
does not make the Go code a Coq-extracted proof, but it is a real, maintained,
runtime-checked correspondence between spec and production code — and it is the
exception, not the rule.

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
- **Snapshot.** Measured on shallow clones on 2026-06-22; upstreams move.
- **Verification claims** for Verdi and IronFleet are from their papers/repos
  and are well established; the etcd TLA+/trace-validation claim is from the
  spec and README in `etcd-io/raft/tla/` (inspected directly). The "no in-repo
  formal artifact" entries mean *none found in the repository*, not that no
  external proof of that algorithm exists.

---

## 5. Takeaway (draft)

Production consensus is 6k–50k lines of trusted code. Proofs of these protocols
exist but, with the notable exception of etcd-raft's in-repo TLA+ + trace
validation, live in separate research artifacts (Verdi, IronFleet) or in papers
whose correspondence to the deployed binary is informal. The interesting, rarely
reported metric is not "is the algorithm proven" but "is there a *maintained,
automated link* from the code that runs to the model that was checked." TRAINS
is built to score on that axis: a small kernel, a co-located spec, a reference
implementation, and differential testing — the proof travels with the code.

*Sources: repositories linked inline; Wilcox et al., "Verdi" (PLDI 2015);
Hawblitzel et al., "IronFleet" (SOSP 2015); etcd-io/raft `tla/` spec + README.*
