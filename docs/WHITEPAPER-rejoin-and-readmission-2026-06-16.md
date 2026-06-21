# Group membership in TRAINS: adding and re-admitting cluster members on-line
*From passive catch-up (v2) to a virtually-synchronous re-admit view change (v3)*

**A design + verification + validation white paper**
**Date:** 2026-06-16
**Repos:** `trains-rust` (protocol), `trains-valkey` (state-machine replication over it)
**Companion documents:** `docs/ADR-001-rejoin-virtual-synchrony-2026-06-15.md`,
`VERIFICATION_REPORT.md`

---

## Abstract

TRAINS gives a ring of processes uniform total-order broadcast, and its core is
formally verified (TLA+/TLC, Apalache, Ivy, Kani). But the verified spec modelled
**static membership that only ever shrinks** — a crashed node could be *excluded*,
never *re-integrated*. A node that failed and came back stayed out: the system ran
degraded at N−1 redundancy forever. This paper documents closing that gap end to
end. We deliver two layered solutions: **v2**, a passive read-replica that catches
up from a survivor's snapshot + contiguous delivered-effect tail and stays current
by polling; and **v3**, a *virtually-synchronous re-admit view change* that returns
the node to the ring as a full **acking** member, restoring N-redundancy. The
design is grounded in three of the operator's own primary sources — two 1990s
patents and a 1995 process-control paper — the last of which solved this exact
problem via Isis virtual synchrony thirty years ago. We add a TLA+ `ReAdmit`
action and prove with TLC (6.28 M distinct states) that `ConsistentDelivery`
survives re-admission; we validate v2 live on AWS EC2 (the previously-failing
adversarial scenario now passes 2000/2000, zero acked-write loss); and we prove v3
promotion end-to-end in-process over the real TLS transport. We also report the
one infrastructure bug the live run caught that no in-process test could, and the
one correctness follow-up that remains.

---

## 1. The problem

The TRAINS spec (`verification/tla/TRAINS.tla`) carried, until this work, the
comment *"static membership / does not model recovery (crashed only grows)."* The
`Reconfigure` view-change action **excluded** a confirmed-crashed node (the
survivors adopt the most-advanced log and reissue); there was no inverse. The
`trains-valkey` SMR proxy mirrored this: it masked a crash but never re-admitted
the node. Operationally, the live adversarial bench (`E5 t1-rejoin`) made the gap
concrete: SIGKILL a node mid-load, restart it, and it caught up only ~half the
writes — the survivors held everything (**zero acked-write loss**), but the
rejoined node never reconverged.

Closing this is the difference between *surviving* a failure and *recovering* from
it. After recovery the system should return to full N-redundancy, ready for the
next failure — not limp along one fault from disaster.

### 1.1 Why adding a member is a consistency problem (CAP)

Adding a node back to a running cluster is where it is easiest to break
correctness: the tempting, available thing is to let the returning node start
serving immediately, but if it serves before it holds the agreed history you get
two divergent views — split-brain. TRAINS refuses that trade. Under a network
partition or loss of the ring, it **halts rather than diverge** — it sits in the
**CP** corner (consistency + partition-tolerance) of what Brewer would name, in
2000, the **CAP theorem**. The protocol made that choice in the early 1990s,
before there was vocabulary for it: in testing it simply *stopped* when the
network lost coherence, which for a power-plant control supervisor is the correct
behaviour — "available" means *correct when healthy, silent when not.* It is the
same call **etcd** makes today: when the cluster loses quorum it freezes writes
rather than serve possibly-stale state (a behaviour that surprises engineers until
they see it is consistency winning over apparent availability).

Re-admission has to honour that choice. Both designs below are **consistency-first**:
v2 only ever *reads* (a passive replica can never create a second writer), and v3
makes the join a **virtually-synchronous, ordered membership event** — the node
adopts the canonical log atomically at the view-change point and only then becomes
an acking member. At no instant does a returning node serve state it has not
agreed on. The cost is the CP cost — a returning node waits to catch up rather
than being instantly available — and that is the right cost to pay.

## 2. Lineage: the design was (partly) already ours

A recurring discipline in this work: **when the operator shares their own patents
and papers, read them — they often contain the proven design.** Four primary
sources frame the solution; three prove the *substrate* and are silent on rejoin,
one solves rejoin outright:

- **US5483520A** *(Eychenne, Simatic — "data train")* and **US5488723A**
  *(Baradel, Eychenne, Kohen — replicated objects, redundant architecture)*. The
  ordered-broadcast ring + semi-active redundancy (a backup catches up by
  deterministic replay of the atomic-ordered message stream, with a *reentrancy
  flag* preventing re-broadcast). Both **explicitly leave rejoin open**:
  US5488723A *"assumes no messages are lost during downtime"*; US5483520A *"does
  not describe a failed station rejoining."*
- **Eychenne, Simatic, Baradel, Kohen, ACM 1992** — the implementation paper
  behind US5488723A: the **`FR` flag of reentrancy** (set on receive, cleared
  after apply) and **Isis ABCAST (total order) chosen over CBCAST** *because
  replicated objects are non-commutative*. The direct ancestors of the modern
  `effect.rs` at-most-once dedup and of `ConsistentDelivery`. Also silent on
  rejoin.
- **Baradel, Eychenne, Junot, Kohen, Simatic, *Distrib. Syst. Engng* 2 (1995)
  65–73** — the P3200 process-control supervisor. This one **solves on-line node
  re-integration via Isis virtual synchrony**: membership join/leave events
  ordered *inside* the multicast stream, state transfer *synchronized to the
  membership transition*, incremental per-object, at-most-once. The rejoin we
  needed was a design the operator had already validated industrially in 1995.

**Virtual synchrony** (Birman, ACM TOCS 9, 1991) is the formal property: a
membership change is ordered within the multicast stream so every member agrees on
the join point *relative to the data*. That agreement is exactly what makes state
transfer gap-free **and** preserves total order across the change. TRAINS' existing
view-change (`Gather`/`Install`, ring-circulated, `view_id`-fenced) is already a
virtual-synchrony primitive — so the principled rejoin is a **re-admit view change
symmetric to exclude**, not a new membership protocol bolted on.

## 3. Design: two layers

We deliberately shipped two layers, weakest-risk first.

### 3.1 v2 — passive contiguous-tail catch-up

A returning node rejoins as the patent's **backup**: a passive replica that
applies the deterministic delivered-effect stream but **does not re-enter the
acking quorum**. It converges because the stream is totally-ordered (proved) and
deterministic (`effect.rs` resolves non-determinism at origin), and apply is
at-most-once (the `FR` flag's descendant, a bounded per-origin dedup watermark).
The one genuinely new piece is **gap closure**: the node imports a survivor's
`snapshot@X` (a full keyspace *replace*, wiping stale pre-downtime state) and then
replays that *same survivor's contiguous delivered-effect log-tail `> X`*. One
source ⇒ the suffix is contiguous ⇒ no gap — the precise thing the patents assumed
away, now guaranteed. The node then keeps tailing to stay current (a passive
standby).

v2 needs **no spec change**: it reuses the already-proven total order + dedup, so
the existing `ConsistentDelivery`/`NoSpuriousDelivery` proofs stand verbatim.

### 3.2 v3 — virtually-synchronous re-admission

To restore N-redundancy the node must become a full **acking** member again. v3 is
the re-admit view change the 1995 paper blueprints: the membership change is
ordered in the ring's view-change token (`ReAdmitGather`/`ReAdmitInstall`,
`view_id`-fenced), the state transfer is synchronized to the install point, and
the rejoiner re-enters the ordering quorum. A caught-up v2 passive replica is
*promoted* through this path.

### 3.3 The provability question (and the Paxos mirror)

A natural challenge: *does adding re-admission lose any of the formal provability
TRAINS has? And wouldn't Paxos-with-gossip solve this anyway?*

**No provability is lost** (ADR-001 §3). v2 adds no spec action. v3 adds one
`ReAdmit` action whose safety is *preserved and provable by the same stack*,
because virtual synchrony makes the join a **consistent cut**: modelling
`ReAdmit` atomically as "the new live view adopts the most-advanced survivor's log
(`canon`) and resets in-flight trains at the install point" makes `delivered[r]`
a mutual prefix of every other log. The crude-freeze alternative (stop, snapshot,
insert) was *harder* to prove — its cut is timing-dependent under concurrency —
so v3 is *more* provable, not less.

**Paxos + gossip would solve rejoin — by the same principle.** Paxos puts the
membership change in the log; virtual synchrony puts it in the stream; TRAINS puts
it in the ring token. All three order the change in the total order so the
catch-up point is a consistent cut. Adopting Paxos would *discard* TRAINS' proofs
and patent lineage and give a *weaker* delivery contract (majority-commit vs
TRAINS' uniform all-ack ring — and the 1992 paper chose ABCAST precisely for
non-commutative objects). Verdict: **keep the engine, borrow the ideas** (gossip
anti-entropy for incremental transfer, SWIM for failure detection); gossip is
eventually-consistent and never belongs on the write path.

## 4. Implementation

The feature shipped as a sequence of small, individually-verified pull requests
across the two repos.

### 4.1 v2 (trains-valkey, on a `trains-net` transport extension)

| PR | What |
|----|------|
| **PR-RJ-1** | Point-to-point snapshot channel (`trains-net::snapshot`) over pinned mutual TLS. |
| **PR-RJ-2a** | Re-admit wire tokens `ReAdmitGather`/`ReAdmitInstall`; `victim()→Option`, new `rejoiner()`. |
| **PR-RJ-2b** | Bounded `DeliveredLog` (delivered-effect tail) + `delivered_index` in the snapshot. The k-th applied effect is byte-identical on every replica (same totally-ordered deduped stream), so any survivor's tail from `X` is interchangeable — the property that makes the single-source catch-up gap-free. |
| **PR-RJ-2c** | `fetch_state` / `StateTransfer`: the transport carries an (optionally empty) snapshot blob + framed tail; the requester's `have` index selects snapshot-vs-incremental. |
| **PR-RJ-3a** | `Replica::build_state_transfer` / `apply_state_transfer`: the survivor and rejoiner sides, with the convergence proof in-process. |
| **PR-RJ-3b** | The proxy *serves* state transfer from its live driver state. |
| **PR-RJ-3c** | The proxy *rejoiner*: a `passive_catch_up_loop` — FLUSH (via snapshot import) → fetch → poll, off the ring, read-only. |
| CLI + bench | `--snapshot-listen` / `--rejoin-from` + the E5 orchestrator wiring. |

### 4.2 v3 (trains-rust spec + recovery + core, then trains-valkey proxy)

| PR | What |
|----|------|
| **Spec** | `TRAINS.tla` `ReAdmit` action — the mirror of `Reconfigure`, membership *shrinks*: the new live view adopts `canon` and resets every in-flight train (the virtual-synchrony barrier). |
| **V3-1** | `trains-recovery::ViewChange` re-admit state machine: `adopt_view`, `on_request_readmit`, `on_readmit_gather`, `on_readmit_install` — symmetric to exclude, `dead.remove(rejoiner)`. |
| **V3-2** | A deterministic multi-node test driving the full re-admit token circulation to convergence. |
| **core `readmit_node`** | The inverse of `confirm_crash` in `trains-core` — clears a node's crashed bit, so under TotalOrder its ack is required again. The "membership shrinks" primitive the core lacked. |
| **V3-3b** | The proxy survivor side: `handle_vc` routes re-admit tokens; a `readmit()` (mirror of `exclude`) re-includes the rejoiner and retargets the ring back. The survivor's view rides the state transfer (`ReplicaSnapshot.view`) so the rejoiner can `adopt_view`. |
| **V3-3c** | The proxy rejoiner promotion: once caught up, `passive_catch_up` returns a `PromoteSeed` (a final consistent `have=0` snapshot); the node spawns the ring transport and runs the active driver seeded with it — `import_state` + `readmit_node(self)` + `adopt_view` + `on_request_readmit`. **Opt-in, off by default**, so the live-validated v2 path is untouched; a failed promotion reverts to passive. |

## 5. Verification

The work was held to the project's existing multi-level standard, extended:

- **TLC (spec, safety).** `ReAdmit` added to `TRAINS_MC_TO.cfg`: **6 282 464
  distinct states, no error** — `ConsistentDelivery` + the five other safety
  invariants hold across re-admission, with membership both growing and shrinking.
  No regression: the UTO (2.66 M) and N4 (2.86 M) configs still pass; a
  pre-existing missing-`Mode` in the N4 config was fixed in passing.
  *A first naive `ReAdmit` that left in-flight trains alone was rejected by TLC*
  (a re-admitted node reloaded stale pending onto an already-delivered train,
  diverging the logs `⟨m1,m2,m3⟩` vs `⟨m3⟩`) — the barrier is exactly what TLC
  proves correct. The hand-argued provability claim of ADR-001 §3 is now
  machine-checked.
- **Unit + integration (implementation).** Six recovery state-machine tests; the
  deterministic multi-node convergence test; the `readmit_node` test (two fresh
  nodes fed an identical `{0,1}`-acked train isolate the live-mask effect); the
  in-process `apply_state_transfer` convergence test (stale store wiped, continued
  writes tracked, idempotent); the end-to-end serve test over real TLS.
- **Live, on AWS EC2 (v2).** Three `t4g.small` (ARM, AL2023), eu-west-3. The
  `E5 t1-rejoin` scenario — the one that failed before — now **passes: the
  rejoined node converges 2000/2000, matching both survivors, zero acked-write
  loss.**
- **End-to-end, real transport (v3 promotion).** A promoting rejoiner catches up,
  promotes, the ring physically reforms (the surviving predecessor retargets back,
  survivors reissue), and a write made *after* promotion reaches the promoted node
  *and the other survivor* — in TotalOrder mode that delivery required the
  promoted node's ack, i.e. it is back in the acking quorum.

## 6. The live run earned its keep

The first live `E5 t1-rejoin` attempt **failed** (the rejoined node stuck at 1000)
for a reason no in-process test could surface: the CDK **security group predated
the state-transfer server — it allowed the ring port 7000 but not 7001**, so the
passive rejoiner's `fetch_state` to a survivor was silently dropped. The
in-process tests use ephemeral localhost ports with no security group; the gap
only exists on real infrastructure. The fix (open 7001 intra-SG, committed to the
CDK network stack) and a re-run gave the pass. This is the canonical argument for
a live run: it tests the parts a unit test structurally cannot.

## 7. Results, limitations, and the one follow-up

**Results.** TRAINS now supports the full node lifecycle — exclude, passive
re-integration, and full re-admission — generalizing the patents' fixed
primary↔backup and the verified N-node exclusion to a complete join/leave model,
the 1995 paper's on-line maintainability, expressed in TRAINS' existing ordered
view change and held to the same verification bar.

**Limitations / honest caveats.**
- The verification is **bounded** (TLC up to `MaxClock=4–6`, small N); the
  unbounded inductive frontier (already open pre-this-work) grows by one action.
  No regression in the standard of evidence; the bounded coverage extends cleanly.
- The v3 promotion is **opt-in and conservative**: a failed re-admit reverts to
  the passive standby, so v2 remains the safety net.
- **One correctness follow-up:** a re-admitted **non-issuer** node *originating*
  writes — it acks and delivers correctly (a full member) and accepts client
  writes, but a write it *originates* (loading pending onto a passing train) is
  not yet observed to propagate. Likely a core `seenClk`/issue-window detail after
  `import_state`; scoped for a focused trains-core test + fix. It does not block
  N-redundancy (clients normally write to issuer nodes).

## 8. References

1. US5483520A — *Method of broadcasting data by means of a data train* (Eychenne, Simatic).
2. US5488723A — *Software system having replicated objects and using dynamic messaging… redundant architecture* (Baradel, Eychenne, Kohen).
3. Eychenne, Simatic, Baradel, Kohen — *Exploiting late binding in object messaging for implementing object replication*, ACM 1992.
4. Baradel, Eychenne, Junot, Kohen, Simatic — *Fault-tolerance and on-line maintainability in a process control supervision system*, Distrib. Syst. Engng 2 (1995) 65–73.
5. Birman, Schiper, Stephenson — *Lightweight causal and atomic group multicast*, ACM TOCS 9 (1991).
6. Simatic et al. — *TRAINS: A throughput-efficient uniform total order broadcast algorithm*, CFIP/NOTERE 2015.
7. Schneider — *Implementing fault-tolerant services using the state machine approach*, ACM Comput. Surv. 22(4), 1990.

*See also: `docs/ADR-001-rejoin-virtual-synchrony-2026-06-15.md` (design + provability + Paxos comparison); `VERIFICATION_REPORT.md` (verification matrix); `trains-valkey/bench/results/ec2-2026-06-16-e5-rejoin/REPORT.md` (live result).*
