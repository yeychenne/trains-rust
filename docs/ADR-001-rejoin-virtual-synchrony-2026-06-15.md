# ADR-001 — Rejoin via virtually-synchronous re-admit; provability analysis

**Status:** Accepted — **IMPLEMENTED (2026-06-16).** v2 passive rejoin is live-
validated on EC2; v3 re-admission is spec-verified (TLC, `ReAdmit`), the recovery
logic + core `readmit_node` primitive are merged, and proxy promotion is proven
end-to-end. One follow-up remains (re-admitted non-issuer originating writes).
See the synthesis in `docs/WHITEPAPER-rejoin-and-readmission-2026-06-16.md`.
**Date:** 2026-06-15 (decision); implementation completed 2026-06-16.
**Context owners:** TRAINS protocol (trains-rust) + trains-valkey proxy
**Companion:** `docs/PLAN-pr-rj-2-readmission-2026-06-15.md` (the staged plan);
`docs/WHITEPAPER-rejoin-and-readmission-2026-06-16.md` (the full write-up).
**Decision in one line:** a recovered node re-enters the ring through a
**re-admit view-change that is ordered in the total-order stream (virtual
synchrony)**, with **state transfer synchronized to the install point** — *not* a
crude freeze, *not* an out-of-band snapshot pull.

---

## 1. Context

E5 `t1-rejoin` fails live: a SIGKILLed-then-restarted node rejoins but converges
to only ~half the writes. `proxy.rs` wires the view-change to **exclude** a crashed
node but never **re-admits** one; `export/import_snapshot` exist but aren't invoked
on rejoin. The TLA+ spec states *"static membership … does not model recovery."*

Four operator-authored primary sources frame the fix — three prove the
*substrate* and are silent on rejoin; one supplies the rejoin design:
- **US5483520A / US5488723A** (1992–93): exclusion + deterministic replay +
  reentrancy guard; **rejoin explicitly left open** ("assumes no messages lost
  during downtime").
- **Eychenne/Simatic/Baradel/Kohen, ACM 1992** (*"Exploiting late binding in
  object messaging for implementing object replication"*): the **implementation**
  paper behind US5488723A. Establishes (a) the **flag of reentrancy `FR`** — *set
  on receive, cleared after the local apply* — the direct ancestor of `effect.rs`'s
  **at-most-once dedup**; (b) **Isis ABCAST (total order)** chosen over CBCAST
  (causal) *because replicated objects are non-commutative* — the exact ordering
  contract `ConsistentDelivery` encodes. **Also silent on rejoin** (its scope is the
  messaging hook + ordering + reentrancy, not downtime recovery).
- **Baradel/Eychenne/Junot/Kohen/Simatic, *Distrib. Syst. Engng* 2 (1995)**: the
  P3200 system **solves** on-line node re-integration via **Isis virtual
  synchrony** — membership join/leave events *ordered inside the multicast stream*,
  state transfer *synchronized with reception of broadcasts*, incremental
  per-object, at-most-once.
- **Birman/Schiper/Stephenson, ACM TOCS 9 (1991)** (the 1995 paper's ref [14]):
  the formal virtual-synchrony model.

**The four-source alignment:** the 1992 paper + both patents prove the *substrate*
(ordered ABCAST + reentrancy guard = TRAINS' total order + dedup) and **leave
rejoin unsolved**; the 1995 paper alone closes it, via virtual synchrony. So the
at-most-once apply that makes catch-up overlap a no-op (§3.3) is not incidental —
it is the `FR` flag, prior-art-proven since 1992.

## 2. Decision

Re-admission is a **virtually-synchronous membership transition**:

1. A returning node `r` triggers a **re-admit view-change** (symmetric to the
   existing exclude `Gather`/`Install`). The re-admit token circulates the ring,
   `view_id`-fenced — i.e. the join is **ordered in the stream relative to the
   trains**. This is the TRAINS realization of *"member-change notifications
   inserted consistently within the message stream."*
2. The **install point `X`** is the join's position in the total order. A survivor
   `s` ships `r` its state **as of `X`** (PR-RJ-1 transport); `r` flushes its stale
   engine, imports, and begins delivering from **`X+1`** — exactly the trains it
   then receives as a re-admitted ring member. *State transfer synchronized to the
   membership transition.*
3. Catch-up is **incremental / per-object** where feasible (1995 paper), and apply
   is **at-most-once** via the existing `(origin, request_id)` dedup (the patent's
   reentrancy guard).
4. **Concurrent joins are serialized** by the coordinator + `view_id` fencing
   (the paper's *"no time guarantees for multiple joins — handle with care"*).

**Why a re-admit view-change and not v2's passive log-tail?** v2 (a passive
replica that pulls a contiguous survivor tail, never re-entering the acking
quorum) is the **lower-risk convergence step** and ships first (it makes
`t1-rejoin` pass). But it leaves the node a read replica; it does **not** restore
the node as a full **acking member** — i.e. it doesn't restore the N-redundancy
the patent/paper exist for. v3 (this ADR) is the principled finish. The single-
survivor contiguous tail of v2 is, formally, the special case of v3 where the
"install point" is "the survivor's current head" — so v2 is a sound stepping stone,
not a different design.

## 3. The provability question (the heart of this ADR)

**Q: Does adding re-admission lose any of the formal provability TRAINS has?**

**A: No — and re-admission's own safety is provable *by the same stack*, to the
same standard as the existing core, *because* it is virtually synchronous. The
crude-freeze alternative would have been *harder* to prove. There is one honest
caveat (the inductive frontier, already open, grows). Details below.**

### 3.1 What is proven today, and the one invariant that matters

The safety keystone is **`ConsistentDelivery`** (P1/P2): for any two non-crashed
processes `p, q`, the delivery logs are **mutual prefixes** —
`IsPrefix(delivered[p], delivered[q]) ∨ IsPrefix(delivered[q], delivered[p])`.
Verified by **TLC** (2.66 M states, depth 61, three model sizes) and **Apalache**
(bounded, length 8), with **Kani** on leaf functions and **DRT** (production vs a
TLA+-mirroring reference) tying the Rust to the spec. The spec models crash
(exclude) but **`crashed` only grows**.

A standing fact the spec maintains: **a crashed node's log is a prefix of every
live node's log** (it stopped extending at its crash point; live nodes only
extended further, and they were mutual-prefix-consistent with it before the crash).

### 3.2 The preservation theorem for re-admit

Model `ReAdmit(r, s)` **atomically** in the spec, for `r ∈ crashed`, survivor `s`:
> `delivered'[r] := delivered[s]`  (copy `s`'s **current** log, length `X`),
> reseed `r`'s clocks / `doneKeys` from `s`, `crashed' := crashed \ {r}`.

**Claim:** `ConsistentDelivery` is preserved by `ReAdmit`.
**Proof (one line, mechanizable):** for any live `q`, `delivered[s]` and
`delivered[q]` are mutual prefixes (the invariant held among live nodes, `s`
included). After the action `delivered'[r] = delivered[s]`, so `delivered'[r]` and
`delivered[q]` are mutual prefixes too. ∎ Subsequent deliveries: `r` now receives
the **same totally-ordered trains** (it's a ring member again) and applies them in
the **same deterministic `(clock, issuer)` order**, so `delivered[r]` extends as a
mutual prefix of the others. `NoSpuriousDelivery` holds because every key `r`
delivers came from a real train (it inherits `s`'s `doneKeys`/`issuedKeys`).

**This is checkable by TLC**: add `ReAdmit` to `Next`, re-run the existing
invariants on the MC configs. The state space grows (more transitions) but the
check is the same kind TLC already does.

### 3.3 Why virtual synchrony is the *enabling* condition (and the freeze isn't)

The atomic spec action assumes `r`'s post-state is **exactly `delivered[s]` up to
`X`** and `r` then sees **exactly `X+1, X+2, …`**. The implementation must
**refine** that. Two failure modes break the refinement — and both are
`ConsistentDelivery` violations:

- **Gap:** snapshot at `X`, but `r` starts receiving at `Y > X+1` (missed
  `X+1..Y`). Then `delivered[r] = ⟨0..X⟩ ⌢ ⟨Y+1..⟩`, which is **not** a prefix of
  `delivered[s] = ⟨0..X⟩ ⌢ ⟨X+1..⟩`. **Divergence.**
- **Overlap without dedup:** `r` re-applies `X+1..Z` already in the snapshot. The
  `(origin, request_id)` dedup makes this a no-op, so overlap is *safe* — but only
  because the reentrancy guard (the patent's FR flag) is present.

**Virtual synchrony eliminates the gap by construction:** the join is *ordered in
the stream*, so the install point `X` and "`r`'s first received train = `X+1`" are
the **same event** — a **consistent cut**. The 1995 paper's *"state transfer
synchronized with reception of broadcasts"* is exactly this. TRAINS' view-change
already orders membership changes in the ring (`view_id`-fenced), so the mechanism
to achieve the consistent cut **already exists**; re-admit reuses it.

A **crude freeze** ("stop the ring, snapshot, insert, resume") *approximates* a
consistent cut but its boundary is timing-dependent under concurrency / async
links — exactly where the gap sneaks in. So v1's freeze was **harder** to argue
correct; v3's virtual synchrony is a **clean, checkable** property. *We gain
provability by choosing v3.*

### 3.4 New proof obligations, and how the stack discharges them

| Obligation | Discharged by | Standard vs today |
|---|---|---|
| `ReAdmit` preserves `ConsistentDelivery` / `NoSpuriousDelivery` (spec) | **TLC** re-run with `ReAdmit ∈ Next` on the MC configs | **Same** (bounded model checking — the project's primary evidence) |
| Concurrent re-admits don't race | `view_id` fencing + coordinator serialization, modeled + TLC-checked | Same as the exclude path's existing fencing |
| Impl refines the atomic `ReAdmit` (consistent cut, no gap) | **DRT** (production re-admit vs reference) + **runtime trace validator** asserting `ConsistentDelivery` across a live re-admit | **Same** — the project already validates the Rust↔spec link by DRT + trace, *not* mechanical refinement, for the entire core (see `VERIFICATION_REPORT.md` honesty caveats) |
| `import_snapshot` leaf correctness | **Kani**/unit on the snapshot apply | Same |

### 3.5 Honest caveats (what we do *not* gain)

1. **Bounded, not inductive — and the open frontier grows.** Today's evidence is
   bounded (TLC ≤ `MaxClock=6, N=4`; Apalache length 8). Adding `ReAdmit`, the
   bounded checks **extend** to cover it (no regression), but the *unbounded
   inductive* proof — already incomplete (`docs/APALACHE-INDUCTIVE-PLAN.md`,
   PR-CORE-5) — now has a larger invariant to eventually discharge.
2. **Refinement is by DRT + trace, not mechanical proof.** This is **not new** —
   it is how the *entire* core is tied to the spec today. Re-admit is held to the
   *same* standard, no weaker. A future Verus/refinement effort would raise the bar
   for everything, re-admit included.
3. **State-transfer atomicity at the engine.** The Rust must take `s`'s snapshot at
   a clean delivered boundary (not mid-apply). This is an implementation invariant
   (one lock/quiesce at `s` for the snapshot read), testable, and is the concrete
   thing the DRT/trace must exercise.

### 3.6 Verdict

- **Existing proofs are untouched** — exclusion + total order remain proven.
- **Re-admit's safety is provable by the same stack** (TLC for the spec action;
  DRT + runtime trace for the Rust refinement) **to the same standard** as the rest
  of TRAINS.
- **Virtual synchrony is what makes it provable** (consistent cut ⇒
  `ConsistentDelivery` preserved). The freeze alternative was strictly weaker.
- **Net cost:** +1 spec action, a TLC re-run, extended DRT/trace, concurrent-join
  serialization — and a larger (already-open) inductive frontier. **No loss of the
  current standard of provability; a clean new obligation that the existing tools
  discharge.**

## 4. Consequences

- TRAINS gains **dynamic membership** (re-admission) — generalizing the patent's
  fixed primary↔backup and the verified N-node exclusion to a full join/leave
  lifecycle, the 1995 paper's on-line maintainability.
- `TRAINS.tla` must grow a `ReAdmit` action + a `joining`/`view`-aware membership
  model; the MC configs re-run. This is the **PR-RJ-2c gate** — *re-admission is
  "verified" only once TLC is green with `ReAdmit` in `Next`.*
- The verification-debt rule stands: **do not ship re-admit labeled "verified"
  before §3.4 row 1 is green.**

## 5. Alternatives rejected

- **v1 crude freeze** — timing-dependent consistent cut; harder to prove (§3.3).
- **v2 passive-only, forever** — sound for convergence but never restores an acking
  member / N-redundancy; kept as the **first step**, not the destination.
- **Out-of-band snapshot pull with no stream ordering** — reintroduces the gap
  (§3.3); the exact bug E5 surfaced.

## 6. Compared alternative: Paxos + gossip

A reasonable challenge: *"Paxos working with gossip solves rejoin — why not just do
that?"* It does solve rejoin — but **by the same underlying principle TRAINS
already has**, and adopting it wholesale would *cost* the verification + lineage
this codebase is built on. Worked through:

### 6.1 How Paxos + gossip would solve it

- **Paxos / Multi-Paxos / Raft** is consensus on a replicated **log**. A lagging or
  returning node is a **learner**: it catches up by pulling snapshot + log-tail from
  the leader/peers, then participates. **Membership change is itself a committed log
  entry** (Raft joint-consensus / Paxos reconfiguration) at a definite slot.
- **Gossip** (SWIM-style failure detection; Dynamo-style anti-entropy with
  Merkle-tree diff) handles the *control* plane — disseminating membership/liveness
  and reconciling divergent state — but is **eventually consistent**: it gives **no
  total order** on its own.
- The natural split is therefore **Paxos for the ordered data log, gossip for
  membership/failure-detection/anti-entropy** (the layering CockroachDB/Spanner-class
  systems use: Raft for the keyspace, gossip for liveness + cluster info).

### 6.2 The deep equivalence — it's the *same* invariant

Paxos-reconfiguration commits the join **at a definite slot in the log** ⇒ the
catch-up point is "replay from that slot." Virtual synchrony orders the join
**in the multicast stream** ⇒ state transfer at that cut. TRAINS' view-change
orders the join **in the ring token** (`view_id`-fenced) ⇒ install point `X`.
**All three are one idea: order the membership change in the total order so the
catch-up point is a consistent cut (§3.3).** "Paxos + gossip solves it" is true —
and it validates v3, because it solves it the *same way*. The choice is not
*principle A vs principle B*; it is *which engine carries the one principle*.

### 6.3 Why not swap TRAINS for Paxos (for *this* codebase)

| Axis | Paxos + gossip | TRAINS v3 (this ADR) |
|---|---|---|
| **Proofs** | Discards TLC/Apalache evidence for the ring total order; must re-verify a *different* protocol (or trust IronFleet/Verdi-raft for *their* spec, not ours) | **Keeps every existing proof**; adds **one** `ReAdmit` action to the spec already verified (§3) |
| **Delivery contract** | **Majority-commit**: an entry is applied once *f+1 of 2f+1* have it — a lagging minority can be behind the applied state | **Uniform / all-ack**: the train traverses **all** live members before delivery — *stronger*, and what non-commutative control objects need (1992 paper: ABCAST-over-CBCAST for exactly this) |
| **Lineage** | Replaces the patented **data-train** ordering — i.e. throws away US5483520A, the contribution being rebuilt | The data-train **is** the ordering engine; re-admit extends it, faithful to the patents + 1995 paper |
| **Gossip on the write path** | Eventually-consistent ⇒ **violates total order** if used for writes; only safe for membership/anti-entropy | N/A — total order is the ring's job |

Swapping in Paxos means **re-paying the entire verification bill** for a protocol
with a *weaker* delivery guarantee than TRAINS' uniform all-ack ring, and
discarding the patent lineage the project exists to rebuild. The principled rejoin
(order the join in the total order) is achievable **inside** the verified spec with
**+1 action** — so the Paxos rewrite buys nothing v3 doesn't already give, at far
higher cost and risk.

### 6.4 What to *borrow* from that world (genuinely useful)

- **Gossip / anti-entropy (Merkle-tree diff) for the incremental per-object state
  transfer** the 1995 paper wanted — a rejoiner reconciles only the changed objects
  vs a full snapshot. A concrete, additive optimization on the v3 transport.
- **SWIM-style failure detection** to *trigger* the view-change more robustly than a
  single coordinator timeout — a control-plane improvement, orthogonal to the
  ordering engine.
- **Reconfiguration-as-a-committed-entry discipline** — which is *exactly* "ReAdmit
  ordered in the stream." Paxos's own best practice **confirms** v3's shape.

**Conclusion:** Paxos + gossip is a valid, well-trodden rejoin architecture and a
useful mirror — but for TRAINS it would *replace* a verified, stronger-delivery,
patent-aligned engine to obtain a property v3 already reaches with one spec action.
Adopt the *ideas* (anti-entropy transfer, gossip failure detection); keep the
engine.

## 7. Validation plan (concrete)

1. **TLA+** ✅ **DONE (2026-06-16)**: `ReAdmit` added to `TRAINS.tla` as a
   virtual-synchrony barrier symmetric to `Reconfigure` (membership shrinks
   instead of grows; the new live view adopts `canon` and all in-flight trains
   reset in the same atomic step). TLC on `TRAINS_MC_TO.cfg` (the mode that
   exercises it): **6 282 464 distinct states, no error** — `ConsistentDelivery`
   + the other five safety invariants hold across re-admission. No regression:
   UTO (2.66M) and N4 (2.86M, after fixing a pre-existing missing-`Mode`) still
   green. Recorded in `VERIFICATION_REPORT.md`. *A first naive `ReAdmit` that
   left in-flight trains alone was REJECTED by TLC — a re-admitted node reloaded
   pending onto a train a survivor had already delivered, diverging the logs;
   the barrier (reset all trains, adopt canon) is what TLC proves correct. This
   is the provability claim of §3 verified, not just argued.*
2. **DRT**: extend the reference + harness with a kill→re-admit schedule; 0
   divergences.
3. **Runtime trace**: emit + validate `ConsistentDelivery` across a re-admit in
   `crates/trains-cli` and the trains-valkey ring.
4. **Live**: E5 `t1-rejoin` converges (rejoined node's keyspace == survivors').

## 8. Staging

- **Now (PR-RJ-2b/2c/RJ-3, v2):** passive contiguous-tail convergence — ships the
  `t1-rejoin` pass at lowest risk; no spec change.
- **Then (v3, this ADR):** virtually-synchronous re-admit — **spec-first**
  (`ReAdmit` + TLC green ✅ **done 2026-06-16**) → trains-recovery logic (PR-RJ-2a
  tokens) → proxy promotion of a caught-up passive replica to a full acking
  member. The spec is now the executable blueprint for the recovery + proxy
  layers: they must refine the atomic `ReAdmit` (reset trains + adopt canon at
  the install point), validated by DRT + runtime trace (§7.2–3).

*Refs: US5483520A, US5488723A; Eychenne/Simatic/Baradel/Kohen, "Exploiting late
binding in object messaging for implementing object replication", ACM 1992;
Baradel et al., Distrib. Syst. Engng 2 (1995) 65–73; Birman/Schiper/Stephenson,
"Lightweight causal and atomic group multicast", ACM TOCS 9 (1991);
`verification/tla/TRAINS.tla`, `VERIFICATION_REPORT.md`,
`docs/APALACHE-INDUCTIVE-PLAN.md`, `docs/PLAN-pr-rj-2-readmission-2026-06-15.md`.*
