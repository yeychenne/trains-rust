# Closing the Reconfiguration Gap in TRAINS: Masking Permanent Crashes for a Replication-Grade Ring Total-Order Broadcast

**Draft — 2026-05-24.** Companion to `TRAINS-EC2-evaluation.md` (empirical chaos data) and `trains-db-replication-feasibility.md` (the use case). Addresses the single blocking finding from Phase G chaos testing:

> **Permanent crash NOT masked — UTO halts at 12,416/50,000; the build has no reconfiguration layer.**

This study explains *why* the ring halts, shows the gap is well-solved in the literature (Totem, virtual synchrony, Raft/VR reconfiguration), confirms TRAINS already has the membership *detection* hooks, and proposes a concrete view-change + recovery design with a validation plan on our existing chaos harness.

---

## 1. The observed gap (evidence)

Phase G, ring of 9, SIGKILL of node 4 during active broadcast (corrected timing):

| Metric | Result |
|---|---|
| Delivered | **12,416 / 50,000** (progress halts at the crash) |
| Nodes 5–8 | each miss the 12 in-flight messages downstream of the dead node |
| Total order | ✅ preserved | 
| No phantom | ✅ preserved |
| Recovery | **none** — the ring never re-forms |

Contrast: a *transient* link partition (4↔5, healed) reached **100%** delivery (≈29 s recovery stall), and a *stop/restart* converged all nine nodes consistently. **The protocol is safe under all faults and live under transient ones; it is the permanent-crash case that lacks liveness.**

## 2. Why a ring TOB halts on a crash

In TRAINS/LCR/Totem the ordering guarantee comes from a *train/token* circulating the logical ring; a message is uniformly delivered once it has propagated far enough around the ring. A permanently dead member **breaks circulation**: its successor never receives the train, so no further message becomes stable and UTO progress stops. This is intrinsic to ring TOB — it is *not* a TRAINS defect, it is the reason every production ring protocol pairs the data path with a **membership/reconfiguration layer**.

Crucially, **TRAINS already has the detection half**: `callbackCircuitChange(circuitView*)` reports `cv_joined`, `cv_departed`, `cv_nmemb` — the protocol *tracks* membership. The gap in the tested build is that a *departure* does not trigger **ring re-formation + resumption**. (The Simatic t4g comparison run, in flight, will show whether the original C implementation already re-forms on departure; if so, the gap is purely in the Rust port and the design below is a re-implementation of an existing capability.)

## 3. The gap is well-solved — prior art

- **Totem single-ring protocol** [Amir, Moser, Melliar-Smith, Agarwal, Ciarfella, ACM TOCS 1995] — *the direct precedent*: a token-ring total-order protocol whose **membership algorithm handles reconfiguration, including restart of a failed processor and remerge of a partitioned network.** Demonstrates that ring TOB + crash recovery is achievable without abandoning the ring.
- **Virtual synchrony** [Birman & Joseph, SOSP 1987; Extended Virtual Synchrony, Moser et al.] — the correctness model: the system installs ordered **views** (membership snapshots) and guarantees that processes transitioning between the same pair of views deliver the **same set of messages** before the view change. This is exactly the property that lets a replica set lose a member without losing or duplicating data.
- **Failure detectors** [Chandra & Toueg, JACM 1996] — ◇S-class detectors give the "suspect a crashed member" signal under partial synchrony; FD-RP [IJCCBS 2017] applies this to Ring Paxos *instead of* a membership service.
- **SMR reconfiguration** — Raft **joint consensus** / single-server changes [Ongaro & Ousterhout 2014]; Viewstamped Replication **view change** [Oki & Liskov 1988]. The quorum discipline (overlapping old/new majorities) that prevents split-brain during a configuration change.

**Takeaway:** masking a permanent crash in a ring TOB is a re-use of Totem-style membership + virtual-synchrony view changes, with quorum rules borrowed from Raft/VR — not new research.

## 4. Proposed design (for the Rust `trains-rust` build)

Four components, layered on the existing data path; each maps to prior art.

**(a) Failure detection.** A heartbeat / train-circulation-timeout detector: if the train fails to advance past member *k* within a bounded time, suspect *k*. Reuse TRAINS's existing circulation timing as the signal; emit a `cv_departed` event (the hook already exists). Tunable timeout trades detection latency vs false positives (Chandra-Toueg ◇S).

**(b) View change (the core).** On a suspected departure:
1. **Flush** — surviving members exchange what they have stably delivered and complete delivery of every message that *any* survivor delivered in the old view (uniform agreement across the boundary — the virtual-synchrony guarantee; this is what prevents the "nodes 5–8 lose 12 messages" outcome).
2. **Agree on the new view** — the surviving set, decided by a **quorum** (majority of the prior view) so a minority partition cannot install a conflicting view (anti-split-brain, per Raft/VR).
3. **Re-form the ring** — splice out the dead member; the dead node's predecessor now points to its successor; resume circulation.
4. **Install view** `v+1` and continue UTO from a clean cut.

**(c) Rejoin / state transfer.** A returning (or replacement) node joins via the existing `cv_joined` path, receives a state snapshot + the messages it missed, then re-enters the ring at view `v+2`. (Our stop/restart result already shows TRAINS converging a returning node to a consistent value — the missing piece is making the *survivors progress during* the outage, which (b) provides.)

**(d) Partition handling.** Quorum-gated view installation: the majority side installs a new view and continues; the minority side blocks (no progress, preserving safety) until remerge — at which point a merge view reconciles (Totem's partition-remerge; Extended Virtual Synchrony).

## 5. Correctness obligation

The one invariant that must not break is **uniform agreement across a view change**: any message delivered by *any* member of view *v* must be delivered by *all* members of view *v+1* before *v+1* installs. The flush step (4b-1) provides this; without it, reconfiguration would *introduce* the data loss we are trying to prevent. This is precisely the contract virtual synchrony formalises and Totem implements — we adopt it rather than invent.

Liveness is conditional on a quorum surviving (majority of the prior view), consistent with all crash-tolerant SMR. With *N=2f+1* members, *f* crashes are masked.

## 6. Validation plan (reuse the Phase G harness)

Re-run the existing chaos configs against the reconfiguration-enabled build, with **new acceptance criteria**:

| Scenario | Current (no reconfig) | Target (with reconfig) |
|---|---|---|
| `fis-kill` (1 node, ring 9) | UTO halts (12,416/50k) | survivors reach **UTO completeness within view v+1**; bounded **MTTR** = detect + flush + ring re-form |
| `fis-kill` 2 nodes | n/a | still complete (N=9 ⇒ f≤4) |
| partition (minority) | (transient) | majority continues; minority blocks; **remerge** on heal, no split-brain |
| `fis-stop-start` | converges (throughput halved) | survivors *progress during* outage; rejoiner state-transfers + catches up |

`analyze_chaos.py` already measures the needed signals (UTO completeness per alive set, total order, no phantom, max-progress-stall as MTTR). Add a "view-change events + view-install latency" metric to the orchestrator (the `callbackCircuitChange` stream is the source).

## 7. Why this matters for AO

This is **milestone #1** of the TRAINS-replicated-Redis feasibility (`trains-db-replication-feasibility.md`): a replication layer that loses all progress when one machine dies is unusable for a multi-machine control plane. With §4 in place, a TRAINS-replicated store tolerates *f* machine failures out of *2f+1* with no lost acknowledged writes (uniformity) and bounded recovery — the bar a Redis-SMR substitute must clear to compete with RedisRaft / Raft-based stores, while keeping TRAINS's throughput profile.

## 8. Phased plan

1. **Failure detector + departure event** wired to circulation timeout (small; the hook exists).
2. **View-change with flush** (the core; the correctness-critical piece) — single-crash, single-AZ first.
3. **Validate** with `fis-kill` (1 then 2 nodes) on the chaos harness; require UTO completeness within the installed view + bounded MTTR.
4. **Rejoin / state transfer**; validate with `fis-stop-start`.
5. **Quorum/partition** view installation; validate with the partition config (minority blocks, majority progresses, remerge).
6. Only then layer the Redis write-proxy (feasibility study Phase 1).

## References

1. Y. Amir, L. E. Moser, P. M. Melliar-Smith, D. A. Agarwal, P. Ciarfella. *The Totem single-ring ordering and membership protocol.* ACM Trans. Computer Systems, 13(4), 1995.
2. K. Birman, T. Joseph. *Exploiting virtual synchrony in distributed systems.* SOSP, 1987.
3. L. E. Moser, Y. Amir, P. M. Melliar-Smith, D. A. Agarwal. *Extended virtual synchrony.* ICDCS, 1994.
4. T. D. Chandra, S. Toueg. *Unreliable failure detectors for reliable distributed systems.* JACM, 43(2), 1996.
5. D. Ongaro, J. Ousterhout. *In search of an understandable consensus algorithm (Raft).* USENIX ATC, 2014 (joint consensus / single-server membership changes; Ongaro PhD thesis, Stanford 2014).
6. B. Oki, B. Liskov. *Viewstamped replication.* PODC, 1988.
7. R. Guerraoui, R. R. Levy, B. Pochon, V. Quéma. *Throughput-optimal total order broadcast for cluster environments.* ACM TOCS, 28(2), 2010 (LCR).
8. M. Simatic, A. Foltz. *TRAINS: A throughput-efficient uniform total order broadcast algorithm.* 2015 (IEEE 7293477; `circuitView` membership API).
9. F. B. Schneider. *Implementing fault-tolerant services using the state machine approach.* ACM Computing Surveys, 22(4), 1990.

*Empirical basis: `bench/reports/TRAINS-EC2-evaluation.md` (Phase G chaos). Use case: `bench/reports/trains-db-replication-feasibility.md`.*
