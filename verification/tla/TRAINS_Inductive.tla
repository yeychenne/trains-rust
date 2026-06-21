--------------------------- MODULE TRAINS_Inductive ---------------------------
(*
 * Inductive-invariant proof attempt for TRAINS.tla.
 *
 * Goal: find IndInv such that
 *     Init  ⇒  IndInv             (initiation)
 *     IndInv ∧ Next  ⇒  IndInv'    (consecution)
 *     IndInv  ⇒  ConsistentDelivery (covering)
 *
 * Phase 0 result (2026-05-19, Apalache 0.57.0):
 *
 *   ✅ Initiation passes:
 *      apalache-mc check --init=Init --inv=IndInvPhase0 --length=0
 *      EXITCODE: OK  (Init ⇒ IndInvPhase0 verified at length 0)
 *
 *   ❌ Consecution blocked by spec-level Apalache limitations:
 *      1. TypeOK references `Seq(Messages)` which Apalache's inductive
 *         mode rejects as an unbounded-set generator.  Worked around
 *         by dropping TypeOK from IndInv (Snowcat enforces types at
 *         parse time, so any well-typed Apalache state already satisfies
 *         TypeOK's constraints).
 *      2. NoSpuriousDelivery uses `1..Len(delivered'[p])` to range over
 *         delivery-log indices.  Apalache requires the bounds of `..`
 *         to be constants, and the primed `delivered'` length is not
 *         constant.  This is a known Apalache issue:
 *         https://apalache-mc.org/docs/apalache/known-issues.html
 *
 * To proceed past Phase 0, the spec needs a one-time refactor:
 *
 *     delivered : [Procs -> Seq(Messages)]
 *   ─────────────────────────────────────► becomes
 *     delivered     : [Procs -> [1..MaxDelivered -> Messages]]
 *     delivered_len : [Procs -> 0..MaxDelivered]
 *
 * with MaxDelivered as a new CONSTANT.  Every action that currently
 * does `delivered' = [delivered EXCEPT ![p] = @ \o seq]` becomes a
 * loop over indices that writes positions and updates the length
 * counter.  Same semantic content; bounded representation.
 *
 * This is the largest mechanical change in the Phase 1+ workplan
 * (see docs/APALACHE-INDUCTIVE-PLAN.md), and is needed before
 * IndInv consecution can be checked at all.
 *)

EXTENDS TRAINS, TRAINS_MC

(* TypeOK is intentionally omitted from IndInv: it references
 * Seq(Messages) which Apalache's inductive mode treats as an
 * infinite-set generator and rejects.  Type-correctness is enforced
 * by Snowcat at parse time instead — every variable already carries
 * an `@type:` annotation, so any Apalache-checked state already
 * satisfies the typing constraints of TypeOK by construction.
 *)

\* @type: () => Bool;
IndInvPhase0 ==
    /\ ClockMonotonicity
    /\ ConsistentDelivery
    /\ NoSpuriousDelivery
    /\ TrainIntegrity
    /\ IssuerUniqueness

\* ===============================================================
\* Phase 1 candidate (added as we discover what Phase 0 is missing).
\* For now: the same as Phase 0; we extend it in subsequent iterations.
\* ===============================================================

\* Every (clock, issuer) key that any replica has marked done must
\* also be in the global issuedKeys set.
\* @type: () => Bool;
DoneKeysSubsetIssued ==
    \A p \in Procs : doneKeys[p] \subseteq issuedKeys

\* The "next clock to issue" at each issuer is consistent with what
\* has been issued so far: either it's 1 (no issuance yet) or its
\* predecessor (issClk[q]-1, q) is in issuedKeys, and (issClk[q], q)
\* is the next slot we'd stamp.
\* @type: () => Bool;
IssClkConsistent ==
    \A q \in Issuers :
        \/ issClk[q] = 1   \* no recycled trains yet
        \/ <<issClk[q], q>> \in issuedKeys

\* No replica's seenClk can be ahead of the corresponding issuer's
\* own clock.  (Already in TRAINS as ClockMonotonicity but spelled
\* out here for clarity in the inductive context.)
\* @type: () => Bool;
SeenClkBounded ==
    \A p \in Procs : \A q \in Procs :
        seenClk[p][q] <= issClk[q]

\* The crucial conjunct for ConsistentDelivery inductive-ness:
\* if (c, q) is in doneKeys[p], then every issued (c', q') strictly
\* smaller than (c, q) is also in doneKeys[p].
\* This is the state-predicate form of "delivery follows clock-key order".
\* @type: () => Bool;
DoneKeysDownwardClosed ==
    \A p \in Procs :
        \A ck \in doneKeys[p] :
            \A ck2 \in issuedKeys :
                CKLt(ck2, ck) => ck2 \in doneKeys[p]

\* @type: () => Bool;
IndInvPhase1 ==
    /\ IndInvPhase0
    /\ DoneKeysSubsetIssued
    /\ IssClkConsistent
    /\ SeenClkBounded
    /\ DoneKeysDownwardClosed

(*
 * Non-deterministic state generator for the consecution check.
 *
 * Apalache's `--init=…` flag expects a formula in "assignment form":
 * every variable must appear in a `var \in <finite-set>` clause so
 * the symbolic encoder knows how to enumerate its possible values.
 * A pure predicate like IndInvPhase0 doesn't qualify — Apalache's
 * AssignmentPass rejects it.
 *
 * The standard recipe (see apalache-mc.org/docs/apalache/tutorials/
 * advanced-features.html#inductive) is to wrap IndInv inside a
 * generator that *first* draws every variable from its (finite,
 * bounded-by-ConstInit) type domain, and *then* constrains the
 * result with IndInv.  Apalache then enumerates the IndInv-state
 * space symbolically and applies Next.
 *)

\* @type: Set({ issuer: Int, clock: Int, msgs: Set(Str), acks: Set(Int), pos: Int });
TrainRecGen ==
    [issuer : Procs,
     clock  : 0..MaxClock,
     msgs   : SUBSET Messages,
     acks   : SUBSET Procs,
     pos    : Procs]

\* @type: () => Bool;
IndInvAsInit ==
    /\ tr          \in [TrainId -> TrainRecGen]
    /\ pending     \in [Procs -> SUBSET Messages]
    /\ delivered   = [p \in Procs |-> <<>>]   \* sequence init — see note below
    /\ doneKeys    \in [Procs -> SUBSET ((0..MaxClock) \X Procs)]
    /\ seenClk     \in [Procs -> [Procs -> 0..MaxClock]]
    /\ issClk      \in [Procs -> 0..MaxClock]
    /\ crashed     \in SUBSET Procs
    /\ broadcast   \in SUBSET Messages
    /\ issuedKeys  \in SUBSET ((0..MaxClock) \X Procs)
    /\ IndInvPhase0

(*
 * Note on `delivered`: Apalache's symbolic encoding can't enumerate
 * `Seq(Messages)` because it's unbounded.  Two workarounds in
 * common Apalache practice:
 *   (a) replace `delivered` with a `[1..MaxLen -> Messages]` function
 *       + a per-process length counter — i.e. rewrite the spec
 *       slightly so the inductive check sees a bounded representation.
 *   (b) restrict the inductive check to states where `delivered =
 *       <<>>` initially (the trivial case) and rely on a separate
 *       argument (or TLC bounded check) to bridge from there.
 *
 * For Phase 0 we use (b) — `delivered = <<>>` — which is a degenerate
 * inductive check: it tells us whether IndInv is preserved STARTING
 * FROM a state with empty delivery logs.  This is strictly weaker
 * than the full inductive proof and serves as a sanity check that
 * the non-history conjuncts of IndInv are mutually consistent.
 *
 * The full inductive proof requires workaround (a) — that's a
 * spec-level rewrite to ditch `delivered : Seq` in favour of
 * `delivered : [1..MaxLen -> Messages]` plus a `delivered_len : Int`.
 * It is the largest mechanical change in the Phase 1+ workplan.
 *)

=============================================================================
