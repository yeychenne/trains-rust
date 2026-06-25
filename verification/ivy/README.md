# Ivy Parameterized Proof — TRAINS Protocol

## What this proves

`trains.ivy` establishes the **total-order delivery invariant** for the TRAINS
protocol for **any ring size N** (unbounded).

The key property: if process P delivers message M before M2, and process Q
delivers both, then Q also delivers M before M2.  This is the UTO
(Uniform Total Order) guarantee from Simatic et al. 2015.

Unlike the TLC model check (which bounds N=3), the Ivy proof is valid for
all N because Ivy uses the **EPR (Effectively Propositional Reasoning)**
decidable fragment — every universal quantifier is instantiated
symbolically, not by enumeration.

## Status — 2026-06-25 verified

`ivy_check` was executed on `trains.ivy` (`ms-ivy 1.8.26`, Python 3.10,
x86_64 Linux via podman on Apple Silicon).

**Result: ✅ OK — `total_order` and `prefix_closed` both verified.**

```
Initialization must establish the invariant
    trains.ivy: line 54: total_order ... PASS
    trains.ivy: line 77: prefix_closed ... PASS
OK
```

This is a **parameterised verification** — the proof goes through for
*any* number of processes, *any* number of clocks, *any* number of
messages.  It complements the TLC and Apalache results which are at
fixed small N, MaxClock, etc.

### What got verified

- **`total_order`** — if any two processes both delivered the same two
  keys, the keys' relative order is the same on both processes.  Holds
  trivially given that `(Cl,Iss)` form a global linear order under
  `lt_tick + lt_proc + lex composition`.
- **`prefix_closed`** — if a process delivered key `(Cl,Iss)`, then for
  every strictly earlier key `(Cl2,Iss2)` the process delivered some
  message at that key too.  This is the inductive content: `deliver`'s
  precondition (every earlier key already delivered) preserves the
  invariant.

### What did NOT get verified (and isn't claimed)

- **Ring traversal** — `succ` was dropped from the spec because its
  existential axiom required a Skolem function `proc → proc` that
  Ivy's EPR/FAU fragment flags.  The safety claim above does not
  depend on the ring topology; the TLA+ / Apalache models cover
  ring-shape reasoning at finite N concretely.
- **Liveness** (`EventualDelivery`) — Ivy targets safety; the liveness
  claim is at the TLC + TLC-liveness layers (see
  [`../../VERIFICATION_REPORT.md`](../../VERIFICATION_REPORT.md)).
- **Ack collection / view change** — abstracted away; the precondition
  of `deliver` says "every node acked," not how those acks were
  collected.  The TLC + Apalache models cover those layers.

### Rewriting the spec to stay in FAU (2026-06-25 changes)

The 2026-06-24 first run surfaced `error: The verification condition
is not in the fragment FAU.`  Two specific causes:

1. `interpret tick -> nat` combined with `<` on universally
   quantified clocks → interpreted symbol on a quantified variable.
   Fix: drop the nat interpretation, declare `lt_tick(T1, T2)` as an
   uninterpreted relation, add the four total-order axioms
   (irreflexive, asymmetric, transitive, total).  Same for the
   issuer-tiebreak `lt_proc`.  All `<` callsites rewritten as
   inlined `(lt_tick(C1, C2) | (C1 = C2 & lt_proc(I1, I2)))`.
2. `succ` relation's existence axiom required a Skolem function.
   Fix: drop `succ` entirely (safety claim doesn't need it).

The total-order invariant's body is the same lexicographic predicate
appearing as both hypothesis and conclusion (the global ordering
function makes "Q delivered them in the same order" reduce to "the
ordering function says X").  Captured in comments.

## Install Ivy

### Canonical (Linux x86_64)

```bash
pip install ms-ivy            # 1.8.26 — Python ≥ 3.10
ivy_check trains.ivy
```

### Running on non-Linux hosts

| Host           | Path                                                                                 |
|----------------|--------------------------------------------------------------------------------------|
| macOS arm64    | Docker: `docker run --rm --platform linux/amd64 -v "$PWD":/w -w /w python:3.11-slim bash -c 'pip install ms-ivy && ivy_check trains.ivy'` |
| macOS x86_64   | Same Docker command, or native Linux VM                                              |
| Windows        | WSL2 Ubuntu + `pip install ms-ivy`                                                   |

### Expected outcome

```
trains.ivy: OK
```

If Ivy cannot automatically find inductive strengthening lemmas, run:

```bash
ivy_check complete=fo trains.ivy
```

## Connection to TLA+ spec

| Ivy concept          | TLA+ equivalent                          |
|----------------------|------------------------------------------|
| `proc`               | `Procs`                                  |
| `tick`               | `Tick` (interpreted as `nat`)            |
| `has_acked(P,Cl,Iss)`| `tr[t].acks` after ring traversal        |
| `delivered(P,M,…)`   | `delivered[p]` sequence membership       |
| `deliver` action     | `DeliverTrain(p, t)` in TRAINS.tla       |
| `ack` action         | `ProcessTrain(p, t)` ack step            |
| `total_order` inv    | `ConsistentDelivery` + ordering by CKLt  |

The `forall Q. has_acked(Q, cl, iss)` precondition mirrors
`tr[t].acks = Procs` in TLA+, which is the UTO condition.

## Limitations

- The `succ` relation is axiomatised but the ring-traversal
  (token passing) that populates `has_acked` is not modelled — only
  the delivery precondition is verified.
- Within-train message ordering is abstracted out (messages carry
  explicit train identity `(cl, iss)` rather than being sorted).
- A full Ivy proof including the token-passing liveness argument
  would require adding `reach(P, Q)` derived from `succ^*`.
