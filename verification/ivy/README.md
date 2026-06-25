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

## Status — 2026-06-25 first run

`ivy_check` was executed on `trains.ivy` for the first time
(`ms-ivy 1.8.26`, Python 3.10, x86_64 Linux via podman).

**Result: ❌ verification condition is outside Ivy's decidable FAU
fragment**

```
error: The verification condition is not in the fragment FAU.
An interpreted symbol is applied to a universally quantified
variable:
trains.ivy: line 23: Cl:tick < Cl2
The quantified variable is Cl:tick
```

The cause is the combination of `interpret tick -> nat` (line 6) with
the universal quantifier over `Cl: tick` in the `total_order`
invariant (line 23) compared via `<`.  Ivy's EPR/FAU fragment forbids
interpreted relations (`<` on `nat`) being applied to universally
quantified variables — that pattern makes the verification condition
undecidable.

This is **not a protocol bug** — TLC + Apalache have verified the
same total-order property at depth 8 in both UTO and TO modes (see
[`../../VERIFICATION_REPORT.md`](../../VERIFICATION_REPORT.md)).  It
is a spec-writing problem in the Ivy file: to stay in FAU we need to
either (a) replace `tick`'s nat interpretation with an uninterpreted
`before(t1, t2)` relation plus the appropriate axioms (transitivity,
totality, irreflexivity), or (b) use Ivy's instantiation pragmas to
take the quantifier out of the formula, or (c) prove the invariant
without quantifying over both clocks at once.

**Follow-up:** rewrite the spec so the verification condition stays
in FAU.  Tracked separately; the existing TLC + Apalache results
already cover this property — Ivy's value is in parameterised
verification for unbounded N, not in re-proving what TLC has already
shown at small N.

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
