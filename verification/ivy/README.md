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

## Install Ivy

```bash
pip install z3-solver
git clone https://github.com/kenmcmil/ivy
cd ivy
python setup.py install
```

Requires Python 3.8+, z3 ≥ 4.12.

## Run the proof

```bash
ivy_check trains.ivy
```

Expected output:

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
