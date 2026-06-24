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

**Status (2026-06-24): the install path below is the canonical one but the
maintained `ms-ivy` distribution only ships Linux x86_64 wheels (1.8.26 on
PyPI); the GitHub `master` branch is still Python 2 in places.  `ivy_check`
has therefore not been executed on this spec yet.  See "Running on
non-Linux hosts" below for the practical paths.**

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
