# Design: extending the runtime trace validator to membership changes

**Status:** Draft, 2026-06-24. Closes verification gap #9 from the
[verification roadmap](#).

**Effort:** S (1-2 days). Tightly scoped: trace schema extension +
validator extension + one new end-to-end test.

## Why

The trace validator (`trains-cli/src/bin/trace_validate.rs`, 243 lines)
re-checks 6 safety invariants on every emitted `TraceRecord`:
`TypeOK`, `ClockMonotonicity`, `ConsistentDelivery`, `NoSpuriousDelivery`,
`TrainIntegrity`, `IssuerUniqueness`.

It walks the protocol's forward-progress events
(`Broadcast`/`TrainArrived`/`Tick`) and the resulting outputs
(`Forward`/`Deliver`/`DeclareCrash`). It does **not** see the
membership-round events: a `Reconfigure` exclude or a `ReAdmit` install
emits `ViewChangeMsg` wire messages (`trains-recovery::view_change::VcAction`)
that bypass the trace stream entirely.

Consequence: a runtime `ReAdmit` that mutated `delivered` (e.g. via the
virtually-synchronous `delivered' = canon.delivered` install) would be
**invisible** to the validator — the trace simply skips the step, and the
post-install state slice would appear as if `delivered` had jumped
discontinuously. Today the validator would either pass silently (best
case) or false-positive on `ConsistentDelivery` (worst case).

We close the gap by giving the trace a faithful representation of every
view change.

## What to change

### 1. Schema — `crates/trains-core/src/trace.rs`

Three additions to existing types, all backwards-compatible (additive variants).

**a. `TraceAction` — new variants:**

```rust
pub enum TraceAction {
    /* existing: Broadcast, TrainArrived, Tick, Initial */

    /// A `Reconfigure` view-change Install token arrived: the local
    /// node is about to drop `victim` from the live view and re-form
    /// the ring around survivors.
    ReconfigureInstall {
        view_id: u32,
        victim:  ProcId,
        canon:   ProcId,  // most-advanced survivor at install time
    },

    /// A `ReAdmit` view-change Install token arrived: the local node
    /// is about to atomically re-admit `rejoiner` and adopt `canon`'s
    /// delivered log + reset every in-flight train.
    ReAdmitInstall {
        view_id:  u32,
        rejoiner: ProcId,
        canon:    ProcId,
    },
}
```

**b. `TraceOutput` — new variant:**

```rust
pub enum TraceOutput {
    /* existing: Forward, Deliver, DeclareCrash */

    /// Emitted *post-install*: the new membership view and the deliver
    /// log length the rejoiner/survivors converged to.  This is the
    /// runtime analogue of the TLA+ `ReAdmit` action's atomic update
    /// of `delivered`, `doneKeys`, `seenClk`, `tr`, `issClk`,
    /// `issuedKeys` — captured as a single membership-change event for
    /// the validator.
    MembershipInstalled {
        view_id:         u32,
        members:         u32,    // bitmask of live nodes after install
        canon_delivered: usize,  // |delivered[canon]| at install
    },
}
```

**c. `NodeState` — new field:**

```rust
pub struct NodeState {
    /* existing: id, seen_clk, iss_clk, pending_count,
                 done_keys_count, delivered_len */

    /// Bitmask of currently-crashed peers in this node's view.
    /// `bit i set ⇒ node i ∈ crashed`.  RING_SIZE ≤ 32 so a u32
    /// suffices.
    pub crashed: u32,
}
```

Backwards compatibility: `NodeState::crashed` defaults to 0 when
deserialised from an older trace (use `#[serde(default)]`).  Same for
the new variants — older validators will see and error on them, which
is the desired behaviour (a v2 trace must be read by a v2 validator).

### 2. Emission — `crates/trains-cli/src/ring.rs` + `crates/trains-cli/src/node.rs`

Two new emit points, both in the view-change hot path:

**a. On receipt of a `ReconfigureInstall` or `ReAdmitInstall` wire
message** (`crates/trains-cli/src/node.rs:229` is the current decode site):

```rust
ViewChangeMsg::ReAdmitInstall { view_id, rejoiner, canon, .. } => {
    emit_trace(
        &trace_tx, &trace_seq, id, &core, &log,
        TraceAction::ReAdmitInstall { view_id, rejoiner, canon },
        Vec::new(),
        true,
    ).await;
    // ... existing handling ...
}
```

**b. After the local node applies the install** (the point where
`view_change::VcAction::Install` is dispatched into the recovery state
machine), emit the `TraceOutput::MembershipInstalled` event with the
post-install members bitmask and canon's delivered length.

### 3. Validator — `crates/trains-cli/src/bin/trace_validate.rs`

Three additions:

**a. Maintain a per-node `crashed_set: u32`.** Update on every record
from `rec.state.crashed`; flag any decrease that isn't paired with a
`ReAdmitInstall` event (Reconfigure can only grow `crashed`; ReAdmit
can only shrink it).

**b. `ConsistentDelivery` survives ReAdmit:** when a
`MembershipInstalled` event fires, the rejoiner's reconstructed
`delivered` log MUST equal `canon`'s reconstructed log at index
`canon_delivered`. Existing `ConsistentDelivery` becomes:

```rust
for (a, b) in pairs_of_live_nodes(crashed_set) {
    let da = delivered_per_node[&a];
    let db = delivered_per_node[&b];
    let min_len = da.len().min(db.len());
    if da[..min_len] != db[..min_len] {
        violate("ConsistentDelivery split between {a} and {b}");
    }
}
```

(currently the validator does the same but with no `crashed_set`
filter — including a known-crashed node and seeing a "divergence" was
already a false positive in some traces).

**c. New invariant: `MembershipMonotonicity`.** Reconfigure can only
grow `crashed`; ReAdmit can only shrink it; the diff between
consecutive records must match the most recent install action:

```rust
let delta = current.crashed ^ previous.crashed;
match last_install_action {
    Some(TraceAction::ReconfigureInstall { victim, .. }) =>
        require(delta == (1 << victim) && (current.crashed & (1 << victim)) != 0),
    Some(TraceAction::ReAdmitInstall { rejoiner, .. }) =>
        require(delta == (1 << rejoiner) && (current.crashed & (1 << rejoiner)) == 0),
    None => require(delta == 0),  // no install ⇒ crashed unchanged
}
```

### 4. Test — `crates/trains-cli/tests/trace_pipeline.rs`

Add an integration test that:
1. Spins up a 3-node ring.
2. `fis-kill`s node 2.
3. Lets the failure detector + reconfigure run.
4. Restarts node 2; lets ReAdmit run.
5. Verifies the trace contains both `ReconfigureInstall` and
   `ReAdmitInstall` records.
6. Runs `trace_validate` on the trace; asserts exit 0.

## Backwards compatibility

- Trace schema is additive — older readers can still parse v1 records.
- A v1 trace fed to a v2 validator works: the new variants don't appear,
  `crashed: 0` is the default, and the existing invariant checks behave
  identically.
- A v2 trace fed to a v1 validator fails to deserialise on the first
  view-change record — desired.

## What's out of scope

- Liveness checks at the validator level (e.g. "ReAdmit eventually
  fires") — that's the TLC liveness check, not the runtime validator.
- View-change *initiation* events (Gather token, Compute token) — they
  matter for protocol-correctness debugging, not for the safety
  invariants the validator re-checks; if needed, add `TraceAction::
  ReAdmitGather` later as a separate change.
- Multi-machine cross-host trace correlation — keep one trace per node
  per process, validate independently. Cross-host correlation is a
  separate project.

## Acceptance criteria

1. `cargo test --workspace` passes — no regression in existing trace
   tests.
2. New `trace_validate_after_readmit` integration test passes.
3. `trace_validate` on the pre-existing E5-rejoin live-fire run
   (`trains-valkey/bench/results/ec2-2026-06-16-e5-rejoin/`) passes
   without false positives. (This requires the bench harness to also
   emit traces — a separate task; for now, validate against a local
   smoke trace.)
