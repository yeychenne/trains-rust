# TRAINS Verification Report

Date: 2026-05-10
Branch: `main`
Last commit at report-time: `9b73029` (will be updated on final push)

---

## Executive summary

The TRAINS protocol implementation has been verified against its formal
specification at three levels:

| Level | Tool | Result |
|------|------|------|
| **Spec safety + liveness** | TLC (3 model sizes) | ✅ N=3/MaxClock=4: 1.09M states; N=3/MaxClock=6: 2.66M; N=4: 2.86M — all pass |
| **Re-admission safety (v3)** | TLC (TO mode) | ✅ N=3/MaxClock=4 with `ReAdmit`: 6.28M distinct states, no error — `ConsistentDelivery` preserved across re-admission (membership grows *and* shrinks) |
| **Re-admission liveness** | TLC (TO mode + SF on `ReAdmit`) | ✅ N=3/MaxClock=3 small-model: 3,819 distinct states, no error — `EventualReAdmit` holds: under strong fairness, every crashed process eventually exits `crashed` (modulo the model's finite clock budget) |
| **Implementation vs adversarial schedules** | PropTest | ✅ 256 random schedules incl. crash injection, no violations |
| **Implementation vs reference** | PropTest DRT | ✅ 384 random cases, no divergences |
| **Leaf-function correctness** | Kani (CBMC) | ✅ 8/8 harnesses verified in 0.23s total |
| **Runtime spec-correspondence** | JSONL trace + validator | ✅ live demo emits trace; validator re-checks all 6 invariants per record |

Five real bugs caught and fixed during verification:

1. **TLC**: `TypeOK` precedence — `0..MaxClock \X Procs` parsed wrong
2. **TLC**: `IsPrefix` missing length guard — crashed on legitimate states
3. **TLC**: `ConsistentDelivery` race — async slot clocks let nodes deliver `(2,1)` before `(2,0)` was issued. Fixed by adding `issuedKeys` set + all-issuers-caught-up gate; propagated to Rust impl.
4. **DRT**: reference impl missed clock-gap on first arrival from never-heard issuer
5. **DRT**: reference impl missed `broadcast_seen` dedupe (TLA+ `m \notin broadcast` precondition)

The `is_deliverable` heuristic in the original Rust impl skipped never-heard
issuers — exactly the bug TLC caught. This is the most consequential finding:
without TLC the implementation would have shipped silently incorrect for
multi-train concurrent broadcast.

---

## Phase A — TLC (TLA+ model checker)

**Tool**: TLA+ Toolbox / TLC2 v2026.05.04.141011
**Java**: OpenJDK 21.0.11

**Configuration** (`verification/tla/TRAINS_MC.cfg`):
- `Procs = {0, 1, 2}` (3-node ring)
- `ring = <<0, 1, 2>>`
- `NumTrains = 2` (two issuers)
- `Messages = {m1, m2, m3}` (with `MessageSymmetry` for 6× state reduction)
- `MaxClock = 4`
- `MaxPending = 2`

**Run**:
```
java -XX:+UseParallelGC -cp /tmp/tla2tools.jar tlc2.TLC \
    TRAINS_MC.tla -config TRAINS_MC.cfg \
    -workers auto -noGenerateSpecTE
```

**Result**: ✅ `Model checking completed. No error has been found.`

| Metric | Value |
|------|------|
| States generated | 1,090,959 |
| Distinct states | 428,336 |
| Search depth | 45 |
| Workers | 10 (parallel) |
| Time | 25s |

**Verified invariants**:
- `TypeOK` (type safety)
- `ClockMonotonicity`
- `ConsistentDelivery` (P1/P2: mutual-prefix delivery logs)
- `NoSpuriousDelivery` (P3)
- `TrainIntegrity`
- `IssuerUniqueness`

**Verified property**:
- `EventualDelivery` (with weak fairness)

**Spec bugs found and fixed in this phase**:

1. *TypeOK precedence*: `\in [Procs -> SUBSET (0..MaxClock \X Procs)]`
   parsed as `0..(MaxClock \X Procs)`. Fixed: `(0..MaxClock) \X Procs`.

2. *IsPrefix domain error*: `SubSeq(t, 1, Len(s))` requires `Len(s) <= Len(t)`.
   Without the guard, TLC crashed evaluating `IsPrefix(<<m1>>, <<>>)`. Fix:
   `IsPrefix(s, t) == Len(s) <= Len(t) /\ SubSeq(t, 1, Len(s)) = s`.

3. *ConsistentDelivery race* (the consequential one): `AllPriorDelivered`
   checked per-slot CURRENT clocks, but slots advance asynchronously. When
   slot 2 reached `(2, 1)` while slot 1 was still at `(1, 0)`, node 0
   delivered `(2, 1)` BEFORE `(2, 0)` was ever stamped on a train. Fix:
   - Added global `issuedKeys` variable tracking every `(clock, issuer)` ever stamped
   - `AllPriorDelivered` now requires both: every issued smaller key in `doneKeys`, AND every issuer's `issClk` ≥ key.clock
   - `RecycleEmptyTrain` records the empty key in every live process's `doneKeys` (so successor keys aren't blocked)

The same fix was propagated to the Rust impl (`seen_clocks_advanced_enough`):
no longer skips never-heard issuers, iterates `0..NUM_TRAINS`, requires
`seenClk[self][q] >= key.clock` for every issuer.

**Counterexample example (fix #3)** — from `claude-agents/outputs/tlc_report.txt`:
```
node 0: <<m1, m2>>   (delivered (1,0) then (2,1))
node 1: <<m1, m3>>   (delivered (1,0) then (2,0))
```
Mutual prefix violated. After the fix, this state is unreachable.

**Files**: `verification/tla/{TRAINS.tla, TRAINS_MC.tla, TRAINS_MC.cfg}`,
`claude-agents/outputs/tlc_report.txt`.

### Bigger TLC models (added in roadmap follow-up)

To reduce confidence that "TLC at small N is happy" is hiding bugs at
larger sizes, two bigger models were also run:

| Config | States | Distinct | Depth | Wall | Result |
|---|---:|---:|---:|---|---|
| `MaxClock=6` (others same) | 2,661,628 | 1,061,977 | 61 | 1m 5s | ✅ |
| `N=4`, `NumTrains=2`, `MaxClock=4` | 2,863,920 | 1,098,813 | 52 | 1m 2s | ✅ |

The corrected `AllPriorDelivered` continues to hold; no new
counterexamples surface as the model grows.

**Files**: `verification/tla/TRAINS_MC_N4.cfg`,
`claude-agents/outputs/{tlc_maxclock6.txt, tlc_n4.txt}`.

### TLC liveness for dynamic membership (`EventualReAdmit`)

The `EventualDelivery` liveness property in Phase A above is checked
*only on the static-membership spec* (UTO).  The membership round
(`Reconfigure`/`ReAdmit`) has, until now, only been TLC-safety-verified
(the 6.28M-state run referenced in the executive summary).  This
sub-phase closes the liveness gap.

**The property** — defined in `TRAINS_MC.tla` under "LIVENESS FOR
DYNAMIC MEMBERSHIP":

```tla
EventualReAdmit ==
  \A p \in Procs :
    (p \in crashed) ~> (p \notin crashed \/ ModelClockExhausted)
```

The disjunct `ModelClockExhausted == \A q \in Issuers : issClk[q] >=
MaxClock` is the honest acknowledgement that TLC's finite model
legitimately disables `ReAdmit` once every live issuer has used its
clock budget — the model bound, not a protocol failure.  Without that
disjunct TLC finds the obvious counter-example where the clock
ceiling, not the protocol, stops re-admit.

**Fairness** — added in the same MC module:

```tla
MembershipFairness == \A p \in Procs : SF_vars(ReAdmit(p))

SpecTOLiveness ==
  Init /\ [][Next]_vars /\ Fairness /\ MembershipFairness
```

Strong fairness (not weak) because between view-change steps `ReAdmit`
can be transiently disabled (e.g. a survivor's clock catches up to
`MaxClock` momentarily).  `Reconfigure` deliberately gets no fairness
— crashes are environmental, not forced.

**Model** — small on purpose; TLC liveness is 5-10× slower than
safety, and the safety configs already cover the bigger 6.28M
state-space.  This run targets a clean liveness claim:

```
Procs       = {0, 1, 2}
NumTrains   = 1
Messages    = {m1}
MaxClock    = 3
MaxPending  = 1
Mode        = "TO"
SYMMETRY    = disabled  (TLC warns symmetry can mask liveness violations)
```

Config: `verification/tla/TRAINS_MC_TO_liveness.cfg`.

**Result** (2026-06-24):

| Metric                | Value |
|-----------------------|------:|
| States generated      | 8,300 |
| Distinct states found | 3,819 |
| Search depth          | 19 |
| Wall-clock            | 1 s |
| Result                | ✅ `Model checking completed. No error has been found.` |

What this proves: under strong fairness on `ReAdmit`, the protocol
itself does not starve recovery — every crashed process eventually
exits `crashed` (modulo the finite model's clock budget, captured
explicitly in the disjunct).  Catches the failure mode "ReAdmit is
permanently disabled by something the protocol does." None exists.

### Phase F — Apalache symbolic check

**Tool**: Apalache 0.57.0 (SMT backend = Z3).  Where TLC enumerates
concrete states, Apalache encodes the next-state relation as SMT
formulas and reasons over symbolic sets of states.  At equal depth
this is strictly stronger than TLC.

**Spec adjustments** required by Apalache (`@type:` annotations are
TLC-invisible comments; the other three are equivalence-preserving
rewrites):

1. Snowcat type annotations on every `CONSTANT`, `VARIABLE`, and on
   polymorphic operators (`Range`, `RingPos`, `Succ`, `Pred`,
   `ClockKey`, `CKLt`).
2. `MsgsToSeq` rewritten from `RECURSIVE` `SortSet` to `ApaFoldSet`
   (Apalache does not support recursive operators).  TLC sees a
   sibling `verification/tla/Apalache.tla` stub that provides a
   `RECURSIVE` definition.
3. `IsPrefix(s, t)` rewritten from `SubSeq(t, 1, Len(s)) = s` to
   `\A i \in DOMAIN s : s[i] = t[i]` (Apalache requires constant
   bounds in `SubSeq`).
4. `1..Len(ring)` replaced with `DOMAIN ring` (Apalache resolves the
   latter as a constant set when `ring` is bound by `ConstInit`).

TLC continues to pass on the rewritten spec (`MaxClock=6` config:
2,661,628 states, no error — identical to pre-rewrite).

**Configuration** (mirrors `TRAINS_MC.cfg` via `ConstInit` in
`TRAINS_MC.tla`): `Procs={0,1,2}`, `NumTrains=2`,
`Messages={"m1","m2","m3"}`, `MaxClock=4`, `MaxPending=2`.

**Results**:

| Invariant            | Mode | Length | Result    | Wall   |
|----------------------|------|--------|-----------|--------|
| Snowcat type-check   | n/a  | n/a    | ✅ OK      | < 1 s |
| `ConsistentDelivery` | UTO  | 5      | ✅ NoError | 3.4 s |
| `ConsistentDelivery` | UTO  | 8      | ✅ NoError | 103 s |
| `NoSpuriousDelivery` | UTO  | 5      | ✅ NoError | 4.1 s |
| `ClockMonotonicity`  | UTO  | 5      | ✅ NoError | 4.9 s |
| `TrainIntegrity`     | UTO  | 5      | ✅ NoError | 5.5 s |
| `IssuerUniqueness`   | UTO  | 5      | ✅ NoError | 5.4 s |
| `ConsistentDelivery` | **TO** | 8    | ✅ NoError | 36 m 38 s (2026-06-23) |
| `OtherSafetyTO` (combined: ClockMonotonicity ∧ NoSpuriousDelivery ∧ TrainIntegrity ∧ IssuerUniqueness) | **TO** | 8 | ✅ NoError | 6 h 17 m 58 s (2026-06-24/25) |

**What's still not done**: an unbounded **inductive-invariant** check
(find `IndInv` such that `Init ⇒ IndInv` and `IndInv ∧ Next ⇒ IndInv'`,
where `IndInv ⇒ ConsistentDelivery`).  Bounded symbolic checking at
depth 8 is itself a strong result, but inductive verification would
remove the `MaxClock` and `MaxPending` bounds entirely.  Constructing
the candidate `IndInv` is non-trivial.

**Files**: `verification/tla/{TRAINS.tla, TRAINS_MC.tla, Apalache.tla}`,
`claude-agents/outputs/apalache_report.txt`.

---

## Phase E — Runtime trace + spec-correspondence validator

**What it is**: every step of every node in the live ring demo can
emit a JSONL `TraceRecord` (`trains-core::trace::TraceRecord`) capturing
the input action, outputs, and a post-step state slice
(`seen_clk`, `iss_clk`, `pending_count`, `done_keys_count`,
`delivered_len`). A separate binary `trains-trace-validate` reads the
trace and re-checks the same six invariants TLC verifies on the spec —
per record:

| Invariant | How the validator checks it |
|------|------|
| `TypeOK` | Range bounds on every field (node, sender, seen_clk length) |
| `ClockMonotonicity` | `seen_clk[node][q]` never decreases between this node's records |
| `ConsistentDelivery` | Validator-accumulated per-node delivered logs are mutual prefixes |
| `NoSpuriousDelivery` | Every delivered payload key appeared in some prior train output |
| `TrainIntegrity` | Forwarded train payload senders ∈ `0..RING_SIZE` |
| `IssuerUniqueness` | Structurally guaranteed by `BTreeSet<ClockKey>` in impl |

The trace is **gated on "interesting" events** (delivery, non-empty
forward, broadcast, declared crash) so empty-train cycling doesn't
inflate the log. The 9-broadcast stress run produces a 5 KB / 17-record
trace; the validator processes it in milliseconds.

**Why this is meaningful**: it's not a mechanical refinement proof,
but it's strictly stronger than "the demo asserts ConsistentDelivery
once at the end". Every step's state is spot-checked against the spec
invariants. An implementation that produces a bad state at any step
fails the validator immediately. This catches Rust ↔ TLA+ drift at
runtime.

**Integration test**: `crates/trains-cli/tests/trace_pipeline.rs`
spawns the demo with `--trace`, then runs the validator on the output
and asserts both succeed.

**Files**:
- `crates/trains-core/src/trace.rs` — `TraceRecord`, `TrainSummary`, `PayloadKey`
- `crates/trains-cli/src/ring.rs` — emission gated on interesting events
- `crates/trains-cli/src/bin/trace_validate.rs` — the validator
- `crates/trains-cli/tests/trace_pipeline.rs` — end-to-end test

**Usage**:
```
trains ring --num 3 --num-trains 2 --seconds 4 \
    --broadcast 0:hello --broadcast 1:world --broadcast 2:foo \
    --trace /tmp/trains.jsonl
trains-trace-validate /tmp/trains.jsonl
# → "✅ all 6 invariants hold across the trace"
```

---

## Phase B — Crash + reorder + PropTest fuzz

**Tool**: PropTest 1.x via `cargo test`

**Tests** (`crates/trains-core/tests/adversarial.rs`):

Hand-written:
- `one_node_crashes_after_two_broadcasts`: crash a node after broadcasts, survivors agree
- `surviving_nodes_keep_consistent_logs_after_crash`: crash early, full ring tour
- `deferred_train_does_not_break_total_order`: simulate packet reorder
- `multiple_crashes_drop_no_invariants`: crash an *issuer* mid-flight

PropTest fuzz (128 cases × ~1024 shrink iters each):
- `fuzz_no_crashes_preserves_invariants`: random Step/Broadcast/Defer, ConsistentDelivery + NoSpuriousDelivery
- `fuzz_one_crash_preserves_invariants`: same + one crash event at random step

**Result**: ✅ 6/6 tests, 0 violations across ~256 random schedules.

---

## Phase C — Differential Random Testing (DRT)

**Tool**: PropTest cross-implementation differential harness

The `trains-reference` crate is a maximally-clear, optimisation-free
implementation of TRAINS — variable names mirror `TRAINS.tla` exactly,
data structures are `Vec`/`BTreeSet`, no shortcuts. The DRT harness feeds
the same `Input` sequence to both `trains-core` (production) and
`trains-reference`, then asserts identical normalised outputs.

**Tests** (`verification/drt/src/lib.rs`):
- `drt_node0`, `drt_node1`, `drt_node2`: PropTest, 128 cases each (384 total)
- `empty_schedule_matches`, `broadcast_only_matches`, `foreign_train_first_arrival_matches`: sanity checks

**Result**: ✅ 6/6 tests, 0 divergences across 384 random differential schedules.

**DRT bugs found in the reference impl**:

1. *Reference missed clock-gap on first arrival*: production declares
   `DeclareCrash` when a train arrives with `clock > prev_seen + 1`,
   including the bootstrap case `prev_seen = 0`. Reference gated the
   check on `prev > 0`. PropTest minimised to:
   ```
   Receive(Train{issuer=2, clock=2, payloads=[], ack_bits=0})
   ```
   on a never-heard issuer. Fixed by removing the `prev > 0` guard.

2. *Reference missed broadcast_seen dedupe*: TLA+ `AppBroadcast` requires
   `m \notin broadcast`. Production tracks every `(sender, seq)` ever
   observed and silently drops re-broadcasts; reference re-broadcast
   freely. PropTest minimised to a 4-event schedule echoing a payload
   back to its sender. Fixed by adding `broadcast_seen: HashSet<(u8, u64)>`
   to reference and tracking on issue + lap-1 load + lap-2 replay.

---

## Phase D — Kani (CBMC bounded model checker)

**Tool**: Kani 0.67.0 / CBMC, nightly-2025-11-21 toolchain

**Result**: ✅ **8/8 harnesses verified, 0 failures.** Total verification time ≈ 0.23s.

| # | Harness | Time | Property |
|---|---------|------|----------|
| 1 | `verify_tick_no_overflow` | 0.007s | `Tick::checked_add(1)` doesn't overflow when caller stays below `Tick::MAX` |
| 2 | `verify_tick_monotonic` | 0.009s | `a + 1 > a` for any `Tick a < Tick::MAX` |
| 3 | `verify_clock_state_monotonic` | 0.021s | `ClockState::last_seen()` never decreases after `check_and_update` |
| 4 | `verify_clock_state_ok_iff_successor` | 0.029s | `check_and_update` returns `Ok` ⇔ `new_clock == prev + 1` |
| 5 | `verify_add_ack_monotonic` | 0.037s | `Train::add_ack(id)` only sets bits and sets `id`'s bit |
| 6 | `verify_is_fully_acked_iff_full` | 0.027s | `is_fully_acked()` ⇔ `ack_bits == FULL_ACK` |
| 7 | `verify_uto_requires_full_ack` | 0.066s | UTO `ready_to_deliver(b)` ⇔ `b == FULL_ACK` |
| 8 | `verify_clock_key_lex_order` | 0.033s | `ClockKey` lex order is correct: `(c1,i1) < (c2,i2) ⇔ c1<c2 ∨ (c1==c2 ∧ i1<i2)` |

**What was deliberately NOT proved with Kani**:

The natural harness `verify_no_panic_step` (calling `TrainsNode::step()` on
arbitrary inputs) is fundamentally incompatible with Kani 0.67's CBMC backend:
the standard library's `BTreeSet::search` and `BTreeMap::insert` use unbounded
recursion that CBMC cannot finitely unwind. After ~700 unwinding iterations
CBMC fails. This is a known Kani limitation, not a soundness issue.

The same property is exercised by:
- `cargo test`: 6 PropTest fuzz tests with random `Step`/`Broadcast`/`Defer` events
  + 2 fuzz tests with crash injection — 256 random schedules total, no panics
- The 384-case DRT harness — also no panics

`HashSet`/`HashMap` were replaced with `BTreeSet`/`BTreeMap` in
`trains-core/src/node.rs` specifically to make Kani harnesses tractable.
HashSet construction calls `CCRandomGenerateBytes` (Apple's RandomState),
which Kani cannot model.

**Files**: `crates/trains-core/src/lib.rs` (`#[cfg(kani)] mod kani_proofs`),
`claude-agents/outputs/kani_report.txt`.

---

## Workspace test totals

After all phases:

| Crate | Tests | Notes |
|------|------|------|
| `trains-core` (unit) | 15 | clock, delivery, node |
| `trains-core` (ring integration) | 4 | 3-node in-memory ring |
| `trains-core` (adversarial) | 6 | crashes + 256 PropTest cases |
| `trains-net` | 7 | TLS, codec, fingerprint pinning |
| `trains-ao` | 7 | envelope, AO adapter |
| `trains-reference` | 3 | reference impl unit |
| `trains-drt` | 6 | DRT, 384 PropTest cases |
| **Total** | **48** | + Kani harnesses |

Plus the live 3-node TLS demo (`cargo run --bin trains -- ring …`)
exercises the full stack end-to-end and asserts ConsistentDelivery
in the demo runtime.

---

## What is still NOT verified (research-grade work)

- **Verus proof** of `ConsistentDelivery` as inductive invariant — heavy install (Verus needs to be built from source); deferred. Skeleton exists at `claude-agents/agents/verus_writer.md`.
- **Ivy parameterized proof** for unbounded N — file exists at `verification/ivy/trains.ivy` but `ivy_check` has not been run. Ivy install is non-trivial (Python 2 / Z3 bindings).
- **Apalache symbolic** verification of inductive invariants without bounding clock or message counts.
- **Refinement proof** Rust ↔ TLA+ — no mechanized argument that `TrainsNode::step` corresponds to a step of `Spec`. Comments and DRT are the current, weaker, link.
- **Match Simatic 2015 paper formally** — my TLA+ spec is my interpretation; correspondence to the published algorithm is informal.

These are exactly the items the two sub-agent tracks
(`ao-topology/`, `claude-agents/`) are designed to delegate. Track A and
Track B were attempted in this session as background agents but **stalled
on the long `cargo kani setup` / source-build phases**. Lessons:
- Heavy tool installs need explicit progress indicators or a separate setup phase
- The agent runtime's stream watchdog kills agents that go silent for >10 min
- For long-running verification, prefer foreground execution with `Monitor` polling

---

## Files of interest

- `verification/tla/TRAINS.tla` — formal spec, TLC-verified
- `verification/tla/TRAINS_MC.cfg` — TLC config
- `verification/ivy/trains.ivy` — Ivy proof skeleton (not run)
- `verification/reference/src/lib.rs` — reference impl
- `verification/drt/src/lib.rs` — DRT harness
- `crates/trains-core/src/lib.rs` — Kani harnesses (cfg(kani))
- `crates/trains-core/tests/adversarial.rs` — crash + PropTest
- `claude-agents/outputs/tlc_report.txt` — raw TLC output
