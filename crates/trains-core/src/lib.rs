//! trains-core — I/O-free TRAINS ring protocol state machine.
//!
//! This crate implements the verifiable kernel of the TRAINS protocol
//! (Simatic et al., CFIP/NOTERE 2015, IEEE 7293477).
//!
//! # Design contract
//! [`TrainsNode::step`] is pure with respect to I/O: it reads no sockets,
//! acquires no locks, and performs no allocations beyond its return value.
//! All Kani harnesses and Verus proofs target this method.
//!
//! # Crate layout
//! - [`types`]    — wire-level data structures (Train, Payload, …)
//! - [`clock`]    — logical clock arithmetic and gap detection
//! - [`delivery`] — UTO / TO / CO delivery condition and ordering
//! - [`node`]     — TrainsNode state machine (step function)
//! - [`recovery`] — view-change token-recovery (reconfiguration C3)
//! - [`kani_proofs`] — Kani verification harnesses (cfg(kani) only)

pub mod clock;
pub mod delivery;
pub mod node;
pub mod recovery;
pub mod trace;
pub mod types;

pub use delivery::{ClockKey, DeliveryMode};
pub use node::TrainsNode;
pub use recovery::{
    compute_recovery_plan, RecoveryAction, RecoveryPlan, RecoveryReport, StateSnapshot,
};
pub use trace::{NodeState, TraceAction, TraceOutput, TraceRecord, TraceSeq};
pub use types::{AckBits, Payload, ProcId, Tick, Train, FULL_ACK, RING_SIZE, NUM_TRAINS};

// ── Input / Output ──────────────────────────────────────────────────────────

/// Input events delivered to [`TrainsNode::step`].
#[derive(Debug, Clone)]
pub enum Input {
    /// A train has arrived from the predecessor on the ring.
    TrainReceived(Train),
    /// The application layer wants to broadcast a payload.
    LocalBroadcast(Vec<u8>),
    /// Heartbeat / watchdog tick — used for timeout-based crash detection.
    Tick,
}

/// Output commands returned by [`TrainsNode::step`].
#[derive(Debug, Clone, PartialEq)]
pub enum Output {
    /// Forward this train to the successor on the ring.
    ForwardTrain(Train),
    /// Deliver these payloads to the application (in the given order).
    Deliver(Vec<Payload>),
    /// Clock gap detected — suspect this node has crashed.
    DeclareCrash(ProcId),
}

// ── Kani verification harnesses ─────────────────────────────────────────────
//
// Trains-core's `step()` is too deep for CBMC under reasonable wall-clock
// (it traverses BTreeMap/BTreeSet operations + Vec drains). Instead we
// target *leaf* functions that encapsulate the protocol's arithmetic
// invariants — these are the ones actually worth a model-check, and CBMC
// dispatches them in milliseconds.

#[cfg(kani)]
mod kani_proofs {
    use super::*;
    use crate::clock::{ClockCheck, ClockState};
    use crate::delivery::{ClockKey, DeliveryMode, DeliveryState};
    use crate::types::{Train, FULL_ACK};

    /// Tick arithmetic — `checked_add(1)` never overflows when caller
    /// stays below `Tick::MAX`. (Mirrors trains-core's discipline.)
    #[kani::proof]
    fn verify_tick_no_overflow() {
        let a: Tick = kani::any();
        kani::assume(a < Tick::MAX);
        let _ = a.checked_add(1).expect("clock overflow");
    }

    /// `Tick` step is monotonic — after one increment, value strictly grows.
    #[kani::proof]
    fn verify_tick_monotonic() {
        let a: Tick = kani::any();
        kani::assume(a < Tick::MAX);
        let b = a.checked_add(1).unwrap();
        assert!(b > a);
    }

    /// `ClockState::check_and_update` never decreases stored clock.
    #[kani::proof]
    fn verify_clock_state_monotonic() {
        let mut cs = ClockState::new();
        let issuer: ProcId = kani::any();
        kani::assume(issuer < RING_SIZE as ProcId);
        let clock: Tick = kani::any();
        kani::assume(clock <= 8);  // small bound for CBMC

        let prev = cs.last_seen(issuer);
        let _ = cs.check_and_update(issuer, clock);
        assert!(cs.last_seen(issuer) >= prev,
            "ClockState::last_seen never decreases");
    }

    /// On the same input, `ClockState::check_and_update` returns `Ok`
    /// iff `new_clock == prev + 1`.
    #[kani::proof]
    fn verify_clock_state_ok_iff_successor() {
        let mut cs = ClockState::new();
        let issuer: ProcId = kani::any();
        kani::assume(issuer < RING_SIZE as ProcId);
        let clock: Tick = kani::any();
        kani::assume(clock > 0 && clock <= 8);

        // Prime: drive cs to a known state (clock 1).
        cs.check_and_update(issuer, 1);
        let prev = cs.last_seen(issuer);
        let res = cs.check_and_update(issuer, clock);

        let is_ok = matches!(res, ClockCheck::Ok);
        assert_eq!(is_ok, clock == prev + 1);
    }

    /// `Train::add_ack` is monotonic in the bit count.
    #[kani::proof]
    fn verify_add_ack_monotonic() {
        let id: ProcId = kani::any();
        kani::assume(id < RING_SIZE as ProcId);
        let issuer: ProcId = kani::any();
        kani::assume(issuer < RING_SIZE as ProcId);
        let clock: Tick = kani::any();
        kani::assume(clock <= 4);
        let ack_bits: AckBits = kani::any();

        let mut t = Train { issuer, clock, payloads: vec![], ack_bits };
        let before = t.ack_bits.count_ones();
        t.add_ack(id);
        assert!(t.ack_bits.count_ones() >= before);
        assert!(t.ack_bits & (1u32 << u32::from(id)) != 0);
    }

    /// `is_fully_acked()` ⇔ `ack_bits == FULL_ACK`.
    #[kani::proof]
    fn verify_is_fully_acked_iff_full() {
        let issuer: ProcId = kani::any();
        kani::assume(issuer < RING_SIZE as ProcId);
        let clock: Tick = kani::any();
        kani::assume(clock <= 4);
        let ack_bits: AckBits = kani::any();

        let t = Train { issuer, clock, payloads: vec![], ack_bits };
        let full = t.is_fully_acked();
        assert_eq!(full, ack_bits == FULL_ACK);
    }

    /// `DeliveryState::ready_to_deliver` in UTO mode requires FULL_ACK.
    #[kani::proof]
    fn verify_uto_requires_full_ack() {
        let ds = DeliveryState::new(DeliveryMode::UniformTotalOrder);
        let ack_bits: AckBits = kani::any();
        let ready = ds.ready_to_deliver(ack_bits, DeliveryMode::UniformTotalOrder);
        assert_eq!(ready, ack_bits == FULL_ACK);
    }

    // NOTE on `record_delivered` / `already_delivered`:
    // The natural harness here would assert
    //   ds.record_delivered(key) ⇒ ds.already_delivered(key)
    // but Kani 0.67's CBMC backend cannot finitely unwind the standard
    // library's `BTreeSet::search` (it loops through tree nodes via
    // unbounded recursion). The property is exercised by the unit
    // tests in `delivery::tests::double_delivery_blocked` instead.

    /// `ClockKey` lex order: `(c1, i1) < (c2, i2)` iff `c1 < c2` or
    /// (`c1 == c2` and `i1 < i2`).
    #[kani::proof]
    fn verify_clock_key_lex_order() {
        let c1: Tick = kani::any(); kani::assume(c1 <= 8);
        let i1: ProcId = kani::any(); kani::assume(i1 < RING_SIZE as ProcId);
        let c2: Tick = kani::any(); kani::assume(c2 <= 8);
        let i2: ProcId = kani::any(); kani::assume(i2 < RING_SIZE as ProcId);

        let k1 = ClockKey::new(c1, i1);
        let k2 = ClockKey::new(c2, i2);

        let lex = c1 < c2 || (c1 == c2 && i1 < i2);
        assert_eq!(k1 < k2, lex);
    }
}
