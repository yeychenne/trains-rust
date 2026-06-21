//! Delivery condition and ordering logic.
//!
//! Corresponds to `DeliverTrain` and `AllPriorDelivered` in TRAINS.tla.
//!
//! ## Total-order delivery
//! Messages from multiple trains are delivered in strictly ascending
//! `(clock, issuer)` order — the `ClockKey` ordering from TRAINS.tla.
//! Within a single train, payloads are delivered in their on-train
//! insertion order; the loader is deterministic by sorting on
//! `(sender, seq)` before forwarding.
//!
//! ## Delivery modes
//! | Mode              | Condition                | TLA+ equivalent        |
//! |-------------------|--------------------------|------------------------|
//! | UniformTotalOrder | ack_bits == FULL_ACK     | acks = Procs           |
//! | TotalOrder        | acks ⊇ non-crashed nodes | acks ⊇ Procs \ crashed |
//! | CausalOrder       | (not yet implemented)    | weaker condition       |

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::types::{AckBits, ProcId, Tick, FULL_ACK};

/// Delivery semantics for [`DeliveryState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(kani, derive(kani::Arbitrary))]
pub enum DeliveryMode {
    /// Strongest: ack from every node required (including those that
    /// will subsequently crash).  Matches the TRAINS.tla invariants.
    UniformTotalOrder,
    /// Ack from all currently non-crashed nodes suffices.
    TotalOrder,
    /// Weakest: causal delivery only (not yet implemented).
    CausalOrder,
}

/// `(clock, issuer)` ordering key — the global total order on trains.
/// Corresponds to `ClockKey` in TRAINS.tla.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClockKey {
    pub clock:  Tick,
    pub issuer: ProcId,
}

impl ClockKey {
    pub fn new(clock: Tick, issuer: ProcId) -> Self {
        Self { clock, issuer }
    }
}

/// Per-process delivery state.
///
/// Tracks the explicit set of `(clock, issuer)` keys delivered so far,
/// matching TRAINS.tla's `doneKeys[p]` directly. We use a `BTreeSet`
/// so `AllPriorDelivered` can be answered by iterating in order from
/// the smallest unprocessed key.
#[derive(Debug, Clone)]
pub struct DeliveryState {
    mode:          DeliveryMode,
    /// All `(clock, issuer)` keys whose payloads have been delivered.
    /// TLA+: `doneKeys[p]`.
    done_keys:     BTreeSet<ClockKey>,
    /// Crash mask: bit i set iff process i is known to have crashed.
    crashed_bits:  AckBits,
}

impl DeliveryState {
    pub fn new(mode: DeliveryMode) -> Self {
        Self {
            mode,
            done_keys:    BTreeSet::new(),
            crashed_bits: 0,
        }
    }

    /// Mark process `p` as crashed (used in TotalOrder mode).
    pub fn mark_crashed(&mut self, p: ProcId) {
        self.crashed_bits |= 1 << p;
    }

    /// Clear process `p`'s crashed bit — the inverse of [`mark_crashed`], for v3
    /// re-admission (the spec's `ReAdmit`: membership shrinks). After this, `p`
    /// is live again, so the surviving-view mask `FULL_ACK & !crashed_bits`
    /// requires its ack once more — i.e. it is a full acking member, not just an
    /// on-ring follower.
    pub fn unmark_crashed(&mut self, p: ProcId) {
        self.crashed_bits &= !(1 << p);
    }

    /// Has process `p` been confirmed crashed?
    pub fn is_crashed(&self, p: ProcId) -> bool {
        self.crashed_bits & (1 << p) != 0
    }

    /// Raw crashed bitmask (for state-transfer snapshots).
    pub fn crashed_bits(&self) -> AckBits {
        self.crashed_bits
    }

    /// Overwrite delivered-keys + crashed mask — used by state transfer when a
    /// rejoining/new node installs a snapshot of the live view.
    pub fn restore(&mut self, done_keys: BTreeSet<ClockKey>, crashed_bits: AckBits) {
        self.done_keys = done_keys;
        self.crashed_bits = crashed_bits;
    }

    /// Returns true if `ack_bits` satisfies the delivery condition for
    /// the configured mode.
    pub fn ready_to_deliver(&self, ack_bits: AckBits, mode: DeliveryMode) -> bool {
        match mode {
            DeliveryMode::UniformTotalOrder => ack_bits == FULL_ACK,
            DeliveryMode::TotalOrder => {
                let live_mask = FULL_ACK & !self.crashed_bits;
                ack_bits & live_mask == live_mask
            }
            DeliveryMode::CausalOrder => false, // not yet implemented
        }
    }

    /// Returns true iff this key has not yet been delivered AND every
    /// strictly-smaller `(clock, issuer)` key that the caller knows about
    /// has already been delivered.
    ///
    /// Corresponds to `AllPriorDelivered(p, ck)` in TRAINS.tla.
    ///
    /// `known_keys` is the set of `(clock, issuer)` keys for which the
    /// caller has *seen* trains — both delivered and undelivered. The
    /// check is: every key in `known_keys` strictly less than `key` must
    /// already be in `done_keys`.
    pub fn is_next_in_order<'a>(
        &self,
        key: ClockKey,
        known_keys: impl IntoIterator<Item = &'a ClockKey>,
    ) -> bool {
        if self.done_keys.contains(&key) {
            return false;
        }
        for k in known_keys {
            if *k < key && !self.done_keys.contains(k) {
                return false;
            }
        }
        true
    }

    /// Has the caller already delivered this key?
    pub fn already_delivered(&self, key: ClockKey) -> bool {
        self.done_keys.contains(&key)
    }

    /// Record that a train with the given key has been delivered.
    pub fn record_delivered(&mut self, key: ClockKey) {
        debug_assert!(!self.done_keys.contains(&key), "double delivery of {key:?}");
        self.done_keys.insert(key);
    }

    pub fn done_keys(&self) -> &BTreeSet<ClockKey> {
        &self.done_keys
    }

    pub fn mode(&self) -> DeliveryMode {
        self.mode
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uto_requires_full_ack() {
        let ds = DeliveryState::new(DeliveryMode::UniformTotalOrder);
        assert!(!ds.ready_to_deliver(0b011, DeliveryMode::UniformTotalOrder));
        assert!( ds.ready_to_deliver(0b111, DeliveryMode::UniformTotalOrder));
    }

    #[test]
    fn to_ignores_crashed() {
        let mut ds = DeliveryState::new(DeliveryMode::TotalOrder);
        ds.mark_crashed(2); // proc 2 crashed
        assert!( ds.ready_to_deliver(0b011, DeliveryMode::TotalOrder));
        assert!(!ds.ready_to_deliver(0b001, DeliveryMode::TotalOrder));
    }

    #[test]
    fn ordering_blocks_until_prior_delivered() {
        let mut ds = DeliveryState::new(DeliveryMode::UniformTotalOrder);
        let k_low  = ClockKey::new(1, 0);
        let k_high = ClockKey::new(2, 0);
        let known  = [k_low, k_high];

        // Cannot deliver k_high while k_low is pending
        assert!(!ds.is_next_in_order(k_high, &known));
        assert!( ds.is_next_in_order(k_low,  &known));

        ds.record_delivered(k_low);
        assert!(ds.is_next_in_order(k_high, &known));
    }

    #[test]
    fn double_delivery_blocked() {
        let mut ds = DeliveryState::new(DeliveryMode::UniformTotalOrder);
        let k = ClockKey::new(1, 0);
        ds.record_delivered(k);
        assert!(!ds.is_next_in_order(k, &[k]));
    }
}
