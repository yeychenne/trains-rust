//! ◇S-style failure detector (Gap B).
//!
//! A single `Output::DeclareCrash` from the core is only a *hint* — a clock
//! gap can be transient reordering, not a crash. This detector accumulates
//! suspicion and only confirms a permanent crash once the evidence is strong
//! enough, so the (correctness-critical) `confirm_crash` / view change is not
//! triggered on a false positive.
//!
//! Evidence sources:
//!   * **clock-gap hints** (`record_gap_hint`) — the core emits
//!     `Output::DeclareCrash(issuer)` when an expected train fails to arrive
//!     in clock order (the protocol's timeout/sequence-gap signal). Each hint
//!     is one strike.
//!   * **successor disconnect** (`record_disconnect`) — the ring transport
//!     lost its connection to a peer (strong evidence); contributes
//!     `disconnect_weight` strikes at once.
//!
//! Anti-false-positive: `note_alive(p)` (called whenever we hear from `p`,
//! e.g. receive a train it issued) clears accrued suspicion. So a transient
//! gap followed by recovery never confirms; only *sustained* silence — N
//! strikes with no intervening sign of life — crosses the threshold. This is
//! the eventually-strong (◇S) property: the detector may suspect a slow node
//! briefly but corrects as soon as it hears from it again, and permanently
//! confirms a node that truly stops.
//!
//! Confirmation is monotonic: once a process is confirmed crashed it stays
//! confirmed (a permanent crash, matching `crashed_bits` in the core).

use std::collections::{BTreeMap, BTreeSet};

use trains_core::ProcId;

/// Accrual of crash suspicion per process.
pub struct FailureDetector {
    /// Strikes accrued per process since we last heard from it.
    strikes: BTreeMap<ProcId, u32>,
    /// Strikes required to confirm a crash.
    threshold: u32,
    /// Strikes a single successor-disconnect contributes (consumed by
    /// `record_disconnect`).
    disconnect_weight: u32,
    /// Processes confirmed crashed (monotonic).
    confirmed: BTreeSet<ProcId>,
}

impl FailureDetector {
    /// `threshold` = strikes to confirm (≥1). `disconnect_weight` = strikes a
    /// disconnect adds (set ≥ threshold to confirm immediately on disconnect).
    pub fn new(threshold: u32, disconnect_weight: u32) -> Self {
        assert!(threshold >= 1, "threshold must be ≥ 1");
        Self {
            strikes: BTreeMap::new(),
            threshold,
            disconnect_weight,
            confirmed: BTreeSet::new(),
        }
    }

    /// Record a clock-gap crash hint for `victim`. Returns `Some(victim)` iff
    /// this strike newly confirms the crash.
    pub fn record_gap_hint(&mut self, victim: ProcId) -> Option<ProcId> {
        self.add_strikes(victim, 1)
    }

    /// Record a ring-transport disconnect from `victim` (strong evidence).
    /// Returns `Some(victim)` iff this newly confirms the crash. Wired to
    /// trains-net's `unreachable_rx` in the node binary (clean-crash signal).
    pub fn record_disconnect(&mut self, victim: ProcId) -> Option<ProcId> {
        let w = self.disconnect_weight;
        self.add_strikes(victim, w)
    }

    fn add_strikes(&mut self, victim: ProcId, n: u32) -> Option<ProcId> {
        if self.confirmed.contains(&victim) {
            return None; // already confirmed — idempotent
        }
        let c = self.strikes.entry(victim).or_insert(0);
        *c = c.saturating_add(n);
        if *c >= self.threshold {
            self.strikes.remove(&victim);
            self.confirmed.insert(victim);
            Some(victim)
        } else {
            None
        }
    }

    /// We heard from `p` (e.g. received a train `p` issued) → it is alive →
    /// clear any accrued (un-confirmed) suspicion. A confirmed crash is
    /// permanent and is NOT reset.
    pub fn note_alive(&mut self, p: ProcId) {
        if !self.confirmed.contains(&p) {
            self.strikes.remove(&p);
        }
    }

    /// Has `p` been confirmed crashed?
    pub fn is_confirmed(&self, p: ProcId) -> bool {
        self.confirmed.contains(&p)
    }

    /// Current accrued strikes for `p` (0 if none / confirmed).
    pub fn suspicion(&self, p: ProcId) -> u32 {
        *self.strikes.get(&p).unwrap_or(&0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_gap_hint_below_threshold_does_not_confirm() {
        let mut fd = FailureDetector::new(3, 3);
        assert_eq!(fd.record_gap_hint(2), None);
        assert_eq!(fd.suspicion(2), 1);
        assert!(!fd.is_confirmed(2));
    }

    #[test]
    fn n_strikes_confirm_on_the_nth() {
        let mut fd = FailureDetector::new(3, 3);
        assert_eq!(fd.record_gap_hint(2), None);
        assert_eq!(fd.record_gap_hint(2), None);
        assert_eq!(fd.record_gap_hint(2), Some(2), "3rd strike confirms");
        assert!(fd.is_confirmed(2));
    }

    #[test]
    fn note_alive_resets_suspicion_no_false_positive() {
        // Transient reordering: a gap, then we hear from the node again.
        let mut fd = FailureDetector::new(3, 3);
        fd.record_gap_hint(2);
        fd.record_gap_hint(2);
        assert_eq!(fd.suspicion(2), 2);
        fd.note_alive(2); // heard from it → not crashed
        assert_eq!(fd.suspicion(2), 0);
        // Two more strikes still don't confirm (counter was reset).
        assert_eq!(fd.record_gap_hint(2), None);
        assert_eq!(fd.record_gap_hint(2), None);
        assert!(!fd.is_confirmed(2));
    }

    #[test]
    fn disconnect_weight_can_confirm_immediately() {
        let mut fd = FailureDetector::new(3, 3); // weight == threshold
        assert_eq!(fd.record_disconnect(1), Some(1), "disconnect confirms at once");
        assert!(fd.is_confirmed(1));
    }

    #[test]
    fn disconnect_weight_below_threshold_accrues() {
        let mut fd = FailureDetector::new(5, 2);
        assert_eq!(fd.record_disconnect(1), None); // 2
        assert_eq!(fd.record_disconnect(1), None); // 4
        assert_eq!(fd.record_disconnect(1), Some(1)); // 6 ≥ 5
    }

    #[test]
    fn confirmation_is_idempotent() {
        let mut fd = FailureDetector::new(1, 1);
        assert_eq!(fd.record_gap_hint(2), Some(2));
        assert_eq!(fd.record_gap_hint(2), None, "further hints don't re-confirm");
        assert_eq!(fd.record_disconnect(2), None);
        assert!(fd.is_confirmed(2));
    }

    #[test]
    fn note_alive_does_not_unconfirm() {
        let mut fd = FailureDetector::new(1, 1);
        fd.record_gap_hint(2);
        assert!(fd.is_confirmed(2));
        fd.note_alive(2); // a stray late message must not resurrect a dead node
        assert!(fd.is_confirmed(2), "confirmed crash is permanent");
    }

    #[test]
    fn independent_victims_tracked_separately() {
        let mut fd = FailureDetector::new(2, 2);
        assert_eq!(fd.record_gap_hint(1), None);
        assert_eq!(fd.record_gap_hint(2), None);
        fd.note_alive(1);
        assert_eq!(fd.record_gap_hint(2), Some(2), "2 unbroken strikes on 2");
        assert!(!fd.is_confirmed(1));
    }
}
