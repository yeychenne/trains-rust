//! Logical clock arithmetic and crash detection.
//!
//! Corresponds to `seenClk` and `issClk` in TRAINS.tla.
//!
//! ## Clock-gap detection
//! When train T from issuer q arrives at process p, p checks:
//!
//!   seenClk[p][q] + 1 < T.clock  →  clock gap  →  q may have crashed
//!   seenClk[p][q] + 1 = T.clock  →  expected    →  normal case
//!   seenClk[p][q]     = T.clock  →  duplicate   →  already processed
//!
//! In the TLA+ spec this is captured implicitly: `seenClk` is only
//! updated when a train arrives and `RecycleTrain` increments `issClk`
//! strictly. The gap condition `T.clock > seenClk[p][q] + 1` means at
//! least one complete train from q went missing.

use crate::types::{ProcId, Tick, RING_SIZE};

/// Per-process clock state.
///
/// `seen[q]` = last clock seen from issuer q.
/// Corresponds to `seenClk[p]` in TRAINS.tla for a fixed process p.
#[derive(Debug, Clone)]
pub struct ClockState {
    seen: [Tick; RING_SIZE],
}

/// Result of checking a new train clock against the last seen value.
#[derive(Debug, PartialEq, Eq)]
pub enum ClockCheck {
    /// Expected next clock — normal progression.
    Ok,
    /// Clock gap: one or more trains from this issuer were lost.
    Gap { expected: Tick, received: Tick },
    /// Already processed this clock — duplicate or out-of-order delivery.
    Duplicate,
}

impl ClockState {
    pub fn new() -> Self {
        Self { seen: [0; RING_SIZE] }
    }

    /// Returns the last clock seen from `issuer`.
    pub fn last_seen(&self, issuer: ProcId) -> Tick {
        self.seen[issuer as usize]
    }

    /// Overwrite the whole seen-vector — used by state transfer when a
    /// rejoining/new node installs a snapshot of the live view.
    pub fn restore(&mut self, seen: [Tick; RING_SIZE]) {
        self.seen = seen;
    }

    /// Checks `new_clock` against the last seen value for `issuer`,
    /// updates the stored clock if the check passes, and returns the
    /// result.
    ///
    /// Corresponds to the `seenClk'` update in `ProcessTrain`.
    pub fn check_and_update(&mut self, issuer: ProcId, new_clock: Tick) -> ClockCheck {
        let prev = self.seen[issuer as usize];
        if new_clock == prev {
            ClockCheck::Duplicate
        } else if new_clock == prev.saturating_add(1) {
            self.seen[issuer as usize] = new_clock;
            ClockCheck::Ok
        } else if new_clock > prev {
            // Gap: trains were lost (at least one process crashed)
            self.seen[issuer as usize] = new_clock;
            ClockCheck::Gap { expected: prev + 1, received: new_clock }
        } else {
            // new_clock < prev: stale / out-of-order
            ClockCheck::Duplicate
        }
    }
}

impl Default for ClockState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_progression() {
        let mut cs = ClockState::new();
        assert_eq!(cs.check_and_update(0, 1), ClockCheck::Ok);
        assert_eq!(cs.check_and_update(0, 2), ClockCheck::Ok);
        assert_eq!(cs.last_seen(0), 2);
    }

    #[test]
    fn gap_detection() {
        let mut cs = ClockState::new();
        let r = cs.check_and_update(1, 3); // jumped from 0 to 3
        assert_eq!(r, ClockCheck::Gap { expected: 1, received: 3 });
        assert_eq!(cs.last_seen(1), 3);
    }

    #[test]
    fn duplicate_ignored() {
        let mut cs = ClockState::new();
        cs.check_and_update(0, 1);
        assert_eq!(cs.check_and_update(0, 1), ClockCheck::Duplicate);
        assert_eq!(cs.last_seen(0), 1);
    }
}
