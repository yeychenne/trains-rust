//! Reusable failure detection + view-change state machine for the TRAINS
//! protocol. Lives outside `trains-cli` so any state-machine-replication
//! consumer (the CLI driver, the `trains-valkey` proxy, future ports) can
//! reach the recovery primitives without depending on the CLI binary's
//! crate. Pure code motion in PR-RD-7 — the modules themselves are
//! unchanged from their pre-extraction `trains-cli` location.
//!
//! # Layout
//! - [`failure_detector`] — strike-based ◇S failure detector. Combines the
//!   "weak evidence" of a clock gap (a missed train) with the "strong
//!   evidence" of a peer being unreachable on the wire; raises a confirmed
//!   crash once the cumulative weight crosses a threshold.
//! - [`view_change`] — distributed view-change token state machine that
//!   masks a confirmed crash by reconfiguring the ring around the victim.

pub mod failure_detector;
pub mod view_change;
