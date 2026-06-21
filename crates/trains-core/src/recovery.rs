//! View-change recovery for ring reconfiguration (C3 — lost-key gap
//! resolution).
//!
//! When a node crashes it can take in-flight trains with it. The lost
//! trains' clock slots were *seen* by survivors on lap-1 (so their
//! `seenClk` advanced) but never *delivered* (the train never completed
//! its lap). `is_deliverable`'s `AllPriorDelivered` gate (see
//! `node.rs`) then blocks every later key forever: the clock sticks and
//! no post-crash broadcast can be delivered.
//!
//! This module implements the Totem-style **token-recovery** that closes
//! those gaps without violating uniform agreement. It is split into a
//! pure merge function ([`compute_recovery_plan`]) — trivially testable
//! and the safety-critical core — and a per-node apply step
//! ([`crate::node::TrainsNode::apply_recovery`]).
//!
//! ## Protocol (virtual-synchrony view change)
//! 1. **Freeze + report.** On a confirmed crash each survivor produces a
//!    [`RecoveryReport`]: its per-issuer highest *seen* clock, the set of
//!    keys it has delivered (`done`), and the payloads it *has* available
//!    for any key (from its recently-delivered cache or a parked, blocked
//!    train) — [`crate::node::TrainsNode::recovery_report`].
//! 2. **Merge.** A coordinator runs [`compute_recovery_plan`] over all
//!    survivors' reports to produce one [`RecoveryPlan`] every survivor
//!    applies identically.
//! 3. **Apply + install view.** Each survivor applies the plan in key
//!    order (deliver-with-agreed-payloads or empty-skip), advances its
//!    issue clock above the agreed boundary, and reissues its token.
//!
//! ## Safety argument (uniform agreement is preserved)
//! The merge classifies every key `(cl, q)` with `cl ≤ boundary[q]`
//! (`boundary[q] = max seen[q]` across survivors):
//! * **Some survivor has payloads for it** ⇒ `Deliver(union of payloads)`.
//!   A key delivered with a payload at *any* survivor is delivered with
//!   the *same* payload at *all* survivors (apply is idempotent via
//!   `already_delivered`). This is the only way to stay uniform — a naive
//!   local "skip" would drop a payload one peer already delivered and
//!   diverge the logs.
//! * **No survivor has any payload for it** ⇒ `Skip` (record an empty
//!   done-key). Safe because the key was delivered *nowhere* with content
//!   (no survivor reported payloads and none had it deliverable-but-
//!   parked). Any payload a lost train *might* have carried is dropped —
//!   a **reliability** gap (at-least-once senders re-broadcast), not a
//!   **safety** (uniform-agreement) gap.
//! * **Delivered by all survivors already** ⇒ no action (already uniform).
//!
//! ## Scope / formal-verification note
//! [`compute_recovery_plan`] is pure and unit-tested here. The full
//! protocol (freeze discipline, coordinator election, wire-level report/
//! plan exchange over `trains-net`) and its TLA+/Kani extension are the
//! remaining integration work; this module + the node seam are validated
//! end-to-end by the in-process ring test
//! `trains-cli/tests/reconfig_integration.rs`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::delivery::ClockKey;
use crate::types::{AckBits, Payload, ProcId, Tick, RING_SIZE};

/// A transferable snapshot of a node's protocol state (PR-R5 state transfer).
///
/// Recovery (C3) repairs the *log tail* of nodes that stayed in the view; a
/// node that was *down* (or brand new) instead needs the *whole* state to
/// catch up — Totem state transfer / virtual-synchrony state merge. A live
/// member exports this; a joiner imports it (via
/// [`crate::node::TrainsNode::export_state`] / `import_state`) and then
/// participates from the current view.
///
/// This carries the *protocol* state. The *application* state (the delivered
/// message log / the replicated store) is transferred alongside by the SMR
/// layer — together they let a joiner reach the same delivered prefix, so
/// ConsistentDelivery holds across the join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSnapshot {
    /// Highest clock seen per issuer (`seenClk`).
    pub seen: [Tick; RING_SIZE],
    /// Keys already delivered/skipped (`doneKeys`).
    pub done_keys: BTreeSet<ClockKey>,
    /// Crashed-member bitmask (the current view's exclusions).
    pub crashed_bits: AckBits,
    /// Per-issuer view floor (old-view fence).
    pub view_floor: [Tick; RING_SIZE],
    /// `(sender, seq)` of every payload seen, to dedup post-join.
    pub broadcast_seen: BTreeSet<(ProcId, u64)>,
}

/// One survivor's snapshot, gathered at the start of a view change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryReport {
    /// Highest clock seen per issuer slot (`seenClk[self]`).
    pub seen: [Tick; RING_SIZE],
    /// Highest clock this survivor has *issued* for a slot it owns (others
    /// are 0). A node can issue a token beyond what any survivor has yet
    /// *seen* — that token may be in flight (and lost) at snapshot time, so
    /// its clock must be folded into the boundary or it becomes a phantom
    /// hole that blocks `AllPriorDelivered` after the reissue.
    pub issued: [Tick; RING_SIZE],
    /// Keys this survivor has already processed (delivered or empty-skipped).
    pub done: BTreeSet<ClockKey>,
    /// Payloads this survivor *has* for a key but may not have delivered
    /// everywhere yet — sourced from its recently-delivered cache and any
    /// parked (ack-complete but order-blocked) train. Empty-valued entries
    /// are omitted.
    pub have: BTreeMap<ClockKey, Vec<Payload>>,
}

/// What every survivor must do with one recovered key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryAction {
    /// Deliver these payloads (already in agreed union order; the applier
    /// re-sorts deterministically) iff not already delivered.
    Deliver(Vec<Payload>),
    /// Record an empty done-key (no payload was delivered anywhere).
    Skip,
}

/// The uniform plan produced by the coordinator and applied by every
/// survivor. `actions` is ordered by `ClockKey` so application proceeds in
/// total order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryPlan {
    /// Per-key recovery action, ascending by `(clock, issuer)`.
    pub actions: BTreeMap<ClockKey, RecoveryAction>,
    /// The agreed delivery boundary per issuer: the highest clock anyone
    /// saw. Survivors install a fresh view by reissuing each surviving
    /// issuer's token at `boundary[q] + 1`.
    pub boundary: [Tick; RING_SIZE],
}

impl RecoveryPlan {
    /// Clock at which surviving issuer `q` should reissue its token
    /// (one past the agreed boundary, so no new key collides with a
    /// recovered one).
    pub fn reissue_clock(&self, q: ProcId) -> Tick {
        self.boundary[q as usize].saturating_add(1)
    }
}

/// Merge survivors' reports into a single recovery plan.
///
/// Pure and deterministic: given the same multiset of reports, every
/// coordinator produces the same plan, so all survivors converge. See the
/// module-level safety argument.
pub fn compute_recovery_plan(reports: &[RecoveryReport]) -> RecoveryPlan {
    // Agreed boundary per issuer: the highest clock any survivor saw OR
    // issued for that slot. Including `issued` covers a token a (surviving)
    // issuer put in flight but no one has seen returned — otherwise its
    // clock becomes a phantom hole one past the boundary.
    let mut boundary = [0 as Tick; RING_SIZE];
    for r in reports {
        for (q, b) in boundary.iter_mut().enumerate() {
            let cand = r.seen[q].max(r.issued[q]);
            if cand > *b {
                *b = cand;
            }
        }
    }

    let mut actions: BTreeMap<ClockKey, RecoveryAction> = BTreeMap::new();

    for (q, &bound) in boundary.iter().enumerate() {
        // Clocks are issued strictly +1 by each issuer, so every value in
        // 1..=boundary[q] corresponds to a real train: it is either
        // delivered everywhere (no action) or a gap to resolve.
        for cl in 1..=bound {
            let key = ClockKey::new(cl, q as ProcId);

            // Already uniform — every survivor processed it.
            if reports.iter().all(|r| r.done.contains(&key)) {
                continue;
            }

            // Union payloads across survivors (dedup by (sender, seq)).
            let mut payloads: Vec<Payload> = Vec::new();
            let mut seen_ids: BTreeSet<(ProcId, u64)> = BTreeSet::new();
            for r in reports {
                if let Some(ps) = r.have.get(&key) {
                    for p in ps {
                        if seen_ids.insert((p.sender, p.seq)) {
                            payloads.push(p.clone());
                        }
                    }
                }
            }

            let action = if payloads.is_empty() {
                RecoveryAction::Skip
            } else {
                RecoveryAction::Deliver(payloads)
            };
            actions.insert(key, action);
        }
    }

    RecoveryPlan { actions, boundary }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pl(sender: ProcId, seq: u64, data: &[u8]) -> Payload {
        Payload { sender, seq, data: data.to_vec() }
    }

    fn report(
        seen: [Tick; RING_SIZE],
        done: &[ClockKey],
        have: &[(ClockKey, Vec<Payload>)],
    ) -> RecoveryReport {
        RecoveryReport {
            seen,
            issued: [0 as Tick; RING_SIZE],
            done: done.iter().copied().collect(),
            have: have.iter().cloned().collect(),
        }
    }

    fn report_issued(
        seen: [Tick; RING_SIZE],
        issued: [Tick; RING_SIZE],
        done: &[ClockKey],
    ) -> RecoveryReport {
        RecoveryReport {
            seen,
            issued,
            done: done.iter().copied().collect(),
            have: Default::default(),
        }
    }

    // Helper: build a [Tick; RING_SIZE] from a prefix (rest 0).
    fn seenv(vals: &[Tick]) -> [Tick; RING_SIZE] {
        let mut a = [0 as Tick; RING_SIZE];
        for (i, v) in vals.iter().enumerate() {
            a[i] = *v;
        }
        a
    }

    #[test]
    fn empty_gap_is_skipped() {
        // Two survivors both saw issuer-0 clock up to 5 but neither
        // delivered (4,0)/(5,0) (lost empty trains). No payloads anywhere
        // ⇒ both gaps become empty Skips.
        let r0 = report(
            seenv(&[5, 0]),
            &[ClockKey::new(1, 0), ClockKey::new(2, 0), ClockKey::new(3, 0)],
            &[],
        );
        let r1 = r0.clone();
        let plan = compute_recovery_plan(&[r0, r1]);

        assert_eq!(plan.boundary[0], 5);
        assert_eq!(plan.actions.get(&ClockKey::new(4, 0)), Some(&RecoveryAction::Skip));
        assert_eq!(plan.actions.get(&ClockKey::new(5, 0)), Some(&RecoveryAction::Skip));
        // Already-delivered keys get no action.
        assert!(!plan.actions.contains_key(&ClockKey::new(1, 0)));
        assert!(!plan.actions.contains_key(&ClockKey::new(3, 0)));
    }

    #[test]
    fn delivered_by_some_with_payload_is_retransmitted_not_skipped() {
        // SAFETY-CRITICAL: survivor A delivered (3,0) with a payload;
        // survivor B never saw it. The merge must DELIVER (retransmit),
        // never Skip — skipping would diverge A and B's logs.
        let key = ClockKey::new(3, 0);
        let a = report(
            seenv(&[3, 0]),
            &[ClockKey::new(1, 0), ClockKey::new(2, 0), key],
            &[(key, vec![pl(0, 7, b"payload")])],
        );
        let b = report(
            seenv(&[2, 0]),
            &[ClockKey::new(1, 0), ClockKey::new(2, 0)],
            &[],
        );
        let plan = compute_recovery_plan(&[a, b]);

        match plan.actions.get(&key) {
            Some(RecoveryAction::Deliver(ps)) => {
                assert_eq!(ps.len(), 1);
                assert_eq!(ps[0].data, b"payload");
            }
            other => panic!("expected Deliver(payload), got {other:?}"),
        }
    }

    #[test]
    fn parked_payload_contributes_to_union() {
        // B has (3,0) parked (ack-complete but order-blocked) with the
        // payload; A is missing it. The plan delivers the parked payload
        // to everyone.
        let key = ClockKey::new(3, 0);
        let a = report(seenv(&[3, 0]), &[ClockKey::new(1, 0), ClockKey::new(2, 0)], &[]);
        let b = report(
            seenv(&[3, 0]),
            &[ClockKey::new(1, 0), ClockKey::new(2, 0)],
            &[(key, vec![pl(1, 2, b"parked")])],
        );
        let plan = compute_recovery_plan(&[a, b]);
        match plan.actions.get(&key) {
            Some(RecoveryAction::Deliver(ps)) => assert_eq!(ps[0].data, b"parked"),
            other => panic!("expected Deliver, got {other:?}"),
        }
    }

    #[test]
    fn union_dedups_payloads_across_survivors() {
        // Both survivors have the same payload for (2,1); union keeps one.
        let key = ClockKey::new(2, 1);
        let p = pl(1, 9, b"dup");
        let a = report(seenv(&[0, 2]), &[ClockKey::new(1, 1)], &[(key, vec![p.clone()])]);
        let b = report(seenv(&[0, 2]), &[ClockKey::new(1, 1)], &[(key, vec![p.clone()])]);
        let plan = compute_recovery_plan(&[a, b]);
        match plan.actions.get(&key) {
            Some(RecoveryAction::Deliver(ps)) => assert_eq!(ps.len(), 1, "deduped"),
            other => panic!("expected Deliver, got {other:?}"),
        }
    }

    #[test]
    fn boundary_is_max_seen_across_survivors() {
        let a = report(seenv(&[7, 3]), &[], &[]);
        let b = report(seenv(&[4, 9]), &[], &[]);
        let plan = compute_recovery_plan(&[a, b]);
        assert_eq!(plan.boundary[0], 7);
        assert_eq!(plan.boundary[1], 9);
        assert_eq!(plan.reissue_clock(0), 8);
        assert_eq!(plan.reissue_clock(1), 10);
    }

    #[test]
    fn in_flight_issued_clock_extends_boundary_and_is_skipped() {
        // Issuer 1 issued up to clock 5 (next_issue_clock-1) but survivors
        // only *saw* up to 4 — clock 5 is an in-flight (possibly lost)
        // token. The boundary must reach 5 and (5,1) must be skipped, else
        // it becomes a phantom hole one past the boundary that blocks the
        // reissued token forever.
        let done: Vec<ClockKey> = (1..=4).map(|c| ClockKey::new(c, 1)).collect();
        let r_self = report_issued(seenv(&[0, 4]), seenv(&[0, 5]), &done);
        let r_peer = report_issued(seenv(&[0, 4]), seenv(&[0, 0]), &done);
        let plan = compute_recovery_plan(&[r_self, r_peer]);

        assert_eq!(plan.boundary[1], 5, "boundary covers the in-flight issued clock");
        assert_eq!(plan.reissue_clock(1), 6);
        assert_eq!(
            plan.actions.get(&ClockKey::new(5, 1)),
            Some(&RecoveryAction::Skip),
            "the in-flight slot is skipped, not left as a phantom hole",
        );
    }

    #[test]
    fn report_and_plan_survive_bincode_roundtrip() {
        // Wire-serializability (PR-R3): reports/plans cross the network as
        // bincode frames (the same codec trains-net uses), so they must
        // round-trip exactly. Note bincode — not JSON — is required: the maps
        // are keyed by ClockKey (a struct), which JSON can't use as a key.
        let cfg = bincode::config::standard();
        let key = ClockKey::new(2, 1);

        let report = report_issued(seenv(&[3, 2]), seenv(&[3, 0]), &[ClockKey::new(1, 0), key]);
        let bytes = bincode::serde::encode_to_vec(&report, cfg).unwrap();
        let (back, _): (RecoveryReport, _) =
            bincode::serde::decode_from_slice(&bytes, cfg).unwrap();
        assert_eq!(back.seen, report.seen);
        assert_eq!(back.issued, report.issued);
        assert_eq!(back.done, report.done);

        let mut actions = BTreeMap::new();
        actions.insert(ClockKey::new(1, 0), RecoveryAction::Skip);
        actions.insert(key, RecoveryAction::Deliver(vec![pl(1, 0, b"x")]));
        let mut boundary = [0u64; RING_SIZE];
        boundary[0] = 3;
        boundary[1] = 2;
        let plan = RecoveryPlan { actions, boundary };
        let bytes = bincode::serde::encode_to_vec(&plan, cfg).unwrap();
        let (back, _): (RecoveryPlan, _) =
            bincode::serde::decode_from_slice(&bytes, cfg).unwrap();
        assert_eq!(back.boundary, plan.boundary);
        assert_eq!(back.actions.len(), plan.actions.len());
        assert_eq!(back.actions.get(&key), plan.actions.get(&key));
    }

    #[test]
    fn fully_delivered_history_yields_no_actions() {
        // Everyone delivered everything up to the boundary → empty plan.
        let done: Vec<ClockKey> = (1..=3).map(|c| ClockKey::new(c, 0)).collect();
        let a = report(seenv(&[3, 0]), &done, &[]);
        let b = report(seenv(&[3, 0]), &done, &[]);
        let plan = compute_recovery_plan(&[a, b]);
        assert!(plan.actions.is_empty(), "no gaps to resolve");
    }

    // ── Property tests ──────────────────────────────────────────────────────
    //
    // `compute_recovery_plan` is BTreeMap/BTreeSet-heavy, so it is not a Kani
    // target (CBMC cannot finitely unwind `BTreeSet::search` — see the note in
    // `lib.rs::kani_proofs`). Following the established precedent for BTree-
    // heavy code, its safety invariants are exercised by property tests
    // instead. The key invariant is the uniform-agreement guarantee: a key any
    // survivor holds a payload for is NEVER silently skipped — it is delivered
    // with a payload union ⊇ that survivor's. A violation would diverge the
    // logs (one node delivers a payload another skipped).
    use proptest::prelude::*;

    prop_compose! {
        fn arb_report()(
            seen_v   in prop::collection::vec(0u64..=5, RING_SIZE),
            issued_v in prop::collection::vec(0u64..=5, RING_SIZE),
            done_v   in prop::collection::vec((1u64..=5, 0u8..RING_SIZE as u8), 0..6),
            have_v   in prop::collection::vec(
                ((1u64..=5, 0u8..RING_SIZE as u8),
                 prop::collection::vec((0u8..RING_SIZE as u8, 0u64..3), 1..3)),
                0..5),
        ) -> RecoveryReport {
            let mut seen = [0u64; RING_SIZE];
            for (i, v) in seen_v.iter().enumerate() { seen[i] = *v; }
            let mut issued = [0u64; RING_SIZE];
            for (i, v) in issued_v.iter().enumerate() { issued[i] = *v; }
            let done: BTreeSet<ClockKey> =
                done_v.into_iter().map(|(c, q)| ClockKey::new(c, q)).collect();
            let mut have: BTreeMap<ClockKey, Vec<Payload>> = BTreeMap::new();
            for ((c, q), ps) in have_v {
                // A node only holds payloads for keys it has SEEN, so keep
                // `have` consistent with `seen` (mirrors `recovery_report`).
                if c > seen[q as usize] { seen[q as usize] = c; }
                let payloads = ps.into_iter()
                    .map(|(s, sq)| Payload { sender: s, seq: sq, data: vec![s, sq as u8] })
                    .collect();
                have.insert(ClockKey::new(c, q), payloads);
            }
            RecoveryReport { seen, issued, done, have }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 300, ..ProptestConfig::default() })]

        #[test]
        fn merge_preserves_uniform_agreement(
            reports in prop::collection::vec(arb_report(), 1..4),
        ) {
            let plan = compute_recovery_plan(&reports);

            // boundary[q] = max(seen, issued) across survivors.
            for q in 0..RING_SIZE {
                let want = reports.iter().map(|r| r.seen[q].max(r.issued[q])).max().unwrap();
                prop_assert_eq!(plan.boundary[q], want);
            }

            // SAFETY: every payload any survivor holds is delivered (never
            // skipped), with the agreed payload set ⊇ that survivor's.
            for r in &reports {
                for (key, ps) in &r.have {
                    if ps.is_empty() { continue; }
                    prop_assert!(key.clock <= plan.boundary[key.issuer as usize]);
                    match plan.actions.get(key) {
                        Some(RecoveryAction::Deliver(out)) => {
                            for p in ps {
                                prop_assert!(
                                    out.iter().any(|o| o.sender == p.sender && o.seq == p.seq),
                                    "recovery dropped a payload a survivor held",
                                );
                            }
                        }
                        Some(RecoveryAction::Skip) => prop_assert!(
                            false, "a key with a payload was skipped (would diverge logs)",
                        ),
                        None => prop_assert!(
                            reports.iter().all(|rr| rr.done.contains(key)),
                            "payload key got no action but was not delivered by all",
                        ),
                    }
                }
            }

            // Determinism: same reports ⇒ identical plan.
            let plan2 = compute_recovery_plan(&reports);
            prop_assert_eq!(plan.actions.len(), plan2.actions.len());
            for (k, a) in &plan.actions {
                prop_assert!(plan2.actions.get(k) == Some(a));
            }
        }
    }
}
