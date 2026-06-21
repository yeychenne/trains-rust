//! Wire messages carried over a ring link.
//!
//! Originally the ring carried only [`Train`]s. Reconfiguration (PR-R3) adds
//! view-change control frames that circulate the ring alongside trains, so the
//! link now carries a tagged [`WireMsg`]. The transport demultiplexes incoming
//! frames: trains go to the normal inbox, view-change frames to a separate
//! channel the node's reconfiguration logic drives.
//!
//! All frames carry a `view_id` so a node can fence stale-view messages (a
//! frame from a superseded view is ignored) — the wire-level counterpart of
//! the core's view floor.

use serde::{Deserialize, Serialize};

use trains_core::{ProcId, RecoveryPlan, RecoveryReport, Train};

/// View-change (reconfiguration) control tokens — the distributed C3 view
/// change. They circulate the re-formed ring like trains (a Totem-style
/// membership token); the coordinator (lowest-id survivor) drives the round.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ViewChangeMsg {
    /// **Gather token.** Initiated by the coordinator with its own report;
    /// each survivor appends its `(id, report)` as the token passes. It
    /// completes when it returns to the coordinator carrying every survivor's
    /// report, at which point the coordinator computes the plan.
    Gather {
        view_id: u64,
        coordinator: ProcId,
        victim: ProcId,
        reports: Vec<(ProcId, RecoveryReport)>,
    },
    /// **Install token.** The coordinator's agreed plan. Every survivor
    /// applies it (idempotently) and reissues as the token passes; it stops
    /// when it returns to a node that already installed this view.
    Install {
        view_id: u64,
        coordinator: ProcId,
        victim: ProcId,
        plan: RecoveryPlan,
    },
    /// **Re-admit Gather token (PR-RJ-2).** A returning node seeds this toward
    /// the coordinator to rejoin a ring it was excluded from; each current
    /// member appends its `(id, report)` as the token passes, mirroring the
    /// exclude `Gather`. The membership operation is *adding* `rejoiner` back.
    ReAdmitGather {
        view_id: u64,
        coordinator: ProcId,
        rejoiner: ProcId,
        reports: Vec<(ProcId, RecoveryReport)>,
    },
    /// **Re-admit Install token (PR-RJ-2).** The coordinator's plan to
    /// re-include `rejoiner`. `snapshot_src` names the survivor the rejoiner
    /// should `fetch_snapshot` from to catch up before it resumes delivering;
    /// the freeze→snapshot→insert sequencing (driver side) keeps its delivery
    /// log a mutual prefix (no join-point gap).
    ReAdmitInstall {
        view_id: u64,
        coordinator: ProcId,
        rejoiner: ProcId,
        plan: RecoveryPlan,
        snapshot_src: ProcId,
    },
}

impl ViewChangeMsg {
    /// The view this token belongs to (for stale-view fencing).
    pub fn view_id(&self) -> u64 {
        match self {
            ViewChangeMsg::Gather { view_id, .. }
            | ViewChangeMsg::Install { view_id, .. }
            | ViewChangeMsg::ReAdmitGather { view_id, .. }
            | ViewChangeMsg::ReAdmitInstall { view_id, .. } => *view_id,
        }
    }

    /// The crashed member an **exclude** view change removes (`None` for the
    /// re-admit tokens, which add a member rather than remove one).
    pub fn victim(&self) -> Option<ProcId> {
        match self {
            ViewChangeMsg::Gather { victim, .. } | ViewChangeMsg::Install { victim, .. } => {
                Some(*victim)
            }
            ViewChangeMsg::ReAdmitGather { .. } | ViewChangeMsg::ReAdmitInstall { .. } => None,
        }
    }

    /// The member a **re-admit** view change adds back (`None` for the exclude
    /// tokens).
    pub fn rejoiner(&self) -> Option<ProcId> {
        match self {
            ViewChangeMsg::ReAdmitGather { rejoiner, .. }
            | ViewChangeMsg::ReAdmitInstall { rejoiner, .. } => Some(*rejoiner),
            ViewChangeMsg::Gather { .. } | ViewChangeMsg::Install { .. } => None,
        }
    }
}

/// Anything sent over a ring link: a circulating train, or a view-change
/// control frame.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WireMsg {
    Train(Train),
    ViewChange(ViewChangeMsg),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use trains_core::{compute_recovery_plan, RecoveryReport, RING_SIZE};

    fn report() -> RecoveryReport {
        RecoveryReport {
            seen: [0; RING_SIZE],
            issued: [0; RING_SIZE],
            done: BTreeSet::new(),
            have: BTreeMap::new(),
        }
    }

    #[test]
    fn readmit_tokens_round_trip_and_accessors() {
        let gather = ViewChangeMsg::ReAdmitGather {
            view_id: 7,
            coordinator: 0,
            rejoiner: 2,
            reports: vec![(0, report())],
        };
        let install = ViewChangeMsg::ReAdmitInstall {
            view_id: 7,
            coordinator: 0,
            rejoiner: 2,
            plan: compute_recovery_plan(&[report()]),
            snapshot_src: 0,
        };
        // Accessors: re-admit tokens have a rejoiner, no victim; exclude tokens
        // are the mirror image.
        assert_eq!(gather.view_id(), 7);
        assert_eq!(gather.rejoiner(), Some(2));
        assert_eq!(gather.victim(), None);
        assert_eq!(install.rejoiner(), Some(2));
        let excl = ViewChangeMsg::Gather { view_id: 1, coordinator: 0, victim: 2, reports: vec![] };
        assert_eq!(excl.victim(), Some(2));
        assert_eq!(excl.rejoiner(), None);

        // serde round-trip over the wire frame (bincode, as the codec uses).
        for m in [gather, install] {
            let wire = WireMsg::ViewChange(m.clone());
            let bytes = bincode::serde::encode_to_vec(&wire, bincode::config::standard()).unwrap();
            let (back, _): (WireMsg, _) =
                bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
            assert_eq!(back, wire);
        }
    }
}
