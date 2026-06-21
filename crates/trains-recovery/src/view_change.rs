//! View-change orchestration state machine (PR-R3 steps 4-5).
//!
//! Pure logic for one node's role in the distributed C3 view change, driven by
//! the Gather/Install tokens that circulate the re-formed ring
//! ([`trains_net::ViewChangeMsg`]). Kept I/O-free and deterministic so it is
//! unit-testable without a network; the driver (the node event loop) owns the
//! core ([`trains_core::TrainsNode`]) and transport and just executes the
//! returned [`VcAction`]s.
//!
//! ## Protocol (Totem-style membership token)
//! 1. The **coordinator** = lowest-id survivor. On a confirmed crash it
//!    initiates a **Gather** token carrying its own [`RecoveryReport`].
//! 2. Each survivor the Gather passes appends its `(id, report)` and forwards.
//! 3. When the Gather returns to the coordinator with every survivor's report,
//!    the coordinator runs [`compute_recovery_plan`], applies it locally, and
//!    sends an **Install** token carrying the plan.
//! 4. Each survivor the Install passes applies the plan (idempotently),
//!    installs the new view, reissues, and forwards. The token stops when it
//!    reaches a node that already installed this view.
//!
//! ## Stale-view fencing (A6)
//! Every token carries a `view_id`. A token with `view_id <= installed_view`
//! is dropped — this fences duplicates/reordered/superseded tokens and
//! terminates each token's circulation after exactly one lap.
//!
//! ## Driver contract
//! Before invoking a handler the driver must already have *excluded* the
//! token's victim in the core (freeze → `confirm_crash` → retarget) and taken
//! a fresh post-freeze `recovery_report()` to pass in. The state machine
//! mirrors the victim into its own `dead` set for coordinator/quorum logic.
//!
//! NOTE: exposed via the library target (src/lib.rs) so integration tests can
//! drive it over the real transport. The live node binary wires this state
//! machine since PR-R3 step 6 / PR-R4 — see `trains-cli/src/node.rs` (reconfig
//! mode, enabled by `--peer-addr` for every node). This crate stays I/O-free
//! and deterministic so the protocol logic remains unit-testable.

use std::collections::BTreeSet;

use trains_core::{compute_recovery_plan, ProcId, RecoveryPlan, RecoveryReport};
use trains_net::ViewChangeMsg;

/// What the driver must do in response to a view-change event.
#[derive(Debug, Clone, PartialEq)]
pub enum VcAction {
    /// Send this token to the successor (over `vc_outbox`).
    Send(ViewChangeMsg),
    /// Apply `plan` to the core (`apply_recovery`, which unfreezes + drains),
    /// then reissue this node's token if it is an issuer.
    Apply(RecoveryPlan),
}

/// Per-node view-change state.
pub struct ViewChange {
    me: ProcId,
    n: usize,
    installed_view: u64,
    dead: BTreeSet<ProcId>,
}

impl ViewChange {
    pub fn new(me: ProcId, n: usize) -> Self {
        Self { me, n, installed_view: 0, dead: BTreeSet::new() }
    }

    pub fn installed_view(&self) -> u64 {
        self.installed_view
    }

    pub fn is_dead(&self, p: ProcId) -> bool {
        self.dead.contains(&p)
    }

    /// Live members (ring positions `0..n` not marked dead).
    fn alive(&self) -> BTreeSet<ProcId> {
        (0..self.n as ProcId).filter(|p| !self.dead.contains(p)).collect()
    }

    /// The coordinator = lowest-id survivor.
    pub fn coordinator(&self) -> ProcId {
        self.alive().into_iter().next().unwrap_or(self.me)
    }

    pub fn is_coordinator(&self) -> bool {
        self.me == self.coordinator()
    }

    /// A confirmed crash of `victim` (from the local failure detector). The
    /// driver has already excluded+frozen the victim and taken `my_report`.
    ///
    /// *Any* detector initiates the Gather — a clean crash is detected only by
    /// the dead node's predecessor (via successor-disconnect), which is not
    /// necessarily the coordinator. The token is addressed to the agreed
    /// coordinator (lowest survivor), seeded with the detector's report; it
    /// circulates, accumulates reports, and the coordinator computes when it
    /// returns complete. Re-initiation is suppressed once the crash is known
    /// (the `dead` set), so concurrent detectors don't spawn rival rounds for
    /// the same — deterministic — coordinator.
    pub fn on_confirm(&mut self, victim: ProcId, my_report: RecoveryReport) -> Vec<VcAction> {
        if self.dead.contains(&victim) {
            return Vec::new();
        }
        self.dead.insert(victim);
        let view_id = self.installed_view + 1;
        vec![VcAction::Send(ViewChangeMsg::Gather {
            view_id,
            coordinator: self.coordinator(),
            victim,
            reports: vec![(self.me, my_report)],
        })]
    }

    /// Handle an incoming Gather token. `my_report` is this node's fresh
    /// post-freeze snapshot (used when appending).
    pub fn on_gather(&mut self, msg: ViewChangeMsg, my_report: RecoveryReport) -> Vec<VcAction> {
        let ViewChangeMsg::Gather { view_id, coordinator, victim, reports } = msg else {
            return Vec::new();
        };
        if view_id <= self.installed_view {
            return Vec::new(); // stale / already installed (A6)
        }
        self.dead.insert(victim); // learn the crash from the token

        // Append our report if it isn't already present.
        let mut reports = reports;
        if !reports.iter().any(|(id, _)| *id == self.me) {
            reports.push((self.me, my_report));
        }

        if self.me == coordinator {
            // We are the home of the token. Compute once every survivor has
            // reported; otherwise keep it circulating to collect the rest.
            let reported: BTreeSet<ProcId> = reports.iter().map(|(id, _)| *id).collect();
            if self.alive().is_subset(&reported) {
                let snapshots: Vec<RecoveryReport> =
                    reports.iter().map(|(_, r)| r.clone()).collect();
                let plan = compute_recovery_plan(&snapshots);
                self.installed_view = view_id; // installing now fences stray tokens
                vec![
                    VcAction::Apply(plan.clone()),
                    VcAction::Send(ViewChangeMsg::Install {
                        view_id,
                        coordinator,
                        victim,
                        plan,
                    }),
                ]
            } else {
                vec![VcAction::Send(ViewChangeMsg::Gather { view_id, coordinator, victim, reports })]
            }
        } else {
            // Non-coordinator: forward toward the coordinator.
            vec![VcAction::Send(ViewChangeMsg::Gather { view_id, coordinator, victim, reports })]
        }
    }

    /// Handle an incoming Install token: apply the plan, install the view, and
    /// forward — unless we already installed this view (then drop, which
    /// terminates the token's circulation).
    pub fn on_install(&mut self, msg: ViewChangeMsg) -> Vec<VcAction> {
        let ViewChangeMsg::Install { view_id, victim, ref plan, .. } = msg else {
            return Vec::new();
        };
        if view_id <= self.installed_view {
            return Vec::new(); // already installed / stale → drop (terminates)
        }
        self.dead.insert(victim);
        self.installed_view = view_id;
        let plan = plan.clone();
        vec![VcAction::Apply(plan), VcAction::Send(msg)]
    }

    // ── v3 re-admission (the mirror of exclude — membership GROWS) ──────────
    //
    // A node excluded by a Reconfigure (and caught up to the live state as a
    // passive replica, PR-RJ-3c) rejoins the *full acking* view through a
    // re-admit view change, symmetric to the exclude path above: the
    // `ReAdmitGather`/`ReAdmitInstall` tokens circulate the ring, `view_id`-
    // fenced, and the install removes the rejoiner from `dead` instead of adding
    // a victim to it. Grounded in `verification/tla/TRAINS.tla`'s `ReAdmit`
    // action (TLC-verified, ADR-001): the new live view adopts the install-point
    // boundary and reissues, the consistent-cut barrier the spec proves keeps
    // `ConsistentDelivery` true across the membership change.

    /// Adopt the view this node caught up to (PR-RJ-3c v2 state transfer) before
    /// requesting re-admission: the installed view id and the `dead` set as seen
    /// by the survivors it synced from. Without this a freshly-restarted node
    /// would seed a stale `view_id` (its own `installed_view` is 0) and the
    /// survivors would fence the `ReAdmitGather`.
    pub fn adopt_view(&mut self, installed_view: u64, dead: impl IntoIterator<Item = ProcId>) {
        self.installed_view = installed_view;
        self.dead = dead.into_iter().collect();
    }

    /// A returning node requests re-admission to the acking view. Symmetric to
    /// [`ViewChange::on_confirm`], but the membership grows. Seeds a
    /// `ReAdmitGather` addressed to the coordinator (lowest-id survivor), carrying
    /// this node's own caught-up [`RecoveryReport`]. Requires [`adopt_view`] to
    /// have run so `coordinator()`/`view_id` resolve against the current view.
    ///
    /// [`adopt_view`]: ViewChange::adopt_view
    pub fn on_request_readmit(&mut self, my_report: RecoveryReport) -> Vec<VcAction> {
        let view_id = self.installed_view + 1;
        vec![VcAction::Send(ViewChangeMsg::ReAdmitGather {
            view_id,
            coordinator: self.coordinator(),
            rejoiner: self.me,
            reports: vec![(self.me, my_report)],
        })]
    }

    /// Handle an incoming `ReAdmitGather`. A survivor appends its report and
    /// forwards toward the coordinator; the coordinator computes the re-admit
    /// plan once every member of the POST-re-admit view (`alive ∪ {rejoiner}`)
    /// has reported, then `Apply`s it and sends `ReAdmitInstall`.
    pub fn on_readmit_gather(
        &mut self,
        msg: ViewChangeMsg,
        my_report: RecoveryReport,
    ) -> Vec<VcAction> {
        let ViewChangeMsg::ReAdmitGather { view_id, coordinator, rejoiner, reports } = msg else {
            return Vec::new();
        };
        if view_id <= self.installed_view {
            return Vec::new(); // stale / already installed (A6)
        }

        // Append our report if it isn't already present.
        let mut reports = reports;
        if !reports.iter().any(|(id, _)| *id == self.me) {
            reports.push((self.me, my_report));
        }

        if self.me == coordinator {
            // The post-re-admit view must all have reported: the current
            // survivors PLUS the rejoiner (still in `dead` until we install).
            let mut target = self.alive();
            target.insert(rejoiner);
            let reported: BTreeSet<ProcId> = reports.iter().map(|(id, _)| *id).collect();
            if target.is_subset(&reported) {
                let snapshots: Vec<RecoveryReport> =
                    reports.iter().map(|(_, r)| r.clone()).collect();
                let plan = compute_recovery_plan(&snapshots);
                self.installed_view = view_id; // fences stray tokens
                self.dead.remove(&rejoiner); // membership GROWS
                vec![
                    VcAction::Apply(plan.clone()),
                    VcAction::Send(ViewChangeMsg::ReAdmitInstall {
                        view_id,
                        coordinator,
                        rejoiner,
                        plan,
                        // The rejoiner confirms/refreshes its catch-up from the
                        // coordinator (any survivor would do; it's already synced
                        // via the v2 passive transfer).
                        snapshot_src: coordinator,
                    }),
                ]
            } else {
                vec![VcAction::Send(ViewChangeMsg::ReAdmitGather {
                    view_id,
                    coordinator,
                    rejoiner,
                    reports,
                })]
            }
        } else {
            vec![VcAction::Send(ViewChangeMsg::ReAdmitGather {
                view_id,
                coordinator,
                rejoiner,
                reports,
            })]
        }
    }

    /// Handle an incoming `ReAdmitInstall`: apply the plan, REMOVE the rejoiner
    /// from `dead` (the view grows), install the view, reissue, and forward —
    /// unless we already installed this view (then drop, terminating the token).
    pub fn on_readmit_install(&mut self, msg: ViewChangeMsg) -> Vec<VcAction> {
        let ViewChangeMsg::ReAdmitInstall { view_id, rejoiner, ref plan, .. } = msg else {
            return Vec::new();
        };
        if view_id <= self.installed_view {
            return Vec::new(); // already installed / stale → drop (terminates)
        }
        self.dead.remove(&rejoiner); // membership grows
        self.installed_view = view_id;
        let plan = plan.clone();
        vec![VcAction::Apply(plan), VcAction::Send(msg)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use trains_core::RING_SIZE;

    fn report() -> RecoveryReport {
        RecoveryReport {
            seen: [0; RING_SIZE],
            issued: [0; RING_SIZE],
            done: BTreeSet::new(),
            have: BTreeMap::new(),
        }
    }

    // n=3, victim=2 (non-issuer); survivors {0,1}, coordinator = 0.

    #[test]
    fn coordinator_initiates_gather_on_confirm() {
        let mut vc = ViewChange::new(0, 3);
        let acts = vc.on_confirm(2, report());
        assert_eq!(acts.len(), 1);
        match &acts[0] {
            VcAction::Send(ViewChangeMsg::Gather { view_id, coordinator, victim, reports }) => {
                assert_eq!((*view_id, *coordinator, *victim), (1, 0, 2));
                assert_eq!(reports.len(), 1);
                assert_eq!(reports[0].0, 0, "coordinator seeds with its own report");
            }
            other => panic!("expected Gather, got {other:?}"),
        }
        assert!(vc.is_dead(2));
    }

    #[test]
    fn detector_initiates_routing_gather_to_coordinator() {
        // The detector (node 1) is NOT the coordinator, but it still
        // initiates — a routing Gather addressed to the lowest survivor (0).
        let mut vc = ViewChange::new(1, 3);
        let acts = vc.on_confirm(2, report());
        assert_eq!(acts.len(), 1);
        match &acts[0] {
            VcAction::Send(ViewChangeMsg::Gather { coordinator, reports, .. }) => {
                assert_eq!(*coordinator, 0, "addressed to lowest survivor");
                assert_eq!(reports[0].0, 1, "seeded with the detector's report");
            }
            other => panic!("expected routing Gather, got {other:?}"),
        }
        assert!(vc.is_dead(2));
    }

    #[test]
    fn coordinator_forwards_incomplete_gather() {
        // 5-node ring, node 4 dead, survivors {0,1,2,3}. Coordinator 0 gets a
        // Gather that doesn't yet cover all survivors → it appends itself and
        // forwards (keeps gathering), it does NOT compute yet.
        let mut vc = ViewChange::new(0, 5);
        let incoming = ViewChangeMsg::Gather {
            view_id: 1, coordinator: 0, victim: 4, reports: vec![(3, report())],
        };
        let acts = vc.on_gather(incoming, report());
        assert_eq!(acts.len(), 1);
        match &acts[0] {
            VcAction::Send(ViewChangeMsg::Gather { reports, .. }) => {
                let ids: BTreeSet<_> = reports.iter().map(|(id, _)| *id).collect();
                assert!(ids.contains(&0) && ids.contains(&3), "coordinator appended itself");
                assert!(!ids.contains(&1), "not yet complete");
            }
            other => panic!("expected forwarded Gather, got {other:?}"),
        }
        assert_eq!(vc.installed_view(), 0, "no install until complete");
    }

    #[test]
    fn non_coordinator_appends_and_forwards_gather() {
        let mut vc = ViewChange::new(1, 3);
        let incoming = ViewChangeMsg::Gather {
            view_id: 1, coordinator: 0, victim: 2, reports: vec![(0, report())],
        };
        let acts = vc.on_gather(incoming, report());
        assert_eq!(acts.len(), 1);
        match &acts[0] {
            VcAction::Send(ViewChangeMsg::Gather { reports, .. }) => {
                assert_eq!(reports.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![0, 1]);
            }
            other => panic!("expected Gather, got {other:?}"),
        }
    }

    #[test]
    fn coordinator_completes_gather_into_apply_and_install() {
        let mut vc = ViewChange::new(0, 3);
        vc.on_confirm(2, report()); // marks 2 dead, initiates
        // Gather returns with both survivors' reports.
        let returned = ViewChangeMsg::Gather {
            view_id: 1, coordinator: 0, victim: 2,
            reports: vec![(0, report()), (1, report())],
        };
        let acts = vc.on_gather(returned, report());
        assert_eq!(acts.len(), 2);
        assert!(matches!(acts[0], VcAction::Apply(_)), "coordinator applies");
        assert!(matches!(acts[1], VcAction::Send(ViewChangeMsg::Install { view_id: 1, .. })));
        assert_eq!(vc.installed_view(), 1, "coordinator installs on completion");
    }

    #[test]
    fn non_coordinator_install_applies_and_forwards_then_drops_on_replay() {
        let mut vc = ViewChange::new(1, 3);
        let dummy_plan: RecoveryPlan = compute_recovery_plan(&[report(), report()]);
        let install = ViewChangeMsg::Install {
            view_id: 1, coordinator: 0, victim: 2, plan: dummy_plan,
        };
        let acts = vc.on_install(install.clone());
        assert_eq!(acts.len(), 2);
        assert!(matches!(acts[0], VcAction::Apply(_)));
        assert!(matches!(acts[1], VcAction::Send(ViewChangeMsg::Install { .. })));
        assert_eq!(vc.installed_view(), 1);
        // Replay (token came back around) → dropped, terminating circulation.
        assert!(vc.on_install(install).is_empty(), "stale install dropped");
    }

    #[test]
    fn stale_gather_is_fenced() {
        let mut vc = ViewChange::new(1, 3);
        // Pretend view 1 is already installed.
        let install = ViewChangeMsg::Install {
            view_id: 1, coordinator: 0, victim: 2,
            plan: compute_recovery_plan(&[report()]),
        };
        vc.on_install(install);
        assert_eq!(vc.installed_view(), 1);
        // A late Gather for view 1 must be dropped.
        let late = ViewChangeMsg::Gather {
            view_id: 1, coordinator: 0, victim: 2, reports: vec![(0, report())],
        };
        assert!(vc.on_gather(late, report()).is_empty());
    }

    // ── v3 re-admission (n=3, node 2 was excluded → view 1, dead={2}) ───────

    #[test]
    fn rejoiner_requests_readmit_seeds_gather_to_coordinator() {
        let mut vc = ViewChange::new(2, 3);
        vc.adopt_view(1, [2]); // caught up to the post-exclusion view
        let acts = vc.on_request_readmit(report());
        assert_eq!(acts.len(), 1);
        match &acts[0] {
            VcAction::Send(ViewChangeMsg::ReAdmitGather {
                view_id, coordinator, rejoiner, reports,
            }) => {
                assert_eq!((*view_id, *coordinator, *rejoiner), (2, 0, 2));
                assert_eq!(reports[0].0, 2, "seeded with the rejoiner's own report");
            }
            other => panic!("expected ReAdmitGather, got {other:?}"),
        }
    }

    #[test]
    fn non_coordinator_appends_and_forwards_readmit_gather() {
        let mut vc = ViewChange::new(1, 3);
        vc.adopt_view(1, [2]);
        let incoming = ViewChangeMsg::ReAdmitGather {
            view_id: 2, coordinator: 0, rejoiner: 2, reports: vec![(2, report())],
        };
        let acts = vc.on_readmit_gather(incoming, report());
        assert_eq!(acts.len(), 1);
        match &acts[0] {
            VcAction::Send(ViewChangeMsg::ReAdmitGather { reports, .. }) => {
                assert_eq!(reports.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![2, 1]);
            }
            other => panic!("expected forwarded ReAdmitGather, got {other:?}"),
        }
        assert!(vc.is_dead(2), "rejoiner still dead until install");
    }

    #[test]
    fn coordinator_completes_readmit_gather_grows_membership() {
        let mut vc = ViewChange::new(0, 3);
        vc.adopt_view(1, [2]);
        assert!(vc.is_dead(2));
        // Gather returns with the rejoiner + the other survivor.
        let returned = ViewChangeMsg::ReAdmitGather {
            view_id: 2, coordinator: 0, rejoiner: 2,
            reports: vec![(2, report()), (1, report())],
        };
        let acts = vc.on_readmit_gather(returned, report());
        assert_eq!(acts.len(), 2);
        assert!(matches!(acts[0], VcAction::Apply(_)), "coordinator applies");
        assert!(matches!(
            acts[1],
            VcAction::Send(ViewChangeMsg::ReAdmitInstall { view_id: 2, rejoiner: 2, .. })
        ));
        assert_eq!(vc.installed_view(), 2);
        assert!(!vc.is_dead(2), "membership grew — rejoiner re-admitted");
    }

    #[test]
    fn coordinator_forwards_incomplete_readmit_gather() {
        // 5-node ring, node 4 excluded, survivors {0,1,2,3}. A ReAdmitGather
        // covering only the rejoiner → coordinator forwards (keeps gathering).
        let mut vc = ViewChange::new(0, 5);
        vc.adopt_view(1, [4]);
        let incoming = ViewChangeMsg::ReAdmitGather {
            view_id: 2, coordinator: 0, rejoiner: 4, reports: vec![(4, report())],
        };
        let acts = vc.on_readmit_gather(incoming, report());
        assert_eq!(acts.len(), 1);
        assert!(matches!(acts[0], VcAction::Send(ViewChangeMsg::ReAdmitGather { .. })));
        assert_eq!(vc.installed_view(), 1, "no install until complete");
        assert!(vc.is_dead(4), "still dead until install");
    }

    #[test]
    fn readmit_install_grows_membership_and_drops_on_replay() {
        let mut vc = ViewChange::new(1, 3);
        vc.adopt_view(1, [2]);
        let install = ViewChangeMsg::ReAdmitInstall {
            view_id: 2, coordinator: 0, rejoiner: 2,
            plan: compute_recovery_plan(&[report(), report()]), snapshot_src: 0,
        };
        let acts = vc.on_readmit_install(install.clone());
        assert_eq!(acts.len(), 2);
        assert!(matches!(acts[0], VcAction::Apply(_)));
        assert!(matches!(acts[1], VcAction::Send(ViewChangeMsg::ReAdmitInstall { .. })));
        assert_eq!(vc.installed_view(), 2);
        assert!(!vc.is_dead(2), "rejoiner re-admitted on install");
        // Replay (token came back around) → dropped, terminating circulation.
        assert!(vc.on_readmit_install(install).is_empty(), "stale readmit install dropped");
    }

    #[test]
    fn stale_readmit_gather_is_fenced() {
        let mut vc = ViewChange::new(1, 3);
        vc.adopt_view(2, std::iter::empty()); // already at view 2
        let late = ViewChangeMsg::ReAdmitGather {
            view_id: 2, coordinator: 0, rejoiner: 2, reports: vec![(2, report())],
        };
        assert!(vc.on_readmit_gather(late, report()).is_empty(), "stale readmit gather fenced");
    }

    /// Multi-node convergence (V3-2): drive the FULL re-admit token circulation
    /// across all three nodes' state machines deterministically (no transport,
    /// so no timing flakiness) and assert they converge — the rejoiner back in
    /// the view, the same installed view everywhere. This is the distributed
    /// protocol — gather around the ring → coordinator computes → install around
    /// the ring → terminate — that `on_request_readmit`/`on_readmit_*` compose
    /// into, the re-admit counterpart of the exclude `reconfig_*` integration.
    #[test]
    fn readmit_view_change_converges_across_the_ring() {
        let succ = |p: ProcId| (p + 1) % 3; // ring <<0,1,2>>

        // Node 2 was excluded (view 1, dead={2}); all three caught up to it.
        let mut vc: Vec<ViewChange> = (0..3)
            .map(|i| {
                let mut v = ViewChange::new(i, 3);
                v.adopt_view(1, [2]);
                v
            })
            .collect();

        // The caught-up rejoiner (node 2) requests re-admission.
        let seeded = vc[2].on_request_readmit(report());
        let mut token = match &seeded[..] {
            [VcAction::Send(m)] => m.clone(),
            other => panic!("expected one ReAdmitGather, got {other:?}"),
        };

        // Circulate the ReAdmitGather until the coordinator emits an Install.
        let mut install = None;
        let mut at = succ(2); // the gather was sent to node 2's successor (0)
        for _ in 0..9 {
            let acts = vc[at as usize].on_readmit_gather(token.clone(), report());
            let mut next = None;
            for a in acts {
                match a {
                    VcAction::Send(m @ ViewChangeMsg::ReAdmitInstall { .. }) => install = Some(m),
                    VcAction::Send(m @ ViewChangeMsg::ReAdmitGather { .. }) => next = Some(m),
                    _ => {} // Apply
                }
            }
            if install.is_some() {
                break;
            }
            token = next.expect("gather keeps circulating until complete");
            at = succ(at);
        }
        let mut token = install.expect("coordinator produced a ReAdmitInstall");

        // Circulate the ReAdmitInstall; each node applies + re-admits node 2,
        // and the token terminates when it reaches an already-installed node.
        let mut at = succ(0); // coordinator (0) installed locally + sent to succ
        for _ in 0..9 {
            let acts = vc[at as usize].on_readmit_install(token.clone());
            match acts.into_iter().find_map(|a| match a {
                VcAction::Send(m) => Some(m),
                _ => None,
            }) {
                Some(m) => token = m,
                None => break, // already installed → circulation terminates
            }
            at = succ(at);
        }

        // Convergence: every node installed the re-admit view and node 2 is live.
        for (i, v) in vc.iter().enumerate() {
            assert_eq!(v.installed_view(), 2, "node {i} installed the re-admit view");
            assert!(!v.is_dead(2), "node {i} re-admitted node 2 (full member again)");
        }
    }
}
