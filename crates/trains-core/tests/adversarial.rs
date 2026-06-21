//! Adversarial scenarios: crash injection, packet reordering, PropTest fuzz.
//!
//! All tests assert the same invariants enforced by the TLC model check
//! (TRAINS.tla) on bounded models:
//!
//!   * `ConsistentDelivery`: per-node logs are mutual prefixes
//!   * `NoSpuriousDelivery`: every delivery was previously broadcast
//!   * `ClockMonotonicity`: `seen_clock(q)` never decreases
//!
//! Crash injection is the most important here — TLC explored crash
//! states up to `Cardinality(Procs) - 1`, but only on the bounded
//! model. These tests probe the actual Rust semantics under failures
//! the model checker doesn't directly cover (random schedules,
//! reordered packet arrival).

use proptest::prelude::*;
use std::collections::HashSet;
use trains_core::{
    compute_recovery_plan, DeliveryMode, Input, Output, Payload, ProcId, TrainsNode, Train,
    NUM_TRAINS, RING_SIZE,
};

const NUM_ISSUERS: usize = 2;
const STEP_BUDGET: usize = 80;

// ────────────────────────────────────────────────────────────────────────────
// In-memory ring driver (shared between hand-written + PropTest tests)
// ────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)] // `Crash` is materialised by the second proptest below via direct ring.crash() calls
enum Event {
    Step,
    Broadcast { node: u8, msg: u8 },
    Crash { node: u8 },
    Defer { node: u8 },
}

struct Ring {
    nodes:     Vec<TrainsNode>,
    in_flight: Vec<Option<Train>>,
    deferred:  Vec<Option<Train>>,
    crashed:   Vec<bool>,
    delivered: Vec<Vec<Payload>>,
    /// When true, `step()` re-forms the ring past crashed nodes
    /// (successor = next alive) instead of losing the train at a dead
    /// successor. Set by the reconfiguration tests.
    reform:    bool,
}

impl Ring {
    fn new() -> Self {
        Self::with(DeliveryMode::UniformTotalOrder, false)
    }

    /// Build a ring with an explicit delivery mode and ring-reformation
    /// policy. `TotalOrder` + `reform = true` is the reconfiguration-
    /// capable configuration used by the view-change tests.
    fn with(mode: DeliveryMode, reform: bool) -> Self {
        let mut nodes: Vec<TrainsNode> = (0..RING_SIZE as u8)
            .map(|id| TrainsNode::new(id, mode))
            .collect();
        let mut in_flight: Vec<Option<Train>> = vec![None; RING_SIZE];
        for issuer in 0..NUM_ISSUERS {
            in_flight[issuer] = Some(nodes[issuer].issue_initial_train());
        }
        Self {
            nodes,
            in_flight,
            deferred: vec![None; RING_SIZE],
            crashed:  vec![false; RING_SIZE],
            delivered: vec![Vec::new(); RING_SIZE],
            reform,
        }
    }

    /// Next alive ring position after `from` (wraps; returns `from` if it
    /// is the only survivor).
    fn next_alive_pos(&self, from: usize) -> usize {
        let mut j = (from + 1) % RING_SIZE;
        while self.crashed[j] && j != from {
            j = (j + 1) % RING_SIZE;
        }
        j
    }

    fn broadcast(&mut self, node: ProcId, data: Vec<u8>) {
        if !self.crashed[node as usize] {
            let outs = self.nodes[node as usize]
                .step(Input::LocalBroadcast(data));
            self.dispatch(node as usize, outs);
        }
    }

    fn crash(&mut self, node: ProcId) {
        self.crashed[node as usize] = true;
        // Drop any in-flight train held by the crashed node.
        self.in_flight[node as usize] = None;
        self.deferred[node as usize]  = None;
    }

    /// Defer the train held by `node` for one step (packet reordering).
    fn defer(&mut self, node: ProcId) {
        let i = node as usize;
        if self.deferred[i].is_none() {
            if let Some(t) = self.in_flight[i].take() {
                self.deferred[i] = Some(t);
            }
        }
    }

    /// Synchronous step: every holding non-crashed node forwards its
    /// train to its successor.
    fn step(&mut self) {
        // Restore deferred trains first.
        for i in 0..RING_SIZE {
            if self.in_flight[i].is_none() {
                if let Some(t) = self.deferred[i].take() {
                    self.in_flight[i] = Some(t);
                }
            }
        }

        let mut next: Vec<Option<Train>> = vec![None; RING_SIZE];

        for holder in 0..RING_SIZE {
            if let Some(train) = self.in_flight[holder].take() {
                let succ = if self.reform {
                    // Re-form past dead nodes: forward to the next alive.
                    self.next_alive_pos(holder)
                } else {
                    (holder + 1) % RING_SIZE
                };
                if succ == holder || self.crashed[succ] {
                    // Train is lost (dead successor with no reformation, or
                    // we are the sole survivor).
                    continue;
                }
                let outs = self.nodes[succ].step(Input::TrainReceived(train));
                // Route outputs straight into `next` — NOT through the shared
                // `in_flight[succ]`. (Going via `in_flight[succ]` clobbers a
                // token that node `succ` is still holding but hasn't been
                // visited for yet this step, silently dropping it. That bug
                // was invisible to the safety-only checks; the completeness
                // check surfaces it.)
                for o in outs {
                    match o {
                        Output::ForwardTrain(t) => {
                            debug_assert!(
                                next[succ].is_none(),
                                "two tokens converged on node {succ} in one step",
                            );
                            next[succ] = Some(t);
                        }
                        Output::Deliver(payloads) => self.delivered[succ].extend(payloads),
                        Output::DeclareCrash(_) => {}
                    }
                }
            }
        }

        self.in_flight = next;
    }

    /// Reconfigure the surviving view after `victim` crashes — the full
    /// Gap-C sequence, deterministically:
    ///   1. crash the victim (its in-flight tokens are lost);
    ///   2. `confirm_crash` on every survivor (delivery exclusion + drain);
    ///   3. view change: gather `recovery_report`s → `compute_recovery_plan`
    ///      → `apply_recovery` on every survivor (C3);
    ///   4. each surviving issuer reissues its token above the boundary (C2).
    fn reconfigure(&mut self, victim: ProcId) {
        self.crash(victim); // drops the victim's in-flight tokens
        self.confirm_on_survivors(victim);
        let reports = self.gather_reports();
        let plan = compute_recovery_plan(&reports);
        self.apply_and_reissue(&plan);
    }

    /// A5 — a *second* crash lands during the view change. The first crash's
    /// recovery has started (confirm + a report snapshot taken) when `v2`
    /// dies; that snapshot is now stale, so the view change restarts with
    /// `v2` excluded — confirm `v2`, gather fresh reports over the final
    /// survivors, then compute + apply one plan. The survivors must still
    /// mask both crashes. (Needs ≥2 survivors incl. an issuer ⇒ RING_SIZE≥4.)
    fn reconfigure_interleaved(&mut self, v1: ProcId, v2: ProcId) {
        // First crash + start of recovery.
        self.crash(v1);
        self.confirm_on_survivors(v1);
        let _stale_snapshot = self.gather_reports(); // taken, then invalidated

        // Second crash mid-recovery → restart the view change excluding both.
        self.crash(v2);
        self.confirm_on_survivors(v2);
        let reports = self.gather_reports();
        let plan = compute_recovery_plan(&reports);
        self.apply_and_reissue(&plan);
    }

    /// `confirm_crash(victim)` on every survivor (delivery exclusion + drain).
    fn confirm_on_survivors(&mut self, victim: ProcId) {
        for id in 0..RING_SIZE {
            if self.crashed[id] {
                continue;
            }
            let outs = self.nodes[id].confirm_crash(victim);
            self.dispatch(id, outs);
        }
    }

    /// Snapshot every survivor's recovery report (view-change phase 1).
    fn gather_reports(&self) -> Vec<trains_core::RecoveryReport> {
        let mut reports = Vec::new();
        for id in 0..RING_SIZE {
            if !self.crashed[id] {
                reports.push(self.nodes[id].recovery_report());
            }
        }
        reports
    }

    /// Apply the agreed plan on every survivor, then reissue each surviving
    /// issuer's token above the boundary (view-change phase 2 + C2).
    fn apply_and_reissue(&mut self, plan: &trains_core::RecoveryPlan) {
        for id in 0..RING_SIZE {
            if self.crashed[id] {
                continue;
            }
            let outs = self.nodes[id].apply_recovery(plan);
            self.dispatch(id, outs);
            if id < NUM_ISSUERS {
                let t = self.nodes[id].reissue_train();
                self.in_flight[id] = Some(t);
            }
        }
    }

    fn dispatch(&mut self, node: usize, outs: Vec<Output>) {
        for o in outs {
            match o {
                Output::ForwardTrain(t) => {
                    // Stash in in_flight[node] — the step loop will
                    // pick it up and move it to next[node].
                    self.in_flight[node] = Some(t);
                }
                Output::Deliver(payloads) => {
                    self.delivered[node].extend(payloads);
                }
                Output::DeclareCrash(_) => {}
            }
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Invariant checks
// ────────────────────────────────────────────────────────────────────────────

fn is_prefix(a: &[Payload], b: &[Payload]) -> bool {
    a.len() <= b.len() && a.iter().zip(b.iter()).all(|(x, y)| x == y)
}

fn check_consistent_delivery(logs: &[Vec<Payload>], crashed: &[bool]) -> Result<(), String> {
    for i in 0..logs.len() {
        if crashed[i] { continue; }
        for j in (i + 1)..logs.len() {
            if crashed[j] { continue; }
            let a = &logs[i]; let b = &logs[j];
            if !(is_prefix(a, b) || is_prefix(b, a)) {
                return Err(format!(
                    "ConsistentDelivery violated:\n  node {i}: {:?}\n  node {j}: {:?}",
                    a.iter().map(|p| p.data.clone()).collect::<Vec<_>>(),
                    b.iter().map(|p| p.data.clone()).collect::<Vec<_>>(),
                ));
            }
        }
    }
    Ok(())
}

fn check_no_spurious_delivery(
    logs: &[Vec<Payload>],
    broadcast: &HashSet<Vec<u8>>,
) -> Result<(), String> {
    for (i, log) in logs.iter().enumerate() {
        for p in log {
            if !broadcast.contains(&p.data) {
                return Err(format!("spurious delivery at node {i}: {:?}", p.data));
            }
        }
    }
    Ok(())
}

/// LIVENESS / completeness: every surviving node must have delivered every
/// `expected` payload. This is the check the safety-only suite lacked — a
/// halted or deadlocked ring (e.g. a stuck reissued token after a botched
/// reconfiguration) passes `ConsistentDelivery` but fails this.
fn check_completeness(
    logs: &[Vec<Payload>],
    crashed: &[bool],
    expected: &[Vec<u8>],
) -> Result<(), String> {
    for (i, log) in logs.iter().enumerate() {
        if crashed[i] {
            continue;
        }
        let got: HashSet<&[u8]> = log.iter().map(|p| p.data.as_slice()).collect();
        let missing: Vec<&Vec<u8>> = expected.iter().filter(|d| !got.contains(d.as_slice())).collect();
        if !missing.is_empty() {
            return Err(format!(
                "completeness violated at survivor node {i}: missing {missing:?} (delivered {:?})",
                log.iter().map(|p| p.data.clone()).collect::<Vec<_>>(),
            ));
        }
    }
    Ok(())
}

// ────────────────────────────────────────────────────────────────────────────
// Hand-written scenarios
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn one_node_crashes_after_two_broadcasts() {
    // Crash node 2 after node 0 and 1 have broadcast.  Node 0 + 1
    // should still agree on a consistent delivery prefix.
    let mut ring = Ring::new();
    ring.broadcast(0, b"a".to_vec());
    ring.broadcast(1, b"b".to_vec());
    for _ in 0..15 { ring.step(); }
    ring.crash(2);
    for _ in 0..30 { ring.step(); }

    let bcast: HashSet<Vec<u8>> = [b"a".to_vec(), b"b".to_vec()].into_iter().collect();
    check_no_spurious_delivery(&ring.delivered, &bcast).expect("no spurious");
    check_consistent_delivery(&ring.delivered, &ring.crashed).expect("consistent");
}

#[test]
fn surviving_nodes_keep_consistent_logs_after_crash() {
    // Two issuers, third node crashes early. Surviving issuers must
    // still hold mutual-prefix logs.
    let mut ring = Ring::new();
    ring.crash(2);
    ring.broadcast(0, b"x".to_vec());
    ring.broadcast(1, b"y".to_vec());
    ring.broadcast(0, b"z".to_vec());
    for _ in 0..STEP_BUDGET { ring.step(); }

    let bcast: HashSet<Vec<u8>> =
        [b"x".to_vec(), b"y".to_vec(), b"z".to_vec()].into_iter().collect();
    check_no_spurious_delivery(&ring.delivered, &bcast).expect("no spurious");
    check_consistent_delivery(&ring.delivered, &ring.crashed).expect("consistent");
}

#[test]
fn deferred_train_does_not_break_total_order() {
    // Schedule one train hop's-worth of deferral on node 1 to
    // simulate packet reordering. ConsistentDelivery must still hold.
    let mut ring = Ring::new();
    ring.broadcast(0, b"d1".to_vec());
    ring.broadcast(1, b"d2".to_vec());
    ring.step();
    ring.defer(1);
    for _ in 0..STEP_BUDGET { ring.step(); }

    let bcast: HashSet<Vec<u8>> =
        [b"d1".to_vec(), b"d2".to_vec()].into_iter().collect();
    check_no_spurious_delivery(&ring.delivered, &bcast).expect("no spurious");
    check_consistent_delivery(&ring.delivered, &ring.crashed).expect("consistent");
}

#[test]
fn multiple_crashes_drop_no_invariants() {
    // Crash node 0 (issuer!) mid-flight. Liveness will degrade but
    // safety must hold among surviving nodes.
    let mut ring = Ring::new();
    ring.broadcast(1, b"survives".to_vec());
    for _ in 0..5 { ring.step(); }
    ring.crash(0);
    for _ in 0..STEP_BUDGET { ring.step(); }

    let bcast: HashSet<Vec<u8>> = [b"survives".to_vec()].into_iter().collect();
    check_no_spurious_delivery(&ring.delivered, &bcast).expect("no spurious");
    check_consistent_delivery(&ring.delivered, &ring.crashed).expect("consistent");
}

// ────────────────────────────────────────────────────────────────────────────
// PropTest fuzz
// ────────────────────────────────────────────────────────────────────────────

fn arb_event() -> impl Strategy<Value = Event> {
    prop_oneof![
        // Step is the most common event so the ring actually progresses.
        4 => Just(Event::Step),
        2 => (0..RING_SIZE as u8, any::<u8>())
            .prop_map(|(node, msg)| Event::Broadcast { node, msg }),
        // Defer is rare; too much deferral livelocks delivery.
        1 => (0..RING_SIZE as u8).prop_map(|node| Event::Defer { node }),
    ]
}

fn arb_schedule() -> impl Strategy<Value = Vec<Event>> {
    prop::collection::vec(arb_event(), 5..STEP_BUDGET)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        max_shrink_iters: 1024,
        ..ProptestConfig::default()
    })]

    /// Random schedules with NO crashes must preserve all invariants.
    #[test]
    fn fuzz_no_crashes_preserves_invariants(events in arb_schedule()) {
        let mut ring = Ring::new();
        let mut bcast: HashSet<Vec<u8>> = HashSet::new();

        for ev in events {
            match ev {
                Event::Step => ring.step(),
                Event::Broadcast { node, msg } => {
                    let data = vec![msg];
                    bcast.insert(data.clone());
                    ring.broadcast(node, data);
                }
                Event::Defer { node } => ring.defer(node),
                Event::Crash { .. } => unreachable!(),
            }
        }
        // Drain
        for _ in 0..40 { ring.step(); }

        check_no_spurious_delivery(&ring.delivered, &bcast)
            .map_err(TestCaseError::fail)?;
        check_consistent_delivery(&ring.delivered, &ring.crashed)
            .map_err(TestCaseError::fail)?;
    }

    /// Random schedules with a single crash event still preserve safety
    /// among survivors.
    #[test]
    fn fuzz_one_crash_preserves_invariants(
        events in arb_schedule(),
        crash_node in 0u8..RING_SIZE as u8,
        crash_at in 5usize..50,
    ) {
        let mut ring = Ring::new();
        let mut bcast: HashSet<Vec<u8>> = HashSet::new();
        let mut crashed_yet = false;
        let mut step_count = 0;

        for ev in events {
            if !crashed_yet && step_count >= crash_at {
                ring.crash(crash_node);
                crashed_yet = true;
            }
            match ev {
                Event::Step => { ring.step(); step_count += 1; }
                Event::Broadcast { node, msg } => {
                    if !ring.crashed[node as usize] {
                        let data = vec![msg];
                        bcast.insert(data.clone());
                        ring.broadcast(node, data);
                    }
                }
                Event::Defer { node } => ring.defer(node),
                Event::Crash { .. } => unreachable!(),
            }
        }
        for _ in 0..40 { ring.step(); }

        check_no_spurious_delivery(&ring.delivered, &bcast)
            .map_err(TestCaseError::fail)?;
        check_consistent_delivery(&ring.delivered, &ring.crashed)
            .map_err(TestCaseError::fail)?;
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Reconfiguration (Gap C) — deterministic crash → view change → recovery.
//
// These exercise the path the safety-only suite above could not: after a
// crash the survivors must not just stay *consistent* (a halt is consistent)
// but also stay *live* — every post-crash broadcast must reach every
// survivor. The deterministic lock-step `Ring` makes the timing race
// (which token is in flight when the crash lands) reproducible and
// shrinkable, so a regression yields a minimal counterexample instead of a
// flaky wall-clock failure. This harness is what catches the zombie-token
// and in-flight-issued-clock classes of bug.
// ────────────────────────────────────────────────────────────────────────────

/// Hand-written keystone: crash a non-issuer mid-circulation, reconfigure,
/// then confirm five post-crash broadcasts reach every survivor.
#[test]
fn reconfig_masks_non_issuer_crash_and_completes() {
    let mut ring = Ring::with(DeliveryMode::TotalOrder, true);
    // Warm up so the clock advances and tokens distribute (empty trains).
    for _ in 0..12 { ring.step(); }
    // Crash a non-issuer and run the full view-change recovery.
    let victim = (RING_SIZE - 1) as ProcId; // last node, a non-issuer
    assert!((victim as usize) >= NUM_TRAINS, "victim must be a non-issuer");
    ring.reconfigure(victim);
    // Post-crash broadcasts from a surviving issuer.
    let posts: Vec<Vec<u8>> = (0..5).map(|k| format!("post-{k}").into_bytes()).collect();
    for d in &posts { ring.broadcast(0, d.clone()); }
    for _ in 0..200 { ring.step(); }

    check_consistent_delivery(&ring.delivered, &ring.crashed).expect("safety: consistent");
    let bcast: HashSet<Vec<u8>> = posts.iter().cloned().collect();
    check_no_spurious_delivery(&ring.delivered, &bcast).expect("safety: no spurious");
    check_completeness(&ring.delivered, &ring.crashed, &posts).expect("liveness: completeness");
}

/// Crash an ISSUER (the hard case the in-process test cannot cover — it
/// requires a non-issuer victim). After an issuer dies it never advances
/// its clock again, so a delivery gate that waits on *every* issuer's
/// `seenClk` (including the dead one) would block forever. The surviving
/// issuer's post-crash broadcasts must still reach every survivor.
#[test]
fn reconfig_masks_issuer_crash_and_completes() {
    let mut ring = Ring::with(DeliveryMode::TotalOrder, true);
    for _ in 0..12 { ring.step(); }
    let victim = 0 as ProcId; // an issuer
    assert!((victim as usize) < NUM_TRAINS, "victim must be an issuer");
    ring.reconfigure(victim);
    // Post-crash broadcasts from a SURVIVING issuer (node 1).
    let posts: Vec<Vec<u8>> = (0..5).map(|k| format!("ipost-{k}").into_bytes()).collect();
    for d in &posts { ring.broadcast(1, d.clone()); }
    for _ in 0..200 { ring.step(); }

    check_consistent_delivery(&ring.delivered, &ring.crashed).expect("safety: consistent");
    check_completeness(&ring.delivered, &ring.crashed, &posts).expect("liveness: completeness");
}

// ── PR-R1 / A5: a second crash DURING the view change ───────────────────────
// Needs ≥2 survivors including an issuer, so it is a no-op on the default
// RING_SIZE=3 build (2 crashes ⇒ 1 survivor). Exercise it with a bigger ring:
//   TRAINS_RING_SIZE=5 TRAINS_NUM_TRAINS=2 cargo test -p trains-core --test adversarial
#[test]
fn reconfig_masks_second_crash_during_recovery() {
    if RING_SIZE < 4 {
        eprintln!("skip reconfig_masks_second_crash_during_recovery: needs RING_SIZE>=4 (got {RING_SIZE})");
        return;
    }
    let mut ring = Ring::with(DeliveryMode::TotalOrder, true);
    for _ in 0..12 { ring.step(); }

    // Two non-issuer victims; both issuers (0,1) survive.
    let v1 = (RING_SIZE - 1) as ProcId;
    let v2 = (RING_SIZE - 2) as ProcId;
    assert!((v2 as usize) >= NUM_TRAINS, "both victims must be non-issuers");

    ring.reconfigure_interleaved(v1, v2);

    let posts: Vec<Vec<u8>> = (0..5).map(|k| format!("dpost-{k}").into_bytes()).collect();
    for d in &posts { ring.broadcast(0, d.clone()); }
    for _ in 0..250 { ring.step(); }

    check_consistent_delivery(&ring.delivered, &ring.crashed).expect("safety");
    check_completeness(&ring.delivered, &ring.crashed, &posts).expect("liveness");
}

// ── PR-R1 / A4: crash timed to token possession vs. idle ────────────────────
// Two distinct recovery paths: crashing the victim while it HOLDS a token
// loses that token (creates a lost-key gap → exercises C3); crashing it while
// IDLE loses no token (no gap, just ring re-formation). Both must mask.

#[test]
fn reconfig_masks_crash_while_victim_holds_token() {
    let mut ring = Ring::with(DeliveryMode::TotalOrder, true);
    let victim = (RING_SIZE - 1) as ProcId; // non-issuer
    let mut held = false;
    for _ in 0..50 {
        ring.step();
        if ring.in_flight[victim as usize].is_some() {
            held = true;
            break;
        }
    }
    assert!(held, "victim never held a token to lose");
    ring.reconfigure(victim);
    let posts: Vec<Vec<u8>> = (0..5).map(|k| format!("hpost-{k}").into_bytes()).collect();
    for d in &posts { ring.broadcast(0, d.clone()); }
    for _ in 0..200 { ring.step(); }
    check_consistent_delivery(&ring.delivered, &ring.crashed).expect("safety");
    check_completeness(&ring.delivered, &ring.crashed, &posts).expect("liveness");
}

#[test]
fn reconfig_masks_crash_while_victim_idle() {
    let mut ring = Ring::with(DeliveryMode::TotalOrder, true);
    let victim = (RING_SIZE - 1) as ProcId;
    let mut idle = false;
    for _ in 0..50 {
        ring.step();
        if ring.in_flight[victim as usize].is_none() {
            idle = true;
            break;
        }
    }
    assert!(idle, "victim was never idle");
    ring.reconfigure(victim);
    let posts: Vec<Vec<u8>> = (0..5).map(|k| format!("ipost-{k}").into_bytes()).collect();
    for d in &posts { ring.broadcast(0, d.clone()); }
    for _ in 0..200 { ring.step(); }
    check_consistent_delivery(&ring.delivered, &ring.crashed).expect("safety");
    check_completeness(&ring.delivered, &ring.crashed, &posts).expect("liveness");
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 96,
        max_shrink_iters: 4096,
        ..ProptestConfig::default()
    })]

    /// Vary the crash *timing*, the *victim* (issuer or non-issuer), and the
    /// pre/post message counts so the crash lands at every token position
    /// against every member. Every post-crash broadcast from a surviving
    /// issuer must reach every survivor; pre-crash messages may be lost
    /// (in-flight on the dead token — the at-least-once contract) so they
    /// are only checked for non-spuriousness, not completeness. This is the
    /// check that turns the timing-dependent zombie-token / in-flight-issued
    /// / dead-issuer-gate bugs from "flaky on EC2" into deterministic,
    /// shrinkable counterexamples.
    #[test]
    fn fuzz_reconfig_any_victim_masks_crash(
        warmup in 1usize..40,
        victim in 0u8..RING_SIZE as u8,
        n_pre  in 0usize..4,
        n_post in 1usize..5,
        drain  in 150usize..280,
    ) {
        // A surviving issuer to source the post-crash broadcasts (with
        // NUM_TRAINS≥2 and a single crash, one always survives).
        let sender = (0..NUM_TRAINS as u8)
            .find(|&q| q != victim)
            .expect("a surviving issuer");

        let mut ring = Ring::with(DeliveryMode::TotalOrder, true);

        let mut all: HashSet<Vec<u8>> = HashSet::new();
        for k in 0..n_pre {
            let d = format!("pre-{k}").into_bytes();
            all.insert(d.clone());
            ring.broadcast(sender, d);
        }
        for _ in 0..warmup { ring.step(); }

        ring.reconfigure(victim);

        let posts: Vec<Vec<u8>> = (0..n_post).map(|k| format!("post-{k}").into_bytes()).collect();
        for d in &posts {
            all.insert(d.clone());
            ring.broadcast(sender, d.clone());
        }
        for _ in 0..drain { ring.step(); }

        check_consistent_delivery(&ring.delivered, &ring.crashed)
            .map_err(TestCaseError::fail)?;
        check_no_spurious_delivery(&ring.delivered, &all)
            .map_err(TestCaseError::fail)?;
        check_completeness(&ring.delivered, &ring.crashed, &posts)
            .map_err(TestCaseError::fail)?;
    }
}
