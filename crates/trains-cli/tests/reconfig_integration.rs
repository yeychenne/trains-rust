//! In-process reconfiguration validation: a real N-node TLS ring; kill a
//! node; survivors re-form the ring and run a view-change recovery, then
//! keep delivering — i.e. the permanent crash is MASKED.
//!
//! Exercises all of Gap C end-to-end:
//!   * C1 — successor retarget (`RingTransport::retarget_successor`): the
//!     survivor whose successor died re-points past the gap.
//!   * C2 — lost-train regeneration (`Node::reissue_train`): a crashed node
//!     may hold in-flight tokens; surviving issuers reissue so circulation
//!     resumes.
//!   * C3 — lost-KEY gap resolution (Totem token-recovery): the lost
//!     tokens' clock slots were seen on lap-1 but never delivered, which
//!     would otherwise block `AllPriorDelivered` forever. The survivors run
//!     a view change — gather per-node [`RecoveryReport`]s, merge them into
//!     one uniform [`RecoveryPlan`] (`compute_recovery_plan`), and each
//!     applies it (`Node::apply_recovery`) — closing the gaps WITHOUT
//!     dropping any payload some peer delivered (uniform agreement holds).
//!
//! Uses TotalOrder delivery (uniform within the surviving view). Runs
//! entirely on localhost — no AWS. This is the fast validation for the
//! reconfiguration work; the distributed fis-kill chaos run (wire-level
//! view-change over trains-net) is the production confirmation.
//!
//! NOTE on the freeze discipline: a production view change must freeze
//! delivery between the report snapshot and plan apply. Here the snapshot→
//! apply window is controlled by the test and all pre-crash trains are
//! empty, so any train that circulates in that window only records empty
//! done-keys (which the plan skips and `apply_recovery` treats
//! idempotently) — no freeze is needed for this in-process validation. The
//! wire-level protocol over trains-net is the remaining integration step.

// Test-harness style: index-based loops keep the node-id ↔ channel
// correspondence explicit; the shared-log type is spelled out once.
#![allow(clippy::type_complexity)]
#![allow(clippy::needless_range_loop)]

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use trains_core::{
    compute_recovery_plan, DeliveryMode, Input, Output, ProcId, RecoveryPlan, RecoveryReport,
    TrainsNode, NUM_TRAINS, RING_SIZE,
};
use trains_net::{NodeIdentity, RingConfig, RingTransport, SpkiFingerprint};

enum Ctrl {
    /// Simulate a crash: abort the transport and stop the node loop.
    Kill,
    /// A confirmed crash of process `u8`: exclude from the delivery
    /// condition and retarget the successor past it (C1 + delivery half).
    ConfirmCrash(u8),
    /// View-change phase 1: reply with this survivor's recovery snapshot.
    Report(oneshot::Sender<RecoveryReport>),
    /// View-change phase 2: apply the agreed plan, then reissue this
    /// issuer's token so the new view starts circulating (C2 + C3).
    ApplyPlan(RecoveryPlan),
}

/// Next alive node after `me` on the ring (wraps; assumes ≥1 alive).
fn next_alive(me: usize, n: usize, dead: &BTreeSet<usize>) -> usize {
    let mut j = (me + 1) % n;
    while dead.contains(&j) && j != me {
        j = (j + 1) % n;
    }
    j
}

#[allow(clippy::too_many_arguments)]
async fn node_loop(
    id: usize,
    n: usize,
    addrs: Arc<Vec<SocketAddr>>,
    identity: NodeIdentity,
    pinned: Vec<SpkiFingerprint>,
    issue_initial: bool,
    mut bc_rx: mpsc::Receiver<Vec<u8>>,
    mut ctrl_rx: mpsc::Receiver<Ctrl>,
    log: Arc<Mutex<Vec<(ProcId, u64)>>>,
) {
    let mut dead: BTreeSet<usize> = BTreeSet::new();
    let mut succ = next_alive(id, n, &dead);

    let mut transport = match RingTransport::spawn(RingConfig {
        identity,
        listen_addr: addrs[id],
        successor_addr: addrs[succ],
        pinned_peer_fingerprints: pinned,
    })
    .await
    {
        Ok(t) => t,
        Err(e) => {
            eprintln!("[{id}] transport spawn failed: {e}");
            return;
        }
    };

    let mut core = TrainsNode::new(id as ProcId, DeliveryMode::TotalOrder);
    if issue_initial {
        let t = core.issue_initial_train();
        let _ = transport.outbox.send(t).await;
    }
    let mut tick = tokio::time::interval(Duration::from_millis(100));

    loop {
        tokio::select! {
            Some(d) = bc_rx.recv() => {
                let outs = core.step(Input::LocalBroadcast(d));
                dispatch_dbg(id, &transport, outs, &log).await;
            }
            Some(t) = transport.inbox.recv() => {
                let outs = core.step(Input::TrainReceived(t));
                dispatch_dbg(id, &transport, outs, &log).await;
            }
            _ = tick.tick() => {
                let outs = core.step(Input::Tick);
                dispatch_dbg(id, &transport, outs, &log).await;
            }
            Some(c) = ctrl_rx.recv() => match c {
                Ctrl::Kill => { eprintln!("[{id}] KILL"); transport.abort(); break; }
                Ctrl::ConfirmCrash(v) => {
                    let outs = core.confirm_crash(v);
                    dispatch_dbg(id, &transport, outs, &log).await;
                    dead.insert(v as usize);
                    let new_succ = next_alive(id, n, &dead);
                    if new_succ != succ {
                        eprintln!("[{id}] retarget succ {succ} -> {new_succ}");
                        succ = new_succ;
                        transport.retarget_successor(addrs[succ]).await;
                    } else {
                        eprintln!("[{id}] confirm_crash({v}); succ unchanged ({succ})");
                    }
                }
                Ctrl::Report(reply) => {
                    let report = core.recovery_report();
                    eprintln!("[{id}] report seen={:?}", report.seen);
                    let _ = reply.send(report);
                }
                Ctrl::ApplyPlan(plan) => {
                    let outs = core.apply_recovery(&plan);
                    eprintln!("[{id}] apply_recovery: {} actions, reissue@{}",
                        plan.actions.len(), plan.reissue_clock(id as ProcId));
                    dispatch_dbg(id, &transport, outs, &log).await;
                    // Install the new view: reissue this issuer's token
                    // above the agreed boundary so circulation resumes.
                    if id < NUM_TRAINS {
                        let t = core.reissue_train();
                        eprintln!("[{id}] reissue train clock={}", t.clock);
                        let _ = transport.outbox.send(t).await;
                    }
                }
            }
        }
    }
}

async fn dispatch_dbg(
    id: usize,
    transport: &RingTransport,
    outs: Vec<Output>,
    log: &Arc<Mutex<Vec<(ProcId, u64)>>>,
) {
    for o in outs {
        match o {
            Output::ForwardTrain(t) => {
                eprintln!(
                    "[{id}] fwd train issuer={} clock={} acks={:04b} payloads={}",
                    t.issuer, t.clock, t.ack_bits, t.payloads.len()
                );
                let _ = transport.outbox.send(t).await;
            }
            Output::Deliver(ps) => {
                let mut l = log.lock().unwrap();
                for p in &ps {
                    eprintln!("[{id}] DELIVER ({},{})", p.sender, p.seq);
                }
                for p in ps {
                    l.push((p.sender, p.seq));
                }
            }
            Output::DeclareCrash(_) => {}
        }
    }
}

/// Permanent crash is MASKED after reconfiguration: kill a non-issuer,
/// re-form the ring + run the view-change recovery, and confirm every
/// survivor delivers all post-crash broadcasts (in bounded time = MTTR).
#[tokio::test]
async fn permanent_crash_is_masked_after_reconfiguration() {
    let n = RING_SIZE;
    assert!(n >= 3, "needs ring ≥3");
    let victim = n - 1; // last node — a non-issuer (issuers are 0..NUM_TRAINS)
    assert!(
        victim >= NUM_TRAINS,
        "victim must be a non-issuer for this test"
    );

    let identities: Vec<NodeIdentity> = (0..n)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]).unwrap())
        .collect();
    let fps: Vec<SpkiFingerprint> = identities.iter().map(|i| i.fingerprint).collect();
    let addrs: Arc<Vec<SocketAddr>> = Arc::new(
        (0..n)
            .map(|_| {
                let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let a = l.local_addr().unwrap();
                drop(l);
                a
            })
            .collect(),
    );

    let logs: Vec<Arc<Mutex<Vec<(ProcId, u64)>>>> =
        (0..n).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
    let mut bc_txs = Vec::new();
    let mut ctrl_txs = Vec::new();
    let mut handles = Vec::new();

    for (id, identity) in identities.into_iter().enumerate() {
        let (bc_tx, bc_rx) = mpsc::channel::<Vec<u8>>(32);
        let (ctrl_tx, ctrl_rx) = mpsc::channel::<Ctrl>(8);
        bc_txs.push(bc_tx);
        ctrl_txs.push(ctrl_tx);
        let h = tokio::spawn(node_loop(
            id,
            n,
            addrs.clone(),
            identity,
            fps.clone(),
            id < NUM_TRAINS,
            bc_rx,
            ctrl_rx,
            logs[id].clone(),
        ));
        handles.push(h);
    }

    // Let the ring form.
    tokio::time::sleep(Duration::from_millis(600)).await;

    // ── Kill the victim. ──────────────────────────────────────────────
    ctrl_txs[victim].send(Ctrl::Kill).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── Confirm the crash on survivors: exclude from delivery + retarget. ─
    for id in 0..n {
        if id != victim {
            ctrl_txs[id]
                .send(Ctrl::ConfirmCrash(victim as u8))
                .await
                .unwrap();
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── View change (C3): gather reports → merge → apply uniformly. ────
    let mut reports = Vec::new();
    for id in 0..n {
        if id != victim {
            let (tx, rx) = oneshot::channel();
            ctrl_txs[id].send(Ctrl::Report(tx)).await.unwrap();
            reports.push(rx.await.unwrap());
        }
    }
    let plan = compute_recovery_plan(&reports);
    eprintln!(
        "[coordinator] recovery plan: {} actions, boundary={:?}",
        plan.actions.len(),
        plan.boundary
    );
    for id in 0..n {
        if id != victim {
            ctrl_txs[id].send(Ctrl::ApplyPlan(plan.clone())).await.unwrap();
        }
    }
    tokio::time::sleep(Duration::from_millis(400)).await;

    // ── Broadcast AFTER the crash, from node 0 — these can only be
    //    delivered via the re-formed ring (skipping the dead node) once the
    //    lost-key gaps have been resolved. ──────────────────────────────
    let n_msgs = 5u64;
    for k in 0..n_msgs {
        bc_txs[0]
            .send(format!("post-{k}").into_bytes())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Allow circulation + 2-lap delivery (bounded MTTR).
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Every surviving node must have delivered all post-crash broadcasts.
    let expected: BTreeSet<(ProcId, u64)> = (0..n_msgs).map(|k| (0u8, k)).collect();
    for id in 0..n {
        if id == victim {
            continue;
        }
        let got: BTreeSet<(ProcId, u64)> = logs[id].lock().unwrap().iter().copied().collect();
        let missing: Vec<_> = expected.difference(&got).collect();
        assert!(
            missing.is_empty(),
            "survivor node {id} missing post-crash deliveries {missing:?} (got {got:?})",
        );
    }

    for tx in &ctrl_txs {
        let _ = tx;
    }
    for h in handles {
        h.abort();
    }
}
