//! End-to-end WIRE-driven reconfiguration (PR-R3 step 6).
//!
//! Like `reconfig_integration`, but the view change runs entirely over the
//! real transport: Gather/Install tokens circulate the re-formed TLS ring via
//! `vc_outbox`/`vc_inbox`, driven by the `ViewChange` state machine — no test
//! oneshot channels. This exercises the whole PR-R3 stack together:
//!   * WireMsg framing + transport demux/mux (step 2),
//!   * the delivery freeze (step 3),
//!   * the coordinator/participant token protocol + stale-view fencing
//!     (steps 4-5),
//! plus C1 (retarget) and C3 (recovery) underneath.
//!
//! Only the coordinator (lowest-id survivor) is told of the crash; every other
//! survivor *learns* it from the circulating token (`msg.victim()`), excludes
//! the victim, and joins the round — closer to the production failure-detector
//! flow than the oneshot variant.

// Test-harness style: many-arg helpers mirror node.rs, index-based loops keep
// the node-id ↔ channel correspondence explicit, and the shared-log type is
// spelled out once. Suppress the style lints rather than obscure the harness.
#![allow(clippy::doc_lazy_continuation)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]
#![allow(clippy::needless_range_loop)]

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use trains_recovery::view_change::{VcAction, ViewChange};
use trains_core::{DeliveryMode, Input, Output, ProcId, TrainsNode, NUM_TRAINS, RING_SIZE};
use trains_net::{
    NodeIdentity, RingConfig, RingTransport, SpkiFingerprint, ViewChangeMsg,
};

enum Ctrl {
    Kill,
    ConfirmCrash(u8),
}

/// Next alive node after `me` on the ring (wraps; assumes ≥1 alive).
fn next_alive(me: usize, n: usize, dead: &BTreeSet<usize>) -> usize {
    let mut j = (me + 1) % n;
    while dead.contains(&j) && j != me {
        j = (j + 1) % n;
    }
    j
}

/// Exclude `victim` from the live view: freeze delivery, confirm the crash in
/// the core, and retarget the successor past it. Idempotent.
async fn exclude(
    victim: usize,
    id: usize,
    n: usize,
    core: &mut TrainsNode,
    transport: &RingTransport,
    dead: &mut BTreeSet<usize>,
    succ: &mut usize,
    addrs: &[SocketAddr],
) {
    if dead.contains(&victim) {
        return;
    }
    dead.insert(victim);
    core.set_frozen(true);
    let _ = core.confirm_crash(victim as ProcId);
    let ns = next_alive(id, n, dead);
    if ns != *succ {
        eprintln!("[{id}] retarget succ {succ} -> {ns}");
        *succ = ns;
        transport.retarget_successor(addrs[ns]).await;
    }
}

/// Execute the view-change state machine's actions.
async fn execute(
    actions: Vec<VcAction>,
    id: usize,
    core: &mut TrainsNode,
    transport: &RingTransport,
    log: &Arc<Mutex<Vec<(ProcId, u64)>>>,
) {
    for a in actions {
        match a {
            VcAction::Send(msg) => {
                eprintln!("[{id}] vc send {}", vc_desc(&msg));
                let _ = transport.vc_outbox.send(msg).await;
            }
            VcAction::Apply(plan) => {
                eprintln!("[{id}] apply_recovery: {} actions, reissue@{}",
                    plan.actions.len(), plan.reissue_clock(id as ProcId));
                let outs = core.apply_recovery(&plan);
                dispatch(id, transport, outs, log).await;
                if id < NUM_TRAINS {
                    let t = core.reissue_train();
                    eprintln!("[{id}] reissue train clock={}", t.clock);
                    let _ = transport.outbox.send(t).await;
                }
            }
        }
    }
}

fn vc_desc(m: &ViewChangeMsg) -> String {
    match m {
        ViewChangeMsg::Gather { view_id, reports, .. } =>
            format!("Gather v{view_id} reports={}", reports.len()),
        ViewChangeMsg::Install { view_id, .. } => format!("Install v{view_id}"),
        other => format!("readmit v{}", other.view_id()), // PR-RJ-2 tokens (not exercised here)
    }
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
    let mut vc = ViewChange::new(id as ProcId, n);
    if issue_initial {
        let t = core.issue_initial_train();
        let _ = transport.outbox.send(t).await;
    }
    let mut tick = tokio::time::interval(Duration::from_millis(100));

    loop {
        tokio::select! {
            Some(d) = bc_rx.recv() => {
                let outs = core.step(Input::LocalBroadcast(d));
                dispatch(id, &transport, outs, &log).await;
            }
            Some(t) = transport.inbox.recv() => {
                let outs = core.step(Input::TrainReceived(t));
                dispatch(id, &transport, outs, &log).await;
            }
            Some(msg) = transport.vc_inbox.recv() => {
                // Learn the crash from the token, then run the protocol with a
                // fresh (post-freeze) snapshot.
                let Some(victim) = msg.victim() else { continue };
                exclude(victim as usize, id, n, &mut core, &transport,
                        &mut dead, &mut succ, &addrs).await;
                let report = core.recovery_report();
                let actions = match &msg {
                    ViewChangeMsg::Gather { .. } => vc.on_gather(msg, report),
                    ViewChangeMsg::Install { .. } => vc.on_install(msg),
                    _ => Vec::new(), // PR-RJ-2 re-admit tokens not exercised here
                };
                execute(actions, id, &mut core, &transport, &log).await;
            }
            _ = tick.tick() => {
                let outs = core.step(Input::Tick);
                dispatch(id, &transport, outs, &log).await;
            }
            Some(c) = ctrl_rx.recv() => match c {
                Ctrl::Kill => { eprintln!("[{id}] KILL"); transport.abort(); break; }
                Ctrl::ConfirmCrash(v) => {
                    exclude(v as usize, id, n, &mut core, &transport,
                            &mut dead, &mut succ, &addrs).await;
                    let report = core.recovery_report();
                    let actions = vc.on_confirm(v, report);
                    execute(actions, id, &mut core, &transport, &log).await;
                }
            }
        }
    }
}

async fn dispatch(
    id: usize,
    transport: &RingTransport,
    outs: Vec<Output>,
    log: &Arc<Mutex<Vec<(ProcId, u64)>>>,
) {
    for o in outs {
        match o {
            Output::ForwardTrain(t) => {
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

/// Permanent crash is MASKED via the wire-driven view change. Kill a
/// non-issuer; only the coordinator is notified; the round propagates over the
/// ring; every survivor delivers all post-crash broadcasts.
#[tokio::test]
async fn permanent_crash_masked_via_wire_view_change() {
    let n = RING_SIZE;
    assert!(n >= 3, "needs ring ≥3");
    let victim = n - 1; // last node — a non-issuer
    assert!(victim >= NUM_TRAINS, "victim must be a non-issuer for this test");

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
            id, n, addrs.clone(), identity, fps.clone(),
            id < NUM_TRAINS, bc_rx, ctrl_rx, logs[id].clone(),
        ));
        handles.push(h);
    }

    // Let the ring form.
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Kill the victim.
    ctrl_txs[victim].send(Ctrl::Kill).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Notify ONLY the coordinator (lowest-id survivor = node 0). It initiates
    // the Gather token; the others learn the crash from the circulating token.
    ctrl_txs[0].send(Ctrl::ConfirmCrash(victim as u8)).await.unwrap();

    // Allow the view change to circulate the ring (gather → plan → install),
    // including the mid-round retargets.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Post-crash broadcasts from node 0 — only deliverable via the re-formed
    // ring after the lost-key gaps are resolved.
    let n_msgs = 5u64;
    for k in 0..n_msgs {
        bc_txs[0].send(format!("post-{k}").into_bytes()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Allow circulation + 2-lap delivery (bounded MTTR).
    tokio::time::sleep(Duration::from_secs(4)).await;

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
