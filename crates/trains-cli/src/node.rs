//! Single-node runtime: bridges trains-core (sync) with trains-net (async).
//!
//! Event loop:
//!   * stdin line       → core.step(LocalBroadcast)
//!   * inbox train      → core.step(TrainReceived)
//!   * vc_inbox frame   → distributed view change (ViewChange) — reconfig mode
//!   * tick             → core.step(Tick)
//!
//! ## Reconfiguration (PR-R3 / PR-R4)
//! When the full ring topology is supplied (`--peer-addr` for every node),
//! the binary runs the distributed view change: the Gap-B failure detector
//! confirms a crash, the node excludes the victim (freeze + confirm_crash +
//! retarget past it), and the coordinator/participant `ViewChange` token
//! protocol circulates Gather/Install frames over `vc_outbox`/`vc_inbox` to
//! recover and reissue — masking the crash. Without `--peer-addr` the binary
//! keeps the legacy behaviour (confirm_crash only; it can't retarget without
//! knowing the ring), so the strict-benchmark path is unchanged.

use std::collections::BTreeSet;
use std::net::SocketAddr;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, BufReader};
use trains_core::{DeliveryMode, Input, Output, ProcId, TrainsNode, NUM_TRAINS, RING_SIZE};
use trains_net::{NodeIdentity, RingConfig, RingTransport, SpkiFingerprint, ViewChangeMsg};

use trains_recovery::failure_detector::FailureDetector;
use trains_recovery::view_change::{VcAction, ViewChange};

/// Clock-gap hints required before a crash is confirmed (◇S detector).
const STRIKE_THRESHOLD: u32 = 3;
/// A successor disconnect is strong evidence — confirm immediately.
const DISCONNECT_WEIGHT: u32 = STRIKE_THRESHOLD;

pub struct NodeArgs {
    pub id: u8,
    pub listen: SocketAddr,
    pub successor: SocketAddr,
    pub identity: NodeIdentity,
    pub pinned: Vec<SpkiFingerprint>,
    pub issue_initial: bool,
    pub mode: DeliveryMode,
    /// Full ring topology: addresses indexed by node id. When present (and of
    /// length `RING_SIZE`), reconfiguration is enabled — the node can retarget
    /// past a crashed successor. Empty → reconfiguration disabled (legacy).
    pub ring_addrs: Vec<SocketAddr>,
}

pub async fn run(args: NodeArgs) -> Result<()> {
    let reconfig = !args.ring_addrs.is_empty();
    if reconfig && args.ring_addrs.len() != RING_SIZE {
        anyhow::bail!(
            "--peer-addr count {} != RING_SIZE {} (trains-core build)",
            args.ring_addrs.len(),
            RING_SIZE,
        );
    }
    let n = if reconfig { args.ring_addrs.len() } else { RING_SIZE };
    let id = args.id as usize;
    let addrs = args.ring_addrs.clone();
    let mut dead: BTreeSet<usize> = BTreeSet::new();
    let mut succ = (id + 1) % n;

    let mut transport = RingTransport::spawn(RingConfig {
        identity:                 args.identity,
        listen_addr:              args.listen,
        successor_addr:           args.successor,
        pinned_peer_fingerprints: args.pinned,
    })
    .await
    .context("spawning ring transport")?;

    let mut core = TrainsNode::new(args.id, args.mode);
    let mut detector = FailureDetector::new(STRIKE_THRESHOLD, DISCONNECT_WEIGHT);
    let mut vc = ViewChange::new(args.id, n);

    if args.issue_initial {
        let t = core.issue_initial_train();
        eprintln!("[{id}] issuing initial train clock={} issuer={}", t.clock, t.issuer);
        transport.outbox.send(t).await.ok();
    }

    if reconfig {
        eprintln!("[{id}] reconfiguration ENABLED (ring of {n})");
    } else {
        eprintln!("[{id}] reconfiguration disabled (no --peer-addr); confirm_crash only");
    }
    eprintln!("[{id}] node ready — type messages, ENTER to broadcast.");

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));

    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line.context("reading stdin")? {
                    Some(s) => {
                        let outs = core.step(Input::LocalBroadcast(s.into_bytes()));
                        process_outs(id, n, reconfig, outs, &mut core, &mut detector,
                                     &mut vc, &mut dead, &mut succ, &transport, &addrs).await;
                    }
                    None => { eprintln!("[{id}] stdin closed; exiting"); break; }
                }
            }
            train = transport.inbox.recv() => {
                match train {
                    Some(t) => {
                        detector.note_alive(t.issuer); // proof issuer is alive
                        let outs = core.step(Input::TrainReceived(t));
                        process_outs(id, n, reconfig, outs, &mut core, &mut detector,
                                     &mut vc, &mut dead, &mut succ, &transport, &addrs).await;
                    }
                    None => { eprintln!("[{id}] transport closed; exiting"); break; }
                }
            }
            Some(msg) = transport.vc_inbox.recv(), if reconfig => {
                handle_vc(msg, id, n, &mut core, &mut vc, &mut dead, &mut succ,
                          &transport, &addrs).await;
            }
            Some(addr) = transport.unreachable_rx.recv(), if reconfig => {
                // The successor has been unreachable for a while — strong
                // evidence of a clean crash (no clock gap to detect it).
                if let Some(victim) = addrs.iter().position(|a| *a == addr) {
                    if !dead.contains(&victim) {
                        if let Some(confirmed) = detector.record_disconnect(victim as ProcId) {
                            eprintln!("[{id}] SUCCESSOR {confirmed} UNREACHABLE → view change");
                            exclude(victim, id, n, &mut core, &transport,
                                    &mut dead, &mut succ, &addrs).await;
                            let report = core.recovery_report();
                            let actions = vc.on_confirm(confirmed, report);
                            execute(actions, id, &mut core, &transport).await;
                        }
                    }
                }
            }
            _ = tick.tick() => {
                let outs = core.step(Input::Tick);
                process_outs(id, n, reconfig, outs, &mut core, &mut detector,
                             &mut vc, &mut dead, &mut succ, &transport, &addrs).await;
            }
        }
    }

    Ok(())
}

/// Next alive node after `me` on the ring (wraps; assumes ≥1 alive).
fn next_alive(me: usize, n: usize, dead: &BTreeSet<usize>) -> usize {
    let mut j = (me + 1) % n;
    while dead.contains(&j) && j != me {
        j = (j + 1) % n;
    }
    j
}

/// Process core outputs: forward trains, deliver, and route `DeclareCrash`
/// hints through the failure detector. On a confirmed crash, begin the
/// distributed view change (reconfig mode) or fall back to confirm_crash.
#[allow(clippy::too_many_arguments)]
async fn process_outs(
    id: usize,
    n: usize,
    reconfig: bool,
    outs: Vec<Output>,
    core: &mut TrainsNode,
    detector: &mut FailureDetector,
    vc: &mut ViewChange,
    dead: &mut BTreeSet<usize>,
    succ: &mut usize,
    transport: &RingTransport,
    addrs: &[SocketAddr],
) {
    for o in outs {
        match o {
            Output::ForwardTrain(t) => {
                let _ = transport.outbox.send(t).await;
            }
            Output::Deliver(payloads) => show_deliveries(id, &payloads),
            Output::DeclareCrash(victim) => {
                if detector.is_confirmed(victim) {
                    continue;
                }
                let Some(confirmed) = detector.record_gap_hint(victim) else {
                    eprintln!("[{id}] crash hint for {victim} (suspicion {}/{})",
                        detector.suspicion(victim), STRIKE_THRESHOLD);
                    continue;
                };
                if reconfig {
                    eprintln!("[{id}] CRASH CONFIRMED {confirmed} → view change");
                    exclude(confirmed as usize, id, n, core, transport, dead, succ, addrs).await;
                    let report = core.recovery_report();
                    let actions = vc.on_confirm(confirmed, report);
                    execute(actions, id, core, transport).await;
                } else {
                    eprintln!("[{id}] CRASH CONFIRMED {confirmed} → confirm_crash \
                               (no --peer-addr; cannot retarget)");
                    let recovery = core.confirm_crash(confirmed);
                    show_recovery(id, recovery);
                }
            }
        }
    }
}

/// Handle an incoming view-change token: learn the crash, run the state
/// machine on a fresh snapshot, execute its actions.
#[allow(clippy::too_many_arguments)]
async fn handle_vc(
    msg: ViewChangeMsg,
    id: usize,
    n: usize,
    core: &mut TrainsNode,
    vc: &mut ViewChange,
    dead: &mut BTreeSet<usize>,
    succ: &mut usize,
    transport: &RingTransport,
    addrs: &[SocketAddr],
) {
    // The trains-cli node binary handles only the exclude (crash-masking) flow.
    // Re-admit tokens (PR-RJ-2) are a proxy-level concern (state transfer); the
    // bare protocol node ignores them.
    let Some(victim) = msg.victim() else { return };
    exclude(victim as usize, id, n, core, transport, dead, succ, addrs).await;
    let report = core.recovery_report();
    let actions = match &msg {
        ViewChangeMsg::Gather { .. } => vc.on_gather(msg, report),
        ViewChangeMsg::Install { .. } => vc.on_install(msg),
        ViewChangeMsg::ReAdmitGather { .. } | ViewChangeMsg::ReAdmitInstall { .. } => Vec::new(),
    };
    execute(actions, id, core, transport).await;
}

/// Exclude `victim`: freeze delivery, confirm the crash, retarget past it.
#[allow(clippy::too_many_arguments)]
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
) {
    for a in actions {
        match a {
            VcAction::Send(msg) => {
                let _ = transport.vc_outbox.send(msg).await;
            }
            VcAction::Apply(plan) => {
                eprintln!("[{id}] apply_recovery ({} actions)", plan.actions.len());
                let outs = core.apply_recovery(&plan);
                show_recovery(id, outs);
                if id < NUM_TRAINS {
                    let t = core.reissue_train();
                    eprintln!("[{id}] reissue train clock={}", t.clock);
                    let _ = transport.outbox.send(t).await;
                }
            }
        }
    }
}

fn show_deliveries(id: usize, payloads: &[trains_core::Payload]) {
    for p in payloads {
        let preview = String::from_utf8_lossy(&p.data);
        println!("[{id}] DELIVER from={} seq={} {:?}", p.sender, p.seq, preview);
    }
}

/// Print `Deliver` outputs from a recovery drain (confirm_crash/apply_recovery
/// emit only `Deliver`).
fn show_recovery(id: usize, outs: Vec<Output>) {
    for o in outs {
        if let Output::Deliver(payloads) = o {
            show_deliveries(id, &payloads);
        }
    }
}
