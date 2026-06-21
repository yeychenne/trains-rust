//! In-process ring demo: spawns N TrainsNodes in parallel, wires their
//! `trains-net` transports into a unidirectional ring, applies a list
//! of broadcasts, runs for a fixed duration, prints a global
//! ConsistentDelivery report, and optionally writes a JSONL trace
//! suitable for `trains-trace-validate`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use trains_core::{
    trace::{NodeState, TraceAction, TraceOutput, TraceRecord, TrainSummary},
    DeliveryMode, Input, Output, Payload, ProcId, TrainsNode,
};
use trains_net::{NodeIdentity, RingConfig, RingTransport};

pub struct RingArgs {
    pub num:        usize,
    pub num_trains: usize,
    pub duration:   Duration,
    pub broadcasts: Vec<(u8, Vec<u8>)>,
    pub trace_path: Option<PathBuf>,
    pub mode:       DeliveryMode,
}

pub async fn run(args: RingArgs) -> Result<()> {
    if args.num != trains_core::RING_SIZE {
        anyhow::bail!(
            "trains-core compiled with RING_SIZE={}, got --num={}",
            trains_core::RING_SIZE, args.num,
        );
    }
    if args.num_trains > args.num {
        anyhow::bail!("--num-trains must be ≤ --num");
    }

    let identities: Vec<NodeIdentity> = (0..args.num)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]))
        .collect::<Result<_, _>>()?;
    let fingerprints: Vec<_> = identities.iter().map(|i| i.fingerprint).collect();

    let mut listen_addrs = Vec::with_capacity(args.num);
    let mut listeners = Vec::with_capacity(args.num);
    for _ in 0..args.num {
        let l = std::net::TcpListener::bind("127.0.0.1:0")
            .context("binding ephemeral port")?;
        listen_addrs.push(l.local_addr()?);
        listeners.push(l);
    }
    drop(listeners);

    let successor_addrs: Vec<SocketAddr> = (0..args.num)
        .map(|i| listen_addrs[(i + 1) % args.num])
        .collect();

    eprintln!("ring topology:");
    for i in 0..args.num {
        eprintln!("  node {} listens on {}, forwards to {}",
            i, listen_addrs[i], successor_addrs[i]);
    }

    let delivered_logs: Vec<Arc<Mutex<Vec<Payload>>>> =
        (0..args.num).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();

    // ── Tracing infrastructure ───────────────────────────────────────
    let trace_seq = Arc::new(AtomicU64::new(0));
    let (trace_tx, mut trace_rx) = if args.trace_path.is_some() {
        let (tx, rx) = mpsc::channel::<TraceRecord>(1024);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let trace_writer_handle: Option<JoinHandle<Result<usize>>> =
        if let (Some(path), Some(rx)) = (args.trace_path.clone(), trace_rx.take()) {
            Some(tokio::spawn(trace_writer_task(path, rx)))
        } else {
            None
        };

    // ── Spawn each node ───────────────────────────────────────────────
    let mut node_handles: Vec<JoinHandle<()>> = Vec::with_capacity(args.num);
    let mut broadcast_senders: Vec<mpsc::Sender<Vec<u8>>> = Vec::with_capacity(args.num);

    for (i, identity) in identities.into_iter().enumerate() {
        let (bc_tx, bc_rx) = mpsc::channel::<Vec<u8>>(16);
        broadcast_senders.push(bc_tx);

        let issue_initial = i < args.num_trains;
        let log           = delivered_logs[i].clone();
        let pinned        = fingerprints.clone();
        let listen        = listen_addrs[i];
        let successor     = successor_addrs[i];
        let trace_tx_n    = trace_tx.clone();
        let trace_seq_n   = trace_seq.clone();

        let mode = args.mode;
        let handle = tokio::spawn(async move {
            let res = run_one_node(
                i as u8, listen, successor, identity, pinned,
                issue_initial, bc_rx, log, trace_tx_n, trace_seq_n, mode,
            ).await;
            if let Err(e) = res {
                eprintln!("[{}] node task error: {e:#}", i);
            }
        });
        node_handles.push(handle);
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    for (node_id, msg) in &args.broadcasts {
        let idx = *node_id as usize;
        if idx >= args.num {
            eprintln!("warn: broadcast for node {} but ring has only {}", node_id, args.num);
            continue;
        }
        broadcast_senders[idx].send(msg.clone()).await.ok();
        eprintln!("→ broadcast at node {}: {:?}",
            node_id, String::from_utf8_lossy(msg));
    }

    tokio::time::sleep(args.duration).await;

    drop(broadcast_senders);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── Shut down deterministically ──────────────────────────────────
    // Order matters: node tasks hold cloned `trace_tx` senders, so the
    // writer task cannot exit until those clones drop. Abort node
    // tasks first, then drop our own sender, then await the writer.
    for h in &node_handles { h.abort(); }
    for h in node_handles { let _ = h.await; }
    drop(trace_tx);
    let trace_count = if let Some(h) = trace_writer_handle {
        h.await.unwrap_or(Ok(0)).unwrap_or(0)
    } else { 0 };

    println!("\n=== delivery logs ===");
    let snapshots: Vec<Vec<Payload>> = delivered_logs.iter()
        .map(|l| l.lock().unwrap().clone())
        .collect();
    for (i, log) in snapshots.iter().enumerate() {
        let strs: Vec<String> = log.iter()
            .map(|p| format!("{:?}@{}", String::from_utf8_lossy(&p.data), p.sender))
            .collect();
        println!("  node {}: [{}]", i, strs.join(", "));
    }

    let mut consistent = true;
    for i in 0..snapshots.len() {
        for j in 0..snapshots.len() {
            let a = &snapshots[i]; let b = &snapshots[j];
            let pre = a.len() <= b.len() && a.iter().zip(b.iter()).all(|(x, y)| x == y);
            if !pre {
                let other = b.len() <= a.len() && b.iter().zip(a.iter()).all(|(x, y)| x == y);
                if !other {
                    consistent = false;
                    eprintln!("ConsistentDelivery violated: node {} vs node {}", i, j);
                }
            }
        }
    }

    println!("\nConsistentDelivery: {}", if consistent { "HOLDS" } else { "VIOLATED" });

    if let Some(p) = &args.trace_path {
        println!("Trace: {} records → {}", trace_count, p.display());
    }

    if !consistent {
        anyhow::bail!("ConsistentDelivery invariant violated");
    }
    Ok(())
}

async fn trace_writer_task(
    path:     PathBuf,
    mut rx:   mpsc::Receiver<TraceRecord>,
) -> Result<usize> {
    use std::io::BufWriter;
    let f = std::fs::File::create(&path)
        .with_context(|| format!("creating trace file {}", path.display()))?;
    let mut w = BufWriter::new(f);
    let mut count = 0;
    while let Some(rec) = rx.recv().await {
        trains_core::trace::write_jsonl(&rec, &mut w)
            .context("writing trace record")?;
        count += 1;
    }
    use std::io::Write;
    w.flush().ok();
    Ok(count)
}

#[allow(clippy::too_many_arguments)] // in-process demo node task; params are clearer inline than boxed
async fn run_one_node(
    id:            ProcId,
    listen:        SocketAddr,
    successor:     SocketAddr,
    identity:      NodeIdentity,
    pinned:        Vec<trains_net::SpkiFingerprint>,
    issue_initial: bool,
    mut bc_rx:     mpsc::Receiver<Vec<u8>>,
    log:           Arc<Mutex<Vec<Payload>>>,
    trace_tx:      Option<mpsc::Sender<TraceRecord>>,
    trace_seq:     Arc<AtomicU64>,
    mode:          DeliveryMode,
) -> Result<()> {
    let mut transport = RingTransport::spawn(RingConfig {
        identity,
        listen_addr: listen,
        successor_addr: successor,
        pinned_peer_fingerprints: pinned,
    }).await?;

    let mut core = TrainsNode::new(id, mode);
    if issue_initial {
        let t = core.issue_initial_train();
        emit_trace(
            &trace_tx, &trace_seq, id, &core, &log,
            TraceAction::Initial { train: TrainSummary::from(&t) }, vec![],
            true, // initial issuance is always interesting
        ).await;
        let _ = transport.outbox.send(t).await;
    }

    let mut tick = tokio::time::interval(Duration::from_millis(200));

    loop {
        tokio::select! {
            data = bc_rx.recv() => {
                if let Some(d) = data {
                    let action = TraceAction::Broadcast { data_len: d.len() };
                    let outs = core.step(Input::LocalBroadcast(d));
                    let trace_outs: Vec<TraceOutput> = outs.iter().map(Into::into).collect();
                    dispatch(id, &mut transport, outs, &log).await;
                    // Snapshot AFTER dispatch so delivered_len is post-step.
                    emit_trace(&trace_tx, &trace_seq, id, &core, &log, action, trace_outs, true).await;
                }
            }
            train = transport.inbox.recv() => {
                match train {
                    Some(t) => {
                        let inbound_carries_payloads = !t.payloads.is_empty();
                        let action = TraceAction::TrainArrived { train: TrainSummary::from(&t) };
                        let outs = core.step(Input::TrainReceived(t));
                        let interesting = inbound_carries_payloads
                            || outs_contain_delivery_or_crash(&outs);
                        let trace_outs: Vec<TraceOutput> = outs.iter().map(Into::into).collect();
                        dispatch(id, &mut transport, outs, &log).await;
                        emit_trace(&trace_tx, &trace_seq, id, &core, &log, action, trace_outs, interesting).await;
                    }
                    None => break,
                }
            }
            _ = tick.tick() => {
                let outs = core.step(Input::Tick);
                let interesting = outs_contain_delivery_or_crash(&outs);
                let trace_outs: Vec<TraceOutput> = outs.iter().map(Into::into).collect();
                dispatch(id, &mut transport, outs, &log).await;
                if interesting {
                    emit_trace(&trace_tx, &trace_seq, id, &core, &log, TraceAction::Tick, trace_outs, true).await;
                }
            }
        }
    }
    Ok(())
}

fn outs_contain_delivery_or_crash(outs: &[Output]) -> bool {
    outs.iter().any(|o| matches!(o,
        Output::Deliver(_) | Output::DeclareCrash(_)))
}

#[allow(clippy::too_many_arguments)]
async fn emit_trace(
    trace_tx:    &Option<mpsc::Sender<TraceRecord>>,
    trace_seq:   &Arc<AtomicU64>,
    id:          ProcId,
    core:        &TrainsNode,
    log:         &Arc<Mutex<Vec<Payload>>>,
    action:      TraceAction,
    outputs:     Vec<TraceOutput>,
    interesting: bool,
) {
    if !interesting { return; }
    let Some(tx) = trace_tx else { return; };
    let seq = trace_seq.fetch_add(1, Ordering::SeqCst);
    let state = NodeState {
        id,
        seen_clk:        core.seen_clocks().to_vec(),
        iss_clk:         core.next_issue_clock(),
        pending_count:   core.pending_len(),
        done_keys_count: core.done_keys_iter().count(),
        delivered_len:   log.lock().unwrap().len(),
    };
    let rec = TraceRecord { seq, node: id, action, outputs, state };
    // Best-effort: drop if channel closed.
    let _ = tx.send(rec).await;
}

async fn dispatch(
    id: ProcId,
    transport: &mut RingTransport,
    outs: Vec<Output>,
    log: &Arc<Mutex<Vec<Payload>>>,
) {
    for o in outs {
        match o {
            Output::ForwardTrain(t) => {
                let _ = transport.outbox.send(t).await;
            }
            Output::Deliver(payloads) => {
                for p in &payloads {
                    eprintln!("[{}] DELIVER from={} seq={} {:?}",
                        id, p.sender, p.seq, String::from_utf8_lossy(&p.data));
                }
                log.lock().unwrap().extend(payloads);
            }
            Output::DeclareCrash(victim) => {
                eprintln!("[{}] DECLARE_CRASH {}", id, victim);
            }
        }
    }
}
