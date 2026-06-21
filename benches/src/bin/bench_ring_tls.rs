//! End-to-end TLS-ring throughput + latency benchmark.
//!
//! Spawns RING_SIZE TLS-ring nodes on loopback (`127.0.0.1`), drives
//! a stream of broadcasts from node 0, and measures:
//!   - sustained msgs/sec at the last node
//!   - per-message broadcast→delivery latency at the **last** node
//!     (largest path through the ring), reported as p50 / p90 / p99
//!
//! ## Why measure latency on node 0 + last node only
//! In a ring with two laps, the slowest delivery is at whichever node
//! is reached last on the second lap. For RING_SIZE=3 with `NUM_TRAINS=2`
//! and broadcast injected at node 0, that's node 2 most of the time.
//! Reporting just one node's latency is conservative + cheap.
//!
//! ## Payload framing
//! First 8 bytes = u64 little-endian = nanoseconds since `start`.
//! Caller is responsible for `--payload >= 8`. The remaining bytes
//! are 0x42 padding.
//!
//! ## Output schema (stdout JSON line)
//!   { "kind": "ring_tls", "ring_size": N, "num_trains": K,
//!     "payload": P, "messages": M, "wall_us": T,
//!     "msgs_per_sec": X, "bytes_per_sec": Y,
//!     "p50_us": ..., "p90_us": ..., "p99_us": ..., "max_us": ... }

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use trains_core::{
    DeliveryMode, Input, Output, Payload, ProcId, TrainsNode, NUM_TRAINS, RING_SIZE,
};
use trains_net::{NodeIdentity, RingConfig, RingTransport};

#[derive(Parser)]
struct Args {
    /// Number of broadcasts to inject.
    #[arg(long, default_value_t = 2_000)]
    target_messages: usize,
    /// Payload size in bytes (≥ 8; first 8 bytes hold a timestamp).
    #[arg(long, default_value_t = 1024)]
    payload: usize,
    /// Wall-clock warmup ms before timing starts (lets TLS handshakes settle).
    #[arg(long, default_value_t = 800)]
    warmup_ms: u64,
    /// Broadcast injection inter-arrival gap. 0 = inject as fast as the
    /// backpressure channel allows (default).
    #[arg(long, default_value_t = 0)]
    inject_gap_us: u64,
    /// Whether to print the JSON line.
    #[arg(long, default_value_t = true)]
    json: bool,
}

#[derive(Serialize)]
struct Report {
    kind:          &'static str,
    ring_size:     usize,
    num_trains:    usize,
    payload:       usize,
    messages:      usize,
    wall_us:       u128,
    msgs_per_sec:  f64,
    bytes_per_sec: f64,
    p50_us:        u128,
    p90_us:        u128,
    p99_us:        u128,
    max_us:        u128,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    assert!(args.payload >= 8, "--payload must be ≥ 8 (timestamp prefix)");

    // ── Identities + ports ───────────────────────────────────────────
    let identities: Vec<NodeIdentity> = (0..RING_SIZE)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]))
        .collect::<Result<_, _>>()?;
    let fingerprints: Vec<_> = identities.iter().map(|i| i.fingerprint).collect();

    let mut listen_addrs = Vec::with_capacity(RING_SIZE);
    let mut listeners = Vec::with_capacity(RING_SIZE);
    for _ in 0..RING_SIZE {
        let l = std::net::TcpListener::bind("127.0.0.1:0")?;
        listen_addrs.push(l.local_addr()?);
        listeners.push(l);
    }
    drop(listeners);
    let successor_addrs: Vec<SocketAddr> =
        (0..RING_SIZE).map(|i| listen_addrs[(i + 1) % RING_SIZE]).collect();

    // ── Per-node delivery state ──────────────────────────────────────
    // We track at every node: how many payloads delivered so far.
    // We collect latency samples only at the *last* node — that's our
    // worst-case observer in a 2-lap ring with `NUM_TRAINS` issuers.
    let delivered_counts: Vec<Arc<AtomicUsize>> =
        (0..RING_SIZE).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    // Latency samples: one shared Vec gated by an mpsc to avoid locks.
    let (lat_tx, mut lat_rx) = mpsc::channel::<u128>(args.target_messages);

    // Sentinel node = the node that observes payloads last on the
    // injection lap. For broadcast at 0, with NumTrains=2, payloads
    // ride the train of node 0 (or 1) on its first lap; the slowest
    // observer is the predecessor of the issuer = RING_SIZE-1.
    let sentinel = (RING_SIZE - 1) as ProcId;

    // ── Bench coordination ───────────────────────────────────────────
    // The sentinel's dispatch fn fires `done_tx` once it has delivered
    // `target_messages`. The main task awaits `done_rx`.
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let done_tx = Arc::new(std::sync::Mutex::new(Some(done_tx)));
    // Shared zero for latency: set once, used by the injector AND by
    // the sentinel's dispatch fn so both speak the same time-axis.
    let bench_zero = Instant::now();

    // ── Spawn nodes ──────────────────────────────────────────────────
    let mut node_handles = Vec::with_capacity(RING_SIZE);
    let mut broadcast_senders = Vec::with_capacity(RING_SIZE);

    for (i, identity) in identities.into_iter().enumerate() {
        let (bc_tx, bc_rx) = mpsc::channel::<Vec<u8>>(1024);
        broadcast_senders.push(bc_tx);

        let cfg = RingConfig {
            identity,
            listen_addr: listen_addrs[i],
            successor_addr: successor_addrs[i],
            pinned_peer_fingerprints: fingerprints.clone(),
        };
        let issue_initial = i < NUM_TRAINS;
        let delivered_count = delivered_counts[i].clone();
        let lat_tx_n = if (i as ProcId) == sentinel { Some(lat_tx.clone()) } else { None };
        let done_tx_n = if (i as ProcId) == sentinel { Some(done_tx.clone()) } else { None };
        let target = args.target_messages;

        let bench_zero_n = bench_zero;
        let handle = tokio::spawn(async move {
            if let Err(e) = run_node(
                i as ProcId, cfg, issue_initial, bc_rx,
                delivered_count, lat_tx_n, done_tx_n, target, bench_zero_n,
            ).await {
                eprintln!("[{}] node error: {e:#}", i);
            }
        });
        node_handles.push(handle);
    }
    drop(lat_tx);

    // ── Warmup ───────────────────────────────────────────────────────
    tokio::time::sleep(Duration::from_millis(args.warmup_ms)).await;

    // ── Inject + time ────────────────────────────────────────────────
    // Use `bench_zero` for the time-axis so the sentinel's clock and
    // the injector's clock agree on t=0.
    let inject_node = 0usize;
    let mut payload = vec![0x42u8; args.payload];
    let start = Instant::now();
    for _ in 0..args.target_messages {
        let nanos = bench_zero.elapsed().as_nanos() as u64;
        payload[..8].copy_from_slice(&nanos.to_le_bytes());
        broadcast_senders[inject_node].send(payload.clone()).await.ok();
        if args.inject_gap_us > 0 {
            tokio::time::sleep(Duration::from_micros(args.inject_gap_us)).await;
        }
    }

    // ── Wait for sentinel via oneshot, with a budget. ────────────────
    let wait_budget = Duration::from_secs(60);
    match tokio::time::timeout(wait_budget, done_rx).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            eprintln!("done_tx dropped before signalling — protocol error");
            std::process::exit(1);
        }
        Err(_) => {
            let counts: Vec<usize> = delivered_counts.iter()
                .map(|c| c.load(Ordering::Relaxed)).collect();
            eprintln!(
                "TIMEOUT after {:?}; delivered counts = {:?}, target = {}",
                wait_budget, counts, args.target_messages,
            );
            std::process::exit(1);
        }
    }
    let wall = start.elapsed();

    // ── Drain latency samples ────────────────────────────────────────
    drop(broadcast_senders);
    let mut latencies = Vec::with_capacity(args.target_messages);
    while let Some(l) = lat_rx.recv().await { latencies.push(l); }
    latencies.sort_unstable();

    // ── Tear down ────────────────────────────────────────────────────
    for h in &node_handles { h.abort(); }
    for h in node_handles { let _ = h.await; }

    // ── Stats ────────────────────────────────────────────────────────
    let wall_us = wall.as_micros();
    let msgs_per_sec = args.target_messages as f64 / wall.as_secs_f64();
    let bytes_per_sec = (args.target_messages * args.payload) as f64 / wall.as_secs_f64();
    let p = |q: f64| -> u128 {
        if latencies.is_empty() { return 0; }
        let i = ((latencies.len() as f64) * q).clamp(0.0, latencies.len() as f64 - 1.0) as usize;
        latencies[i] / 1_000  // ns → us
    };
    let report = Report {
        kind: "ring_tls",
        ring_size: RING_SIZE, num_trains: NUM_TRAINS, payload: args.payload,
        messages: args.target_messages, wall_us, msgs_per_sec, bytes_per_sec,
        p50_us: p(0.50), p90_us: p(0.90), p99_us: p(0.99),
        max_us: latencies.last().copied().unwrap_or(0) / 1_000,
    };
    eprintln!(
        "ring_tls: N={} K={} payload={}B → {:.0} msgs/sec, \
         {:.1} MiB/sec, p50={}us p99={}us max={}us ({:.3}s)",
        RING_SIZE, NUM_TRAINS, args.payload, msgs_per_sec,
        bytes_per_sec / (1024.0 * 1024.0),
        report.p50_us, report.p99_us, report.max_us, wall.as_secs_f64(),
    );
    if args.json {
        println!("{}", serde_json::to_string(&report).unwrap());
    }
    Ok(())
}

type DoneTx = Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>;

#[allow(clippy::too_many_arguments)]
async fn run_node(
    id: ProcId,
    cfg: RingConfig,
    issue_initial: bool,
    mut bc_rx: mpsc::Receiver<Vec<u8>>,
    delivered_count: Arc<AtomicUsize>,
    lat_tx: Option<mpsc::Sender<u128>>,
    done_tx: Option<DoneTx>,
    target: usize,
    bench_zero: Instant,
) -> anyhow::Result<()> {
    let mut transport = RingTransport::spawn(cfg).await?;
    let mut core = TrainsNode::new(id, DeliveryMode::UniformTotalOrder);
    if issue_initial {
        let t = core.issue_initial_train();
        let _ = transport.outbox.send(t).await;
    }

    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let mut step_count = 0u64;

    loop {
        tokio::select! {
            data = bc_rx.recv() => {
                let Some(d) = data else { break; };
                let outs = core.step(Input::LocalBroadcast(d));
                dispatch(id, &mut transport, outs,
                    &delivered_count, &lat_tx, &done_tx, target, bench_zero).await;
            }
            train = transport.inbox.recv() => {
                let Some(t) = train else { break; };
                let outs = core.step(Input::TrainReceived(t));
                dispatch(id, &mut transport, outs,
                    &delivered_count, &lat_tx, &done_tx, target, bench_zero).await;
            }
            _ = tick.tick() => {
                let outs = core.step(Input::Tick);
                dispatch(id, &mut transport, outs,
                    &delivered_count, &lat_tx, &done_tx, target, bench_zero).await;
            }
        }
        // Yield to the runtime every N steps so the main task gets to
        // poll its timer (otherwise tight train-recycling can starve
        // other tasks on the same worker).
        step_count = step_count.wrapping_add(1);
        if step_count.is_multiple_of(64) {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn dispatch(
    _id: ProcId,
    transport: &mut RingTransport,
    outs: Vec<Output>,
    delivered_count: &Arc<AtomicUsize>,
    lat_tx: &Option<mpsc::Sender<u128>>,
    done_tx: &Option<DoneTx>,
    target: usize,
    bench_zero: Instant,
) {
    for o in outs {
        match o {
            Output::ForwardTrain(t) => {
                let _ = transport.outbox.send(t).await;
            }
            Output::Deliver(payloads) => {
                let now_ns = bench_zero.elapsed().as_nanos();
                for p in &payloads {
                    if let Some(tx) = lat_tx.as_ref() {
                        if p.data.len() >= 8 {
                            let ts = u64::from_le_bytes(p.data[..8].try_into().unwrap()) as u128;
                            let lat = now_ns.saturating_sub(ts);
                            let _ = tx.try_send(lat);
                        }
                    }
                }
                let prev = delivered_count.fetch_add(payloads.len(), Ordering::Relaxed);
                if prev + payloads.len() >= target {
                    if let Some(d) = done_tx.as_ref() {
                        if let Some(tx) = d.lock().unwrap().take() {
                            let _ = tx.send(());
                        }
                    }
                }
                drop::<Vec<Payload>>(payloads);
            }
            Output::DeclareCrash(_) => { /* unexpected */ }
        }
    }
}
