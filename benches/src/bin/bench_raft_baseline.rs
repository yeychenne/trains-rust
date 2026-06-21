//! "Raft critical path" baseline — a leader-broadcast-majority-commit
//! benchmark over the same TLS transport stack that `bench_ring_tls`
//! uses (rustls + rcgen + pinned-fingerprint mTLS).
//!
//! ## Honest disclaimer
//!
//! This is **not** a real Raft implementation. There is no leader
//! election, no log persistence, no snapshotting, no commit-index
//! catch-up on lagging followers. It approximates the *critical path*
//! that a healthy Raft cluster's steady-state commit loop exercises:
//!
//!   1. Client appends a payload to the leader's log.
//!   2. Leader sends an AppendEntries-equivalent to all followers in
//!      parallel.
//!   3. Leader waits until a **majority** (incl. self) has acked.
//!   4. Leader commits + records latency.
//!
//! Real Raft would also: persist the log (slower), send heartbeats
//! (slightly slower), and handle follower lag / re-replication. So
//! this number is an **upper bound** on real Raft throughput on
//! identical hardware — which makes the comparison favourable to
//! Raft, not to TRAINS.
//!
//! ## Topology
//!
//! 3 nodes: leader (id=0) and 2 followers (id=1, id=2). Star: leader
//! holds 2 long-lived mTLS connections, one to each follower. Each
//! follower listens on a port and accepts the leader.
//!
//! ## Wire protocol
//!
//! Leader → follower frames: `[4 B BE length][length bytes payload]`.
//! Follower → leader ack frames: `[4 B BE 0]` (zero-length frame =
//! one ack).
//!
//! ## Output schema (stdout JSON line)
//!   { "kind": "raft_baseline", "nodes": 3, "payload": P,
//!     "messages": M, "wall_us": T, "msgs_per_sec": X,
//!     "bytes_per_sec": Y, "p50_us": ..., "p90_us": ...,
//!     "p99_us": ..., "max_us": ... }

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use trains_net::{NodeIdentity, PinnedFingerprintVerifier};

const NUM_NODES:   usize = 3;
const NUM_FOLLOWERS: usize = NUM_NODES - 1;

#[derive(Parser)]
struct Args {
    /// Number of broadcasts to commit (leader-side).
    #[arg(long, default_value_t = 2_000)]
    target_messages: usize,
    /// Payload size in bytes (≥ 8; first 8 bytes hold a timestamp).
    #[arg(long, default_value_t = 1024)]
    payload: usize,
    /// Inject gap in microseconds.
    #[arg(long, default_value_t = 0)]
    inject_gap_us: u64,
    /// Maximum outstanding (un-acked) broadcasts before applying
    /// backpressure. Raft tunes this with `max_inflight_msgs`.
    #[arg(long, default_value_t = 64)]
    max_inflight: usize,
    /// Whether to print the JSON line.
    #[arg(long, default_value_t = true)]
    json: bool,
}

#[derive(Serialize)]
struct Report {
    kind:          &'static str,
    nodes:         usize,
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

    let _ = rustls::crypto::ring::default_provider().install_default();

    // ── Identities + listener ports ──────────────────────────────────
    let identities: Vec<NodeIdentity> = (0..NUM_NODES)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]))
        .collect::<Result<_, _>>()?;
    let fingerprints: Vec<_> = identities.iter().map(|i| i.fingerprint).collect();

    // Followers listen; leader connects.
    let mut follower_addrs = Vec::with_capacity(NUM_FOLLOWERS);
    let mut follower_listeners = Vec::with_capacity(NUM_FOLLOWERS);
    for _ in 0..NUM_FOLLOWERS {
        let l = std::net::TcpListener::bind("127.0.0.1:0")?;
        follower_addrs.push(l.local_addr()?);
        follower_listeners.push(l);
    }
    drop(follower_listeners);

    // ── Spawn followers (TLS acceptors) ──────────────────────────────
    for i in 0..NUM_FOLLOWERS {
        let id = identities[i + 1].clone_for_server();
        let leader_fp = vec![fingerprints[0]];
        let listen = follower_addrs[i];
        tokio::spawn(async move {
            if let Err(e) = run_follower(listen, id, leader_fp).await {
                eprintln!("[follower {}] error: {e:#}", i + 1);
            }
        });
    }

    // Small delay to let listeners settle.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // ── Leader: connect to each follower over TLS ────────────────────
    let leader_id = &identities[0];
    let leader_cfg = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedFingerprintVerifier::new(
            (0..NUM_FOLLOWERS).map(|i| fingerprints[i + 1]).collect(),
        )))
        .with_client_auth_cert(leader_id.cert_chain.clone(), leader_id.key.clone_key())
        .context("leader TLS config")?;
    let connector = TlsConnector::from(Arc::new(leader_cfg));

    let mut follower_streams: Vec<tokio_rustls::client::TlsStream<TcpStream>> =
        Vec::with_capacity(NUM_FOLLOWERS);
    for addr in follower_addrs.iter().take(NUM_FOLLOWERS) {
        let tcp = TcpStream::connect(addr).await?;
        let dns = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let tls = connector.connect(dns, tcp).await?;
        follower_streams.push(tls);
    }

    // ── Per-follower writer + ack-reader tasks ───────────────────────
    // Channels: leader→follower-writer (broadcast payload), follower-
    // reader→leader-aggregator (ack received).
    let mut send_txs: Vec<mpsc::Sender<Vec<u8>>> = Vec::with_capacity(NUM_FOLLOWERS);
    let (ack_tx, mut ack_rx) = mpsc::channel::<usize>(args.max_inflight * NUM_FOLLOWERS * 2);

    for (i, tls_stream) in follower_streams.into_iter().enumerate() {
        let (snd_tx, mut snd_rx) = mpsc::channel::<Vec<u8>>(args.max_inflight * 2);
        send_txs.push(snd_tx);
        let ack_tx_n = ack_tx.clone();
        let (mut r, mut w) = tokio::io::split(tls_stream);
        // Writer task: drains snd_rx → frames → write.
        tokio::spawn(async move {
            while let Some(payload) = snd_rx.recv().await {
                let len = payload.len() as u32;
                if w.write_all(&len.to_be_bytes()).await.is_err() { break; }
                if w.write_all(&payload).await.is_err() { break; }
            }
        });
        // Reader task: reads 4-byte zero-length frames as acks.
        tokio::spawn(async move {
            let mut buf = [0u8; 4];
            while r.read_exact(&mut buf).await.is_ok() {
                let _ = u32::from_be_bytes(buf); // expected 0
                if ack_tx_n.send(i).await.is_err() { break; }
            }
        });
    }
    drop(ack_tx);

    // ── Bench loop ──────────────────────────────────────────────────
    let bench_zero = Instant::now();
    let mut payload = vec![0x42u8; args.payload];
    let mut latencies: Vec<u128> = Vec::with_capacity(args.target_messages);
    // Per-broadcast: how many acks we still need (majority = 1 for 3 nodes).
    // Leader self-counts as ack-1, so we need 1 follower ack.
    // For each inflight msg, store its inject time (ns since bench_zero).
    let mut inflight_inject_times: std::collections::VecDeque<u128> =
        std::collections::VecDeque::with_capacity(args.max_inflight);
    // We also track how many acks each inflight msg has received.
    let mut inflight_acks: std::collections::VecDeque<u8> =
        std::collections::VecDeque::with_capacity(args.max_inflight);

    let bench_start = Instant::now();
    let mut sent = 0usize;
    let mut committed = 0usize;
    while committed < args.target_messages {
        // 1. Send broadcasts up to the inflight window.
        while sent < args.target_messages
            && (sent - committed) < args.max_inflight
        {
            let nanos = bench_zero.elapsed().as_nanos();
            payload[..8].copy_from_slice(&(nanos as u64).to_le_bytes());
            // Send to BOTH followers in parallel (true Raft pattern).
            for st in &send_txs {
                let _ = st.send(payload.clone()).await;
            }
            inflight_inject_times.push_back(nanos);
            inflight_acks.push_back(0);
            sent += 1;
            if args.inject_gap_us > 0 {
                tokio::time::sleep(Duration::from_micros(args.inject_gap_us)).await;
            }
        }

        // 2. Drain acks.
        match ack_rx.recv().await {
            Some(_follower_idx) => {
                // Each ack belongs to the OLDEST in-flight not-yet-
                // committed broadcast for that follower. Since we send
                // in order and each follower acks in order, we know
                // acks come back in send order — so the head of the
                // inflight deque is the right slot to update.
                if let Some(acks) = inflight_acks.front_mut() {
                    *acks += 1;
                    if *acks >= 1 {
                        // Majority reached (leader self-ack + 1 follower).
                        let inject_t = inflight_inject_times.pop_front().unwrap();
                        inflight_acks.pop_front();
                        let lat = bench_zero.elapsed().as_nanos().saturating_sub(inject_t);
                        latencies.push(lat);
                        committed += 1;
                    }
                }
            }
            None => break,
        }
    }
    let wall = bench_start.elapsed();
    // (We do receive ack from the second follower too; we ignore it.)
    drop(send_txs);
    while ack_rx.try_recv().is_ok() {}

    latencies.sort_unstable();
    let wall_us = wall.as_micros();
    let msgs_per_sec = args.target_messages as f64 / wall.as_secs_f64();
    let bytes_per_sec = (args.target_messages * args.payload) as f64 / wall.as_secs_f64();
    let p = |q: f64| -> u128 {
        if latencies.is_empty() { return 0; }
        let i = ((latencies.len() as f64) * q).clamp(0.0, latencies.len() as f64 - 1.0) as usize;
        latencies[i] / 1_000
    };
    let report = Report {
        kind: "raft_baseline",
        nodes: NUM_NODES, payload: args.payload, messages: args.target_messages,
        wall_us, msgs_per_sec, bytes_per_sec,
        p50_us: p(0.50), p90_us: p(0.90), p99_us: p(0.99),
        max_us: latencies.last().copied().unwrap_or(0) / 1_000,
    };
    eprintln!(
        "raft_baseline: N=3 payload={}B → {:.0} msgs/sec, \
         {:.1} MiB/sec, p50={}us p99={}us max={}us ({:.3}s)",
        args.payload, msgs_per_sec, bytes_per_sec / (1024.0 * 1024.0),
        report.p50_us, report.p99_us, report.max_us, wall.as_secs_f64(),
    );
    if args.json {
        println!("{}", serde_json::to_string(&report).unwrap());
    }
    Ok(())
}

async fn run_follower(
    listen: SocketAddr,
    identity: NodeIdentity,
    pinned_leader: Vec<trains_net::SpkiFingerprint>,
) -> anyhow::Result<()> {
    let server_cfg = rustls::ServerConfig::builder()
        .with_client_cert_verifier(Arc::new(PinnedFingerprintVerifier::new(pinned_leader)))
        .with_single_cert(identity.cert_chain.clone(), identity.key.clone_key())
        .context("follower TLS config")?;
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));
    let listener = TcpListener::bind(listen).await?;
    let (sock, _peer) = listener.accept().await?;
    let tls = acceptor.accept(sock).await?;
    let (mut r, mut w) = tokio::io::split(tls);
    loop {
        let mut len_buf = [0u8; 4];
        if r.read_exact(&mut len_buf).await.is_err() { break; }
        let len = u32::from_be_bytes(len_buf) as usize;
        // Read + discard the payload.
        let mut buf = vec![0u8; len];
        if r.read_exact(&mut buf).await.is_err() { break; }
        // Apply (no-op for a benchmark).
        // Ack: send a zero-length frame.
        if w.write_all(&0u32.to_be_bytes()).await.is_err() { break; }
    }
    Ok(())
}

// Helper: `NodeIdentity` doesn't impl Clone (key is private). We need
// to pass identity to spawn tasks. Use a small extension trait to copy
// the fields.
trait CloneForServer {
    fn clone_for_server(&self) -> NodeIdentity;
}

impl CloneForServer for NodeIdentity {
    fn clone_for_server(&self) -> NodeIdentity {
        NodeIdentity {
            cert_chain:  self.cert_chain.clone(),
            key:         self.key.clone_key(),
            fingerprint: self.fingerprint,
        }
    }
}
