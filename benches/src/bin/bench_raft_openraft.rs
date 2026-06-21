//! Real-Raft-library baseline using **openraft 0.9** (PR-CORE-4).
//!
//! Replaces the discount that a reviewer applies to `bench_raft_baseline`
//! (which is a hand-rolled leader→majority commit loop, not a real Raft). Here
//! a genuine 3-node openraft cluster — real log replication, real commit quorum,
//! real state machine apply — runs over an **in-process network** (RPCs call the
//! target node's `Raft` handlers directly; no TLS, no sockets). That makes this
//! an even *more* favourable upper bound for Raft than the hand-rolled one: it
//! pays no encryption/framing cost, so when TRAINS approaches it the comparison
//! is conservative.
//!
//! Storage + state machine come from `openraft-memstore`; we provide only the
//! `RaftNetwork`.
//!
//! ## Two measurements (this is the point of the PR)
//!   * **commit** — client_write returns when the entry is committed by a
//!     quorum (majority applied). Apples-to-`bench_raft_baseline`'s "majority
//!     ack", and *stronger* than it (real apply, not a bare ack).
//!   * **applied-all** — additionally wait until **all 3** state machines have
//!     applied the entry. Apples-to-TRAINS' "broadcast → all-node delivery".
//!
//! ## Output schema (one stdout JSON line per (kind, payload))
//!   { "kind": "raft_openraft_commit" | "raft_openraft_applied_all",
//!     "nodes": 3, "payload": P, "messages": M, "inflight": W,
//!     "wall_us": T, "msgs_per_sec": X, "bytes_per_sec": Y,
//!     "p50_us": .., "p90_us": .., "p99_us": .., "max_us": .. }

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use clap::Parser;
use serde::Serialize;
use tokio::sync::Semaphore;

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::Adaptor;
use openraft::{Config, Raft, RaftNetwork, RaftNetworkFactory};
use openraft_memstore::{ClientRequest, MemNodeId, MemStore, TypeConfig};

const N: usize = 3;

#[derive(Parser)]
struct Args {
    /// Messages to commit per payload size.
    #[arg(long, default_value_t = 2_000)]
    target_messages: usize,
    /// Max outstanding (un-committed) client writes — the analogue of Raft's
    /// `max_inflight_msgs`. Bounds how much the leader pipelines.
    #[arg(long, default_value_t = 64)]
    inflight: usize,
    /// Comma-separated payload sizes in bytes.
    #[arg(long, default_value = "64,1024,16384")]
    payloads: String,
}

// ── In-process network ────────────────────────────────────────────────────────
//
// Each `Raft` instance needs a network at construction time, but the network
// must call *other* `Raft` instances that don't exist yet. We break the cycle
// with a shared registry the factory clones and fills in after all nodes are
// built; RPCs only fire once `initialize()` starts elections, by which point
// the registry is populated.

type Registry = Arc<Mutex<HashMap<MemNodeId, Raft<TypeConfig>>>>;

#[derive(Clone)]
struct Router {
    peers: Registry,
}

struct Conn {
    target: MemNodeId,
    peers: Registry,
}

impl Conn {
    fn target_raft(&self) -> Raft<TypeConfig> {
        self.peers
            .lock()
            .unwrap()
            .get(&self.target)
            .expect("target registered before any RPC")
            .clone()
    }
}

impl RaftNetworkFactory<TypeConfig> for Router {
    type Network = Conn;

    async fn new_client(&mut self, target: MemNodeId, _node: &()) -> Conn {
        Conn { target, peers: self.peers.clone() }
    }
}

impl RaftNetwork<TypeConfig> for Conn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _opt: RPCOption,
    ) -> Result<AppendEntriesResponse<MemNodeId>, RPCError<MemNodeId, (), RaftError<MemNodeId>>>
    {
        self.target_raft()
            .append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<MemNodeId>,
        _opt: RPCOption,
    ) -> Result<VoteResponse<MemNodeId>, RPCError<MemNodeId, (), RaftError<MemNodeId>>> {
        self.target_raft()
            .vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _opt: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<MemNodeId>,
        RPCError<MemNodeId, (), RaftError<MemNodeId, InstallSnapshotError>>,
    > {
        self.target_raft()
            .install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

// ── Cluster bring-up ──────────────────────────────────────────────────────────

async fn build_cluster() -> (Vec<Raft<TypeConfig>>, MemNodeId) {
    let config = Arc::new(
        Config {
            cluster_name: "bench".to_string(),
            // Tight timers: in-process, so elections settle in a few ms.
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    );

    let peers: Registry = Arc::new(Mutex::new(HashMap::new()));
    let router = Router { peers: peers.clone() };

    let mut rafts = Vec::with_capacity(N);
    for id in 0..N as MemNodeId {
        let (log_store, state_machine) = Adaptor::new(Arc::new(MemStore::new()));
        let raft = Raft::new(id, config.clone(), router.clone(), log_store, state_machine)
            .await
            .expect("Raft::new");
        peers.lock().unwrap().insert(id, raft.clone());
        rafts.push(raft);
    }

    // Form the cluster: node 0 initialises the voter set {0,1,2}.
    let members: BTreeMap<MemNodeId, ()> = (0..N as MemNodeId).map(|i| (i, ())).collect();
    rafts[0].initialize(members).await.expect("initialize");

    let leader = wait_for_leader(&rafts).await;
    (rafts, leader)
}

async fn wait_for_leader(rafts: &[Raft<TypeConfig>]) -> MemNodeId {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(l) = rafts[0].metrics().borrow().current_leader {
            return l;
        }
        if Instant::now() > deadline {
            panic!("no leader elected within 10s");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Highest log index applied across ALL nodes (min of per-node last_applied).
fn min_applied(rafts: &[Raft<TypeConfig>]) -> u64 {
    rafts
        .iter()
        .map(|r| {
            r.metrics()
                .borrow()
                .last_applied
                .map(|l| l.index)
                .unwrap_or(0)
        })
        .min()
        .unwrap_or(0)
}

fn leader_last_log(rafts: &[Raft<TypeConfig>], leader: MemNodeId) -> u64 {
    rafts[leader as usize]
        .metrics()
        .borrow()
        .last_log_index
        .unwrap_or(0)
}

// ── One payload-size pass ─────────────────────────────────────────────────────

#[derive(Serialize)]
struct Row {
    kind: &'static str,
    nodes: usize,
    payload: usize,
    messages: usize,
    inflight: usize,
    wall_us: u128,
    msgs_per_sec: f64,
    bytes_per_sec: f64,
    p50_us: u128,
    p90_us: u128,
    p99_us: u128,
    max_us: u128,
}

fn pct(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

async fn run_payload(
    rafts: &[Raft<TypeConfig>],
    leader: MemNodeId,
    payload: usize,
    messages: usize,
    inflight: usize,
) -> (Row, Row) {
    let lead = rafts[leader as usize].clone();
    let status = "x".repeat(payload);

    // Drive `messages` client writes with a bounded in-flight window; each task
    // records its quorum-commit latency.
    let sem = Arc::new(Semaphore::new(inflight));
    let lat: Arc<Mutex<Vec<u128>>> = Arc::new(Mutex::new(Vec::with_capacity(messages)));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(messages);
    for serial in 0..messages as u64 {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let raft = lead.clone();
        let lat = lat.clone();
        let req = ClientRequest {
            client: "bench".to_string(),
            serial,
            status: status.clone(),
        };
        handles.push(tokio::spawn(async move {
            let t0 = Instant::now();
            // client_write resolves on quorum-commit (majority applied).
            raft.client_write(req).await.expect("client_write");
            lat.lock().unwrap().push(t0.elapsed().as_micros());
            drop(permit);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let commit_wall = start.elapsed();

    // applied-all: wait until every node has applied the leader's last log.
    let target = leader_last_log(rafts, leader);
    let applied_deadline = Instant::now() + Duration::from_secs(30);
    while min_applied(rafts) < target {
        if Instant::now() > applied_deadline {
            panic!("nodes did not all apply within 30s");
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let applied_wall = start.elapsed();

    let mut lat = Arc::try_unwrap(lat).unwrap().into_inner().unwrap();
    lat.sort_unstable();

    let bytes = (messages * payload) as f64;
    let commit = Row {
        kind: "raft_openraft_commit",
        nodes: N,
        payload,
        messages,
        inflight,
        wall_us: commit_wall.as_micros(),
        msgs_per_sec: messages as f64 / commit_wall.as_secs_f64(),
        bytes_per_sec: bytes / commit_wall.as_secs_f64(),
        p50_us: pct(&lat, 0.50),
        p90_us: pct(&lat, 0.90),
        p99_us: pct(&lat, 0.99),
        max_us: *lat.last().unwrap_or(&0),
    };
    let applied = Row {
        kind: "raft_openraft_applied_all",
        nodes: N,
        payload,
        messages,
        inflight,
        wall_us: applied_wall.as_micros(),
        msgs_per_sec: messages as f64 / applied_wall.as_secs_f64(),
        bytes_per_sec: bytes / applied_wall.as_secs_f64(),
        // applied-all is a batch wall-clock measure; per-message percentiles are
        // the commit ones (the apply tail is shared, not per-message).
        p50_us: commit.p50_us,
        p90_us: commit.p90_us,
        p99_us: commit.p99_us,
        max_us: commit.max_us,
    };
    (commit, applied)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = Args::parse();
    let payloads: Vec<usize> = args
        .payloads
        .split(',')
        .map(|s| s.trim().parse().expect("payload size"))
        .collect();

    let (rafts, leader) = build_cluster().await;

    // Warm-up: a few writes so the leader's replication streams are hot and the
    // first measured pass isn't paying connection/stream setup.
    for serial in 0..50u64 {
        rafts[leader as usize]
            .client_write(ClientRequest {
                client: "warmup".to_string(),
                serial,
                status: "x".repeat(64),
            })
            .await
            .expect("warmup write");
    }

    for p in payloads {
        let (commit, applied) = run_payload(&rafts, leader, p, args.target_messages, args.inflight).await;
        println!("{}", serde_json::to_string(&commit).unwrap());
        println!("{}", serde_json::to_string(&applied).unwrap());
    }

    for r in &rafts {
        r.shutdown().await.ok();
    }
}
