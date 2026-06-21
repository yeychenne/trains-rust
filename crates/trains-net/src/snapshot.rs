//! Point-to-point state transfer for replica rejoin (PR-RJ-1 + PR-RJ-2c).
//!
//! The ring transport ([`crate::transport`]) is unidirectional — built for
//! *circulating* trains, not for a point-to-point request/response. State
//! transfer is the opposite shape: a rejoining node asks **one** survivor for
//! the state it missed and gets it back. So it gets its own short-lived,
//! bidirectional channel over the same mutual-TLS + SPKI-fingerprint pinning as
//! the ring.
//!
//! # What moves (PR-RJ-2c)
//! A rejoiner catches up with **a snapshot + a contiguous tail of delivered
//! effects** (`ADR-001` in trains-rust; the SMR `DeliveredLog` in trains-valkey).
//! The requester tells the survivor how far it has already applied (its
//! delivered-index `have`); the survivor replies with whatever closes the gap:
//!
//!   * a **snapshot** blob (empty when the requester is recent enough that the
//!     retained tail alone suffices — e.g. a fresh node whose survivor never
//!     evicted any log entry just replays the whole tail), **and**
//!   * a **tail**: zero or more opaque, framed delivered-effect entries the
//!     requester applies in order.
//!
//! Both are opaque bytes here — the transport stays free of any application/SMR
//! state. trains-valkey serializes a `ReplicaSnapshot` into the snapshot blob and
//! a `DeliveredEntry` into each tail frame, and decides (in the request handler)
//! which to send based on `have` vs its log's low-water mark.
//!
//! # Wire protocol (one request per connection)
//! ```text
//!   client → server:  [REQ_MARKER (1)] [u64 BE have]
//!   server → client:  [u64 BE snap_len] [snap_len bytes]
//!                      [u64 BE n_tail]  ( [u64 BE frame_len] [frame_len bytes] ){n_tail}
//! ```
//! A `snap_len` of 0 means "no snapshot, just apply the tail". Polling to stay
//! current is the same request with a higher `have` each round (PR-RJ-3).

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::tls::{NodeIdentity, PinnedFingerprintVerifier, SpkiFingerprint};

/// One-byte "send me state transfer" request marker.
pub const REQ_MARKER: u8 = 0x53; // 'S'
/// Hard cap on the snapshot blob (refuse to allocate beyond this). 1 GiB is far
/// above any bench keyspace; chunking large snapshots is a v2 concern.
pub const MAX_SNAPSHOT_LEN: u64 = 1 << 30;
/// Hard cap on a single tail frame (one serialized delivered effect). 64 MiB is
/// far above any single Redis write; bounds a malformed/hostile length.
pub const MAX_TAIL_FRAME_LEN: u64 = 1 << 26;
/// Hard cap on the number of tail frames in one response. The survivor's
/// delivered-effect log is itself bounded (trains-valkey `DEFAULT_CAP`), so this
/// only fences a hostile/garbled count from forcing an unbounded allocation.
pub const MAX_TAIL_FRAMES: u64 = 1 << 24;

/// The reply to a state-transfer request: an (optionally empty) snapshot blob
/// plus the contiguous tail of delivered-effect frames to apply after it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateTransfer {
    /// Serialized snapshot blob, or empty when only the tail is needed.
    pub snapshot: Vec<u8>,
    /// Opaque delivered-effect frames, in delivery order, to apply in sequence.
    pub tail: Vec<Vec<u8>>,
}

/// A request the state-transfer server hands to its owner: "the peer has applied
/// up to [`have`](SnapshotRequest::have); fill in what closes the gap". The
/// proxy driver replies via [`SnapshotRequest::reply`].
pub struct SnapshotRequest {
    have: u64,
    tx: oneshot::Sender<StateTransfer>,
}

impl SnapshotRequest {
    /// The requester's current delivered-index — the count of effects it has
    /// already applied. `0` means a fresh node that has applied nothing.
    pub fn have(&self) -> u64 {
        self.have
    }

    /// Provide the state transfer for this request: a snapshot (empty if the
    /// retained tail alone covers `have`) plus the tail frames to apply.
    pub fn reply(self, transfer: StateTransfer) {
        let _ = self.tx.send(transfer);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls config: {0}")]
    TlsConfig(String),
    #[error("snapshot too large: {0} bytes (max {MAX_SNAPSHOT_LEN})")]
    TooLarge(u64),
    #[error("tail frame too large: {0} bytes (max {MAX_TAIL_FRAME_LEN})")]
    FrameTooLarge(u64),
    #[error("too many tail frames: {0} (max {MAX_TAIL_FRAMES})")]
    TooManyFrames(u64),
    #[error("provider unavailable (owner dropped the request channel)")]
    ProviderGone,
}

fn server_config(
    identity: &NodeIdentity,
    pinned: &[SpkiFingerprint],
) -> Result<rustls::ServerConfig, SnapshotError> {
    rustls::ServerConfig::builder()
        .with_client_cert_verifier(Arc::new(PinnedFingerprintVerifier::new(pinned.to_vec())))
        .with_single_cert(identity.cert_chain.clone(), identity.key.clone_key())
        .map_err(|e| SnapshotError::TlsConfig(e.to_string()))
}

fn client_config(
    identity: &NodeIdentity,
    pinned: &[SpkiFingerprint],
) -> Result<rustls::ClientConfig, SnapshotError> {
    rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedFingerprintVerifier::new(pinned.to_vec())))
        .with_client_auth_cert(identity.cert_chain.clone(), identity.key.clone_key())
        .map_err(|e| SnapshotError::TlsConfig(e.to_string()))
}

/// **Client.** Fetch the state transfer that closes the gap from `have` to a
/// survivor's current head (snapshot + tail). Poll with a rising `have` to stay
/// current (PR-RJ-3 passive standby).
pub async fn fetch_state(
    addr: SocketAddr,
    identity: &NodeIdentity,
    pinned: &[SpkiFingerprint],
    have: u64,
) -> Result<StateTransfer, SnapshotError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let connector = TlsConnector::from(Arc::new(client_config(identity, pinned)?));
    let tcp = TcpStream::connect(addr).await?;
    // Pinning ignores the hostname; the cert SANs use "localhost".
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(server_name, tcp).await?;

    // Request: marker + the requester's delivered-index.
    let mut req = [0u8; 9];
    req[0] = REQ_MARKER;
    req[1..].copy_from_slice(&have.to_be_bytes());
    tls.write_all(&req).await?;
    tls.flush().await?;

    // Response: snapshot blob, then a framed tail.
    let snapshot = read_lp_blob(&mut tls, MAX_SNAPSHOT_LEN, SnapshotError::TooLarge).await?;
    let n_tail = read_u64(&mut tls).await?;
    if n_tail > MAX_TAIL_FRAMES {
        return Err(SnapshotError::TooManyFrames(n_tail));
    }
    let mut tail = Vec::with_capacity(n_tail.min(1024) as usize);
    for _ in 0..n_tail {
        tail.push(read_lp_blob(&mut tls, MAX_TAIL_FRAME_LEN, SnapshotError::FrameTooLarge).await?);
    }
    Ok(StateTransfer { snapshot, tail })
}

/// **Client (compat).** Fetch just a full snapshot (PR-RJ-1 shape): `have = 0`,
/// tail discarded. Kept for callers that only need the bulk state.
pub async fn fetch_snapshot(
    addr: SocketAddr,
    identity: &NodeIdentity,
    pinned: &[SpkiFingerprint],
) -> Result<Vec<u8>, SnapshotError> {
    Ok(fetch_state(addr, identity, pinned, 0).await?.snapshot)
}

/// **Server.** Accept state-transfer requests forever; for each, ask the owner
/// (via `requests`) for the transfer that closes the requester's gap and write
/// it back. Spawn this and keep the handle; dropping/aborting it stops the
/// server. `requests` is the channel the owner (proxy driver) services with
/// [`SnapshotRequest::reply`].
pub async fn serve_snapshots(
    listen_addr: SocketAddr,
    identity: NodeIdentity,
    pinned: Vec<SpkiFingerprint>,
    requests: mpsc::Sender<SnapshotRequest>,
) -> Result<(), SnapshotError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let acceptor = TlsAcceptor::from(Arc::new(server_config(&identity, &pinned)?));
    let listener = TcpListener::bind(listen_addr).await?;
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "snapshot accept failed");
                return Err(e.into());
            }
        };
        let acceptor = acceptor.clone();
        let requests = requests.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_one(acceptor, tcp, requests).await {
                tracing::debug!(%peer, error = %e, "snapshot request failed");
            }
        });
    }
}

async fn handle_one(
    acceptor: TlsAcceptor,
    tcp: TcpStream,
    requests: mpsc::Sender<SnapshotRequest>,
) -> Result<(), SnapshotError> {
    let mut tls = acceptor.accept(tcp).await?;
    let mut marker = [0u8; 1];
    tls.read_exact(&mut marker).await?;
    if marker[0] != REQ_MARKER {
        return Ok(()); // not a state-transfer request; ignore
    }
    let have = read_u64(&mut tls).await?;

    let (tx, rx) = oneshot::channel();
    requests
        .send(SnapshotRequest { have, tx })
        .await
        .map_err(|_| SnapshotError::ProviderGone)?;
    let StateTransfer { snapshot, tail } = rx.await.map_err(|_| SnapshotError::ProviderGone)?;

    // Snapshot blob (length-prefixed; may be empty).
    tls.write_all(&(snapshot.len() as u64).to_be_bytes()).await?;
    tls.write_all(&snapshot).await?;
    // Framed tail: count, then each frame length-prefixed. Frames are small, so
    // batch them into one buffer and write once.
    let mut framed = Vec::new();
    framed.extend_from_slice(&(tail.len() as u64).to_be_bytes());
    for frame in &tail {
        framed.extend_from_slice(&(frame.len() as u64).to_be_bytes());
        framed.extend_from_slice(frame);
    }
    tls.write_all(&framed).await?;
    tls.flush().await?;
    Ok(())
}

/// Read a `u64` big-endian length/count.
async fn read_u64<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<u64, SnapshotError> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b).await?;
    Ok(u64::from_be_bytes(b))
}

/// Read a length-prefixed blob, rejecting a length over `max` via `too_large`.
async fn read_lp_blob<R: AsyncReadExt + Unpin>(
    r: &mut R,
    max: u64,
    too_large: fn(u64) -> SnapshotError,
) -> Result<Vec<u8>, SnapshotError> {
    let len = read_u64(r).await?;
    if len > max {
        return Err(too_large(len));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn free_addr() -> SocketAddr {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        a
    }

    fn clone_id(id: &NodeIdentity) -> NodeIdentity {
        NodeIdentity { cert_chain: id.cert_chain.clone(), key: id.key.clone_key(), fingerprint: id.fingerprint }
    }

    /// Spawn an owner that replies to every request with `make(have)`, and a
    /// server in front of it. Returns the server addr + the two identities.
    async fn spawn_server(
        make: impl Fn(u64) -> StateTransfer + Send + 'static,
    ) -> (SocketAddr, NodeIdentity, NodeIdentity) {
        let server_id = NodeIdentity::generate(vec!["localhost".into()]).unwrap();
        let client_id = NodeIdentity::generate(vec!["localhost".into()]).unwrap();
        let server_pins = vec![client_id.fingerprint];
        let addr = free_addr();

        let (req_tx, mut req_rx) = mpsc::channel::<SnapshotRequest>(4);
        tokio::spawn(async move {
            while let Some(r) = req_rx.recv().await {
                let t = make(r.have());
                r.reply(t);
            }
        });
        let sid = clone_id(&server_id);
        tokio::spawn(async move {
            let _ = serve_snapshots(addr, sid, server_pins, req_tx).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        (addr, server_id, client_id)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_and_tail_round_trip() {
        // PR-RJ-2c: a snapshot blob plus a framed tail both survive the wire,
        // in order and byte-for-byte.
        let snap = b"keyspace-snapshot".repeat(50);
        let tail: Vec<Vec<u8>> = (0..5).map(|i| format!("effect-{i}").into_bytes()).collect();
        let (s, t) = (snap.clone(), tail.clone());
        let (addr, server_id, client_id) =
            spawn_server(move |_have| StateTransfer { snapshot: s.clone(), tail: t.clone() }).await;

        let got = fetch_state(addr, &client_id, &[server_id.fingerprint], 0).await.expect("fetch");
        assert_eq!(got.snapshot, snap, "snapshot blob round-trips");
        assert_eq!(got.tail, tail, "tail frames round-trip in order");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn have_index_reaches_the_provider_for_incremental_tail() {
        // PR-RJ-2c: the requester's `have` is delivered to the owner, which uses
        // it to serve only the missing tail (no snapshot) — the polling path.
        let (addr, server_id, client_id) = spawn_server(|have| StateTransfer {
            snapshot: Vec::new(), // incremental: no snapshot
            tail: (have..have + 3).map(|i| i.to_be_bytes().to_vec()).collect(),
        })
        .await;

        let got = fetch_state(addr, &client_id, &[server_id.fingerprint], 42).await.expect("fetch");
        assert!(got.snapshot.is_empty(), "incremental response carries no snapshot");
        assert_eq!(
            got.tail,
            vec![42u64.to_be_bytes().to_vec(), 43u64.to_be_bytes().to_vec(), 44u64.to_be_bytes().to_vec()],
            "provider served the tail starting at the requester's have-index",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_tail_round_trips() {
        // Boundary: snapshot present, zero tail frames (requester already at head
        // the instant the snapshot was taken).
        let (addr, server_id, client_id) =
            spawn_server(|_| StateTransfer { snapshot: vec![1, 2, 3], tail: Vec::new() }).await;
        let got = fetch_state(addr, &client_id, &[server_id.fingerprint], 7).await.expect("fetch");
        assert_eq!(got.snapshot, vec![1, 2, 3]);
        assert!(got.tail.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_snapshot_compat_returns_just_the_blob() {
        // PR-RJ-1 compat wrapper still works (have=0, tail discarded).
        let (addr, server_id, client_id) =
            spawn_server(|_| StateTransfer { snapshot: vec![9, 9, 9], tail: vec![vec![1]] }).await;
        let got = fetch_snapshot(addr, &client_id, &[server_id.fingerprint]).await.expect("fetch");
        assert_eq!(got, vec![9, 9, 9]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wrong_pin_is_rejected() {
        let server_id = NodeIdentity::generate(vec!["localhost".into()]).unwrap();
        let client_id = NodeIdentity::generate(vec!["localhost".into()]).unwrap();
        let rogue = NodeIdentity::generate(vec!["localhost".into()]).unwrap();
        let addr = free_addr();
        let (req_tx, mut req_rx) = mpsc::channel::<SnapshotRequest>(4);
        tokio::spawn(async move {
            while let Some(r) = req_rx.recv().await {
                r.reply(StateTransfer { snapshot: vec![1, 2, 3], tail: Vec::new() });
            }
        });
        // server pins only `client_id`; the rogue client is not allow-listed
        let sid = clone_id(&server_id);
        tokio::spawn(async move { let _ = serve_snapshots(addr, sid, vec![client_id.fingerprint], req_tx).await; });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let r = fetch_state(addr, &rogue, &[server_id.fingerprint], 0).await;
        assert!(r.is_err(), "a non-pinned client must be rejected");
    }
}
