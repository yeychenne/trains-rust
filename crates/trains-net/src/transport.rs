//! Ring-topology TLS transport.
//!
//! `RingTransport` is the runtime-side of `trains-net`: each node has
//! exactly one inbound TLS connection (from its predecessor) and one
//! outbound TLS connection (to its successor). It exposes:
//!
//!   * an `mpsc::Receiver<Train>` for trains arriving from the predecessor
//!   * an `mpsc::Sender<Train>`  for trains to forward to the successor
//!
//! The trains-core kernel (`TrainsNode::step`) is sync; this module
//! is the async I/O boundary.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use trains_core::Train;

use crate::codec::{read_msg, write_msg, CodecError};
use crate::tls::{NodeIdentity, PinnedFingerprintVerifier, SpkiFingerprint};
use crate::wire::{ViewChangeMsg, WireMsg};

const RECONNECT_BACKOFF: Duration = Duration::from_millis(500);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(5);
const CHANNEL_CAP: usize = 64;
/// Consecutive failed connection attempts to the successor before we report it
/// as unreachable (the failure-detector's strong-evidence signal for a clean
/// crash, which produces no clock gap).
const UNREACHABLE_FAILURES: u32 = 5;
/// `TCP_USER_TIMEOUT` for ring sockets — how long the kernel will retransmit
/// without an ACK before forcibly closing the connection. Default Linux
/// retransmit budget is ~15 min, which on EC2 means `send()` to a half-closed
/// peer succeeds for *minutes* before the connector even sees an error → the
/// failure detector never strikes (`unreachable_rx` only fires on connect
/// failures, and an established TCP connection never tries to connect again).
/// 3 s is the smallest value that comfortably accommodates EC2 inter-AZ
/// jitter while keeping mean-time-to-detection in the seconds range. PR-RD-6;
/// no-op on platforms that don't expose `TCP_USER_TIMEOUT` (macOS).
// Helper is a no-op off Linux/Android; the constant is still useful for docs
// and symmetry but unreferenced — silence the warning on those platforms.
#[cfg_attr(
    not(any(target_os = "linux", target_os = "android")),
    allow(dead_code)
)]
const RING_TCP_USER_TIMEOUT: Duration = Duration::from_secs(3);

/// Apply `TCP_USER_TIMEOUT` to a ring TCP socket. Errors from the underlying
/// `setsockopt` are logged at WARN and swallowed — peer-death detection
/// degrades to the default TCP retransmit budget, which is the pre-RD-6
/// behavior, not a regression. Linux/Android only (the option doesn't exist
/// on macOS/BSD); a no-op everywhere else.
fn apply_ring_socket_opts(stream: &TcpStream, side: &'static str) {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        let sref = socket2::SockRef::from(stream);
        if let Err(e) = sref.set_tcp_user_timeout(Some(RING_TCP_USER_TIMEOUT)) {
            tracing::warn!(side, error=%e, "set_tcp_user_timeout failed; peer-death detection falls back to TCP defaults");
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = (stream, side);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec: {0}")]
    Codec(#[from] CodecError),
    #[error("rustls: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("tls config: {0}")]
    TlsConfig(String),
}

/// Configuration for one ring node's transport layer.
pub struct RingConfig {
    /// This node's TLS identity (cert + key).
    pub identity: NodeIdentity,
    /// SocketAddr to listen on (predecessor connects here).
    pub listen_addr: SocketAddr,
    /// SocketAddr of the successor node (we connect to it).
    pub successor_addr: SocketAddr,
    /// SPKI fingerprints we accept from peers (predecessor + successor;
    /// in practice they're often distinct, sometimes shared).
    pub pinned_peer_fingerprints: Vec<SpkiFingerprint>,
}

/// The async I/O endpoints owned by a node.
pub struct RingTransport {
    /// Trains arriving from predecessor.
    pub inbox: mpsc::Receiver<Train>,
    /// Trains to forward to successor.
    pub outbox: mpsc::Sender<Train>,
    /// View-change (reconfiguration) frames arriving from predecessor.
    pub vc_inbox: mpsc::Receiver<ViewChangeMsg>,
    /// View-change frames to forward to successor. Travels the same ring
    /// link as trains (muxed into one stream); demuxed on receipt.
    pub vc_outbox: mpsc::Sender<ViewChangeMsg>,
    /// Fires with the successor's address when it has been unreachable for
    /// [`UNREACHABLE_FAILURES`] consecutive connection attempts — the
    /// strong-evidence crash signal for the failure detector (a clean crash
    /// produces no clock gap, so this is how the predecessor notices).
    pub unreachable_rx: mpsc::Receiver<SocketAddr>,
    /// Control channel to re-point the outbound connection at a new
    /// successor address at runtime (ring reconfiguration, Gap C). The
    /// connector drops its current connection and reconnects to the new
    /// address, retaining any in-flight message.
    retarget_tx: mpsc::Sender<SocketAddr>,
    _listener_task:  JoinHandle<()>,
    _connector_task: JoinHandle<()>,
    _mux_task:       JoinHandle<()>,
    /// Per-inbound-connection task handles (one per accepted TLS connection).
    /// Tracked so [`RingTransport::abort`] can close them — the listener's
    /// `tokio::spawn` is otherwise untracked, and aborting the listener task
    /// only stops *new* accepts; existing connections (and their open TCP
    /// sockets) survive. Without abort-on-call, the predecessor's writer keeps
    /// pushing into a dead-end kernel buffer for the full TCP retransmit
    /// budget, blocking the proxy's runtime — the failure mode that flaked
    /// `crash_masking` during PR-RD-7 verification (REPORT 2026-05-26).
    /// PR-RD-8.
    conn_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl RingTransport {
    /// Spawn listener + outbound-connector tasks; return the handle.
    pub async fn spawn(cfg: RingConfig) -> Result<Self, TransportError> {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let pinned = Arc::new(cfg.pinned_peer_fingerprints);

        // ── Inbound: TLS server, framed reader → demux to inbox/vc_inbox ──
        let (in_tx, in_rx) = mpsc::channel::<Train>(CHANNEL_CAP);
        let (vc_in_tx, vc_in_rx) = mpsc::channel::<ViewChangeMsg>(CHANNEL_CAP);

        let server_cfg = rustls::ServerConfig::builder()
            .with_client_cert_verifier(Arc::new(PinnedFingerprintVerifier::new(
                (*pinned).clone(),
            )))
            .with_single_cert(cfg.identity.cert_chain.clone(), cfg.identity.key.clone_key())
            .map_err(|e| TransportError::TlsConfig(e.to_string()))?;
        let acceptor = TlsAcceptor::from(Arc::new(server_cfg));
        let listener = TcpListener::bind(cfg.listen_addr).await?;
        let conn_tasks: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
        let listener_task = tokio::spawn(listener_loop(
            listener,
            acceptor,
            in_tx,
            vc_in_tx,
            conn_tasks.clone(),
        ));

        // ── Outbound: outbox (trains) + vc_outbox (view-change) muxed into
        //    one WireMsg stream → TLS client framed writer ──
        let (out_tx, mut out_rx) = mpsc::channel::<Train>(CHANNEL_CAP);
        let (vc_out_tx, mut vc_out_rx) = mpsc::channel::<ViewChangeMsg>(CHANNEL_CAP);
        let (wire_tx, wire_rx) = mpsc::channel::<WireMsg>(CHANNEL_CAP);

        // Mux: tag each outbound message and feed the single connector stream.
        let mux_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    maybe = out_rx.recv() => match maybe {
                        Some(t) => if wire_tx.send(WireMsg::Train(t)).await.is_err() { break },
                        None => break,
                    },
                    maybe = vc_out_rx.recv() => match maybe {
                        Some(v) => if wire_tx.send(WireMsg::ViewChange(v)).await.is_err() { break },
                        None => break,
                    },
                }
            }
        });

        let client_cfg = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedFingerprintVerifier::new(
                (*pinned).clone(),
            )))
            .with_client_auth_cert(cfg.identity.cert_chain.clone(), cfg.identity.key.clone_key())
            .map_err(|e| TransportError::TlsConfig(e.to_string()))?;
        let connector = TlsConnector::from(Arc::new(client_cfg));
        let (retarget_tx, retarget_rx) = mpsc::channel::<SocketAddr>(8);
        let (unreachable_tx, unreachable_rx) = mpsc::channel::<SocketAddr>(8);
        let connector_task = tokio::spawn(connector_loop(
            connector,
            cfg.successor_addr,
            wire_rx,
            retarget_rx,
            unreachable_tx,
        ));

        Ok(Self {
            inbox: in_rx,
            outbox: out_tx,
            vc_inbox: vc_in_rx,
            vc_outbox: vc_out_tx,
            unreachable_rx,
            retarget_tx,
            _listener_task:  listener_task,
            _connector_task: connector_task,
            _mux_task:       mux_task,
            conn_tasks,
        })
    }

    /// Re-point the outbound connection at a new successor address.
    /// Used for ring reconfiguration: when a downstream node crashes,
    /// the survivor whose successor died calls this with the dead node's
    /// successor so the train circulates past the gap. The connector
    /// drops its current TLS connection and reconnects to `addr`.
    pub async fn retarget_successor(&self, addr: SocketAddr) {
        let _ = self.retarget_tx.send(addr).await;
    }

    /// Abort the listener + connector tasks AND every accepted per-connection
    /// task, simulating a node crash: the node stops accepting inbound
    /// connections, stops forwarding outbound, and the inbound TLS streams
    /// are dropped so peers see the close promptly.
    ///
    /// Used by reconfiguration tests to "kill" a node so its predecessor
    /// must re-route past it. Pre-PR-RD-8, only the listener/connector/mux
    /// tasks were aborted; the per-connection accept tasks lived on with
    /// open TLS streams, so the predecessor's writer kept pushing into a
    /// half-closed socket for the full TCP retransmit budget (~15 min on
    /// Linux), blocking the proxy's tokio runtime and flaking the
    /// `crash_masking` test. Now every owned task is aborted, dropping
    /// every TLS stream and letting the peer's read return EOF.
    pub fn abort(&self) {
        self._listener_task.abort();
        self._connector_task.abort();
        self._mux_task.abort();
        // Lock briefly to read the handle list; aborts are non-blocking, no
        // need to await joins. Tasks not yet pushed (mid-accept, before the
        // post-spawn push) are accepted as best-effort.
        if let Ok(tasks) = self.conn_tasks.lock() {
            for t in tasks.iter() {
                t.abort();
            }
        }
    }

    /// Number of accepted-connection task handles currently tracked. Test
    /// hook (PR-RD-8); not part of the production API. May be marginally
    /// stale relative to "alive tasks" because completed tasks are not
    /// pruned — but post-[`abort`], every handle is non-alive.
    #[doc(hidden)]
    pub fn connection_task_count(&self) -> usize {
        self.conn_tasks.lock().map(|t| t.len()).unwrap_or(0)
    }
}

async fn listener_loop(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    in_tx: mpsc::Sender<Train>,
    vc_in_tx: mpsc::Sender<ViewChangeMsg>,
    conn_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
) {
    loop {
        let (sock, peer) = match listener.accept().await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error=%e, "accept failed; retrying");
                sleep(RECONNECT_BACKOFF).await;
                continue;
            }
        };
        // PR-RD-6: bound peer-death detection latency to RING_TCP_USER_TIMEOUT
        // on Linux. Inbound side — without this, a writer on the other end
        // can keep its half of the connection alive indefinitely.
        apply_ring_socket_opts(&sock, "accept");
        let acceptor = acceptor.clone();
        let in_tx = in_tx.clone();
        let vc_in_tx = vc_in_tx.clone();
        let handle = tokio::spawn(async move {
            tracing::info!(%peer, "predecessor connected");
            match acceptor.accept(sock).await {
                Ok(mut tls) => {
                    loop {
                        match read_msg(&mut tls).await {
                            // Demux: trains → inbox, view-change → vc_inbox.
                            Ok(WireMsg::Train(train)) => {
                                if in_tx.send(train).await.is_err() {
                                    tracing::warn!("inbox closed; dropping connection");
                                    break;
                                }
                            }
                            Ok(WireMsg::ViewChange(vc)) => {
                                if vc_in_tx.send(vc).await.is_err() {
                                    tracing::warn!("vc_inbox closed; dropping connection");
                                    break;
                                }
                            }
                            Err(CodecError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                                tracing::info!(%peer, "predecessor disconnected");
                                break;
                            }
                            Err(e) => {
                                tracing::warn!(error=%e, "decode error; closing");
                                break;
                            }
                        }
                    }
                }
                Err(e) => tracing::warn!(error=%e, "TLS handshake failed"),
            }
        });
        // Track the handle so `RingTransport::abort` can close this connection
        // promptly (PR-RD-8). Lock briefly; if the mutex is poisoned (caller's
        // abort path panicked) we still spawn — best-effort tracking.
        if let Ok(mut tasks) = conn_tasks.lock() {
            tasks.push(handle);
        }
    }
}

async fn connector_loop(
    connector: TlsConnector,
    initial_addr: SocketAddr,
    mut wire_rx: mpsc::Receiver<WireMsg>,
    mut retarget_rx: mpsc::Receiver<SocketAddr>,
    unreachable_tx: mpsc::Sender<SocketAddr>,
) {
    let mut addr = initial_addr;
    let mut backoff = RECONNECT_BACKOFF;
    // Consecutive connect failures to `addr`; once it crosses the threshold we
    // report the successor unreachable (once per streak). Reset on a
    // successful connect or a retarget to a new address.
    let mut fail_streak: u32 = 0;
    let mut notified = false;
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    // `pending` is a single message we picked up from the outbound stream
    // but haven't yet successfully written.  We retain it across connect/
    // handshake/write retries so that startup races (predecessor not yet
    // listening, TLS not yet ready, transient network errors) do not drop
    // the ring's first train.  trains-core's clock-gap detection is *not*
    // enough — the issuer never re-issues a dropped slot, so an empty ring
    // deadlocks.  Keeping the message in flight here is the simplest
    // robustness improvement (and view-change frames are also retransmitted
    // at the protocol level — A6).
    let mut pending: Option<WireMsg> = None;
    // PR-RD-9: when the reader task detects peer-close while the connector
    // is idle (no `pending`, nothing in `wire_rx`), we still need to drive
    // the reconnect-and-fail-streak path so the failure detector strikes.
    // This flag forces the next outer-loop iteration to skip the
    // wire_rx-wait and proceed directly to `TcpStream::connect`, even with
    // no message in hand. Cleared once the connect attempt resolves (the
    // peer is either back, or this iteration has ticked `fail_streak`).
    let mut force_reconnect = false;
    loop {
        // Acquire a message, either the one we're retrying or a fresh one.
        // PR-RD-9: if `force_reconnect`, skip the wait and probe-connect
        // with no message — the connect attempt itself drives `fail_streak`.
        let msg_opt: Option<WireMsg> = if force_reconnect {
            force_reconnect = false;
            None
        } else {
            match pending.take() {
                Some(m) => Some(m),
                None => tokio::select! {
                    maybe = wire_rx.recv() => match maybe {
                        Some(m) => Some(m),
                        None => {
                            tracing::info!("outbox closed; shutting down connector");
                            return;
                        }
                    },
                    Some(new_addr) = retarget_rx.recv() => {
                        if new_addr != addr {
                            tracing::info!(old=%addr, new=%new_addr, "retarget successor (idle)");
                            addr = new_addr;
                            backoff = RECONNECT_BACKOFF;
                            fail_streak = 0;
                            notified = false;
                        }
                        continue;
                    }
                },
            }
        };

        // Connect TCP.
        // PR-RD-9: remember whether this iteration was a probe (no msg) — if
        // the connect fails, we need to keep probing on the next iteration
        // instead of falling back to the wire_rx-wait. Without this, a
        // peer-close detection only ever ticks `fail_streak` once.
        let was_probe = msg_opt.is_none();
        let tcp = match TcpStream::connect(addr).await {
            Ok(s) => {
                // PR-RD-6: bound peer-death detection latency on this outbound
                // ring socket. Outbound side is the one observed to hang on
                // EC2 (REPORT.md 2026-05-26) — when the successor dies, the
                // kernel's default retransmit budget keeps `send()` happy for
                // ~15 min, so the connector never sees an error and never
                // re-enters the connect/fail_streak path. With this option,
                // the kernel forces a close after RING_TCP_USER_TIMEOUT, the
                // write fails, the loop reconnects, the reconnect fails, and
                // `unreachable_rx` fires within ~5 × backoff seconds.
                apply_ring_socket_opts(&s, "connect");
                fail_streak = 0;
                notified = false;
                s
            }
            Err(e) => {
                tracing::warn!(addr=%addr, error=%e, "connect failed; will retry");
                if let Some(m) = msg_opt {
                    pending = Some(m);
                }
                if was_probe {
                    // Keep probing until the peer comes back or we get
                    // retargeted; pre-PR-RD-9 the loop would fall back to
                    // wire_rx-wait after one failure and the failure detector
                    // never struck.
                    force_reconnect = true;
                }
                // Persistent connect failure ⇒ the successor is (probably) dead.
                // Report it once so the node's failure detector can act (a clean
                // crash never yields a clock gap, so this is the trigger).
                fail_streak += 1;
                if fail_streak >= UNREACHABLE_FAILURES && !notified {
                    let _ = unreachable_tx.try_send(addr);
                    notified = true;
                    tracing::warn!(addr=%addr, "successor unreachable; reported");
                }
                // Honor a retarget during backoff so we stop hammering a
                // dead successor as soon as the failure detector re-points us.
                tokio::select! {
                    _ = sleep(backoff) => {
                        backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
                    }
                    Some(new_addr) = retarget_rx.recv() => {
                        if new_addr != addr {
                            tracing::info!(old=%addr, new=%new_addr, "retarget successor (backoff)");
                            addr = new_addr;
                            backoff = RECONNECT_BACKOFF;
                            fail_streak = 0;
                            notified = false;
                        }
                    }
                }
                continue;
            }
        };
        // TLS handshake.
        let tls = match connector.connect(server_name.clone(), tcp).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error=%e, "TLS handshake to successor failed; will retry");
                if let Some(m) = msg_opt {
                    pending = Some(m);
                }
                sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
                continue;
            }
        };
        backoff = RECONNECT_BACKOFF;

        // PR-RD-9: split the TLS stream and spawn a reader task that drains
        // the read half. The connector itself only writes, so without this
        // task the underlying socket's "peer closed" signal (FIN/RST/TLS
        // alert) is invisible to the application — only `write_msg` would
        // notice, and only if there's a pending message to send. The E1 EC2
        // run (2026-05-26) hung in exactly this state: upstream stopped
        // issuing trains because the client was blocked, so the connector
        // never tried to write, so the dead peer was never observed.
        //
        // The reader signals `peer_close_rx` on EOF or error; the inner
        // `select!` reacts and breaks to the reconnect path, where the
        // failure detector strikes via the existing `fail_streak` chain.
        // Together with PR-RD-6's `TCP_USER_TIMEOUT`, this closes both
        // halves of the "detect peer death fast" story (kernel timeout for
        // half-closed sockets + application-level FIN detection for fully
        // closed ones).
        let (mut tls_read, mut tls_write) = tokio::io::split(tls);
        let (peer_close_tx, mut peer_close_rx) = mpsc::channel::<()>(1);
        let reader_task = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut buf = [0u8; 256];
            loop {
                match tls_read.read(&mut buf).await {
                    Ok(0) => {
                        tracing::info!("peer closed read side; signalling connector");
                        break;
                    }
                    // The ring is one-way at the protocol level — any bytes
                    // here are TLS-layer noise (renegotiation alert, etc.).
                    // Drop them; the only thing we care about is the close
                    // signal.
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!(error=%e, "read error on successor socket");
                        break;
                    }
                }
            }
            let _ = peer_close_tx.try_send(());
        });

        // Send the first message if we have one (PR-RD-9: msg_opt may be
        // None when we're probe-reconnecting after a peer-close).
        if let Some(msg) = msg_opt {
            if let Err(e) = write_msg(&mut tls_write, &msg).await {
                tracing::warn!(error=%e, "write failed; will retry");
                pending = Some(msg);
                reader_task.abort();
                continue;
            }
        }
        // Stream subsequent messages until the connection dies or we are
        // retargeted to a new successor. PR-RD-9 added the `peer_close_rx`
        // arm so an idle connector still observes a dead peer; on signal,
        // we set `force_reconnect` so the outer loop probes the peer with a
        // new connect attempt instead of waiting for the next message.
        loop {
            tokio::select! {
                maybe = wire_rx.recv() => match maybe {
                    Some(m) => {
                        if let Err(e) = write_msg(&mut tls_write, &m).await {
                            tracing::warn!(error=%e, "write failed; will retry");
                            pending = Some(m);
                            break;
                        }
                    }
                    None => {
                        tracing::info!("outbox closed; closing connection");
                        reader_task.abort();
                        return;
                    }
                },
                Some(new_addr) = retarget_rx.recv() => {
                    if new_addr != addr {
                        tracing::info!(old=%addr, new=%new_addr,
                            "retarget successor (active); reconnecting");
                        addr = new_addr;
                        backoff = RECONNECT_BACKOFF;
                        fail_streak = 0;
                        notified = false;
                        break; // drop tls, reconnect to new addr
                    }
                }
                _ = peer_close_rx.recv() => {
                    // PR-RD-9: the reader saw EOF / error. Force the next
                    // outer-loop iteration to attempt a reconnect even if
                    // there's no message to send — that's the only path
                    // through the connect-fail / fail_streak / unreachable
                    // chain when upstream is idle.
                    tracing::warn!(addr=%addr, "peer closed (reader signal); probing reconnect");
                    force_reconnect = true;
                    break;
                }
            }
        }
        // Abort the reader (which still owns tls_read) so the underlying
        // TLS stream is fully released before we try to reconnect.
        reader_task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trains_core::{Payload, Train};

    fn pick_port() -> SocketAddr {
        // Bind to port 0, get the assigned port.
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        addr
    }

    /// PR-RD-6: verify `apply_ring_socket_opts` actually sets
    /// `TCP_USER_TIMEOUT` to `RING_TCP_USER_TIMEOUT`. Linux/Android only —
    /// macOS/BSD don't expose the socket option, and the helper is a no-op
    /// there.
    ///
    /// We exercise both sides of the helper: the connect-side stream (held
    /// by the client) AND the accept-side stream (returned by `accept`).
    /// Without this, an EC2 ring node whose successor dies will hang for
    /// minutes (REPORT.md 2026-05-26) before the failure detector strikes;
    /// regressing the setsockopt call would silently reintroduce that hang.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[tokio::test]
    async fn ring_socket_opts_sets_tcp_user_timeout_both_sides() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_fut = tokio::spawn(async move { listener.accept().await.unwrap() });

        let client = TcpStream::connect(addr).await.unwrap();
        apply_ring_socket_opts(&client, "connect");
        let (server, _peer) = accept_fut.await.unwrap();
        apply_ring_socket_opts(&server, "accept");

        for (stream, label) in [(&client, "connect"), (&server, "accept")] {
            let got = socket2::SockRef::from(stream)
                .tcp_user_timeout()
                .expect("tcp_user_timeout");
            assert_eq!(
                got,
                Some(RING_TCP_USER_TIMEOUT),
                "{label} side: expected TCP_USER_TIMEOUT = {RING_TCP_USER_TIMEOUT:?}, got {got:?}",
            );
        }
    }

    /// Two-node "ring": A→B. We send a train through A's outbox and
    /// receive it on B's inbox.  Verifies the codec, TLS handshake,
    /// and channel plumbing.
    #[tokio::test]
    async fn one_hop_train_delivery() {
        let _ = tracing_subscriber::fmt::try_init();

        let id_a = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let id_b = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();

        let addr_a = pick_port();
        let addr_b = pick_port();

        // A's pinned peer: B's fingerprint (B is A's successor).
        // A also accepts itself as the predecessor (single-machine test).
        let pinned_for_a = vec![id_a.fingerprint, id_b.fingerprint];
        let pinned_for_b = vec![id_a.fingerprint, id_b.fingerprint];

        let mut t_b = RingTransport::spawn(RingConfig {
            identity: id_b,
            listen_addr:    addr_b,
            successor_addr: addr_a,
            pinned_peer_fingerprints: pinned_for_b,
        }).await.unwrap();

        let t_a = RingTransport::spawn(RingConfig {
            identity: id_a,
            listen_addr:    addr_a,
            successor_addr: addr_b,
            pinned_peer_fingerprints: pinned_for_a,
        }).await.unwrap();

        // Send a train from A to B.
        let train = Train {
            issuer:   0,
            clock:    1,
            payloads: vec![Payload { sender: 0, seq: 0, data: b"ping".to_vec() }],
            ack_bits: 0b001,
        };
        t_a.outbox.send(train.clone()).await.unwrap();

        // Receive it on B.
        let received = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            t_b.inbox.recv(),
        ).await
            .expect("timed out waiting for train")
            .expect("inbox closed");

        assert_eq!(received, train);
    }

    /// PR-RD-8: `abort()` must close inbound accepted-connection tasks,
    /// not just the listener/connector/mux. Pre-PR-RD-8, the per-connection
    /// tasks (one `tokio::spawn` per TLS accept inside `listener_loop`) were
    /// untracked; aborting the listener only stopped new accepts. Existing
    /// connections survived with open `TcpStream`s, so the predecessor's
    /// writer kept pushing into the dead-end kernel buffer for the full TCP
    /// retransmit budget — the in-process flake that surfaced during
    /// PR-RD-7 verification (`crash_masking` taking 394 s vs 3.9 s).
    ///
    /// We test the mechanism, not the timing-sensitive end-to-end: after
    /// `abort()`, every tracked connection-task `JoinHandle::is_finished()`
    /// must report true within a tight budget (50 ms).
    #[tokio::test]
    async fn abort_propagates_to_accepted_connection_tasks() {
        let _ = tracing_subscriber::fmt::try_init();

        let id_a = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let id_b = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let addr_a = pick_port();
        let addr_b = pick_port();
        let fp = vec![id_a.fingerprint, id_b.fingerprint];

        let mut t_b = RingTransport::spawn(RingConfig {
            identity: id_b, listen_addr: addr_b, successor_addr: addr_a,
            pinned_peer_fingerprints: fp.clone(),
        }).await.unwrap();
        let t_a = RingTransport::spawn(RingConfig {
            identity: id_a, listen_addr: addr_a, successor_addr: addr_b,
            pinned_peer_fingerprints: fp,
        }).await.unwrap();

        // Drive at least one train through so the ring is fully connected
        // (TLS handshake on the accept side has happened on both nodes).
        let train = Train {
            issuer:   0,
            clock:    1,
            payloads: vec![Payload { sender: 0, seq: 0, data: b"ping".to_vec() }],
            ack_bits: 0b001,
        };
        t_a.outbox.send(train.clone()).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), t_b.inbox.recv())
            .await.expect("ring never delivered the train");

        // B must have accepted A's inbound connection (the train was delivered
        // via that link). A's accept side may not have anything to track yet —
        // B's connector to A is best-effort; this test focuses on B's
        // accepted-side cleanup. Give the post-accept push up to 100 ms.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
        while t_b.connection_task_count() == 0 {
            if tokio::time::Instant::now() >= deadline {
                panic!("B never tracked the inbound accept from A");
            }
            tokio::task::yield_now().await;
        }

        // Abort B and assert its accepted-connection tasks are all finished
        // within a tight budget. Tokio aborts are not synchronous — they
        // unwind on the next await point — so a short yield is required.
        t_b.abort();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
        loop {
            let all_done = {
                let tasks = t_b.conn_tasks.lock().unwrap();
                tasks.iter().all(|h| h.is_finished())
            };
            if all_done {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                let tasks = t_b.conn_tasks.lock().unwrap();
                let alive = tasks.iter().filter(|h| !h.is_finished()).count();
                panic!(
                    "after abort(): {alive} of {} accepted-connection tasks still alive",
                    tasks.len(),
                );
            }
            tokio::task::yield_now().await;
        }
    }

    /// PR-R3: a view-change frame travels the same ring link as trains and
    /// is demultiplexed onto the receiver's `vc_inbox` (not `inbox`).
    #[tokio::test]
    async fn one_hop_view_change_delivery() {
        let _ = tracing_subscriber::fmt::try_init();

        let id_a = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let id_b = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let addr_a = pick_port();
        let addr_b = pick_port();
        let fp = vec![id_a.fingerprint, id_b.fingerprint];

        let mut t_b = RingTransport::spawn(RingConfig {
            identity: id_b, listen_addr: addr_b, successor_addr: addr_a,
            pinned_peer_fingerprints: fp.clone(),
        }).await.unwrap();
        let t_a = RingTransport::spawn(RingConfig {
            identity: id_a, listen_addr: addr_a, successor_addr: addr_b,
            pinned_peer_fingerprints: fp,
        }).await.unwrap();

        let vc = ViewChangeMsg::Gather { view_id: 1, coordinator: 0, victim: 2, reports: vec![] };
        t_a.vc_outbox.send(vc.clone()).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(5), t_b.vc_inbox.recv())
            .await.expect("timed out waiting for view-change frame")
            .expect("vc_inbox closed");
        assert_eq!(received, vc, "view-change frame should arrive on vc_inbox");
    }

    /// Gap C: retargeting the successor redirects forwarded trains to a
    /// new node (the ring-reconfiguration primitive). A→B initially;
    /// after `retarget_successor(C)`, trains go to C, not B.
    #[tokio::test]
    async fn retarget_redirects_successor() {
        let _ = tracing_subscriber::fmt::try_init();

        let id_a = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let id_b = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let id_c = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let fp_all = vec![id_a.fingerprint, id_b.fingerprint, id_c.fingerprint];
        let addr_a = pick_port();
        let addr_b = pick_port();
        let addr_c = pick_port();

        let mut t_b = RingTransport::spawn(RingConfig {
            identity: id_b, listen_addr: addr_b, successor_addr: addr_a,
            pinned_peer_fingerprints: fp_all.clone(),
        }).await.unwrap();
        let mut t_c = RingTransport::spawn(RingConfig {
            identity: id_c, listen_addr: addr_c, successor_addr: addr_a,
            pinned_peer_fingerprints: fp_all.clone(),
        }).await.unwrap();
        let t_a = RingTransport::spawn(RingConfig {
            identity: id_a, listen_addr: addr_a, successor_addr: addr_b,
            pinned_peer_fingerprints: fp_all.clone(),
        }).await.unwrap();

        // First train → B (A's initial successor).
        let t1 = Train { issuer: 0, clock: 1,
            payloads: vec![Payload { sender: 0, seq: 0, data: b"to-b".to_vec() }],
            ack_bits: 0b001 };
        t_a.outbox.send(t1.clone()).await.unwrap();
        let got_b = tokio::time::timeout(Duration::from_secs(5), t_b.inbox.recv())
            .await.expect("timeout waiting on B").expect("B inbox closed");
        assert_eq!(got_b, t1, "first train should reach B");

        // Reconfigure: A's successor is now C.
        t_a.retarget_successor(addr_c).await;
        tokio::time::sleep(Duration::from_millis(400)).await; // reconnect

        // Next train → C (not B).
        let t2 = Train { issuer: 0, clock: 2,
            payloads: vec![Payload { sender: 0, seq: 1, data: b"to-c".to_vec() }],
            ack_bits: 0b001 };
        t_a.outbox.send(t2.clone()).await.unwrap();
        let got_c = tokio::time::timeout(Duration::from_secs(5), t_c.inbox.recv())
            .await.expect("timeout waiting on C").expect("C inbox closed");
        assert_eq!(got_c, t2, "after retarget, train should reach C");
    }

    /// PR-R4: a successor that never accepts connections is reported on
    /// `unreachable_rx` after repeated failures — the clean-crash detection
    /// signal (no clock gap to reveal it).
    #[tokio::test]
    async fn unreachable_successor_is_reported() {
        let _ = tracing_subscriber::fmt::try_init();
        let id_a = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let fp_a = id_a.fingerprint;
        let addr_a = pick_port();
        let dead = pick_port(); // nothing ever listens here

        let mut t_a = RingTransport::spawn(RingConfig {
            identity: id_a,
            listen_addr: addr_a,
            successor_addr: dead,
            pinned_peer_fingerprints: vec![fp_a],
        }).await.unwrap();

        // Give the connector something to send so it tries (and keeps failing)
        // to reach the dead successor.
        let t = Train { issuer: 0, clock: 1, payloads: vec![], ack_bits: 0b001 };
        t_a.outbox.send(t).await.unwrap();

        // UNREACHABLE_FAILURES attempts over exponential backoff (~7.5s).
        let got = tokio::time::timeout(Duration::from_secs(25), t_a.unreachable_rx.recv())
            .await
            .expect("timed out waiting for unreachable report")
            .expect("unreachable channel closed");
        assert_eq!(got, dead, "the dead successor's address is reported");
    }

    /// PR-RD-9: the E1 EC2 hang regression. `unreachable_rx` must fire even
    /// when the connector has **no pending writes** at the moment the peer
    /// dies — the "coupled failure" mode where upstream traffic has stopped
    /// (chaos client blocked) so the connector idles.
    ///
    /// Setup: two-node ring A→B with reconfiguration *disabled* (so B's
    /// connector to a third node is irrelevant — only A's connector to B
    /// matters). Send ONE train to establish the connection. Wait briefly
    /// so the connection settles. Abort B (PR-RD-8 closes B's accepted-
    /// connection task, which drops the TLS stream; A's reader observes
    /// EOF). Assert A's `unreachable_rx` fires.
    ///
    /// Pre-PR-RD-9, A's connector idled in `select!{wire_rx, retarget_rx}`
    /// after the first message went through, observing the close only on a
    /// subsequent write. The test would time out within the 25 s budget
    /// because no further writes happen. Post-PR-RD-9, the reader task
    /// observes the close immediately and signals the connector, which
    /// breaks to the reconnect path and (after `UNREACHABLE_FAILURES`
    /// reconnects fail) fires `unreachable_rx`.
    #[tokio::test]
    async fn unreachable_fires_when_peer_dies_with_no_pending_writes() {
        let _ = tracing_subscriber::fmt::try_init();

        let id_a = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let id_b = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let fp_a = id_a.fingerprint;
        let fp_b = id_b.fingerprint;
        let addr_a = pick_port();
        let addr_b = pick_port();

        // B accepts from A; B's own connector points back at A (so the ring
        // closes), but A is the only one we'll measure.
        let t_b = RingTransport::spawn(RingConfig {
            identity: id_b,
            listen_addr:    addr_b,
            successor_addr: addr_a,
            pinned_peer_fingerprints: vec![fp_a, fp_b],
        }).await.unwrap();
        let mut t_a = RingTransport::spawn(RingConfig {
            identity: id_a,
            listen_addr:    addr_a,
            successor_addr: addr_b,
            pinned_peer_fingerprints: vec![fp_a, fp_b],
        }).await.unwrap();

        // Drive ONE train so A's connector establishes its outbound TLS to B.
        let train = Train {
            issuer: 0, clock: 1,
            payloads: vec![Payload { sender: 0, seq: 0, data: b"warmup".to_vec() }],
            ack_bits: 0b001,
        };
        t_a.outbox.send(train).await.unwrap();
        // Give the connection time to fully establish + the reader task to
        // start (~200ms is comfortable; the test budget below is the real
        // assertion).
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Kill B — PR-RD-8 ensures the accepted-connection task that holds
        // A's inbound side is aborted, so the TLS stream is dropped and FIN
        // travels to A. NOTHING else writes to A's outbox after this — the
        // assertion below tests that A still observes B's death.
        t_b.abort();

        // Pre-PR-RD-9: this times out at 25 s (the connector idles forever).
        // Post-PR-RD-9: the reader signals immediately; reconnect fails 5
        // times over ~12.5 s backoff; unreachable fires. Total expected
        // ≤ ~15 s; we allow 25 s as before for headroom.
        let got = tokio::time::timeout(
            Duration::from_secs(25),
            t_a.unreachable_rx.recv(),
        )
            .await
            .expect("timed out: connector did not observe peer death with no pending writes")
            .expect("unreachable channel closed");
        assert_eq!(got, addr_b, "the dead peer's address is reported");
    }

    /// A peer with the WRONG fingerprint should be rejected.
    #[tokio::test]
    async fn wrong_fingerprint_rejected() {
        let _ = tracing_subscriber::fmt::try_init();

        let id_a = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let id_b = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let id_imposter = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let fp_a = id_a.fingerprint;
        let fp_b = id_b.fingerprint;
        let addr_a = pick_port();
        let addr_b = pick_port();

        // B pins ONLY id_a's fingerprint, but the connector will use id_imposter.
        let _t_b = RingTransport::spawn(RingConfig {
            identity: id_b,
            listen_addr:    addr_b,
            successor_addr: addr_a,
            pinned_peer_fingerprints: vec![fp_a],
        }).await.unwrap();

        let t_imposter = RingTransport::spawn(RingConfig {
            identity: id_imposter,
            listen_addr:    addr_a,
            successor_addr: addr_b,
            pinned_peer_fingerprints: vec![fp_b],
        }).await.unwrap();

        // Push a train; B's verifier should reject the imposter's cert,
        // so the train should never arrive on B's inbox.
        let train = Train {
            issuer: 0, clock: 1, payloads: vec![], ack_bits: 0b001,
        };
        t_imposter.outbox.send(train).await.unwrap();

        // Wait briefly; expect timeout.
        let r = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            async {
                let mut rx = _t_b.inbox;
                rx.recv().await
            },
        ).await;
        assert!(r.is_err(), "imposter train should not arrive (got {:?})", r);
    }
}
