//! QUIC ring transport — AWS s2n-quic alternative to `transport.rs`.
//!
//! Exposes [`QuicRingTransport`] with the same inbox/outbox mpsc interface
//! as [`crate::transport::RingTransport`], but uses QUIC over UDP instead
//! of TLS over TCP. Advantages for the ring:
//!
//!   - UDP-based: sidesteps the macOS TCP-on-Wi-Fi throughput pathology
//!   - No TCP head-of-line blocking under packet loss
//!   - Connection migration: survives Tailscale path changes transparently
//!
//! TLS is provided by the same rustls stack as the TCP transport, reusing
//! [`crate::tls::PinnedFingerprintVerifier`] unchanged. The wire framing
//! (length-prefixed bincode) is identical; [`crate::codec::read_train`] and
//! [`crate::codec::write_train`] work directly over QUIC streams because
//! s2n-quic's `ReceiveStream`/`SendStream` implement `tokio::io::AsyncRead`
//! and `tokio::io::AsyncWrite`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use s2n_quic::{client::Connect, Client, Server};
use s2n_quic::provider::tls::rustls as quic_tls;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use trains_core::Train;

use crate::codec::{read_train, write_train, CodecError};
use crate::tls::PinnedFingerprintVerifier;
use crate::transport::RingConfig;

const RECONNECT_BACKOFF: Duration = Duration::from_millis(500);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(5);
const CHANNEL_CAP: usize = 64;

/// ALPN protocol label — both ends must agree or the handshake fails.
const TRAINS_ALPN: &[u8] = b"trains-1";

#[derive(Debug, thiserror::Error)]
pub enum QuicTransportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec: {0}")]
    Codec(#[from] CodecError),
    #[error("rustls: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("tls config: {0}")]
    TlsConfig(String),
    #[error("quic: {0}")]
    Quic(String),
}

/// QUIC-backed ring transport.
///
/// Drop-in replacement for [`crate::transport::RingTransport`] using the
/// same [`RingConfig`]: same identity, same pinned fingerprints, same
/// listen/successor addresses. The kernel-layer interface (`inbox`, `outbox`)
/// is identical.
pub struct QuicRingTransport {
    /// Trains arriving from the predecessor.
    pub inbox: mpsc::Receiver<Train>,
    /// Trains to forward to the successor.
    pub outbox: mpsc::Sender<Train>,
    _listener_task: JoinHandle<()>,
    _connector_task: JoinHandle<()>,
}

impl QuicRingTransport {
    /// Spawn listener + connector tasks and return the transport handle.
    pub async fn spawn(cfg: RingConfig) -> Result<Self, QuicTransportError> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let pinned = Arc::new(cfg.pinned_peer_fingerprints);

        // ── Inbound: QUIC server ──────────────────────────────────────────
        let (in_tx, in_rx) = mpsc::channel::<Train>(CHANNEL_CAP);

        let mut server_rustls = rustls::ServerConfig::builder()
            .with_client_cert_verifier(Arc::new(PinnedFingerprintVerifier::new(
                (*pinned).clone(),
            )))
            .with_single_cert(cfg.identity.cert_chain.clone(), cfg.identity.key.clone_key())
            .map_err(|e| QuicTransportError::TlsConfig(e.to_string()))?;
        server_rustls.alpn_protocols = vec![TRAINS_ALPN.to_vec()];

        let server_tls = quic_tls::Server::from(Arc::new(server_rustls));
        let server = Server::builder()
            .with_tls(server_tls)
            .map_err(|e| QuicTransportError::Quic(e.to_string()))?
            .with_io(cfg.listen_addr)
            .map_err(|e| QuicTransportError::Quic(e.to_string()))?
            .start()
            .map_err(|e| QuicTransportError::Quic(e.to_string()))?;

        let listener_task = tokio::spawn(listener_loop(server, in_tx));

        // ── Outbound: QUIC client ─────────────────────────────────────────
        let (out_tx, out_rx) = mpsc::channel::<Train>(CHANNEL_CAP);

        let mut client_rustls = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedFingerprintVerifier::new(
                (*pinned).clone(),
            )))
            .with_client_auth_cert(cfg.identity.cert_chain.clone(), cfg.identity.key.clone_key())
            .map_err(|e| QuicTransportError::TlsConfig(e.to_string()))?;
        client_rustls.alpn_protocols = vec![TRAINS_ALPN.to_vec()];

        let client_tls = quic_tls::Client::from(Arc::new(client_rustls));
        let client = Client::builder()
            .with_tls(client_tls)
            .map_err(|e| QuicTransportError::Quic(e.to_string()))?
            .with_io("0.0.0.0:0")
            .map_err(|e| QuicTransportError::Quic(e.to_string()))?
            .start()
            .map_err(|e| QuicTransportError::Quic(e.to_string()))?;

        let connector_task = tokio::spawn(connector_loop(client, cfg.successor_addr, out_rx));

        Ok(Self {
            inbox: in_rx,
            outbox: out_tx,
            _listener_task: listener_task,
            _connector_task: connector_task,
        })
    }
}

async fn listener_loop(mut server: Server, in_tx: mpsc::Sender<Train>) {
    loop {
        let mut connection = match server.accept().await {
            Some(conn) => conn,
            None => {
                tracing::info!("QUIC server shut down");
                return;
            }
        };
        let in_tx = in_tx.clone();
        tokio::spawn(async move {
            tracing::info!("predecessor connected via QUIC");
            // Accept the single receive stream the predecessor opens.
            match connection.accept_receive_stream().await {
                Ok(Some(mut stream)) => loop {
                    match read_train(&mut stream).await {
                        Ok(train) => {
                            if in_tx.send(train).await.is_err() {
                                tracing::warn!("inbox closed; dropping QUIC connection");
                                return;
                            }
                        }
                        Err(CodecError::Io(e))
                            if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                        {
                            tracing::info!("predecessor stream finished");
                            return;
                        }
                        Err(e) => {
                            tracing::warn!(error=%e, "decode error; closing stream");
                            return;
                        }
                    }
                },
                Ok(None) => tracing::info!("predecessor closed connection with no stream"),
                Err(e) => tracing::warn!(error=%e, "stream accept error"),
            }
        });
    }
}

async fn connector_loop(
    client: Client,
    addr: SocketAddr,
    mut out_rx: mpsc::Receiver<Train>,
) {
    let mut backoff = RECONNECT_BACKOFF;
    // Retain a train across connect/stream-open/write retries — identical
    // reasoning as in `transport::connector_loop`: the issuer never re-issues
    // a dropped slot, so losing the first train would deadlock the ring.
    let mut pending: Option<Train> = None;

    loop {
        let train = match pending.take() {
            Some(t) => t,
            None => match out_rx.recv().await {
                Some(t) => t,
                None => {
                    tracing::info!("outbox closed; shutting down QUIC connector");
                    return;
                }
            },
        };

        // Establish QUIC connection. The server name is passed to the TLS SNI
        // extension; our PinnedFingerprintVerifier ignores it, but a non-empty
        // name is required by the protocol.
        let connect = Connect::new(addr).with_server_name("trains");
        let mut connection = match client.connect(connect).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(addr=%addr, error=%e,
                    clock=train.clock, issuer=train.issuer,
                    "QUIC connect failed; will retry");
                pending = Some(train);
                sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
                continue;
            }
        };
        backoff = RECONNECT_BACKOFF;

        // Open a unidirectional send stream; the server accepts a matching
        // receive stream via accept_receive_stream().
        let mut stream = match connection.open_send_stream().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error=%e,
                    clock=train.clock, issuer=train.issuer,
                    "open_send_stream failed; will retry");
                pending = Some(train);
                sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
                continue;
            }
        };

        // Write the pending train.
        if let Err(e) = write_train(&mut stream, &train).await {
            tracing::warn!(error=%e,
                clock=train.clock, issuer=train.issuer,
                "write failed; reconnecting");
            pending = Some(train);
            continue;
        }

        // Stream subsequent trains until the connection fails.
        loop {
            match out_rx.recv().await {
                Some(t) => {
                    if let Err(e) = write_train(&mut stream, &t).await {
                        tracing::warn!(error=%e,
                            clock=t.clock, issuer=t.issuer,
                            "write failed; reconnecting");
                        pending = Some(t);
                        break;
                    }
                }
                None => {
                    tracing::info!("outbox closed; shutting down QUIC connector");
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::NodeIdentity;
    use trains_core::{Payload, Train};

    fn pick_port() -> SocketAddr {
        // Bind UDP port 0 to get a free ephemeral port for QUIC.
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = s.local_addr().unwrap();
        drop(s);
        addr
    }

    /// Two-node "ring" over QUIC: A→B. Send a train through A's outbox,
    /// receive it on B's inbox. Verifies codec, QUIC handshake, and
    /// fingerprint pinning in the happy path.
    #[tokio::test]
    async fn quic_one_hop_train_delivery() {
        let _ = tracing_subscriber::fmt::try_init();

        let id_a = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let id_b = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let addr_a = pick_port();
        let addr_b = pick_port();
        let pinned = vec![id_a.fingerprint, id_b.fingerprint];

        let mut t_b = QuicRingTransport::spawn(RingConfig {
            identity: id_b,
            listen_addr: addr_b,
            successor_addr: addr_a,
            pinned_peer_fingerprints: pinned.clone(),
        })
        .await
        .unwrap();

        let t_a = QuicRingTransport::spawn(RingConfig {
            identity: id_a,
            listen_addr: addr_a,
            successor_addr: addr_b,
            pinned_peer_fingerprints: pinned,
        })
        .await
        .unwrap();

        let train = Train {
            issuer: 0,
            clock: 1,
            payloads: vec![Payload { sender: 0, seq: 0, data: b"ping-quic".to_vec() }],
            ack_bits: 0b001,
        };
        t_a.outbox.send(train.clone()).await.unwrap();

        let received = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            t_b.inbox.recv(),
        )
        .await
        .expect("timed out waiting for QUIC train")
        .expect("inbox closed");

        assert_eq!(received, train);
    }

    /// A peer with the wrong fingerprint must be rejected before any
    /// train reaches the inbox.
    #[tokio::test]
    async fn quic_wrong_fingerprint_rejected() {
        let _ = tracing_subscriber::fmt::try_init();

        let id_a = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let id_b = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let id_imposter = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let fp_a = id_a.fingerprint;
        let fp_b = id_b.fingerprint;
        let addr_a = pick_port();
        let addr_b = pick_port();

        let mut _t_b = QuicRingTransport::spawn(RingConfig {
            identity: id_b,
            listen_addr: addr_b,
            successor_addr: addr_a,
            pinned_peer_fingerprints: vec![fp_a], // pins only A; imposter not in set
        })
        .await
        .unwrap();

        let t_imposter = QuicRingTransport::spawn(RingConfig {
            identity: id_imposter,
            listen_addr: addr_a,
            successor_addr: addr_b,
            pinned_peer_fingerprints: vec![fp_b],
        })
        .await
        .unwrap();

        let train = Train { issuer: 0, clock: 1, payloads: vec![], ack_bits: 0b001 };
        t_imposter.outbox.send(train).await.unwrap();

        let r = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            async { _t_b.inbox.recv().await },
        )
        .await;
        assert!(r.is_err(), "imposter train should not arrive via QUIC (got {:?})", r);
    }
}
