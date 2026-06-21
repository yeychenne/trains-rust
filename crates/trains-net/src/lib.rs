//! trains-net — TLS ring transport for the TRAINS protocol.
//!
//! Pure async I/O on top of [`trains_core`].  Each node has exactly
//! one inbound TLS connection (from predecessor) and one outbound
//! (to successor).  Peer authentication uses **SPKI fingerprint
//! pinning**: every node's CLI receives, out-of-band, the SHA-256
//! fingerprints of its peers' self-signed certs.

pub mod codec;
pub mod quic_transport;
pub mod snapshot;
pub mod tls;
pub mod transport;
pub mod wire;

pub use codec::{
    encode_msg, encode_train, read_msg, read_train, write_msg, write_train, CodecError,
    MAX_FRAME_LEN,
};
pub use quic_transport::{QuicRingTransport, QuicTransportError};
pub use snapshot::{
    fetch_snapshot, fetch_state, serve_snapshots, SnapshotError, SnapshotRequest, StateTransfer,
    MAX_SNAPSHOT_LEN, MAX_TAIL_FRAMES, MAX_TAIL_FRAME_LEN,
};
pub use tls::{NodeIdentity, PinnedFingerprintVerifier, SpkiFingerprint, TlsError};
pub use transport::{RingConfig, RingTransport, TransportError};
pub use wire::{ViewChangeMsg, WireMsg};
