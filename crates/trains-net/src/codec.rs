//! Length-prefixed bincode framing for TRAINS wire messages.
//!
//! Frame layout: `[u32 BE length][bincode-encoded Train]`.
//! The 4-byte length lets the receiver allocate exactly once per frame.

use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use trains_core::Train;

use crate::wire::WireMsg;

// MAX_FRAME_LEN is emitted by build.rs from the TRAINS_MAX_FRAME_LEN_MB
// env var (default 16 MiB). Compile-time tunable per deployment so that
// big-NIC instances (c7gn.8xlarge @ 100 Gbps, c7i.16xlarge @ 25 Gbps)
// can absorb bursty workloads without hitting the protocol cap that
// makes sense on t4g.medium (~5 Gbps NIC). See `build.rs` for the
// operator-facing sizing rule of thumb.
include!(concat!(env!("OUT_DIR"), "/frame_config.rs"));

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("bincode encode: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("bincode decode: {0}")]
    Decode(#[from] bincode::error::DecodeError),
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(u32),
}

/// Encode a `Train` to a length-prefixed frame.
pub fn encode_train(train: &Train) -> Result<Vec<u8>, CodecError> {
    let body = bincode::serde::encode_to_vec(train, bincode::config::standard())?;
    let len: u32 = body.len().try_into().map_err(|_| {
        CodecError::FrameTooLarge(u32::MAX)
    })?;
    if len > MAX_FRAME_LEN {
        return Err(CodecError::FrameTooLarge(len));
    }
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Read one length-prefixed frame and decode it as a `Train`.
pub async fn read_train<R>(reader: &mut R) -> Result<Train, CodecError>
where
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_LEN {
        return Err(CodecError::FrameTooLarge(len));
    }
    let mut body = vec![0u8; len as usize];
    reader.read_exact(&mut body).await?;
    let (train, _) = bincode::serde::decode_from_slice::<Train, _>(
        &body,
        bincode::config::standard(),
    )?;
    Ok(train)
}

/// Write a `Train` as a length-prefixed frame.
pub async fn write_train<W>(writer: &mut W, train: &Train) -> Result<(), CodecError>
where
    W: AsyncWriteExt + Unpin,
{
    let frame = encode_train(train)?;
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

// ── WireMsg framing (trains + view-change control frames) ────────────────────
//
// Same `[u32 BE length][bincode body]` layout as the train framing; the body
// is a tagged `WireMsg` so the TLS ring can carry reconfiguration frames
// (PR-R3). The QUIC transport stays train-only (`encode_train`/`read_train`).

/// Encode a [`WireMsg`] to a length-prefixed frame.
pub fn encode_msg(msg: &WireMsg) -> Result<Vec<u8>, CodecError> {
    let body = bincode::serde::encode_to_vec(msg, bincode::config::standard())?;
    let len: u32 = body.len().try_into().map_err(|_| CodecError::FrameTooLarge(u32::MAX))?;
    if len > MAX_FRAME_LEN {
        return Err(CodecError::FrameTooLarge(len));
    }
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Read one length-prefixed frame and decode it as a [`WireMsg`].
pub async fn read_msg<R>(reader: &mut R) -> Result<WireMsg, CodecError>
where
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_LEN {
        return Err(CodecError::FrameTooLarge(len));
    }
    let mut body = vec![0u8; len as usize];
    reader.read_exact(&mut body).await?;
    let (msg, _) = bincode::serde::decode_from_slice::<WireMsg, _>(
        &body,
        bincode::config::standard(),
    )?;
    Ok(msg)
}

/// Write a [`WireMsg`] as a length-prefixed frame.
pub async fn write_msg<W>(writer: &mut W, msg: &WireMsg) -> Result<(), CodecError>
where
    W: AsyncWriteExt + Unpin,
{
    let frame = encode_msg(msg)?;
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use trains_core::{Payload, Train};

    fn sample_train() -> Train {
        Train {
            issuer:   1,
            clock:    42,
            payloads: vec![
                Payload { sender: 1, seq: 0, data: b"hello".to_vec() },
                Payload { sender: 2, seq: 7, data: b"world".to_vec() },
            ],
            ack_bits: 0b101,
        }
    }

    #[tokio::test]
    async fn roundtrip() {
        let t = sample_train();
        let bytes = encode_train(&t).unwrap();
        let mut cur = std::io::Cursor::new(bytes);
        let decoded = read_train(&mut cur).await.unwrap();
        assert_eq!(t, decoded);
    }

    #[tokio::test]
    async fn frame_includes_length_prefix() {
        let t = sample_train();
        let bytes = encode_train(&t).unwrap();
        assert!(bytes.len() > 4);
        let stated_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(stated_len as usize, bytes.len() - 4);
    }

    #[tokio::test]
    async fn wire_msg_train_roundtrip() {
        let msg = WireMsg::Train(sample_train());
        let bytes = encode_msg(&msg).unwrap();
        let mut cur = std::io::Cursor::new(bytes);
        let decoded = read_msg(&mut cur).await.unwrap();
        assert_eq!(msg, decoded);
    }

    #[tokio::test]
    async fn wire_msg_view_change_roundtrip() {
        use crate::wire::ViewChangeMsg;
        let msg = WireMsg::ViewChange(ViewChangeMsg::Gather {
            view_id: 7,
            coordinator: 0,
            victim: 2,
            reports: vec![],
        });
        let bytes = encode_msg(&msg).unwrap();
        let mut cur = std::io::Cursor::new(bytes);
        let decoded = read_msg(&mut cur).await.unwrap();
        assert_eq!(msg, decoded);
    }

    // ────────────────────────────────────────────────────────────────────
    // R-05 (T-tr-20b, T-tr-21) — regression: an attacker who frames a
    // length-prefix above MAX_FRAME_LEN must be rejected BEFORE the
    // reader allocates a body buffer for it. This protects against the
    // "send a 4-GiB length prefix and watch the proxy OOM" class of
    // attack on the ring port. The cap itself is compile-time
    // configurable via TRAINS_MAX_FRAME_LEN_MB; this test asserts the
    // gate fires regardless of the configured size.
    // ────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn oversize_train_frame_is_rejected_before_allocation() {
        // Hand-craft a 4-byte header that claims a body of MAX_FRAME_LEN + 1.
        // We feed nothing else — the reader must error on the length check,
        // never reaching the body-read step.
        let oversize = MAX_FRAME_LEN.saturating_add(1);
        let header = oversize.to_be_bytes();
        let mut cur = std::io::Cursor::new(header.to_vec());
        let err = read_train(&mut cur).await
            .expect_err("read_train must reject oversize header");
        match err {
            CodecError::FrameTooLarge(n) => assert_eq!(n, oversize),
            other => panic!("expected FrameTooLarge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn oversize_wire_msg_frame_is_rejected_before_allocation() {
        let oversize = MAX_FRAME_LEN.saturating_add(1);
        let header = oversize.to_be_bytes();
        let mut cur = std::io::Cursor::new(header.to_vec());
        let err = read_msg(&mut cur).await
            .expect_err("read_msg must reject oversize header");
        match err {
            CodecError::FrameTooLarge(n) => assert_eq!(n, oversize),
            other => panic!("expected FrameTooLarge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn exactly_max_frame_len_is_accepted_at_header_check() {
        // The boundary case: a header that claims exactly MAX_FRAME_LEN
        // must pass the header check (the body read will then fail with
        // UnexpectedEof since the cursor has only the 4-byte header, but
        // the error class must NOT be FrameTooLarge).
        let header = MAX_FRAME_LEN.to_be_bytes();
        let mut cur = std::io::Cursor::new(header.to_vec());
        let err = read_msg(&mut cur).await
            .expect_err("body read must still fail because we sent no body");
        match err {
            CodecError::Io(io_err) => {
                assert_eq!(io_err.kind(), io::ErrorKind::UnexpectedEof,
                           "expected UnexpectedEof on body, got {io_err}");
            }
            CodecError::FrameTooLarge(_) =>
                panic!("MAX_FRAME_LEN itself must NOT be rejected by the header check"),
            other => panic!("expected Io(UnexpectedEof), got {other:?}"),
        }
    }
}
