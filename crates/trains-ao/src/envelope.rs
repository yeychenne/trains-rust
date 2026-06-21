//! AO inbox/outbox envelope schema.
//!
//! Envelopes are JSON objects with a `kind` discriminant. The shape
//! mirrors the kinds an AO host already speaks: small, flat, schema-
//! stable so a host can dispatch without parsing the inner blob.

use serde::{Deserialize, Serialize};
use trains_core::{Payload, ProcId, Train};

#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// Tag for the `kind` field on every envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeKind {
    /// Application asks the node to broadcast a payload.
    AppBroadcast,
    /// Transport delivered a train from predecessor.
    TrainArrived,
    /// Watchdog tick.
    Tick,
    /// Node forwards a train to successor.
    ForwardTrain,
    /// Node delivers payloads to the application.
    Deliver,
    /// Node detected a clock-gap crash.
    DeclareCrash,
}

/// An AO envelope. The `data` field holds the kind-specific payload as
/// raw JSON, so the host can serialize/deserialize without knowing the
/// concrete shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub kind: EnvelopeKind,
    /// Source (for inbox) or target (for outbox), if applicable.
    #[serde(default)]
    pub from: Option<ProcId>,
    /// Free-form per-kind body.
    pub data: serde_json::Value,
}

impl Envelope {
    pub fn from_json(s: &str) -> Result<Self, EnvelopeError> {
        Ok(serde_json::from_str(s)?)
    }
    pub fn to_json(&self) -> Result<String, EnvelopeError> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn app_broadcast(data: Vec<u8>) -> Self {
        Self {
            kind: EnvelopeKind::AppBroadcast,
            from: None,
            data: serde_json::json!({ "bytes": data }),
        }
    }

    pub fn train_arrived(t: &Train) -> Self {
        Self {
            kind: EnvelopeKind::TrainArrived,
            from: Some(t.issuer),
            data: serde_json::to_value(t).expect("Train: Serialize"),
        }
    }

    pub fn forward_train(t: &Train) -> Self {
        Self {
            kind: EnvelopeKind::ForwardTrain,
            from: Some(t.issuer),
            data: serde_json::to_value(t).expect("Train: Serialize"),
        }
    }

    pub fn deliver(payloads: &[Payload]) -> Self {
        Self {
            kind: EnvelopeKind::Deliver,
            from: None,
            data: serde_json::to_value(payloads).expect("Payload: Serialize"),
        }
    }

    pub fn declare_crash(victim: ProcId) -> Self {
        Self {
            kind: EnvelopeKind::DeclareCrash,
            from: Some(victim),
            data: serde_json::Value::Null,
        }
    }

    pub fn tick() -> Self {
        Self { kind: EnvelopeKind::Tick, from: None, data: serde_json::Value::Null }
    }

    /// Extract `data.bytes` field for `AppBroadcast` envelopes.
    pub fn as_app_broadcast_bytes(&self) -> Option<Vec<u8>> {
        if !matches!(self.kind, EnvelopeKind::AppBroadcast) {
            return None;
        }
        let arr = self.data.get("bytes")?.as_array()?;
        let mut out = Vec::with_capacity(arr.len());
        for v in arr {
            out.push(v.as_u64()? as u8);
        }
        Some(out)
    }

    /// Extract a `Train` for `TrainArrived` envelopes.
    pub fn as_train(&self) -> Option<Train> {
        if !matches!(self.kind, EnvelopeKind::TrainArrived) {
            return None;
        }
        serde_json::from_value(self.data.clone()).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_broadcast_round_trips() {
        let env = Envelope::app_broadcast(b"hi".to_vec());
        let s = env.to_json().unwrap();
        let parsed = Envelope::from_json(&s).unwrap();
        assert_eq!(parsed.kind, EnvelopeKind::AppBroadcast);
        assert_eq!(parsed.as_app_broadcast_bytes(), Some(b"hi".to_vec()));
    }

    #[test]
    fn train_envelope_round_trips() {
        let t = Train {
            issuer: 1, clock: 5,
            payloads: vec![Payload { sender: 1, seq: 0, data: b"x".to_vec() }],
            ack_bits: 0b011,
        };
        let env = Envelope::train_arrived(&t);
        let s = env.to_json().unwrap();
        let parsed = Envelope::from_json(&s).unwrap();
        assert_eq!(parsed.kind, EnvelopeKind::TrainArrived);
        assert_eq!(parsed.as_train(), Some(t));
    }

    #[test]
    fn unknown_kind_extractors_return_none() {
        let env = Envelope::tick();
        assert!(env.as_train().is_none());
        assert!(env.as_app_broadcast_bytes().is_none());
    }
}
