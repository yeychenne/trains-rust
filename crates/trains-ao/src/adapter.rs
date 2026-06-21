//! AO adapter wrapping a [`trains_core::TrainsNode`].
//!
//! The host calls [`AoTrainsNode::feed`] with each inbox envelope and
//! gets back zero or more outbox envelopes.  No async, no I/O, no
//! channels — the adapter is a pure function of the inbox event stream
//! so it composes cleanly with whatever runtime the AO host uses.

use trains_core::{DeliveryMode, Input, Output, ProcId, Train, TrainsNode};

use crate::envelope::{Envelope, EnvelopeKind};

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("envelope kind {0:?} cannot be used as inbox input")]
    NotAnInputKind(EnvelopeKind),
    #[error("envelope payload missing or malformed for kind {0:?}")]
    BadPayload(EnvelopeKind),
}

/// AO node that wraps a `TrainsNode` behind the envelope contract.
pub struct AoTrainsNode {
    inner: TrainsNode,
    id:    ProcId,
}

impl AoTrainsNode {
    pub fn new(id: ProcId, mode: DeliveryMode) -> Self {
        Self { inner: TrainsNode::new(id, mode), id }
    }

    pub fn id(&self) -> ProcId { self.id }

    /// Have this node issue its initial train, returning the
    /// corresponding outbox envelope (which the AO host should hand to
    /// the transport).
    pub fn issue_initial(&mut self) -> Envelope {
        let t = self.inner.issue_initial_train();
        Envelope::forward_train(&t)
    }

    /// Feed one inbox envelope and collect outbox envelopes.
    pub fn feed(&mut self, env: &Envelope) -> Result<Vec<Envelope>, AdapterError> {
        let input = match env.kind {
            EnvelopeKind::AppBroadcast => {
                let bytes = env.as_app_broadcast_bytes()
                    .ok_or(AdapterError::BadPayload(env.kind))?;
                Input::LocalBroadcast(bytes)
            }
            EnvelopeKind::TrainArrived => {
                let train: Train = env.as_train()
                    .ok_or(AdapterError::BadPayload(env.kind))?;
                Input::TrainReceived(train)
            }
            EnvelopeKind::Tick => Input::Tick,
            other => return Err(AdapterError::NotAnInputKind(other)),
        };

        let outs = self.inner.step(input);
        Ok(outs.into_iter().map(|o| match o {
            Output::ForwardTrain(t)  => Envelope::forward_train(&t),
            Output::Deliver(ps)      => Envelope::deliver(&ps),
            Output::DeclareCrash(v)  => Envelope::declare_crash(v),
        }).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trains_core::Payload;

    #[test]
    fn broadcast_envelope_queues_no_outputs() {
        let mut n = AoTrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        let env = Envelope::app_broadcast(b"hi".to_vec());
        let outs = n.feed(&env).unwrap();
        assert!(outs.is_empty());
    }

    #[test]
    fn issue_initial_yields_forward_train() {
        let mut n = AoTrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        let env = n.issue_initial();
        assert_eq!(env.kind, EnvelopeKind::ForwardTrain);
    }

    #[test]
    fn train_arrival_round_trips_through_envelope() {
        // After Phase A's TLC fix, delivery requires every issuer's
        // clock to have caught up. Issue self's train + an echo to
        // satisfy that gate, then feed lap-1 + lap-2 of a foreign train.
        let mut n = AoTrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        let _ = n.issue_initial();                            // self issued at clock=1
        let self_echo = Train {
            issuer: 0, clock: 1, payloads: vec![],
            ack_bits: trains_core::FULL_ACK,
        };
        let _ = n.feed(&Envelope::train_arrived(&self_echo)).unwrap(); // primes seenClk[0]

        let t = Train {
            issuer: 1, clock: 1,
            payloads: vec![Payload { sender: 1, seq: 0, data: b"x".to_vec() }],
            ack_bits: trains_core::FULL_ACK,
        };
        let _ = n.feed(&Envelope::train_arrived(&t)).unwrap();        // lap-1
        let outs = n.feed(&Envelope::train_arrived(&t)).unwrap();      // lap-2 → deliver
        let kinds: Vec<EnvelopeKind> = outs.iter().map(|e| e.kind).collect();
        assert!(kinds.contains(&EnvelopeKind::Deliver), "got: {:?}", kinds);
        assert!(kinds.contains(&EnvelopeKind::ForwardTrain));
    }

    #[test]
    fn unsupported_kind_errors() {
        let mut n = AoTrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        let env = Envelope::deliver(&[]);  // outbox-only kind
        let r = n.feed(&env);
        assert!(matches!(r, Err(AdapterError::NotAnInputKind(_))));
    }
}
