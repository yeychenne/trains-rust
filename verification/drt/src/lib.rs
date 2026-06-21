//! Differential random testing harness — `trains-core` vs `trains-reference`.
//!
//! On every step, the same `Input` is fed to both implementations and
//! their outputs are compared after normalisation. Any divergence is a
//! bug — either in the production impl or in the reference.
//!
//! Normalisation: outputs are sorted by a stable key so that any
//! incidental output ordering doesn't trigger false-positive
//! divergences.

use trains_core::{Output, Payload, ProcId, Train};

/// A normalised view of a step's outputs, ordered for comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepOutputs {
    pub forwarded:    Vec<Train>,
    pub delivered:    Vec<Vec<Payload>>,
    pub crash_decls:  Vec<ProcId>,
}

impl StepOutputs {
    pub fn from(outs: Vec<Output>) -> Self {
        let mut forwarded   = Vec::new();
        let mut delivered   = Vec::new();
        let mut crash_decls = Vec::new();
        for o in outs {
            match o {
                Output::ForwardTrain(t)  => forwarded.push(t),
                Output::Deliver(p)       => delivered.push(p),
                Output::DeclareCrash(v)  => crash_decls.push(v),
            }
        }
        // Sort each bucket independently so order-of-emit doesn't matter.
        forwarded.sort_by(|a, b| {
            (a.issuer, a.clock, a.ack_bits).cmp(&(b.issuer, b.clock, b.ack_bits))
                .then_with(|| a.payloads.cmp(&b.payloads))
        });
        delivered.sort();
        crash_decls.sort();
        Self { forwarded, delivered, crash_decls }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use trains_core::{DeliveryMode, Input, RING_SIZE, Payload as CorePayload};

    /// Symbolic input event for one node.
    #[derive(Debug, Clone)]
    enum Ev {
        Broadcast(Vec<u8>),
        Receive(Train),
        Tick,
    }

    fn run_drt(node_id: ProcId, events: Vec<Ev>) -> Result<(), String> {
        let mut prod = trains_core::TrainsNode::new(node_id, DeliveryMode::UniformTotalOrder);
        let mut refn = trains_reference::ReferenceNode::new(node_id, DeliveryMode::UniformTotalOrder);

        // Issue initial trains for both impls if this node is an issuer.
        if (node_id as usize) < trains_core::NUM_TRAINS {
            let _ = prod.issue_initial_train();
            let _ = refn.issue_initial_train();
        }

        for (idx, ev) in events.iter().enumerate() {
            let input = match ev {
                Ev::Broadcast(d) => Input::LocalBroadcast(d.clone()),
                Ev::Receive(t)   => Input::TrainReceived(t.clone()),
                Ev::Tick         => Input::Tick,
            };
            let prod_outs = prod.step(input.clone());
            let refn_outs = refn.step(input);

            let p = StepOutputs::from(prod_outs);
            let r = StepOutputs::from(refn_outs);
            if p != r {
                return Err(format!(
                    "divergence at event #{idx} ({:?}):\n  PROD: {:#?}\n  REF:  {:#?}",
                    ev, p, r
                ));
            }
        }
        Ok(())
    }

    fn arb_payload(sender: ProcId) -> impl Strategy<Value = Payload> {
        (0u64..3, prop::collection::vec(any::<u8>(), 0..3))
            .prop_map(move |(seq, data)| Payload { sender, seq, data })
    }

    fn arb_train(node_id: ProcId) -> impl Strategy<Value = Train> {
        (
            (0u8..RING_SIZE as u8).prop_filter("not self", move |&i| i != node_id),
            1u64..5,
            // 0..3 payloads
            (0u8..3u8).prop_flat_map(|sender|
                prop::collection::vec(arb_payload(sender), 0..2)
            ),
            any::<u8>(),
        ).prop_map(|(issuer, clock, payloads, ack_bits)|
            Train { issuer, clock, payloads, ack_bits: ack_bits.into() })
    }

    fn arb_ev(node_id: ProcId) -> impl Strategy<Value = Ev> {
        prop_oneof![
            2 => prop::collection::vec(any::<u8>(), 0..3).prop_map(Ev::Broadcast),
            5 => arb_train(node_id).prop_map(Ev::Receive),
            1 => Just(Ev::Tick),
        ]
    }

    fn arb_schedule(node_id: ProcId) -> impl Strategy<Value = Vec<Ev>> {
        prop::collection::vec(arb_ev(node_id), 1..15)
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 4096,
            ..ProptestConfig::default()
        })]

        #[test]
        fn drt_node0(events in arb_schedule(0)) {
            run_drt(0, events).map_err(TestCaseError::fail)?;
        }

        #[test]
        fn drt_node1(events in arb_schedule(1)) {
            run_drt(1, events).map_err(TestCaseError::fail)?;
        }

        #[test]
        fn drt_node2(events in arb_schedule(2)) {
            run_drt(2, events).map_err(TestCaseError::fail)?;
        }
    }

    /// Sanity test: identical empty schedules produce identical outputs.
    #[test]
    fn empty_schedule_matches() {
        run_drt(0, vec![]).unwrap();
        run_drt(1, vec![]).unwrap();
        run_drt(2, vec![]).unwrap();
    }

    /// Sanity test: a broadcast → no deliveries from either impl.
    #[test]
    fn broadcast_only_matches() {
        run_drt(0, vec![Ev::Broadcast(b"x".to_vec())]).unwrap();
    }

    /// Sanity test: a fully-acked foreign train hits both impls' lap-1
    /// path (no delivery on first sight, both forward).
    #[test]
    fn foreign_train_first_arrival_matches() {
        let train = Train {
            issuer: 1, clock: 1,
            payloads: vec![CorePayload { sender: 1, seq: 0, data: b"hi".to_vec() }],
            ack_bits: 0b111,
        };
        run_drt(0, vec![Ev::Receive(train)]).unwrap();
    }
}
