//! In-memory 3-node ring integration test.
//!
//! Exercises the full TRAINS protocol with NumTrains = 2:
//! issuers 0 and 1 each run a train slot through the ring, payloads
//! are delivered in the same total order at every node, and the
//! ConsistentDelivery invariant from TRAINS.tla holds.

use trains_core::{DeliveryMode, Input, Output, Payload, TrainsNode, Train, RING_SIZE};

const NUM_ISSUERS: u8 = 2;

/// Drive a 3-node ring forward by passing the train from each node to
/// its successor.  Returns the per-node delivery log.
fn run_ring(steps: usize, broadcasts: &[(u8, &[u8])]) -> Vec<Vec<Payload>> {
    let mut nodes: Vec<TrainsNode> = (0..RING_SIZE as u8)
        .map(|id| TrainsNode::new(id, DeliveryMode::UniformTotalOrder))
        .collect();

    // In-flight trains, indexed by the node currently holding each.
    let mut in_flight: Vec<Option<Train>> = vec![None; RING_SIZE];

    // Initial trains: only NUM_ISSUERS nodes issue trains.
    for issuer in 0..NUM_ISSUERS {
        let t = nodes[issuer as usize].issue_initial_train();
        in_flight[issuer as usize] = Some(t);
    }

    // Per-node delivery logs.
    let mut delivered: Vec<Vec<Payload>> = vec![Vec::new(); RING_SIZE];

    // Apply broadcasts in the first step.
    for (node_id, data) in broadcasts {
        nodes[*node_id as usize].step(Input::LocalBroadcast(data.to_vec()));
    }

    // Drive the ring: each step, every holding node passes its train
    // to its successor and processes the train it received.
    for _ in 0..steps {
        let mut next_in_flight: Vec<Option<Train>> = vec![None; RING_SIZE];

        for (holder_idx, slot) in in_flight.iter_mut().enumerate() {
            if let Some(train) = slot.take() {
                let succ = (holder_idx + 1) % RING_SIZE;
                let outs = nodes[succ].step(Input::TrainReceived(train));
                for o in outs {
                    match o {
                        Output::ForwardTrain(t) => {
                            // The successor now holds the train.
                            next_in_flight[succ] = Some(t);
                        }
                        Output::Deliver(payloads) => {
                            delivered[succ].extend(payloads);
                        }
                        Output::DeclareCrash(_) => {}
                    }
                }
            }
        }

        in_flight = next_in_flight;
    }

    delivered
}

/// Returns true iff a is a prefix of b.
fn is_prefix(a: &[Payload], b: &[Payload]) -> bool {
    a.len() <= b.len() && a.iter().zip(b.iter()).all(|(x, y)| x == y)
}

#[test]
fn empty_ring_no_deliveries() {
    let logs = run_ring(20, &[]);
    for log in &logs {
        assert!(log.is_empty(), "no broadcasts → no deliveries");
    }
}

#[test]
fn single_broadcast_delivered_at_all_nodes() {
    let logs = run_ring(30, &[(0, b"hello")]);
    for (i, log) in logs.iter().enumerate() {
        assert!(!log.is_empty(),
            "node {i} should have delivered something, got {} entries", log.len());
        assert!(log.iter().any(|p| p.data == b"hello"),
            "node {i} log: {:?}", log);
    }
}

#[test]
fn consistent_delivery_invariant_holds() {
    // P1/P2 from TRAINS.tla — every two nodes' delivery logs are mutual
    // prefixes.
    let logs = run_ring(
        40,
        &[(0, b"a0"), (1, b"a1"), (2, b"a2"), (0, b"b0"), (1, b"b1")],
    );

    for i in 0..RING_SIZE {
        for j in 0..RING_SIZE {
            assert!(
                is_prefix(&logs[i], &logs[j]) || is_prefix(&logs[j], &logs[i]),
                "ConsistentDelivery violated:\nnode {i}: {:?}\nnode {j}: {:?}",
                logs[i].iter().map(|p| String::from_utf8_lossy(&p.data)).collect::<Vec<_>>(),
                logs[j].iter().map(|p| String::from_utf8_lossy(&p.data)).collect::<Vec<_>>(),
            );
        }
    }
}

#[test]
fn no_spurious_delivery() {
    // P3 — every delivered payload was previously broadcast.
    let broadcasts: Vec<(u8, &[u8])> = vec![
        (0, b"x"), (0, b"y"), (1, b"z"),
    ];
    let logs = run_ring(40, &broadcasts);

    let broadcast_set: std::collections::HashSet<&[u8]> =
        broadcasts.iter().map(|(_, d)| *d).collect();

    for log in &logs {
        for payload in log {
            assert!(
                broadcast_set.contains(payload.data.as_slice()),
                "spurious delivery: {:?}", payload.data,
            );
        }
    }
}
