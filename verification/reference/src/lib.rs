//! Reference implementation of the TRAINS protocol.
//!
//! Variable names mirror `verification/tla/TRAINS.tla` exactly. There
//! are no performance optimisations: data structures are `Vec` /
//! `BTreeSet` / `HashMap`; loops are sequential; clarity wins over
//! everything else. The point of this crate is to be **obviously
//! correct** so the production `trains-core` can be checked against it
//! via differential random testing.
//!
//! ## TLA+ → Rust mapping
//!
//! | TLA+                    | Rust on `ReferenceNode`                   |
//! |-------------------------|-------------------------------------------|
//! | `pending[self]`         | `pending: BTreeSet<Payload>`              |
//! | `delivered[self]`       | `delivered: Vec<Payload>`                 |
//! | `seenClk[self][q]`      | `seen_clk[q]`                             |
//! | `issClk[self]`          | `iss_clk` (only meaningful when self issues)|
//! | `doneKeys[self]`        | `done_keys: BTreeSet<(u64, u8)>`          |
//! | `broadcast`             | `broadcast: BTreeSet<Payload>`            |
//! | `issuedKeys`            | (managed externally by the driver)        |

use std::collections::BTreeSet;

use trains_core::{
    DeliveryMode, Input as CoreInput, Output as CoreOutput, Payload, ProcId, Tick, Train,
    NUM_TRAINS, RING_SIZE,
};

/// Reference state for one ring participant.
pub struct ReferenceNode {
    /// `self`
    pub id: ProcId,
    /// `seenClk[self]` — last clock seen from each issuer.
    pub seen_clk: [Tick; RING_SIZE],
    /// `issClk[self]` — next clock to stamp on a self-issued train.
    /// Initialised to 1 (matches TLA+ `Init`).
    pub iss_clk: Tick,
    /// Per-process outgoing sequence number.
    pub next_seq: u64,
    /// `pending[self]` — payloads queued for the next train.
    pub pending: Vec<Payload>,
    /// `doneKeys[self]` — (clock, issuer) keys delivered or skipped-as-empty.
    pub done_keys: BTreeSet<(Tick, ProcId)>,
    /// `delivered[self]` — application delivery log (in delivery order).
    pub delivered: Vec<Payload>,
    /// Trains we've seen but cannot deliver yet (ack-complete +
    /// payload non-empty + AllPriorDelivered NOT yet satisfied).
    pub parked: Vec<Train>,
    /// Set of `(sender, seq)` keys ever observed — TLA+ `broadcast`.
    /// Suppresses re-broadcast of any payload whose key has been seen.
    pub broadcast_seen: std::collections::HashSet<(ProcId, u64)>,
    /// Mode (only `UniformTotalOrder` is implemented).
    pub mode: DeliveryMode,
}

impl ReferenceNode {
    pub fn new(id: ProcId, mode: DeliveryMode) -> Self {
        assert!((id as usize) < RING_SIZE, "id out of range");
        Self {
            id,
            seen_clk:       [0; RING_SIZE],
            iss_clk:        1,
            next_seq:       0,
            pending:        Vec::new(),
            done_keys:      BTreeSet::new(),
            delivered:      Vec::new(),
            parked:         Vec::new(),
            broadcast_seen: std::collections::HashSet::new(),
            mode,
        }
    }

    /// Issue this node's initial train (TLA+: part of `Init`).
    pub fn issue_initial_train(&mut self) -> Train {
        self.issue_new_train()
    }

    /// `seenClk[self][q]` accessor.
    pub fn seen_clock(&self, q: ProcId) -> Tick {
        self.seen_clk[q as usize]
    }

    /// Step function — pure analogue of `TrainsNode::step`.
    pub fn step(&mut self, input: CoreInput) -> Vec<CoreOutput> {
        match input {
            CoreInput::LocalBroadcast(data) => self.app_broadcast(data),
            CoreInput::TrainReceived(t)     => self.handle_train(t),
            CoreInput::Tick                 => Vec::new(),
        }
    }

    // ── TLA+: AppBroadcast ────────────────────────────────────────────

    fn app_broadcast(&mut self, data: Vec<u8>) -> Vec<CoreOutput> {
        // TLA+ `m \notin broadcast` — suppress re-broadcast of a key
        // already observed (e.g., echoed back via a foreign train).
        let key = (self.id, self.next_seq);
        if !self.broadcast_seen.insert(key) {
            return Vec::new();
        }
        self.pending.push(Payload {
            sender: self.id,
            seq:    self.next_seq,
            data,
        });
        self.next_seq += 1;
        Vec::new()
    }

    // ── TLA+: ProcessTrain + DeliverTrain + RecycleTrain ──────────────

    fn handle_train(&mut self, train: Train) -> Vec<CoreOutput> {
        if train.issuer == self.id {
            self.handle_returning(train)
        } else {
            self.handle_foreign(train)
        }
    }

    /// TLA+: `ProcessTrain(self, t)` for foreign trains.
    /// Two-lap scheme: lap 1 collects ack/payloads; lap 2 propagates a
    /// closed train (deliver + forward unchanged).
    fn handle_foreign(&mut self, mut train: Train) -> Vec<CoreOutput> {
        let mut out = Vec::with_capacity(2);
        let prev = self.seen_clk[train.issuer as usize];
        let is_replay = prev >= train.clock && prev > 0;

        if is_replay {
            // Lap 2.
            for p in &train.payloads {
                self.broadcast_seen.insert((p.sender, p.seq));
            }
            self.try_deliver(&train, &mut out);
            out.push(CoreOutput::ForwardTrain(train));
            self.drain_parked(&mut out);
            return out;
        }

        // Lap 1.
        // Clock-gap detection — matches `clock_state.check_and_update`.
        // We declare a gap when train.clock > prev + 1, including the
        // first-arrival case (prev = 0, clock ≥ 2): clock=1 must have
        // existed in the protocol's start state and is missing.
        let expected = prev.saturating_add(1);
        if train.clock > expected {
            out.push(CoreOutput::DeclareCrash(train.issuer));
        }
        self.seen_clk[train.issuer as usize] = train.clock;

        // Load pending onto the train (deterministic order).
        let mut taken: Vec<Payload> = std::mem::take(&mut self.pending);
        taken.sort_by_key(|a| (a.sender, a.seq));
        for p in &taken {
            self.broadcast_seen.insert((p.sender, p.seq));
        }
        train.payloads.extend(taken);
        // Track foreign payloads we've now observed.
        for p in &train.payloads {
            self.broadcast_seen.insert((p.sender, p.seq));
        }

        // Add our ack.
        train.add_ack(self.id);

        out.push(CoreOutput::ForwardTrain(train));
        self.drain_parked(&mut out);
        out
    }

    /// TLA+: returning train at issuer — first lap delivers, second recycles.
    fn handle_returning(&mut self, train: Train) -> Vec<CoreOutput> {
        let mut out = Vec::with_capacity(2);
        // Track our own clock advance.
        if train.clock > self.seen_clk[self.id as usize] {
            self.seen_clk[self.id as usize] = train.clock;
        }
        let key = (train.clock, train.issuer);
        if self.done_keys.contains(&key) {
            // Lap 2 return → recycle.
            let new_train = self.issue_new_train();
            out.push(CoreOutput::ForwardTrain(new_train));
            return out;
        }
        // Lap 1 return: deliver + forward unchanged.
        self.try_deliver(&train, &mut out);
        self.drain_parked(&mut out);
        out.push(CoreOutput::ForwardTrain(train));
        out
    }

    fn issue_new_train(&mut self) -> Train {
        let clock = self.iss_clk;
        self.iss_clk = self.iss_clk.saturating_add(1);
        let mut taken: Vec<Payload> = std::mem::take(&mut self.pending);
        taken.sort_by_key(|a| (a.sender, a.seq));
        for p in &taken {
            self.broadcast_seen.insert((p.sender, p.seq));
        }
        let mut t = Train { issuer: self.id, clock, payloads: taken, ack_bits: 0 };
        t.add_ack(self.id);
        t
    }

    /// TLA+: `DeliverTrain(self, t)`.
    fn try_deliver(&mut self, train: &Train, out: &mut Vec<CoreOutput>) {
        if !ready_to_deliver(train.ack_bits, self.mode) { return; }
        let key = (train.clock, train.issuer);
        if self.done_keys.contains(&key) { return; }

        if !self.is_deliverable(key) {
            // Park unless already parked.
            if !self.parked.iter().any(|t| (t.clock, t.issuer) == key) {
                self.parked.push(train.clone());
            }
            return;
        }

        // Mark delivered. Empty trains are recorded but produce no Deliver.
        self.done_keys.insert(key);
        if !train.payloads.is_empty() {
            let mut payloads = train.payloads.clone();
            payloads.sort_by_key(|a| (a.sender, a.seq));
            self.delivered.extend(payloads.iter().cloned());
            out.push(CoreOutput::Deliver(payloads));
        }
    }

    /// TLA+: `AllPriorDelivered(self, ck)` direct mirror.
    fn is_deliverable(&self, key: (Tick, ProcId)) -> bool {
        // (1) any smaller parked key blocks us
        for t in &self.parked {
            if (t.clock, t.issuer) < key { return false; }
        }
        // (2) every known smaller key already in done_keys
        for q in 0..RING_SIZE as ProcId {
            let q_seen = self.seen_clk[q as usize];
            if q_seen == 0 { continue; }
            for cl in 1..=q_seen {
                let k = (cl, q);
                if k < key && !self.done_keys.contains(&k) {
                    return false;
                }
            }
        }
        // (3) every issuer's clock has caught up to key.0
        for q in 0..NUM_TRAINS as ProcId {
            if self.seen_clk[q as usize] < key.0 {
                return false;
            }
        }
        true
    }

    fn drain_parked(&mut self, out: &mut Vec<CoreOutput>) {
        loop {
            let candidate = self.parked.iter()
                .map(|t| (t.clock, t.issuer))
                .min();
            let Some(key) = candidate else { break };
            if !self.is_deliverable(key) { break; }
            // Remove the parked train at this key.
            let idx = self.parked.iter()
                .position(|t| (t.clock, t.issuer) == key)
                .expect("just observed");
            let train = self.parked.remove(idx);
            self.done_keys.insert(key);
            if !train.payloads.is_empty() {
                let mut p = train.payloads.clone();
                p.sort_by_key(|a| (a.sender, a.seq));
                self.delivered.extend(p.iter().cloned());
                out.push(CoreOutput::Deliver(p));
            }
        }
    }
}

fn ready_to_deliver(ack_bits: u32, mode: DeliveryMode) -> bool {
    match mode {
        DeliveryMode::UniformTotalOrder =>
            ack_bits == ((1u32 << RING_SIZE) - 1),
        DeliveryMode::TotalOrder | DeliveryMode::CausalOrder =>
            ack_bits == ((1u32 << RING_SIZE) - 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trains_core::Train;

    #[test]
    fn issue_initial_stamps_clock_one() {
        let mut n = ReferenceNode::new(0, DeliveryMode::UniformTotalOrder);
        let t = n.issue_initial_train();
        assert_eq!(t.clock, 1);
        assert_eq!(t.issuer, 0);
        assert_eq!(t.ack_bits, 0b001);
    }

    #[test]
    fn broadcast_queues_payload() {
        let mut n = ReferenceNode::new(0, DeliveryMode::UniformTotalOrder);
        n.step(CoreInput::LocalBroadcast(b"hi".to_vec()));
        assert_eq!(n.pending.len(), 1);
    }

    #[test]
    fn lap1_foreign_does_not_deliver() {
        let mut n = ReferenceNode::new(0, DeliveryMode::UniformTotalOrder);
        let _ = n.issue_initial_train();
        let t = Train {
            issuer: 1, clock: 1,
            payloads: vec![Payload { sender: 1, seq: 0, data: b"x".to_vec() }],
            ack_bits: 0b111,
        };
        let outs = n.step(CoreInput::TrainReceived(t));
        assert!(!outs.iter().any(|o| matches!(o, CoreOutput::Deliver(_))));
    }
}
