//! [`TrainsNode`] — the TRAINS ring-protocol state machine.
//!
//! This is the verifiable kernel of the protocol. [`TrainsNode::step`]
//! is a pure function `(self, Input) → Vec<Output>` with no I/O.
//!
//! ## Correspondence with TRAINS.tla
//!
//! | TLA+ action          | Rust trigger / branch                                  |
//! |----------------------|--------------------------------------------------------|
//! | `AppBroadcast`       | `Input::LocalBroadcast(data)`                          |
//! | `ProcessTrain`       | `Input::TrainReceived(t)` — non-issuer branch          |
//! | `DeliverTrain`       | inside `step` once acks complete + AllPriorDelivered   |
//! | `RecycleTrain`       | `Input::TrainReceived(t)` — issuer branch, msgs ≠ ∅    |
//! | `RecycleEmptyTrain`  | `Input::TrainReceived(t)` — issuer branch, msgs = ∅    |
//! | `CrashProcess`       | external; node simply stops receiving                  |
//!
//! ## State variables (TLA+ → Rust field)
//!
//! | TLA+               | Rust                                                  |
//! |--------------------|-------------------------------------------------------|
//! | `seenClk[self][q]` | `clock_state.seen[q]`                                 |
//! | `issClk[self]`     | `next_issue_clock`                                    |
//! | `pending[self]`    | `pending`                                             |
//! | `doneKeys[self]`   | `delivery.done_keys`                                  |
//! | `delivered[self]`  | (output stream — caller owns the log)                 |
//! | `broadcast`        | `broadcast_seen` (local approximation; deduplicates)  |

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    clock::{ClockCheck, ClockState},
    delivery::{ClockKey, DeliveryMode, DeliveryState},
    recovery::{RecoveryAction, RecoveryPlan, RecoveryReport, StateSnapshot},
    types::{Payload, ProcId, Tick, Train, NUM_TRAINS, RING_SIZE},
    Input, Output,
};

/// Upper bound on the recently-delivered-payload cache (see
/// [`TrainsNode::delivered_cache`]). Recovery only ever reads keys near the
/// current clock, so a recency window is sufficient and keeps memory
/// bounded (Kani-friendly).
const DELIVERED_CACHE_CAP: usize = 1024;

/// The state machine for a single ring participant.
pub struct TrainsNode {
    /// This node's process ID (0..RING_SIZE).
    id: ProcId,

    /// Logical clock state — last clock seen per issuer.
    /// TLA+: `seenClk[self]`.
    clock_state: ClockState,

    /// Delivery mode + `doneKeys[self]`.
    delivery: DeliveryState,

    /// The next clock value to stamp on a train this node issues.
    /// TLA+: `issClk[self]`. Starts at 1 (matches `Init` in TRAINS.tla).
    next_issue_clock: Tick,

    /// Per-process outgoing sequence number (used to make payloads unique).
    next_seq: u64,

    /// Payloads waiting for the next train to circulate.
    /// TLA+: `pending[self]`.
    pending: Vec<Payload>,

    /// Trains we've seen (ack-complete) but cannot deliver yet because
    /// some strictly-earlier `(clock, issuer)` train hasn't reached us.
    /// `BTreeMap` (not `HashMap`) so `min()` is O(1) and so Kani can
    /// model construction without `CCRandomGenerateBytes`.
    parked: BTreeMap<ClockKey, Train>,

    /// `(sender, seq)` of every payload we've ever issued or accepted on
    /// a train. Enforces TLA+ `m \notin broadcast`. `BTreeSet` for
    /// determinism + Kani compatibility.
    broadcast_seen: BTreeSet<(ProcId, u64)>,

    /// Recently-delivered keys → their payloads. Read only during view-
    /// change recovery so a survivor can supply payloads for a key a peer
    /// delivered but this node missed (or vice-versa). Bounded to
    /// [`DELIVERED_CACHE_CAP`] most-recent keys; not part of `step()`
    /// semantics. `BTreeMap` for deterministic eviction (smallest key).
    delivered_cache: BTreeMap<ClockKey, Vec<Payload>>,

    /// Per-issuer view floor installed by [`Self::apply_recovery`]: the
    /// agreed recovery boundary. Trains at or below it are stale tokens
    /// from a *prior* view (their slots were already resolved by recovery);
    /// re-forwarding or recycling them would spawn zombie tokens that race
    /// the freshly-reissued ones. We absorb them. All-zero until the first
    /// view change, so normal operation is unaffected (Totem-style
    /// old-view fencing).
    view_floor: [Tick; RING_SIZE],

    /// View-change freeze: while true, delivery is suspended (no `Deliver`,
    /// no `done_keys` mutation, no parking). Set during a view change between
    /// the report snapshot and `apply_recovery`, so deliveries can't race the
    /// snapshot and make the agreed plan stale. Trains are still forwarded by
    /// the caller, so the ring keeps moving. Off in normal operation.
    frozen: bool,
}

impl TrainsNode {
    pub fn new(id: ProcId, mode: DeliveryMode) -> Self {
        assert!((id as usize) < RING_SIZE, "id must be < RING_SIZE");
        Self {
            id,
            clock_state:      ClockState::new(),
            delivery:         DeliveryState::new(mode),
            next_issue_clock: 1,
            next_seq:         0,
            pending:          Vec::new(),
            parked:           BTreeMap::new(),
            broadcast_seen:   BTreeSet::new(),
            delivered_cache:  BTreeMap::new(),
            view_floor:       [0; RING_SIZE],
            frozen:           false,
        }
    }

    pub fn id(&self) -> ProcId { self.id }
    pub fn next_issue_clock(&self) -> Tick { self.next_issue_clock }
    pub fn pending_len(&self) -> usize { self.pending.len() }
    pub fn parked_len(&self) -> usize { self.parked.len() }

    /// Last clock seen from `issuer`. TLA+: `seenClk[self][issuer]`.
    pub fn seen_clock(&self, issuer: ProcId) -> Tick {
        self.clock_state.last_seen(issuer)
    }

    /// Snapshot of every `seenClk[self][q]` for `q in 0..RING_SIZE`.
    /// Used by the trace recorder.
    pub fn seen_clocks(&self) -> [Tick; RING_SIZE] {
        let mut out = [0; RING_SIZE];
        for (i, slot) in out.iter_mut().enumerate() {
            *slot = self.clock_state.last_seen(i as ProcId);
        }
        out
    }

    /// Iterator over the `(clock, issuer)` keys this node has marked
    /// done — either via `DeliverTrain` (with payloads) or via
    /// `RecycleEmptyTrain` (vacuously). TLA+: `doneKeys[self]`.
    pub fn done_keys_iter(&self) -> impl Iterator<Item = ClockKey> + '_ {
        self.delivery.done_keys().iter().copied()
    }

    /// Mark process `p` as crashed (used in TotalOrder mode).
    pub fn mark_crashed(&mut self, p: ProcId) {
        self.delivery.mark_crashed(p);
    }

    /// Confirm process `p` has permanently crashed and recover delivery.
    ///
    /// This is the caller↔core seam for *reconfiguration* (see
    /// `bench/reports/trains-reconfiguration-study.md`). A single
    /// `Output::DeclareCrash` is only a *hint* (a clock gap can be
    /// transient reordering); the caller's failure detector decides a
    /// node is truly dead and calls this. We then:
    ///   1. exclude `p` from the delivery condition — under
    ///      [`DeliveryMode::TotalOrder`] the live set becomes
    ///      `FULL_ACK & !crashed`, so trains that collected every
    ///      *surviving* node's ack become deliverable (uniform within
    ///      the surviving view); and
    ///   2. re-drain any parked trains the reduced live set now unblocks.
    ///
    /// Returns the now-deliverable [`Output`]s. Note: full end-to-end
    /// recovery also requires the network layer to re-form the ring so
    /// trains circulate past `p` (trains-net successor re-wiring — the
    /// remaining milestone); this method closes the delivery-condition
    /// half in the verified core.
    pub fn confirm_crash(&mut self, p: ProcId) -> Vec<Output> {
        self.delivery.mark_crashed(p);
        let mut outputs = Vec::new();
        self.drain_parked(&mut outputs);
        outputs
    }

    /// Re-admit process `p` to the live view — the inverse of [`confirm_crash`]
    /// for v3 re-admission (the verified spec's `ReAdmit`: membership shrinks).
    /// Clears `p`'s crashed bit, so under [`DeliveryMode::TotalOrder`] the live
    /// set `FULL_ACK & !crashed` includes `p` again and trains must collect its
    /// ack to deliver — `p` is a full acking member, restoring N-redundancy.
    ///
    /// This is the delivery-condition half; the caller (driver) must also re-form
    /// the ring (trains-net successor re-wiring back through `p`) and the
    /// reconfiguration layer ([`trains_recovery::ViewChange::on_readmit_install`])
    /// drives the ordered membership-change token. Symmetric to how
    /// `confirm_crash` pairs with exclude + retarget-past.
    pub fn readmit_node(&mut self, p: ProcId) {
        self.delivery.unmark_crashed(p);
    }

    // ── Core step function ───────────────────────────────────────────────────

    /// Pure state-machine step. All Kani harnesses and Verus proofs
    /// target this method. No I/O; allocation is bounded by inputs.
    pub fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::LocalBroadcast(data)  => self.handle_broadcast(data),
            Input::TrainReceived(train)  => self.handle_train(train),
            Input::Tick                  => self.handle_tick(),
        }
    }

    // ── AppBroadcast (TRAINS.tla: AppBroadcast action) ───────────────────────

    fn handle_broadcast(&mut self, data: Vec<u8>) -> Vec<Output> {
        let key = (self.id, self.next_seq);
        if !self.broadcast_seen.insert(key) {
            // Should never happen with monotonic next_seq, but defensive.
            return vec![];
        }
        self.pending.push(Payload {
            sender: self.id,
            seq:    self.next_seq,
            data,
        });
        self.next_seq = self.next_seq.saturating_add(1);
        vec![]
    }

    // ── ProcessTrain / RecycleTrain dispatch ─────────────────────────────────

    fn handle_train(&mut self, train: Train) -> Vec<Output> {
        // Old-view fencing: a train at or below the installed view floor is
        // a stale token from a prior view — its slot was already resolved
        // by recovery. Absorb it (do not deliver, recycle, or forward) so
        // it cannot spawn zombie tokens that race the reissued ones. The
        // floor is all-zero until the first view change, so this is a
        // no-op in normal operation.
        if (train.issuer as usize) < RING_SIZE
            && train.clock <= self.view_floor[train.issuer as usize]
        {
            return Vec::new();
        }

        // Issuer branch → this is a *returning* train. Two cases:
        //   (a) lap-1 return: train is now FULL_ACK but other nodes
        //       haven't seen the FULL_ACK version yet. We deliver
        //       locally, then forward the FULL_ACK train UNCHANGED
        //       so successors see + deliver too.
        //   (b) lap-2 return: train comes back FULL_ACK with key
        //       already in our doneKeys → every node has seen it →
        //       safe to recycle (TLA+: RecycleTrain / RecycleEmptyTrain).
        if train.issuer == self.id {
            self.handle_returning_train(train)
        } else {
            self.handle_foreign_train(train)
        }
    }

    /// TLA+: ProcessTrain(p, t) when p ≠ tr[t].issuer.
    ///
    /// Two-phase semantics distinguished by `seenClk`:
    ///   * **Lap 1** — first sight of `(clock, issuer)`. Load pending,
    ///     add ack, forward. **Do NOT deliver yet** — the train's
    ///     payloads are still being accumulated as it circulates.
    ///   * **Lap 2** — replay of a clock we already saw. The train is
    ///     "closed" (FULL_ACK with all loaded payloads). Try to
    ///     deliver, forward unchanged.
    ///
    /// This guarantees every node sees the *same* train contents at
    /// delivery time, satisfying TLA+ `ConsistentDelivery`.
    fn handle_foreign_train(&mut self, mut train: Train) -> Vec<Output> {
        let mut outputs = Vec::with_capacity(2);

        let prev_seen = self.clock_state.last_seen(train.issuer);
        let is_replay = prev_seen >= train.clock && prev_seen > 0;

        if is_replay {
            // Lap-2 propagation: track payloads, attempt delivery, forward.
            for p in &train.payloads {
                self.broadcast_seen.insert((p.sender, p.seq));
            }
            self.try_deliver(&train, &mut outputs);
            outputs.push(Output::ForwardTrain(train));
            self.drain_parked(&mut outputs);
            return outputs;
        }

        // Lap-1: collect acks + payloads.
        let check = self.clock_state.check_and_update(train.issuer, train.clock);
        if let ClockCheck::Gap { .. } = check {
            outputs.push(Output::DeclareCrash(train.issuer));
        }
        for p in self.pending.drain(..) {
            self.broadcast_seen.insert((p.sender, p.seq));
            train.payloads.push(p);
        }
        for p in &train.payloads {
            self.broadcast_seen.insert((p.sender, p.seq));
        }
        train.add_ack(self.id);

        // No delivery on lap-1 (payloads still being collected by other nodes).
        outputs.push(Output::ForwardTrain(train));
        // But a previously-parked train may have its `is_deliverable`
        // gates change because we just bumped seenClk for an issuer.
        self.drain_parked(&mut outputs);

        outputs
    }

    /// TLA+: RecycleTrain(t) / RecycleEmptyTrain(t) / DeliverTrain at issuer.
    ///
    /// The train has come full circle back to its issuer.  We need a
    /// two-phase scheme so that every node sees the FULL_ACK version:
    ///
    ///   * **Lap-1 return** — the key has not been delivered locally
    ///     yet.  Deliver if non-empty, then **forward the train
    ///     unchanged** so the next lap propagates FULL_ACK to every
    ///     other node.  No clock increment.
    ///
    ///   * **Lap-2 return** — the key is already in `doneKeys`, which
    ///     means every node has now delivered it.  Recycle: bump the
    ///     clock, start a fresh tour.
    fn handle_returning_train(&mut self, train: Train) -> Vec<Output> {
        let mut outputs = Vec::with_capacity(2);

        let key = ClockKey::new(train.clock, train.issuer);

        // Track the highest clock we've issued (no gap detection needed
        // for self).
        let _ = self.clock_state.check_and_update(self.id, train.clock);

        if self.delivery.already_delivered(key) {
            // Lap-2 return at issuer → every node has processed this
            // key (delivered it or recorded an empty done) → recycle.
            let new_train = self.issue_new_train();
            outputs.push(Output::ForwardTrain(new_train));
            return outputs;
        }

        // Lap-1 return: deliver (or record empty), then forward the
        // train UNCHANGED for the second propagation lap so other
        // nodes also see it.
        self.try_deliver(&train, &mut outputs);
        self.drain_parked(&mut outputs);
        outputs.push(Output::ForwardTrain(train));
        outputs
    }

    /// Build a new train stamped with our next clock; load any pending
    /// payloads we have right now (TLA+: msgs starts empty but the
    /// next ProcessTrain immediately appends pending — we collapse the
    /// two for efficiency).
    fn issue_new_train(&mut self) -> Train {
        // Strict increment matches TLA+ RecycleTrain `newClk = issClk[iss] + 1`.
        let clock = self.next_issue_clock;
        self.next_issue_clock = self.next_issue_clock.saturating_add(1);

        let mut payloads: Vec<Payload> = Vec::new();
        for p in self.pending.drain(..) {
            self.broadcast_seen.insert((p.sender, p.seq));
            payloads.push(p);
        }

        // Pre-ack ourselves: the issuer trivially acks its own train.
        let mut t = Train {
            issuer: self.id,
            clock,
            payloads,
            ack_bits: 0,
        };
        t.add_ack(self.id);
        t
    }

    /// Has every issuer's clock caught up to `key.clock`?
    ///
    /// Concretely: for every issuer `q` (the first `NUM_TRAINS` ring
    /// positions), require `seenClk[self][q] >= key.clock`.
    ///
    /// This matches the TLA+ guard
    ///   `\A q \in Issuers : issClk[q] >= ck[1]`
    /// in `AllPriorDelivered`. It is **not** safe to skip never-heard
    /// issuers: TLC found a real `ConsistentDelivery` violation in
    /// which slow slots later issued smaller-keyed trains that
    /// re-introduced an out-of-order key. We must wait until every
    /// issuer's clock is provably at least `key.clock`.
    ///
    /// For self (`q == self.id`), `clock_state.last_seen(self)` is
    /// updated on `handle_returning_train` so this naturally reflects
    /// our own `issClk`.
    ///
    /// A **confirmed-crashed** issuer is skipped: it will never advance its
    /// clock again, so waiting on it would block all post-crash delivery
    /// forever. This is safe — and distinct from skipping a merely-slow
    /// issuer (which TLC showed breaks `ConsistentDelivery`) — because
    /// `crashed_bits` is only set by `confirm_crash` (a *permanent* crash)
    /// and the view-change recovery has already resolved every key the dead
    /// issuer owned up to the agreed boundary, so no smaller-clock train
    /// from it can still be in flight. Pre-crash, `crashed_bits == 0`, so
    /// this is identical to the original gate.
    fn seen_clocks_advanced_enough(&self, key: ClockKey) -> bool {
        for q in 0..NUM_TRAINS as ProcId {
            if self.delivery.is_crashed(q) {
                continue;
            }
            let q_seen = self.clock_state.last_seen(q);
            if q_seen < key.clock {
                return false;
            }
        }
        true
    }

    /// Common delivery logic. Deliverability requires:
    ///   * `ready_to_deliver` per the configured mode (UTO/TO/CO);
    ///   * not already in `done_keys`; AND
    ///   * `is_deliverable` (no smaller-key trains still outstanding).
    ///
    /// For ack-complete trains with EMPTY payloads we still record the
    /// key in `done_keys` (so successor keys aren't blocked) but emit
    /// no `Output::Deliver`. TLA+ handles this via `RecycleEmptyTrain`
    /// advancing the slot clock without populating `delivered`.
    fn try_deliver(&mut self, train: &Train, outputs: &mut Vec<Output>) {
        if self.frozen {
            return; // view-change freeze: suspend delivery (still forwarded)
        }
        let mode = self.delivery.mode();
        if !self.delivery.ready_to_deliver(train.ack_bits, mode) {
            return;
        }

        let key = ClockKey::new(train.clock, train.issuer);
        if self.delivery.already_delivered(key) {
            return;
        }

        if !self.is_deliverable(key) {
            self.parked.entry(key).or_insert_with(|| train.clone());
            return;
        }

        self.delivery.record_delivered(key);
        if !train.payloads.is_empty() {
            self.cache_delivered(key, &train.payloads);
            outputs.push(Output::Deliver(sort_payloads(&train.payloads)));
        }
    }

    /// Remember a delivered key's payloads for possible retransmission
    /// during a later view change. Bounded to the most-recent
    /// [`DELIVERED_CACHE_CAP`] keys (evicts the smallest).
    fn cache_delivered(&mut self, key: ClockKey, payloads: &[Payload]) {
        if payloads.is_empty() {
            return;
        }
        self.delivered_cache.insert(key, payloads.to_vec());
        while self.delivered_cache.len() > DELIVERED_CACHE_CAP {
            let Some(min) = self.delivered_cache.keys().next().copied() else { break };
            self.delivered_cache.remove(&min);
        }
    }

    /// `key` is deliverable iff every condition holds:
    ///  (1) no parked train has a strictly-smaller key
    ///       (else we'd deliver out of order — the parked one comes first);
    ///  (2) every `(cl, q)` we have *seen* with `(cl, q) < key` is already
    ///       in `doneKeys[self]`  (TLA+: AllPriorDelivered);
    ///  (3) every other active issuer's `seenClk` has caught up to
    ///       `key.clock - 1`, ensuring no smaller-clock train from `q`
    ///       can still be in flight to us.
    fn is_deliverable(&self, key: ClockKey) -> bool {
        // (1) Smaller-keyed parked train blocks us.
        for parked in self.parked.keys() {
            if *parked < key { return false; }
        }
        // (2) All known smaller keys must already be delivered.
        for q in 0..RING_SIZE as ProcId {
            let q_seen = self.clock_state.last_seen(q);
            if q_seen == 0 { continue; }
            for cl in 1..=q_seen {
                let known = ClockKey::new(cl, q);
                if known < key && !self.delivery.already_delivered(known) {
                    return false;
                }
            }
        }
        // (3) seenClk gate (catches in-flight smaller-clock trains).
        self.seen_clocks_advanced_enough(key)
    }

    /// After a state change, repeatedly drain parked trains that
    /// have just become deliverable. Picks the smallest parked key
    /// first (it can't be blocked by another parked key) and
    /// re-checks `is_deliverable`. Empty trains record `done_keys`
    /// without emitting a Deliver.
    fn drain_parked(&mut self, outputs: &mut Vec<Output>) {
        if self.frozen {
            return; // view-change freeze: do not deliver parked trains
        }
        loop {
            let candidate = self.parked.keys().copied().min();
            let Some(key) = candidate else { break };
            if !self.is_deliverable(key) { break; }
            let train = self.parked.remove(&key).expect("key just observed");
            self.delivery.record_delivered(key);
            if !train.payloads.is_empty() {
                self.cache_delivered(key, &train.payloads);
                outputs.push(Output::Deliver(sort_payloads(&train.payloads)));
            }
        }
    }

    fn handle_tick(&mut self) -> Vec<Output> {
        // Future use: timeout-based train reconstruction.
        vec![]
    }

    // ── Bootstrap helper ─────────────────────────────────────────────────────

    /// Used at start-up: each issuer process emits its initial train.
    /// TLA+: `Init` creates `NumTrains` trains, each at `ring[t]` with
    /// `clock = 1` and `acks = {}`. The runtime calls this once for
    /// each train slot owned by this node.
    pub fn issue_initial_train(&mut self) -> Train {
        self.issue_new_train()
    }

    /// Re-issue this issuer's train at the current clock — token recovery
    /// for reconfiguration. When a node crashes it may have been holding
    /// in-flight trains, which are then lost; circulation halts and the
    /// `seen_clocks` delivery gate starves. After confirming the crash,
    /// each surviving issuer calls this to regenerate its train so the
    /// re-formed ring resumes. (Continues the issuer's clock sequence;
    /// the caller forwards the returned train to its successor.)
    pub fn reissue_train(&mut self) -> Train {
        self.issue_new_train()
    }

    // ── State transfer (reconfiguration PR-R5) ───────────────────────────────

    /// Export this node's protocol state so a rejoining/new node can install it
    /// and catch up to the current view. The *application* delivered log is
    /// transferred separately by the caller (the SMR layer).
    pub fn export_state(&self) -> StateSnapshot {
        StateSnapshot {
            seen: self.seen_clocks(),
            done_keys: self.delivery.done_keys().clone(),
            crashed_bits: self.delivery.crashed_bits(),
            view_floor: self.view_floor,
            broadcast_seen: self.broadcast_seen.clone(),
        }
    }

    /// Install a [`StateSnapshot`] into this (typically fresh) node, making it
    /// consistent with the view the snapshot was taken in. The caller installs
    /// the matching application delivered log alongside.
    ///
    /// The node's own issue clock is set above the boundary for its slot
    /// (`max(seen, view_floor) + 1`), so any token it reissues cannot collide
    /// with a key from before it joined. Transient buffers are cleared.
    pub fn import_state(&mut self, snap: StateSnapshot) {
        self.clock_state.restore(snap.seen);
        self.delivery.restore(snap.done_keys, snap.crashed_bits);
        self.view_floor = snap.view_floor;
        self.broadcast_seen = snap.broadcast_seen;

        let slot = self.id as usize;
        let base = snap.seen[slot].max(snap.view_floor[slot]);
        self.next_issue_clock = base.saturating_add(1);

        self.frozen = false;
        self.pending.clear();
        self.parked.clear();
        self.delivered_cache.clear();
    }

    /// Enter/leave the view-change freeze (suspends delivery). The
    /// orchestration layer freezes at the start of a view change (before
    /// snapshotting via [`Self::recovery_report`]); [`Self::apply_recovery`]
    /// lifts it when the new view is installed.
    pub fn set_frozen(&mut self, frozen: bool) {
        self.frozen = frozen;
    }

    /// Is delivery currently frozen for a view change?
    pub fn is_frozen(&self) -> bool {
        self.frozen
    }

    // ── View-change recovery (reconfiguration C3) ────────────────────────────

    /// Snapshot this survivor's state for a view-change merge. See
    /// [`crate::recovery`] for the protocol and safety argument. `have`
    /// carries payloads this node could retransmit for a key (from its
    /// recently-delivered cache or a parked, order-blocked train) so the
    /// coordinator's merge can deliver a key uniformly even if only some
    /// survivors received it.
    pub fn recovery_report(&self) -> RecoveryReport {
        let seen = self.seen_clocks();

        // Report the highest clock we've *issued* for our own slot
        // (`next_issue_clock - 1`); a token at that clock may be in flight
        // (and lost) and must be folded into the agreed boundary.
        let mut issued = [0 as Tick; RING_SIZE];
        issued[self.id as usize] = self.next_issue_clock.saturating_sub(1);

        let done = self.delivery.done_keys().clone();

        let mut have: BTreeMap<ClockKey, Vec<Payload>> = BTreeMap::new();
        for (k, ps) in &self.delivered_cache {
            if !ps.is_empty() {
                have.insert(*k, ps.clone());
            }
        }
        for (k, t) in &self.parked {
            if !t.payloads.is_empty() {
                have.entry(*k).or_insert_with(|| t.payloads.clone());
            }
        }

        RecoveryReport { seen, issued, done, have }
    }

    /// Apply an agreed [`RecoveryPlan`] (from [`crate::compute_recovery_plan`])
    /// to close the lost-key gaps and install the new view.
    ///
    /// Processes recovered keys in `(clock, issuer)` order — delivering
    /// agreed payloads (idempotently; never double-delivering a key already
    /// in `done_keys`) or recording empty skips — then advances this node's
    /// issue clock above the agreed boundary so its reissued token cannot
    /// collide with a recovered key. Finally re-drains any parked trains the
    /// closed gaps now unblock. Returns the newly-deliverable [`Output`]s.
    ///
    /// Uniform agreement is preserved by construction: every survivor
    /// applies the *same* plan in the *same* order, and a key with a payload
    /// at any survivor is delivered with that payload at all of them.
    pub fn apply_recovery(&mut self, plan: &RecoveryPlan) -> Vec<Output> {
        let mut outputs = Vec::new();

        for (key, action) in &plan.actions {
            // Reflect every recovered slot in our clock state so future
            // AllPriorDelivered / seen-clock gates are consistent across the
            // surviving view (this survivor may be behind the boundary).
            let _ = self.clock_state.check_and_update(key.issuer, key.clock);

            // A stale parked copy must not survive into drain_parked (it
            // would double-record). Drop it whether or not we deliver here.
            self.parked.remove(key);

            if self.delivery.already_delivered(*key) {
                continue;
            }

            match action {
                RecoveryAction::Deliver(ps) => {
                    self.delivery.record_delivered(*key);
                    if !ps.is_empty() {
                        for p in ps {
                            self.broadcast_seen.insert((p.sender, p.seq));
                        }
                        self.cache_delivered(*key, ps);
                        outputs.push(Output::Deliver(sort_payloads(ps)));
                    }
                }
                RecoveryAction::Skip => {
                    self.delivery.record_delivered(*key);
                }
            }
        }

        // Install the new view: never reissue at or below a recovered clock,
        // and fence out every stale token from the prior view.
        for q in 0..RING_SIZE {
            if plan.boundary[q] > self.view_floor[q] {
                self.view_floor[q] = plan.boundary[q];
            }
        }
        let want = plan.reissue_clock(self.id);
        if want > self.next_issue_clock {
            self.next_issue_clock = want;
        }

        // The new view is installed → lift the freeze and resume delivery,
        // draining anything the closed gaps now unblock.
        self.frozen = false;
        self.drain_parked(&mut outputs);
        outputs
    }
}

/// Deterministic order: sort by (sender, seq). Matches the TLA+ spec's
/// `MsgsToSeq` (any total order on payloads will do, as long as every
/// process applies the same one).
fn sort_payloads(p: &[Payload]) -> Vec<Payload> {
    let mut v: Vec<Payload> = p.to_vec();
    v.sort_by_key(|a| (a.sender, a.seq));
    v
}

// ────────────────────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FULL_ACK;

    fn payload(sender: ProcId, seq: u64, data: &[u8]) -> Payload {
        Payload { sender, seq, data: data.to_vec() }
    }

    fn fully_acked_train(issuer: ProcId, clock: Tick, payloads: Vec<Payload>) -> Train {
        Train { issuer, clock, payloads, ack_bits: FULL_ACK }
    }

    #[test]
    fn broadcast_queues_payload() {
        let mut node = TrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        let out = node.step(Input::LocalBroadcast(b"hello".to_vec()));
        assert!(out.is_empty());
        assert_eq!(node.pending_len(), 1);
    }

    #[test]
    fn fully_acked_foreign_train_lap2_delivers_and_forwards() {
        // Two-lap semantics: foreign trains deliver on their *second*
        // arrival (lap-2 propagation), not their first.  Per TLA+
        // AllPriorDelivered: every issuer's clock must be ≥ key.clock
        // before delivery, so we issue our own train first (NUM_TRAINS=2,
        // issuers = {0, 1}, self=0).
        let mut node = TrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        let _ = node.issue_initial_train(); // primes self's issClk
        // Echo it back so seen_clock(self) = 1.
        let self_t = Train { issuer: 0, clock: 1, payloads: vec![],
                             ack_bits: FULL_ACK };
        node.step(Input::TrainReceived(self_t));

        let train = fully_acked_train(1, 1, vec![payload(1, 0, b"msg")]);

        // Lap-1: no delivery.
        let outs1 = node.step(Input::TrainReceived(train.clone()));
        assert_eq!(
            outs1.iter().filter(|o| matches!(o, Output::Deliver(_))).count(),
            0,
            "lap-1 must not deliver",
        );

        // Lap-2: deliver.
        let outs2 = node.step(Input::TrainReceived(train));
        assert_eq!(
            outs2.iter().filter(|o| matches!(o, Output::Deliver(_))).count(),
            1,
            "lap-2 should deliver",
        );
    }

    #[test]
    fn confirm_crash_recovers_delivery_under_total_order() {
        // RING_SIZE=3, FULL_ACK=0b111. A train acked by {0,1} but not by
        // node 2 cannot be delivered while node 2 is considered live (the
        // Phase-G "crash halts UTO" gap). Once node 2 is confirmed
        // crashed, the surviving view is {0,1} and the train becomes
        // deliverable — the reconfiguration recovery path, at the
        // delivery layer.
        let mut node = TrainsNode::new(0, DeliveryMode::TotalOrder);
        let _ = node.issue_initial_train(); // self issClk = 1
        // Prime seenClk for self (0) and the other issuer (1).
        node.step(Input::TrainReceived(Train {
            issuer: 0, clock: 1, payloads: vec![], ack_bits: FULL_ACK,
        }));
        node.step(Input::TrainReceived(Train {
            issuer: 1, clock: 1, payloads: vec![], ack_bits: 0b011,
        }));

        // Train from issuer 1 acked by {0,1} only (node 2 absent).
        let t = Train {
            issuer: 1, clock: 1,
            payloads: vec![payload(1, 0, b"msg")],
            ack_bits: 0b011,
        };

        // While node 2 is considered live, the live mask is 0b111 ⇒ no delivery.
        let pre = node.step(Input::TrainReceived(t.clone()));
        assert_eq!(
            pre.iter().filter(|o| matches!(o, Output::Deliver(_))).count(), 0,
            "must not deliver while node 2 is considered live",
        );

        // Confirm node 2 crashed; live mask becomes 0b011.
        let _ = node.confirm_crash(2);

        // The same train now satisfies the delivery condition for the
        // surviving view and is delivered.
        let post = node.step(Input::TrainReceived(t));
        assert_eq!(
            post.iter().filter(|o| matches!(o, Output::Deliver(_))).count(), 1,
            "after confirming node 2 crashed, the train delivers under the surviving view",
        );
    }

    #[test]
    fn readmit_node_restores_full_acking_membership() {
        // v3 (PR-V3-3): readmit_node is the inverse of confirm_crash. Two fresh
        // nodes fed the IDENTICAL {0,1}-acked train isolate the live-mask effect
        // — the only difference is whether node 2 was re-admitted.
        let primed = || {
            let mut node = TrainsNode::new(0, DeliveryMode::TotalOrder);
            let _ = node.issue_initial_train();
            node.step(Input::TrainReceived(Train {
                issuer: 0, clock: 1, payloads: vec![], ack_bits: FULL_ACK,
            }));
            node.step(Input::TrainReceived(Train {
                issuer: 1, clock: 1, payloads: vec![], ack_bits: 0b011,
            }));
            node
        };
        let train = || Train {
            issuer: 1, clock: 1,
            payloads: vec![payload(1, 0, b"msg")],
            ack_bits: 0b011,
        };
        let delivers = |n: &mut TrainsNode| {
            n.step(Input::TrainReceived(train()))
                .iter()
                .filter(|o| matches!(o, Output::Deliver(_)))
                .count()
        };

        // Excluded only: surviving view {0,1} ⇒ the {0,1}-acked train delivers.
        let mut excluded = primed();
        let _ = excluded.confirm_crash(2);
        assert_eq!(delivers(&mut excluded), 1, "excluded: {{0,1}} acks suffice");

        // Excluded THEN re-admitted: node 2 live again ⇒ live mask 0b111 ⇒ the
        // same {0,1}-acked train must NOT deliver (its ack is required again).
        let mut readmitted = primed();
        let _ = readmitted.confirm_crash(2);
        readmitted.readmit_node(2);
        assert_eq!(
            delivers(&mut readmitted), 0,
            "after re-admit, node 2 is a full acking member — its ack is required",
        );
    }

    #[test]
    fn state_transfer_makes_joiner_consistent() {
        // Source node A delivers (1,1), advancing its done_keys + clocks.
        let mut a = TrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        let _ = a.issue_initial_train();
        a.step(Input::TrainReceived(Train {
            issuer: 0, clock: 1, payloads: vec![], ack_bits: FULL_ACK,
        }));
        let t = fully_acked_train(1, 1, vec![payload(1, 0, b"m")]);
        a.step(Input::TrainReceived(t.clone())); // lap-1
        let delivered = a
            .step(Input::TrainReceived(t.clone()))
            .iter()
            .filter(|o| matches!(o, Output::Deliver(_)))
            .count();
        assert_eq!(delivered, 1, "A delivers (1,1) on lap-2");

        // A joiner B (same id) installs A's exported state.
        let snap = a.export_state();
        let mut b = TrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        b.import_state(snap.clone());

        // B's protocol state now matches A's exactly.
        assert_eq!(b.export_state(), snap, "import then export round-trips");
        assert!(b.done_keys_iter().any(|k| k == ClockKey::new(1, 1)));
        assert_eq!(b.seen_clock(1), 1);
        assert!(b.next_issue_clock() >= 2, "issue clock above the joined boundary");

        // B must NOT re-deliver an already-delivered key it imported.
        let redelivered = b
            .step(Input::TrainReceived(t))
            .iter()
            .filter(|o| matches!(o, Output::Deliver(_)))
            .count();
        assert_eq!(redelivered, 0, "joiner must not re-deliver imported keys");
    }

    #[test]
    fn freeze_suspends_delivery_until_lifted() {
        // A fully-acked, in-order train delivers normally; once frozen the
        // same delivery is suspended; lifting the freeze (here via the
        // explicit setter) resumes it.
        let mut node = TrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        let _ = node.issue_initial_train();
        // Prime seenClk(0)=seenClk(1)=1 so the all-issuers gate passes.
        node.step(Input::TrainReceived(Train { issuer: 0, clock: 1, payloads: vec![], ack_bits: FULL_ACK }));
        node.step(Input::TrainReceived(Train { issuer: 1, clock: 1, payloads: vec![], ack_bits: 0b011 }));

        let t = fully_acked_train(1, 1, vec![payload(1, 0, b"m")]);

        // Frozen: lap-1 then lap-2 produce no delivery.
        node.set_frozen(true);
        assert!(node.is_frozen());
        node.step(Input::TrainReceived(t.clone()));
        let frozen_deliveries = node
            .step(Input::TrainReceived(t.clone()))
            .iter()
            .filter(|o| matches!(o, Output::Deliver(_)))
            .count();
        assert_eq!(frozen_deliveries, 0, "no delivery while frozen");

        // Unfreeze: the next replay delivers.
        node.set_frozen(false);
        let n = node
            .step(Input::TrainReceived(t))
            .iter()
            .filter(|o| matches!(o, Output::Deliver(_)))
            .count();
        assert_eq!(n, 1, "delivery resumes once unfrozen");
    }

    #[test]
    fn apply_recovery_records_skips_and_delivers() {
        use crate::recovery::{RecoveryAction, RecoveryPlan};
        use std::collections::BTreeMap;

        let mut node = TrainsNode::new(0, DeliveryMode::TotalOrder);
        let _ = node.confirm_crash(2); // surviving view {0,1}

        // Plan: (1,0) was a lost empty slot → Skip; (2,0) was delivered
        // with a payload at a peer → Deliver (retransmit) here.
        let mut actions = BTreeMap::new();
        actions.insert(ClockKey::new(1, 0), RecoveryAction::Skip);
        actions.insert(
            ClockKey::new(2, 0),
            RecoveryAction::Deliver(vec![payload(0, 5, b"R")]),
        );
        let mut boundary = [0 as Tick; RING_SIZE];
        boundary[0] = 2;
        let plan = RecoveryPlan { actions, boundary };

        let outs = node.apply_recovery(&plan);
        let delivered: Vec<_> = outs
            .iter()
            .filter_map(|o| match o {
                Output::Deliver(p) => Some(p),
                _ => None,
            })
            .collect();
        assert_eq!(delivered.len(), 1, "retransmitted (2,0) delivers once");
        assert_eq!(delivered[0][0].data, b"R");

        assert!(node.done_keys_iter().any(|k| k == ClockKey::new(1, 0)));
        assert!(node.done_keys_iter().any(|k| k == ClockKey::new(2, 0)));
        assert!(
            node.next_issue_clock() >= 3,
            "issue clock installed above the agreed boundary",
        );

        // Idempotent: re-applying the same plan double-delivers nothing.
        let outs2 = node.apply_recovery(&plan);
        assert!(
            !outs2.iter().any(|o| matches!(o, Output::Deliver(_))),
            "no double delivery on re-apply",
        );
    }

    #[test]
    fn partial_ack_no_delivery_but_forwarded() {
        let mut node = TrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        let train = Train {
            issuer: 1, clock: 1,
            payloads: vec![payload(1, 0, b"x")],
            ack_bits: 0b011,
        };
        let outputs = node.step(Input::TrainReceived(train));
        assert!(!outputs.iter().any(|o| matches!(o, Output::Deliver(_))));
        assert!( outputs.iter().any(|o| matches!(o, Output::ForwardTrain(_))));
    }

    #[test]
    fn clock_gap_triggers_declare_crash() {
        let mut node = TrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        node.step(Input::TrainReceived(fully_acked_train(1, 1, vec![payload(1, 0, b"a")])));
        let outputs = node.step(Input::TrainReceived(fully_acked_train(1, 3, vec![payload(1, 1, b"b")])));
        assert!(outputs.iter().any(|o| matches!(o, Output::DeclareCrash(1))));
    }

    #[test]
    fn pending_loaded_onto_foreign_train() {
        let mut node = TrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        node.step(Input::LocalBroadcast(b"queued".to_vec()));
        assert_eq!(node.pending_len(), 1);

        // Foreign train arrives partially-acked
        let train = Train {
            issuer: 1, clock: 1, payloads: vec![], ack_bits: 0b011,
        };
        let outputs = node.step(Input::TrainReceived(train));
        assert_eq!(node.pending_len(), 0, "pending drained onto train");

        let fwd = outputs.iter().find_map(|o| match o {
            Output::ForwardTrain(t) => Some(t),
            _ => None,
        }).unwrap();
        assert_eq!(fwd.payloads.len(), 1);
        assert_eq!(fwd.ack_bits & 0b001, 0b001, "self ack added");
    }

    #[test]
    fn returning_train_two_lap_then_recycle() {
        // Two-lap scheme:
        //   Lap-1 return: deliver locally + forward train UNCHANGED so
        //                 successors see + deliver too.
        //   Lap-2 return: every node has delivered → recycle (bump clock).
        // Prime issuer 1's seenClk so all-issuers gate passes.
        let mut node = TrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        let initial = node.issue_initial_train();
        assert_eq!(initial.clock, 1);
        assert_eq!(initial.issuer, 0);
        assert_eq!(initial.ack_bits, 0b001);

        // Echo a train from issuer 1 at clock=1 to advance seenClk[0][1].
        node.step(Input::TrainReceived(Train {
            issuer: 1, clock: 1, payloads: vec![], ack_bits: FULL_ACK,
        }));

        let returned = Train {
            issuer:   0,
            clock:    1,
            payloads: vec![payload(2, 0, b"hi")],
            ack_bits: FULL_ACK,
        };

        // Lap-1 return.
        let outs1 = node.step(Input::TrainReceived(returned.clone()));
        let deliveries: Vec<_> = outs1.iter().filter_map(|o| match o {
            Output::Deliver(p) => Some(p),
            _ => None,
        }).collect();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0][0].data, b"hi");

        let fwd1: Vec<&Train> = outs1.iter().filter_map(|o| match o {
            Output::ForwardTrain(t) => Some(t),
            _ => None,
        }).collect();
        assert_eq!(fwd1.len(), 1);
        assert_eq!(fwd1[0].clock, 1, "lap-1 return forwards UNCHANGED");
        assert_eq!(fwd1[0].payloads.len(), 1);

        // Lap-2 return: same key, already in doneKeys → recycle.
        let outs2 = node.step(Input::TrainReceived(returned));
        let fwd2: Vec<&Train> = outs2.iter().filter_map(|o| match o {
            Output::ForwardTrain(t) => Some(t),
            _ => None,
        }).collect();
        assert!(!outs2.iter().any(|o| matches!(o, Output::Deliver(_))),
                "no double delivery on lap-2");
        assert_eq!(fwd2.len(), 1);
        assert_eq!(fwd2[0].clock, 2, "RecycleTrain bumps issClk strictly");
        assert_eq!(fwd2[0].issuer, 0);
        assert_eq!(fwd2[0].payloads.len(), 0, "fresh train starts empty");
        assert_eq!(fwd2[0].ack_bits, 0b001, "issuer pre-acks");
    }

    #[test]
    fn out_of_order_arrival_parks_then_drains() {
        // Two-lap semantics: lap-1 sets seenClk; lap-2 attempts deliver.
        // Scenario: arrange laps such that on lap-2, (3,1) arrives
        // BEFORE (2,2) → (3,1) must park, then (2,2)'s lap-2 unblocks it.
        let mut node = TrainsNode::new(0, DeliveryMode::UniformTotalOrder);

        let t_31 = fully_acked_train(1, 3, vec![payload(1, 2, b"B")]);
        let t_22 = fully_acked_train(2, 2, vec![payload(2, 1, b"A")]);

        // Lap-1: send both trains so seenClk[1]=3, seenClk[2]=2.
        // (We've also acked them, but we don't deliver on lap-1.)
        node.step(Input::TrainReceived(t_31.clone()));
        node.step(Input::TrainReceived(t_22.clone()));
        assert_eq!(node.seen_clock(1), 3);
        assert_eq!(node.seen_clock(2), 2);

        // Lap-2: (3,1) arrives first. is_deliverable?
        // Smaller known keys: (1,1),(2,1),(3,1) from issuer 1 — but
        // (3,1) IS this key. (1,2),(2,2) from issuer 2 — both smaller
        // than (3,1) and not yet delivered → PARK.
        let outs1 = node.step(Input::TrainReceived(t_31));
        assert!(!outs1.iter().any(|o| matches!(o, Output::Deliver(_))),
                "(3,1) must wait for smaller keys");
        assert_eq!(node.parked_len(), 1);

        // Lap-2: (2,2) arrives. Smaller keys (1,*) all need to be
        // delivered, but we never saw any (1,0)/(1,1)/(1,2). seenClk
        // doesn't say (1,1) exists — actually seenClk[1]=3 means we
        // know (1,1),(2,1),(3,1) all exist. (1,1) and (2,1) are
        // smaller than (2,2) and not delivered → PARK.
        let outs2 = node.step(Input::TrainReceived(t_22));
        assert!(!outs2.iter().any(|o| matches!(o, Output::Deliver(_))),
                "(2,2) must wait for (1,1) and (2,1)");
        assert_eq!(node.parked_len(), 2);

        // For this synthetic test we won't drain — verifying parking
        // behaviour is the point. Drain is exercised in the integration
        // ring test where realistic train sequences supply all keys.
    }

    #[test]
    fn duplicate_train_not_redelivered() {
        // Three arrivals: lap-1 no deliver, lap-2 deliver, lap-3 no-op.
        // Prime self's clock so the all-issuers gate passes.
        let mut node = TrainsNode::new(0, DeliveryMode::UniformTotalOrder);
        let _ = node.issue_initial_train();
        node.step(Input::TrainReceived(Train {
            issuer: 0, clock: 1, payloads: vec![], ack_bits: FULL_ACK,
        }));

        let t = fully_acked_train(1, 1, vec![payload(1, 0, b"x")]);

        let n1 = node.step(Input::TrainReceived(t.clone())).iter()
            .filter(|o| matches!(o, Output::Deliver(_))).count();
        let n2 = node.step(Input::TrainReceived(t.clone())).iter()
            .filter(|o| matches!(o, Output::Deliver(_))).count();
        let n3 = node.step(Input::TrainReceived(t)).iter()
            .filter(|o| matches!(o, Output::Deliver(_))).count();

        assert_eq!(n1, 0, "lap-1: no deliver");
        assert_eq!(n2, 1, "lap-2: deliver");
        assert_eq!(n3, 0, "lap-3: no double-delivery");
    }
}
