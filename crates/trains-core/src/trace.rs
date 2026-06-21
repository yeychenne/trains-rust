//! Per-step trace records for offline / runtime invariant checking.
//!
//! Every `TrainsNode::step()` can optionally record a snapshot of its
//! relevant state slice, the input action, and the produced outputs.
//! A separate validator (`trains-trace-validate`) consumes the trace
//! and re-checks the same six invariants TLC verifies on the spec
//! (`TypeOK`, `ClockMonotonicity`, `ConsistentDelivery`,
//! `NoSpuriousDelivery`, `TrainIntegrity`, `IssuerUniqueness`).
//!
//! The intent is **runtime refinement-correspondence**: an
//! implementation step that produces a state violating any of those
//! invariants is direct evidence the implementation has drifted from
//! the spec — even though we have no mechanical refinement proof.

use serde::{Deserialize, Serialize};

use crate::types::{Payload, ProcId, Tick, Train};
use crate::Output;

/// Logical timestamp on a trace record — incremented monotonically on
/// every emission, regardless of which node emitted.
pub type TraceSeq = u64;

/// Compact summary of a `Train` for trace records. Captures everything
/// the validator needs (issuer, clock, ack_bits, payload identities)
/// without the raw payload data bytes — keeping trace files tractable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainSummary {
    pub issuer:      ProcId,
    pub clock:       Tick,
    pub ack_bits:    u32,
    /// `(sender, seq)` of every payload on the train (no data bytes).
    pub payload_keys: Vec<(ProcId, u64)>,
}

impl From<&Train> for TrainSummary {
    fn from(t: &Train) -> Self {
        Self {
            issuer:   t.issuer,
            clock:    t.clock,
            ack_bits: t.ack_bits,
            payload_keys: t.payloads.iter().map(|p| (p.sender, p.seq)).collect(),
        }
    }
}

/// Compact summary of a delivered `Payload` — same rationale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadKey {
    pub sender: ProcId,
    pub seq:    u64,
}

impl From<&Payload> for PayloadKey {
    fn from(p: &Payload) -> Self {
        Self { sender: p.sender, seq: p.seq }
    }
}

/// What kind of input drove this step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceAction {
    /// `Input::LocalBroadcast` — data length only (no bytes).
    Broadcast { data_len: usize },
    /// `Input::TrainReceived(t)`.
    TrainArrived { train: TrainSummary },
    /// `Input::Tick`.
    Tick,
    /// Synthesised at startup: this issuer issued its initial train.
    Initial { train: TrainSummary },
}

/// What outputs the step produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceOutput {
    Forward { train: TrainSummary },
    Deliver { payloads: Vec<PayloadKey> },
    DeclareCrash { victim: ProcId },
}

impl From<&Output> for TraceOutput {
    fn from(o: &Output) -> Self {
        match o {
            Output::ForwardTrain(t)  => TraceOutput::Forward { train: TrainSummary::from(t) },
            Output::Deliver(p)       => TraceOutput::Deliver {
                payloads: p.iter().map(PayloadKey::from).collect(),
            },
            Output::DeclareCrash(v)  => TraceOutput::DeclareCrash { victim: *v },
        }
    }
}

/// Snapshot of one node's relevant state, post-step.
///
/// Note: we deliberately do NOT include the full `delivered` log here —
/// the validator reconstructs it by accumulating `Deliver` outputs as
/// it walks the trace. Including `delivered` per record would make the
/// trace size O(records × log_length) which becomes hundreds of MB
/// for any non-trivial run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeState {
    pub id:            ProcId,
    pub seen_clk:      Vec<Tick>,        // [Tick; RING_SIZE] flattened
    pub iss_clk:       Tick,             // next clock to issue (TLA+: issClk[self])
    pub pending_count: usize,
    /// Number of `(clock, issuer)` keys this node has marked done.
    /// Stored as a count (not a list) to keep the trace bounded:
    /// done_keys grows linearly with empty-train cycles, and dumping
    /// the full list inflates the trace by GiB/sec.
    pub done_keys_count: usize,
    /// Length of the per-node delivered log post-step.
    pub delivered_len: usize,
}

/// One emission. Bundles input + outputs + post-step node state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceRecord {
    pub seq:     TraceSeq,
    pub node:    ProcId,
    pub action:  TraceAction,
    pub outputs: Vec<TraceOutput>,
    pub state:   NodeState,
}

/// JSON-Lines on-disk format: one [`TraceRecord`] per line.
pub fn write_jsonl<W: std::io::Write>(rec: &TraceRecord, w: &mut W) -> std::io::Result<()> {
    let line = serde_json::to_string(rec)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    w.write_all(line.as_bytes())?;
    w.write_all(b"\n")
}

/// Read a trace from JSONL. Returns the parsed records in file order.
pub fn read_jsonl<R: std::io::BufRead>(r: R) -> std::io::Result<Vec<TraceRecord>> {
    let mut out = Vec::new();
    for (i, line) in r.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() { continue; }
        let rec: TraceRecord = serde_json::from_str(&line).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("trace line {}: {}", i + 1, e),
            )
        })?;
        out.push(rec);
    }
    Ok(out)
}
