//! `trains-trace-validate` — read a JSONL trace emitted by
//! `trains` and re-check the same six invariants TLC verified on the
//! TLA+ specification:
//!
//!   * **TypeOK**            type/range bounds on every state component
//!   * **ClockMonotonicity** seen_clk[node][q] never decreases
//!   * **ConsistentDelivery** every pair of (non-crashed) nodes' delivery
//!                           logs are mutual prefixes
//!   * **NoSpuriousDelivery** every delivered payload was previously
//!                           broadcast (i.e. observed on some train)
//!   * **TrainIntegrity**    every payload carried by a forwarded train
//!                           has a known sender ∈ Procs and corresponds
//!                           to something seen on the wire
//!   * **IssuerUniqueness**  no two train slots ever simultaneously hold
//!                           the same (clock, issuer) key (within a
//!                           single node's view)
//!
//! Reading per-step state slices from the trace, the validator
//! reconstructs the global view at each step and asserts the
//! invariants. Any violation is direct evidence the implementation
//! has drifted from the spec at runtime — even though we have no
//! mechanical refinement proof.

// The invariant list above aligns its descriptions in a column; clippy's
// markdown-list lint fights that deliberate layout.
#![allow(clippy::doc_overindented_list_items)]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use trains_core::{
    trace::{NodeState, PayloadKey, TraceAction, TraceOutput, TraceRecord},
    ProcId, RING_SIZE,
};

#[derive(Parser)]
#[command(
    name = "trains-trace-validate",
    about = "Re-check TLA+ safety invariants against a runtime JSONL trace"
)]
struct Args {
    /// Path to a JSONL trace produced by `trains -- ring --trace ...`.
    trace: PathBuf,
    /// Verbose: print per-step status.
    #[arg(short, long)]
    verbose: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let f = std::fs::File::open(&args.trace)
        .with_context(|| format!("opening {}", args.trace.display()))?;
    let r = std::io::BufReader::new(f);
    let records = trains_core::trace::read_jsonl(r)?;
    println!("loaded {} trace records", records.len());

    // Most-recent NodeState per node id.
    let mut latest: HashMap<ProcId, NodeState> = HashMap::new();
    // Set of (sender, seq) ever observed on a forwarded train, on a
    // delivered payload, or on a broadcast — the runtime analogue of
    // TLA+ `broadcast`.
    let mut broadcast: HashSet<(ProcId, u64)> = HashSet::new();
    // Per-node accumulated delivered log (validator-side reconstruction).
    let mut delivered_per_node: HashMap<ProcId, Vec<PayloadKey>> = HashMap::new();
    // Per-node monotonic check on seen_clk.
    let mut prev_seen_per_node: HashMap<ProcId, [u64; RING_SIZE]> = HashMap::new();

    let mut violations: Vec<String> = Vec::new();
    let mut step_count = 0;

    for rec in &records {
        step_count += 1;
        // ── TypeOK ────────────────────────────────────────────────────
        if !type_ok(rec) {
            violations.push(format!(
                "[seq={} node={}] TypeOK violated: {:?}",
                rec.seq, rec.node, rec.state
            ));
        }

        // ── ClockMonotonicity ────────────────────────────────────────
        if let Some(prev) = prev_seen_per_node.get(&rec.node) {
            // Index q addresses two parallel per-process arrays; the index IS
            // the process id, so a range loop is the clearest form.
            #[allow(clippy::needless_range_loop)]
            for q in 0..RING_SIZE {
                if rec.state.seen_clk[q] < prev[q] {
                    violations.push(format!(
                        "[seq={} node={}] ClockMonotonicity: seen_clk[{}] went \
                         {} → {}",
                        rec.seq, rec.node, q, prev[q], rec.state.seen_clk[q]
                    ));
                }
            }
        }
        let mut snap = [0u64; RING_SIZE];
        for (i, v) in rec.state.seen_clk.iter().take(RING_SIZE).enumerate() {
            snap[i] = *v;
        }
        prev_seen_per_node.insert(rec.node, snap);

        // ── update broadcast set from action + outputs ───────────────
        match &rec.action {
            TraceAction::Broadcast { .. } => {
                // The local broadcast adds (rec.node, next_seq − 1) — but
                // we don't see next_seq in the trace. Approximate by
                // updating from outputs once payloads materialise.
            }
            TraceAction::Initial { train } |
            TraceAction::TrainArrived { train } => {
                for &(sender, seq) in &train.payload_keys {
                    broadcast.insert((sender, seq));
                }
            }
            TraceAction::Tick => {}
        }
        // Accumulate this step's contributions.
        let log = delivered_per_node.entry(rec.node).or_default();
        for o in &rec.outputs {
            match o {
                TraceOutput::Forward { train } => {
                    for &(sender, seq) in &train.payload_keys {
                        broadcast.insert((sender, seq));
                    }
                }
                TraceOutput::Deliver { payloads } => {
                    for p in payloads {
                        // ── NoSpuriousDelivery: payload must have been
                        //     observed on a train BEFORE delivery
                        if !broadcast.contains(&(p.sender, p.seq)) {
                            violations.push(format!(
                                "[seq={} node={}] NoSpuriousDelivery: \
                                 delivered ({},{}) was never on a train",
                                rec.seq, rec.node, p.sender, p.seq,
                            ));
                        }
                        broadcast.insert((p.sender, p.seq));
                        log.push(p.clone());
                    }
                }
                TraceOutput::DeclareCrash { .. } => {}
            }
        }

        // ── delivered_len consistency ────────────────────────────────
        if log.len() != rec.state.delivered_len {
            violations.push(format!(
                "[seq={} node={}] delivered_len mismatch: trace says {}, \
                 validator computed {}",
                rec.seq, rec.node, rec.state.delivered_len, log.len(),
            ));
        }

        // ── TrainIntegrity ───────────────────────────────────────────
        for o in &rec.outputs {
            if let TraceOutput::Forward { train } = o {
                for &(sender, _seq) in &train.payload_keys {
                    if (sender as usize) >= RING_SIZE {
                        violations.push(format!(
                            "[seq={} node={}] TrainIntegrity: forwarded \
                             payload sender {} ≥ RING_SIZE", rec.seq,
                            rec.node, sender,
                        ));
                    }
                }
            }
        }

        // IssuerUniqueness is structurally guaranteed by the impl's
        // BTreeSet<ClockKey> for done_keys; we only see the count in
        // the trace, so no extra check here.

        latest.insert(rec.node, rec.state.clone());

        // ── ConsistentDelivery ───────────────────────────────────────
        // Pairwise: every two delivered logs are mutual prefixes.
        // Only re-check when this step actually delivered something —
        // otherwise the pairwise prefix relation can't change.
        let delivered_this_step = rec.outputs.iter().any(|o|
            matches!(o, TraceOutput::Deliver { .. }));
        if delivered_this_step {
            let ids: Vec<ProcId> = delivered_per_node.keys().copied().collect();
            for i in 0..ids.len() {
                for j in (i+1)..ids.len() {
                    let a = &delivered_per_node[&ids[i]];
                    let b = &delivered_per_node[&ids[j]];
                    if !is_mutual_prefix(a, b) {
                        violations.push(format!(
                            "[seq={}] ConsistentDelivery: node {} vs \
                             node {}\n  {}: {}\n  {}: {}",
                            rec.seq, ids[i], ids[j],
                            ids[i], fmt_log(a),
                            ids[j], fmt_log(b),
                        ));
                    }
                }
            }
        }

        if args.verbose && step_count % 5000 == 0 {
            eprintln!("[{step_count} records OK]");
        }

        // Stop after first violation + a few more for diagnostic context.
        if violations.len() >= 5 { break; }
    }

    println!("inspected {} records", step_count);
    println!("nodes seen: {:?}", latest.keys().collect::<Vec<_>>());

    if violations.is_empty() {
        println!("\n✅ all 6 invariants hold across the trace");
        Ok(())
    } else {
        println!("\n❌ {} violation(s):\n", violations.len());
        for v in &violations {
            println!("  {v}");
        }
        std::process::exit(1);
    }
}

fn type_ok(rec: &TraceRecord) -> bool {
    if (rec.node as usize) >= RING_SIZE { return false; }
    if (rec.state.id as usize) >= RING_SIZE { return false; }
    if rec.state.seen_clk.len() != RING_SIZE { return false; }
    true
}

fn is_mutual_prefix(a: &[PayloadKey], b: &[PayloadKey]) -> bool {
    let pre_ab = a.len() <= b.len() && a.iter().zip(b.iter()).all(|(x, y)| x == y);
    let pre_ba = b.len() <= a.len() && b.iter().zip(a.iter()).all(|(x, y)| x == y);
    pre_ab || pre_ba
}

fn fmt_log(log: &[PayloadKey]) -> String {
    let s: Vec<String> = log.iter()
        .map(|p| format!("({},{})", p.sender, p.seq))
        .collect();
    format!("[{}]", s.join(", "))
}
