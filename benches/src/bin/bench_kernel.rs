//! Kernel-only throughput benchmark.
//!
//! Drives `TrainsNode::step` directly in a single thread, simulating
//! the ring in memory. No I/O, no tokio, no allocation beyond what
//! `step()` returns. This measures the *ceiling* of what the protocol
//! kernel can do on this hardware.
//!
//! ## Methodology
//! 1. Build `RING_SIZE` (= 3) `TrainsNode`s, mode = UTO.
//! 2. Seed `NUM_TRAINS` (= 2) initial trains by calling
//!    `issue_initial_train()` on the first `NUM_TRAINS` nodes.
//! 3. Run a round-robin scheduler:
//!      - At each tick, for each node, drain its inbox and call `step()`.
//!      - Route every `ForwardTrain(t)` output to the successor's inbox.
//!      - Tally every `Deliver(payloads)` output.
//!    Concurrently, inject `LocalBroadcast(payload)` at a chosen node
//!    every K kernel steps until `target_broadcasts` payloads have
//!    been queued.
//! 4. Stop once every node's delivery count reaches `target_broadcasts`
//!    (or a wall-clock timeout fires; that's a failure of the bench
//!    setup, not the protocol).
//! 5. Print msgs/sec, bytes/sec, and `step()` calls/sec.
//!
//! ## Why one binary, not a parameter sweep
//! `NUM_TRAINS` and `RING_SIZE` are compile-time consts in
//! `trains-core/src/types.rs`. To sweep them, `scripts/run_benches.sh`
//! recompiles this binary per cell.
//!
//! ## Output schema (stdout, one JSON line)
//!   { "kind": "kernel", "ring_size": N, "num_trains": K, "payload": P,
//!     "messages": M, "wall_us": T, "msgs_per_sec": X,
//!     "bytes_per_sec": Y, "steps": S, "steps_per_sec": Z }

// The doc block above uses deliberate column alignment that clippy's
// markdown-list lint fights; keep the prose readable instead.
#![allow(clippy::doc_lazy_continuation)]

use std::collections::VecDeque;
use std::time::Instant;

use clap::Parser;
use serde::Serialize;
use trains_core::{
    DeliveryMode, Input, Output, ProcId, Train, TrainsNode, NUM_TRAINS, RING_SIZE,
};

#[derive(Parser)]
struct Args {
    /// How many payloads each node should observe before we stop timing.
    #[arg(long, default_value_t = 5_000)]
    target_messages: usize,
    /// Payload size in bytes (filled with 0x42).
    #[arg(long, default_value_t = 1024)]
    payload: usize,
    /// Inject one local broadcast at node 0 every N kernel events.
    /// Smaller = more pressure. Default tuned so the ring stays full.
    #[arg(long, default_value_t = 1)]
    broadcast_every: usize,
    /// Print machine-readable JSON to stdout (in addition to human prose).
    #[arg(long, default_value_t = true)]
    json: bool,
}

#[derive(Serialize)]
struct Report {
    kind:           &'static str,
    ring_size:      usize,
    num_trains:     usize,
    payload:        usize,
    messages:       usize,
    wall_us:        u128,
    msgs_per_sec:   f64,
    bytes_per_sec:  f64,
    steps:          u64,
    steps_per_sec:  f64,
}

fn main() {
    let args = Args::parse();

    // ── Build the ring ──────────────────────────────────────────────
    let mut nodes: Vec<TrainsNode> = (0..RING_SIZE)
        .map(|i| TrainsNode::new(i as ProcId, DeliveryMode::UniformTotalOrder))
        .collect();

    // Per-node inbox of pending Train arrivals.
    let mut inbox: Vec<VecDeque<Train>> = (0..RING_SIZE).map(|_| VecDeque::new()).collect();

    // Per-node delivery counter (number of payloads delivered to that node).
    let mut delivered: Vec<usize> = vec![0; RING_SIZE];

    // ── Seed initial trains ─────────────────────────────────────────
    // Each issuer at position `i` issues a train whose first hop is
    // node `(i + 1) % RING_SIZE`.
    for i in 0..NUM_TRAINS {
        let t = nodes[i].issue_initial_train();
        inbox[(i + 1) % RING_SIZE].push_back(t);
    }

    // ── Workload generator ──────────────────────────────────────────
    // We inject `target_messages` broadcasts at node 0, then stop
    // injecting and let the ring drain.
    let payload_bytes = vec![0x42u8; args.payload];
    let mut to_inject: usize = args.target_messages;

    // ── Pump the simulator ──────────────────────────────────────────
    let mut steps: u64 = 0;
    let start = Instant::now();
    let timeout = std::time::Duration::from_secs(120);
    let mut tick_counter: usize = 0;
    let mut periodic_tick_counter: usize = 0;

    loop {
        // Termination: every node has reached the target.
        if delivered.iter().all(|&d| d >= args.target_messages) {
            break;
        }
        if start.elapsed() > timeout {
            eprintln!(
                "TIMEOUT after {:?}; delivered = {:?}, to_inject = {}",
                start.elapsed(), delivered, to_inject,
            );
            std::process::exit(1);
        }

        let mut work_done = false;

        // 1) Inject a broadcast on node 0 every `broadcast_every` steps.
        if to_inject > 0 && tick_counter.is_multiple_of(args.broadcast_every) {
            let outs = nodes[0].step(Input::LocalBroadcast(payload_bytes.clone()));
            steps += 1;
            route_outputs(0, outs, &mut inbox, &mut delivered);
            to_inject -= 1;
            work_done = true;
        }
        tick_counter = tick_counter.wrapping_add(1);

        // 2) Drain each inbox: one train per node per outer loop iteration.
        //    This emulates a fair round-robin scheduler.
        for n in 0..RING_SIZE {
            if let Some(t) = inbox[n].pop_front() {
                let outs = nodes[n].step(Input::TrainReceived(t));
                steps += 1;
                route_outputs(n, outs, &mut inbox, &mut delivered);
                work_done = true;
            }
        }

        // 3) If nothing happened, deliver a Tick to flush any pending
        //    state (drain parked etc.). Without this, an empty ring
        //    could stall after the last broadcast.
        if !work_done {
            // Spread the Tick across nodes round-robin so we don't
            // starve any of them.
            let n = periodic_tick_counter % RING_SIZE;
            periodic_tick_counter = periodic_tick_counter.wrapping_add(1);
            let outs = nodes[n].step(Input::Tick);
            steps += 1;
            route_outputs(n, outs, &mut inbox, &mut delivered);
        }
    }

    let wall = start.elapsed();
    let wall_us = wall.as_micros();
    let total_msgs = args.target_messages;
    let msgs_per_sec = total_msgs as f64 / wall.as_secs_f64();
    let bytes_per_sec = (total_msgs * args.payload) as f64 / wall.as_secs_f64();
    let steps_per_sec = steps as f64 / wall.as_secs_f64();

    let report = Report {
        kind: "kernel",
        ring_size: RING_SIZE,
        num_trains: NUM_TRAINS,
        payload: args.payload,
        messages: total_msgs,
        wall_us,
        msgs_per_sec,
        bytes_per_sec,
        steps,
        steps_per_sec,
    };

    eprintln!(
        "kernel: N={} K={} payload={}B → {:.0} msgs/sec, \
         {:.1} MiB/sec, {:.0} steps/sec ({} steps, {:.3}s)",
        RING_SIZE, NUM_TRAINS, args.payload,
        msgs_per_sec, bytes_per_sec / (1024.0 * 1024.0),
        steps_per_sec, steps, wall.as_secs_f64(),
    );

    if args.json {
        println!("{}", serde_json::to_string(&report).unwrap());
    }
}

#[inline]
fn route_outputs(
    from: usize,
    outs: Vec<Output>,
    inbox: &mut [VecDeque<Train>],
    delivered: &mut [usize],
) {
    let succ = (from + 1) % RING_SIZE;
    for o in outs {
        match o {
            Output::ForwardTrain(t) => inbox[succ].push_back(t),
            Output::Deliver(payloads) => delivered[from] += payloads.len(),
            Output::DeclareCrash(_) => {
                // No crash in this bench. Print and continue.
                eprintln!("warn: spurious DeclareCrash at node {}", from);
            }
        }
    }
}
