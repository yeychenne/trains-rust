#!/usr/bin/env bash
# Run the in-process benchmark suite and emit one JSONL file of results.
#
# Covers all four in-process harnesses across the standard payload sweep
# (64 B / 1 KiB / 16 KiB), N trials each (median is taken when reading):
#   - bench_kernel          TrainsNode::step ceiling, no I/O
#   - bench_ring_tls        real trains-net TLS ring (N=3), loopback
#   - bench_raft_baseline   hand-rolled leader→majority commit over the same mTLS
#   - bench_raft_openraft   REAL openraft 0.9 cluster (N=3), in-process network
#
# Cross-host (Tailscale/Ethernet) numbers come from the trains-valkey repo's
# multi-machine driver, not this script.
#
# Usage:
#   scripts/run_benches.sh [TRIALS] [OUT.jsonl]
# Defaults: 3 trials → benches/results/<YYYY-MM-DD>.jsonl
set -euo pipefail

cd "$(dirname "$0")/.."

TRIALS="${1:-3}"
OUT="${2:-benches/results/$(date +%F).jsonl}"
PAYLOADS=(64 1024 16384)
MSGS=2000

echo "building release benches..." >&2
cargo build --release -p trains-benches >&2

mkdir -p "$(dirname "$OUT")"
: > "$OUT"

run() {  # run <binary> <extra-args...>
    local bin="$1"; shift
    "./target/release/$bin" "$@" 2>/dev/null | grep '^{' >> "$OUT"
}

for t in $(seq 1 "$TRIALS"); do
    echo "trial $t/$TRIALS" >&2
    for p in "${PAYLOADS[@]}"; do
        run bench_kernel        --target-messages "$MSGS" --payload "$p"
        run bench_ring_tls      --target-messages "$MSGS" --payload "$p"
        run bench_raft_baseline --target-messages "$MSGS" --payload "$p"
    done
    # The openraft bench sweeps payloads itself (one cluster, all sizes).
    run bench_raft_openraft --target-messages "$MSGS" \
        --payloads "$(IFS=,; echo "${PAYLOADS[*]}")"
done

echo "wrote $(grep -c '^{' "$OUT") result rows to $OUT" >&2
