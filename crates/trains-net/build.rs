// Emit MAX_FRAME_LEN at build time.
//
// Set TRAINS_MAX_FRAME_LEN_MB before `cargo build` (or `cross build`)
// to tune the maximum accepted frame size to the deployment's NIC
// capacity. The constant caps both per-train batch size and individual
// payload sizes; setting it too low truncates bursty workloads, setting
// it too high lets a single train monopolize ring time and inflates
// tail latency.
//
// Rule of thumb (operator-facing):
//   NIC capacity ≤   5 Gbps  →  16 MB  (default — matches t4g.medium)
//   NIC capacity ≤  25 Gbps  →  64 MB  (c7i.16xlarge + ENA Express)
//   NIC capacity ≤ 100 Gbps  → 256 MB  (c7gn.8xlarge + ENA Express)
//
// These numbers approximate ~5× the NIC's per-train-cycle throughput
// at typical TRAINS round-trip times (3-10 ms), leaving headroom for
// startup-burst absorption without locking the ring out for too long.
//
// If the env var is absent, the default (16 MiB) is used, matching
// the constant value before 2026-05-23.
fn main() {
    let max_frame_len_mb: u32 = std::env::var("TRAINS_MAX_FRAME_LEN_MB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);

    assert!(
        (1..=4096).contains(&max_frame_len_mb),
        "TRAINS_MAX_FRAME_LEN_MB must be 1..=4096 MiB (got {max_frame_len_mb})"
    );

    let max_frame_len: u64 = (max_frame_len_mb as u64) * 1024 * 1024;
    assert!(
        max_frame_len <= u32::MAX as u64,
        "TRAINS_MAX_FRAME_LEN_MB={max_frame_len_mb} overflows u32 frame length"
    );

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let code = format!(
        "/// Maximum accepted frame length. Compile-time tunable via\n\
         /// TRAINS_MAX_FRAME_LEN_MB (default 16 MiB). See `build.rs`.\n\
         pub const MAX_FRAME_LEN: u32 = {max_frame_len};\n"
    );
    std::fs::write(format!("{out_dir}/frame_config.rs"), code).unwrap();

    println!("cargo:rerun-if-env-changed=TRAINS_MAX_FRAME_LEN_MB");
}
