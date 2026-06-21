// Emit RING_SIZE and NUM_TRAINS constants at build time.
//
// Set env vars before `cargo build` (or `cross build`) to produce a
// binary tuned for a specific ring configuration:
//
//   TRAINS_RING_SIZE=10 TRAINS_NUM_TRAINS=3 cargo build --release
//
// If the env vars are absent, the defaults (3 / 2) are used, matching
// the TLA+ model and the existing test suite.
fn main() {
    let ring_size: usize = std::env::var("TRAINS_RING_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let num_trains: usize = std::env::var("TRAINS_NUM_TRAINS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    assert!(
        (2..=30).contains(&ring_size),
        "TRAINS_RING_SIZE must be 2..=30 (got {ring_size})"
    );
    assert!(
        num_trains >= 1 && num_trains <= ring_size,
        "TRAINS_NUM_TRAINS must be 1..=RING_SIZE (got {num_trains}, RING_SIZE={ring_size})"
    );

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let code = format!(
        "pub const RING_SIZE: usize = {ring_size};\npub const NUM_TRAINS: usize = {num_trains};\n"
    );
    std::fs::write(format!("{out_dir}/ring_config.rs"), code).unwrap();

    println!("cargo:rerun-if-env-changed=TRAINS_RING_SIZE");
    println!("cargo:rerun-if-env-changed=TRAINS_NUM_TRAINS");
}
