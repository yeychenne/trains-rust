//! End-to-end test of the trace pipeline.
//!
//! Spawns the `trains` binary with `--trace`, then runs
//! `trains-trace-validate` on the resulting JSONL file. Asserts both
//! exit successfully and that the validator reports all six
//! invariants hold.
//!
//! This is an integration test (not a unit test) because it exercises
//! the binaries-as-installed; that's the contract the user actually
//! runs.

use std::process::Command;

fn cargo_bin(name: &str) -> std::path::PathBuf {
    // CARGO_BIN_EXE_<name> is set by Cargo for each [[bin]] entry of
    // the package containing the test. Both `trains` and
    // `trains-trace-validate` live in trains-cli, so this works.
    let env = format!("CARGO_BIN_EXE_{name}");
    std::path::PathBuf::from(std::env::var(&env)
        .unwrap_or_else(|_| panic!("missing env var {env}")))
}

#[test]
fn ring_demo_emits_trace_then_validates() {
    let trace_path = std::env::temp_dir()
        .join(format!("trains-test-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&trace_path);

    // 1. Run the ring demo with --trace.
    let demo = Command::new(cargo_bin("trains"))
        .args([
            "ring", "--num", "3", "--num-trains", "2", "--seconds", "3",
            "--broadcast", "0:hello",
            "--broadcast", "1:world",
            "--broadcast", "2:foo",
            "--trace",
        ])
        .arg(&trace_path)
        .output()
        .expect("failed to run trains ring");

    assert!(
        demo.status.success(),
        "trains ring failed: stdout={:?}\nstderr={:?}",
        String::from_utf8_lossy(&demo.stdout),
        String::from_utf8_lossy(&demo.stderr),
    );

    let stdout = String::from_utf8_lossy(&demo.stdout);
    assert!(stdout.contains("ConsistentDelivery: HOLDS"),
        "demo did not assert ConsistentDelivery: {}", stdout);

    // Trace file must exist + be non-empty.
    let metadata = std::fs::metadata(&trace_path)
        .expect("trace file not produced");
    assert!(metadata.len() > 0, "trace file is empty");

    // 2. Run the validator on the trace.
    let val = Command::new(cargo_bin("trains-trace-validate"))
        .arg(&trace_path)
        .output()
        .expect("failed to run trains-trace-validate");

    let stdout = String::from_utf8_lossy(&val.stdout);
    assert!(
        val.status.success(),
        "validator failed: stdout={}\nstderr={}",
        stdout,
        String::from_utf8_lossy(&val.stderr),
    );
    assert!(
        stdout.contains("all 6 invariants hold"),
        "validator output unexpected: {}", stdout,
    );

    let _ = std::fs::remove_file(&trace_path);
}
