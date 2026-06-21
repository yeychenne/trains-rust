//! PR-CORE-2: OS-process-level crash-masking proof.
//!
//! The in-process harnesses (`reconfig_integration`, `reconfig_wire_integration`)
//! prove the view-change protocol over the real transport, but no test ever
//! SIGKILLed a real `trains node` OS process. This one does:
//!
//! 1. `trains keygen` ×3 into a tempdir, capturing each fingerprint.
//! 2. Spawn three `trains node` OS processes (full `--peer-addr` topology,
//!    `--delivery-mode to`, stdin/stdout/stderr piped).
//! 3. Phase 1: 5 broadcasts per node via stdin; wait until every node
//!    delivers all 15.
//! 4. SIGKILL node 2 (`Child::kill`).
//! 5. Phase 2: 5 more broadcasts into each survivor; assert both survivors
//!    deliver all 10 phase-2 payloads within the deadline, in identical
//!    (from, seq) order — the crash is masked, total order preserved.
//!
//! Detection path under test: node 2's death closes its listener, node 1's
//! connector accumulates `UNREACHABLE_FAILURES` refused connects (~7.5 s of
//! backoff), `unreachable_rx` fires, node 1 confirms the crash, retargets to
//! node 0, and the Gather/Install view-change tokens circulate the re-formed
//! 2-ring; issuers reissue and delivery resumes.

// Test-harness style: line-oriented process plumbing reads clearer with
// explicit loops/indexing than with iterator chains.
#![allow(clippy::needless_range_loop)]

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use trains_core::{NUM_TRAINS, RING_SIZE};

const BIN: &str = env!("CARGO_BIN_EXE_trains");
const POLL: Duration = Duration::from_millis(50);

// Liveness budgets, NOT correctness. Locally the whole test runs in ~8 s (the
// ~7.5 s unreachable-backoff before detection dominates). On a loaded CI runner
// the same real-wall-clock waits can stretch several-fold (process scheduling,
// 3 OS processes + TLS ring formation), which previously timed these out and
// produced spurious failures. These are sized with generous headroom over the
// local baseline so a starved runner still passes; the safety assertions
// (identical phase-2 delivery order, no duplicates) are unchanged — widening a
// timeout cannot make a masked crash look masked when it isn't.
const READY_DEADLINE: Duration = Duration::from_secs(30);
const PHASE1_DEADLINE: Duration = Duration::from_secs(60);
const PHASE2_DEADLINE: Duration = Duration::from_secs(120);

/// One spawned `trains node` OS process with its piped streams.
struct NodeProc {
    child: Child,
    /// `None` after the process is killed (drops the pipe).
    stdin: Option<ChildStdin>,
    stdout: Arc<Mutex<Vec<String>>>,
    stderr: Arc<Mutex<Vec<String>>>,
}

impl NodeProc {
    fn send_line(&mut self, line: &str) {
        let stdin = self.stdin.as_mut().expect("stdin already closed");
        writeln!(stdin, "{line}").expect("write to node stdin");
        stdin.flush().expect("flush node stdin");
    }

    /// SIGKILL the process (and reap it).
    fn kill(&mut self) {
        self.stdin.take(); // close the pipe
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Kills every child on drop so a panicking test leaves no orphans.
struct Ring(Vec<NodeProc>);

impl Drop for Ring {
    fn drop(&mut self) {
        for node in &mut self.0 {
            node.kill();
        }
    }
}

/// Collect lines from a pipe into a shared log on a background thread.
fn spawn_line_collector<R: std::io::Read + Send + 'static>(
    pipe: R,
) -> Arc<Mutex<Vec<String>>> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let log2 = log.clone();
    std::thread::spawn(move || {
        for line in BufReader::new(pipe).lines() {
            match line {
                Ok(l) => log2.lock().unwrap().push(l),
                Err(_) => break,
            }
        }
    });
    log
}

/// `trains keygen --out <path>` → fingerprint hex parsed from stdout.
fn keygen(out: &Path) -> String {
    let output = Command::new(BIN)
        .args(["keygen", "--out"])
        .arg(out)
        .output()
        .expect("run trains keygen");
    assert!(output.status.success(), "keygen failed: {output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find_map(|l| l.strip_prefix("fingerprint: "))
        .unwrap_or_else(|| panic!("no fingerprint in keygen output: {stdout}"))
        .trim()
        .to_string()
}

/// Three free localhost ports (bind-then-drop; all bound before any is
/// dropped to avoid handing out the same port twice).
fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port"))
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect()
}

/// A `[<id>] DELIVER from=<sender> seq=<seq> "<payload>"` stdout line.
fn parse_deliver(line: &str) -> Option<(u8, u64, String)> {
    let rest = line.split(" DELIVER from=").nth(1)?;
    let (from, rest) = rest.split_once(" seq=")?;
    let (seq, payload) = rest.split_once(' ')?;
    let payload = payload.trim().strip_prefix('"')?.strip_suffix('"')?;
    Some((from.parse().ok()?, seq.parse().ok()?, payload.to_string()))
}

/// All deliveries seen so far on `node` whose payload satisfies `pred`,
/// in stdout (= delivery) order.
fn deliveries_matching(
    node: &NodeProc,
    pred: impl Fn(&str) -> bool,
) -> Vec<(u8, u64, String)> {
    node.stdout
        .lock()
        .unwrap()
        .iter()
        .filter_map(|l| parse_deliver(l))
        .filter(|(_, _, p)| pred(p))
        .collect()
}

/// Poll until `pred()` or the deadline; on timeout dump all logs and panic.
fn wait_until(ring: &Ring, deadline: Duration, what: &str, pred: impl Fn() -> bool) {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if pred() {
            return;
        }
        std::thread::sleep(POLL);
    }
    dump_logs(ring);
    panic!("timed out after {deadline:?} waiting for: {what}");
}

fn dump_logs(ring: &Ring) {
    for (id, node) in ring.0.iter().enumerate() {
        eprintln!("===== node {id} stdout =====");
        for l in node.stdout.lock().unwrap().iter() {
            eprintln!("{l}");
        }
        eprintln!("===== node {id} stderr =====");
        for l in node.stderr.lock().unwrap().iter() {
            eprintln!("{l}");
        }
    }
}

fn spawn_node(
    id: usize,
    ports: &[u16],
    identity: &Path,
    fps: &str,
) -> NodeProc {
    let n = ports.len();
    let mut cmd = Command::new(BIN);
    cmd.args(["node", "--id", &id.to_string()])
        .args(["--listen", &format!("127.0.0.1:{}", ports[id])])
        .args(["--successor", &format!("127.0.0.1:{}", ports[(id + 1) % n])])
        .arg("--identity")
        .arg(identity)
        .args(["--peer-fp", fps])
        .args(["--delivery-mode", "to"]);
    for (peer, port) in ports.iter().enumerate() {
        cmd.args(["--peer-addr", &format!("{peer}=127.0.0.1:{port}")]);
    }
    if id < NUM_TRAINS {
        cmd.arg("--issue-initial");
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn trains node");
    let stdin = child.stdin.take().unwrap();
    let stdout = spawn_line_collector(child.stdout.take().unwrap());
    let stderr = spawn_line_collector(child.stderr.take().unwrap());
    NodeProc { child, stdin: Some(stdin), stdout, stderr }
}

/// PR-CORE-2 / DE-review B2: a permanent peer crash (real SIGKILL of a real
/// OS process) is masked — both survivors keep delivering, in identical order.
///
/// `#[ignore]` by default: this spawns 3 OS processes + a TLS ring and depends
/// on a ~7.5 s real-wall-clock detection backoff, which intermittently times out
/// on starved shared CI runners (false failures that block unrelated PRs). It is
/// NOT skipped in CI — the dedicated, retried `live-crash` job runs it with
/// `--ignored` (see `.github/workflows/ci.yml`), and the deterministic in-process
/// `reconfig_wire_integration`/`reconfig_integration` tests cover the same
/// view-change protocol in the always-on `test` job. Run locally with:
///   cargo test -p trains-cli --test live_crash_masking -- --ignored
#[test]
#[ignore = "OS-process + timing-sensitive; run via the dedicated retried live-crash CI job or --ignored"]
fn sigkill_of_live_node_process_is_masked() {
    assert_eq!(
        RING_SIZE, 3,
        "test assumes the default trains-core build (TRAINS_RING_SIZE=3)"
    );
    assert_eq!(
        NUM_TRAINS, 2,
        "test assumes the default trains-core build (TRAINS_NUM_TRAINS=2)"
    );

    // Unique tempdir (no tempfile dep in this crate).
    let dir: PathBuf = std::env::temp_dir().join(format!(
        "trains-live-crash-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir_all(&dir).unwrap();

    // 1. Identities + fingerprints.
    let identities: Vec<PathBuf> =
        (0..RING_SIZE).map(|i| dir.join(format!("node{i}.json"))).collect();
    let fps: Vec<String> = identities.iter().map(|p| keygen(p)).collect();
    let fps_joined = fps.join(",");

    // 2. Ports + 3. spawn the ring.
    let ports = free_ports(RING_SIZE);
    let mut ring = Ring(
        (0..RING_SIZE)
            .map(|i| spawn_node(i, &ports, &identities[i], &fps_joined))
            .collect(),
    );

    // Wait for all three processes to come up.
    wait_until(&ring, READY_DEADLINE, "all nodes ready", || {
        ring.0.iter().all(|n| {
            n.stderr.lock().unwrap().iter().any(|l| l.contains("node ready"))
        })
    });

    // 4. Phase 1: 5 broadcasts per node; expect all 15 delivered everywhere.
    let per_node = 5usize;
    for i in 0..RING_SIZE {
        for k in 0..per_node {
            ring.0[i].send_line(&format!("p1-n{i}-{k}"));
        }
    }
    let phase1_total = RING_SIZE * per_node;
    wait_until(
        &ring,
        PHASE1_DEADLINE,
        "phase-1: all 3 nodes deliver all 15 broadcasts",
        || {
            ring.0.iter().all(|n| {
                deliveries_matching(n, |p| p.starts_with("p1-")).len() == phase1_total
            })
        },
    );

    // 5. SIGKILL node 2 (a non-issuer; its successor edge 1→2 dies with it).
    let kill_at = Instant::now();
    ring.0[2].kill();
    eprintln!("--- node 2 SIGKILLed ---");

    // 6. Phase 2: broadcasts into the survivors only.
    for i in 0..2 {
        for k in 0..per_node {
            ring.0[i].send_line(&format!("p2-n{i}-{k}"));
        }
    }
    let phase2_total = 2 * per_node;
    wait_until(
        &ring,
        PHASE2_DEADLINE,
        "phase-2: both survivors deliver all 10 post-kill broadcasts",
        || {
            ring.0[..2].iter().all(|n| {
                deliveries_matching(n, |p| p.starts_with("p2-")).len() == phase2_total
            })
        },
    );
    eprintln!(
        "--- crash masked: all phase-2 payloads delivered {:.2}s after SIGKILL ---",
        kill_at.elapsed().as_secs_f64()
    );

    // Identical delivery order on both survivors (ConsistentDelivery).
    let order0 = deliveries_matching(&ring.0[0], |p| p.starts_with("p2-"));
    let order1 = deliveries_matching(&ring.0[1], |p| p.starts_with("p2-"));
    if order0 != order1 {
        dump_logs(&ring);
        panic!(
            "survivors disagree on phase-2 delivery order:\n  node0: {order0:?}\n  node1: {order1:?}"
        );
    }

    // No duplicate deliveries snuck in via the recovery drain.
    let mut uniq: Vec<(u8, u64)> = order0.iter().map(|(f, s, _)| (*f, *s)).collect();
    uniq.sort_unstable();
    uniq.dedup();
    assert_eq!(uniq.len(), phase2_total, "duplicate phase-2 deliveries: {order0:?}");

    // 7. Ring's Drop kills the survivors; best-effort tempdir cleanup.
    drop(ring);
    let _ = std::fs::remove_dir_all(&dir);
}
