//! `trains` — CLI driver for a TRAINS ring node.
//!
//! Subcommands:
//!   * `keygen --out PATH`            generate self-signed identity, print fingerprint
//!   * `node --id N ...`              run a node, REPL on stdin
//!   * `ring --num N --num-trains M`  spawn N nodes in-process for a local demo

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use trains_core::DeliveryMode;

mod node;
mod ring;
// `failure_detector` and `view_change` live in the library target (src/lib.rs)
// so integration tests can drive them; the binary uses them via `trains_cli::`.

/// Delivery-condition mode, selectable on the CLI.
///
/// `uto` (default) is the strict benchmark/verification path (every node must
/// ack). `to` is uniform-within-the-surviving-view — required for crash
/// masking / reconfiguration (the live set is `FULL_ACK & !crashed`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum DeliveryModeArg {
    /// Uniform total order — ack from every node required.
    Uto,
    /// Total order within the surviving view — ack from every live node.
    To,
}

impl From<DeliveryModeArg> for DeliveryMode {
    fn from(m: DeliveryModeArg) -> Self {
        match m {
            DeliveryModeArg::Uto => DeliveryMode::UniformTotalOrder,
            DeliveryModeArg::To => DeliveryMode::TotalOrder,
        }
    }
}

#[derive(Parser)]
#[command(name = "trains", version, about = "TRAINS ring-protocol CLI")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate a self-signed identity and print its fingerprint.
    Keygen {
        /// Output path for the identity JSON file.
        #[arg(long)]
        out: PathBuf,
        /// DNS names to put in the cert (default: localhost).
        #[arg(long, default_value = "localhost")]
        sni: Vec<String>,
    },
    /// Run a single ring node, REPL on stdin.
    Node {
        #[arg(long)]
        id: u8,
        #[arg(long)]
        listen: std::net::SocketAddr,
        #[arg(long)]
        successor: std::net::SocketAddr,
        #[arg(long)]
        identity: PathBuf,
        /// Pinned peer fingerprint(s), comma-separated hex (64 chars each).
        #[arg(long)]
        peer_fp: String,
        /// Issue an initial train at startup (only set this on issuer nodes).
        #[arg(long)]
        issue_initial: bool,
        /// Delivery mode: `uto` (strict, default) or `to` (crash-masking).
        #[arg(long, value_enum, default_value_t = DeliveryModeArg::Uto)]
        delivery_mode: DeliveryModeArg,
        /// Ring topology for reconfiguration: repeat `--peer-addr <id>=<addr>`
        /// for EVERY node (including self). Enables the distributed view change
        /// (retarget past a crashed node). Omit to disable reconfiguration.
        #[arg(long = "peer-addr")]
        peer_addr: Vec<String>,
    },
    /// Spawn N nodes in-process and broadcast a few messages.
    Ring {
        /// Number of nodes (must equal RING_SIZE).
        #[arg(long, default_value_t = 3)]
        num: usize,
        /// Number of issuers (≤ num).
        #[arg(long, default_value_t = 2)]
        num_trains: usize,
        /// How many seconds to run.
        #[arg(long, default_value_t = 3)]
        seconds: u64,
        /// Broadcasts of the form "node:msg" (e.g., "0:hello").
        #[arg(long)]
        broadcast: Vec<String>,
        /// Optional path to write a JSONL trace of every step
        /// (consumed by `trains-trace-validate`).
        #[arg(long)]
        trace: Option<PathBuf>,
        /// Delivery mode: `uto` (strict, default) or `to` (crash-masking).
        #[arg(long, value_enum, default_value_t = DeliveryModeArg::Uto)]
        delivery_mode: DeliveryModeArg,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,trains_net=warn"))
        )
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Keygen { out, sni } => {
            let id = trains_net::NodeIdentity::generate(sni)
                .context("generating identity")?;
            id.save(&out).context("saving identity")?;
            println!("identity:    {}", out.display());
            println!("fingerprint: {}", id.fingerprint.to_hex());
            Ok(())
        }
        Cmd::Node { id, listen, successor, identity, peer_fp, issue_initial, delivery_mode, peer_addr } => {
            let id_obj = trains_net::NodeIdentity::load(&identity)
                .with_context(|| format!("loading identity from {}", identity.display()))?;
            let pinned = peer_fp
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(trains_net::SpkiFingerprint::from_hex)
                .collect::<Result<Vec<_>, _>>()
                .context("parsing --peer-fp")?;
            let ring_addrs = parse_ring_addrs(&peer_addr).context("parsing --peer-addr")?;
            node::run(node::NodeArgs {
                id, listen, successor, identity: id_obj, pinned,
                issue_initial, mode: delivery_mode.into(), ring_addrs,
            }).await
        }
        Cmd::Ring { num, num_trains, seconds, broadcast, trace, delivery_mode } => {
            let parsed_bcasts = broadcast.iter()
                .map(|s| {
                    let (n, m) = s.split_once(':')
                        .ok_or_else(|| anyhow::anyhow!("--broadcast must be 'NODE:MSG', got {s}"))?;
                    Ok::<_, anyhow::Error>((
                        n.parse::<u8>().context("node id")?,
                        m.as_bytes().to_vec(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            ring::run(ring::RingArgs {
                num, num_trains, duration: Duration::from_secs(seconds),
                broadcasts: parsed_bcasts, trace_path: trace,
                mode: delivery_mode.into(),
            }).await
        }
    }
}

/// Parse `--peer-addr <id>=<addr>` entries into addresses indexed by node id.
/// Empty input → empty vec (reconfiguration disabled). Ids must form the
/// contiguous range `0..N`.
fn parse_ring_addrs(entries: &[String]) -> Result<Vec<std::net::SocketAddr>> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let mut pairs: Vec<(usize, std::net::SocketAddr)> = Vec::with_capacity(entries.len());
    for e in entries {
        let (id_s, addr_s) = e
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--peer-addr must be 'ID=ADDR', got {e}"))?;
        let id: usize = id_s.trim().parse().with_context(|| format!("peer id in {e}"))?;
        let addr: std::net::SocketAddr =
            addr_s.trim().parse().with_context(|| format!("peer addr in {e}"))?;
        pairs.push((id, addr));
    }
    pairs.sort_by_key(|(id, _)| *id);
    for (expected, (id, _)) in pairs.iter().enumerate() {
        if *id != expected {
            anyhow::bail!(
                "--peer-addr ids must be a contiguous 0..N range; got id {id} at position {expected}"
            );
        }
    }
    Ok(pairs.into_iter().map(|(_, a)| a).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_mode_arg_maps_to_core_mode() {
        assert_eq!(DeliveryMode::from(DeliveryModeArg::Uto), DeliveryMode::UniformTotalOrder);
        assert_eq!(DeliveryMode::from(DeliveryModeArg::To), DeliveryMode::TotalOrder);
    }

    #[test]
    fn ring_delivery_mode_defaults_to_uto_and_parses_to() {
        let cli = Cli::try_parse_from(["trains", "ring"]).unwrap();
        let Cmd::Ring { delivery_mode, .. } = cli.cmd else { panic!("expected ring") };
        assert_eq!(delivery_mode, DeliveryModeArg::Uto, "default must stay uto");

        let cli = Cli::try_parse_from(["trains", "ring", "--delivery-mode", "to"]).unwrap();
        let Cmd::Ring { delivery_mode, .. } = cli.cmd else { panic!("expected ring") };
        assert_eq!(delivery_mode, DeliveryModeArg::To);
    }

    #[test]
    fn ring_addrs_parse_indexed_by_id() {
        // Out-of-order ids sort into a 0..N-indexed vector.
        let v = parse_ring_addrs(&[
            "2=127.0.0.1:30".into(),
            "0=127.0.0.1:10".into(),
            "1=127.0.0.1:20".into(),
        ]).unwrap();
        assert_eq!(v.len(), 3);
        assert_eq!(v[0].port(), 10);
        assert_eq!(v[1].port(), 20);
        assert_eq!(v[2].port(), 30);

        // Empty → reconfiguration disabled.
        assert!(parse_ring_addrs(&[]).unwrap().is_empty());

        // Non-contiguous ids are rejected.
        assert!(parse_ring_addrs(&["0=127.0.0.1:10".into(), "2=127.0.0.1:30".into()]).is_err());
        // Malformed entry rejected.
        assert!(parse_ring_addrs(&["0:127.0.0.1:10".into()]).is_err());
    }

    #[test]
    fn node_delivery_mode_flag_parses() {
        let cli = Cli::try_parse_from([
            "trains", "node",
            "--id", "0",
            "--listen", "127.0.0.1:9000",
            "--successor", "127.0.0.1:9001",
            "--identity", "/tmp/id.json",
            "--peer-fp", "aa",
            "--delivery-mode", "to",
        ]).unwrap();
        let Cmd::Node { delivery_mode, issue_initial, .. } = cli.cmd else { panic!("expected node") };
        assert_eq!(delivery_mode, DeliveryModeArg::To);
        assert!(!issue_initial, "issue_initial defaults off");
    }
}
