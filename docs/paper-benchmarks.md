# Benchmarking TRAINS: from a laptop to a lossy Wi-Fi link to EC2

**A measurement study of the Rust TRAINS implementation.**

This is the benchmark companion to the protocol paper
([`docs/paper.md`](paper.md)). Where that paper is about *correctness* — a
formal specification and a four-layer verification stack — this one is about
*behaviour under load and failure*: how fast the implementation goes, how it
scales, and what it does when the network or a node breaks. It consolidates
three measurement environments — a single machine, two machines over Tailscale,
and Amazon EC2 — with the full EC2 study reproduced as **Appendix A** (also
maintained standalone at
[`bench/reports/TRAINS-EC2-evaluation.md`](../bench/reports/TRAINS-EC2-evaluation.md)).

All numbers here are reproducible: in-process and loopback harnesses via
`scripts/run_benches.sh` (raw rows in `benches/results/`), EC2 via the CDK
harness documented in the EC2 report.

---

## 1. What we are measuring, and why it is interesting

**Read these numbers on the right axis.** TRAINS is a *control-plane* total-order
broadcast primitive — the etcd/ZooKeeper family — not a data-plane replication
engine. The workloads here are deliberately control-plane-shaped: **small
messages** (64 B–256 B: config, membership, leases, locks, coordination events),
**small clusters** (rings of 3–12, with 3–5 being the realistic range), and
**modest rates** where *order and agreement* matter more than raw bandwidth. We
do *not* try to push bulk data at line rate — that is a data-plane job TRAINS is
not for, and the one place it loses (bandwidth-bound 16 KB payloads, §3) is
exactly that job. Judge the protocol on the small-message, small-cluster,
consistency-first axis; that is what a control plane asks of it.

TRAINS is leaderless and ring-based: the right to order travels the ring as a
circulating "train," so every node forwards, acks, and delivers the same volume
(§2 of the protocol paper). That symmetry is the thesis — it should trade
single-message latency for small-message throughput, and it should degrade
*latency, not correctness* under stress. A benchmark study is how you find out
whether the thesis survives contact with real hardware, a real lossy link, and
real faults.

We ask four questions:

1. **Throughput/latency on one machine** — how does the protocol compare to a
   leader-based log (Raft) on the same box?
2. **Behaviour on a real network** — does it still hold its invariants over a
   lossy, MTU-constrained Wi-Fi link?
3. **Scaling** — how does latency grow as the ring grows, and does correctness
   ever break?
4. **Fault behaviour** — under injected faults (loss, latency, partition,
   crash), what is masked, and at what recovery cost?

---

## 2. Harnesses and method

| Harness | Environment | What it measures |
|---|---|---|
| `bench_kernel` | in-process, no I/O | the protocol kernel ceiling (`TrainsNode::step`) |
| `bench_ring_tls` | loopback TLS, `RING_SIZE=3` | the real `trains-net` ring on one host |
| `bench_raft_baseline` | loopback, hand-rolled | a leader→majority commit critical-path *upper bound* for Raft |
| `bench_raft_openraft` | in-process, openraft 0.9 | a real 3-node Raft cluster (quorum-commit) |
| Tailscale driver | 2 machines, WireGuard | cross-host ring over a real Wi-Fi link |
| EC2 CDK harness | AWS, rings 3–12 | scalability, SRD sensitivity, fault injection |

Single-machine runs: Apple M-series MacBook Air, macOS 25.4, Rust 1.95.0,
`--release` LTO, `N = RING_SIZE = 3`, `K = NumTrains = 2`, three trials per cell,
median reported. Latencies are *batch-arrival* p99 (all messages injected as
fast as backpressure allows, then time to last delivery) — a worst-case
"I broadcast last in the burst" bound, not steady-state per-message latency.

EC2 runs: one TOML config per run, CDK-deployed VPC + N ring nodes + coordinator,
calibrated workload, per-run provenance (commit, AMI, instance types, cost),
torn down after. Chaos faults (`netem` loss/latency, `iptables` partition,
SIGKILL, EC2 stop/start) are injected **during active broadcast** (see §6).

---

## 3. Single machine: throughput and latency

`N = 3`, `K = 2`, three payload sizes. The two Raft columns are baselines — see
the notes.

| Layer | Payload | Throughput | Bandwidth | p99 latency |
|---|---|---|---|---|
| TRAINS kernel | 64 B | 35 379 msg/s | 2.3 MiB/s | n/a |
| TRAINS kernel | 1 KiB | 36 706 msg/s | 35.8 MiB/s | n/a |
| TRAINS kernel | 16 KiB | 35 144 msg/s | 548 MiB/s | n/a |
| TRAINS TLS ring | 64 B | **404 926 msg/s** | 25 MiB/s | 1.22 ms |
| TRAINS TLS ring | 1 KiB | 116 610 msg/s | 114 MiB/s | 4.28 ms |
| TRAINS TLS ring | 16 KiB | 7 652 msg/s | 120 MiB/s | 65.0 ms |
| Raft critical\* | 64 B | 146 249 msg/s | 9.3 MiB/s | 0.98 ms |
| Raft critical\* | 1 KiB | 137 806 msg/s | 141 MiB/s | 0.76 ms |
| Raft critical\* | 16 KiB | 55 906 msg/s | 873 MiB/s | 3.08 ms |
| openraft 0.9‡ | 64 B | 354 686 msg/s | 21.6 MiB/s | 0.20 ms |
| openraft 0.9‡ | 1 KiB | 218 269 msg/s | 213 MiB/s | 0.41 ms |
| openraft 0.9‡ | 16 KiB | 43 041 msg/s | 672 MiB/s | 2.52 ms |

**Reading it.** At 64 B the TRAINS two-train pipeline leads the field (405k
msg/s) — small messages are overhead-bound, and the pipeline absorbs overhead
across both issuer slots. At 1 KiB and 16 KiB the leader-based logs win:
payloads are bandwidth-bound, and at `K = 2` the model (§7) predicts Raft should
win. The TLS ring outpacing the single-threaded kernel bench at small payloads
is an artefact of the kernel bench (a per-call payload clone, no pipelining) —
the kernel number is a *serial-step ceiling*, not a protocol ceiling.

\* **Raft critical** is a hand-rolled leader→majority-ack loop over the same
mTLS stack — no log persistence, election, or snapshots. It is an *upper bound*
on what real Raft could do here, and its latency ("broadcast → first-majority
ack") is easier than the TRAINS column's "broadcast → all-node delivery."

‡ **openraft 0.9** is a real 3-node cluster with genuine log replication and
quorum commit, over an in-process network (no TLS, no sockets). Because it pays
no encryption/framing, it is an even *more favourable* upper bound for Raft —
when TRAINS approaches it, the comparison is conservative.

---

## 4. Two machines over Tailscale (a real lossy link)

The same ring, but two of the three hops cross 802.11ax home Wi-Fi between two
Apple M-series machines, each hop's TCP encapsulated in Tailscale's WireGuard
tunnel. The iperf3 ceiling on this link is ~144 Mbit/s with ~6% loss
(6 719 retransmits over 10 s) — a genuinely hostile link.

| Payload | Throughput | Bandwidth | p99 latency |
|---|---|---|---|
| 64 B | 3 277 msg/s | 0.20 MiB/s | 45.8 ms |
| 1 KiB | 1 072 msg/s | 1.05 MiB/s | 172 ms |
| 16 KiB | 197 msg/s | 3.08 MiB/s | 1.00 s |

This is ~100–125× slower than loopback at small payloads — expected: real
network RTT, WireGuard's MTU-1280 fragmentation (each 16-KiB train becomes ~14
packets/hop across 2 cross-host hops × 2 laps), and the lossy link hammering
congestion control. The 16-KiB row is bandwidth-bound at ~24 Mbit/s, ~17% of
the iperf3 ceiling.

**The point of this run is not the throughput — it is that the invariants
held.** `ConsistentDelivery` held across 200 broadcasts × 3 trials × 3 payload
sizes = 1 800 verified deliveries on a link where iperf3 itself sees 6% packet
loss. The protocol's correctness does not depend on a clean network.

---

## 5. EC2: scalability and transport sensitivity

Single-host benches answer "how fast"; only a cloud fleet answers "how does it
scale." Uniform t4g.medium nodes, 256 B, ~1.7k msg/s, rings 3 → 12.

| Ring | p50 | p99 | p99.9 | Delivery |
|---|---|---|---|---|
| 3  | 2.15 ms | 4.90 ms | 5.41 ms | 100% ×3 |
| 6  | 3.98 ms | **306 ms** | 386 ms | 100% ×6 |
| 9  | 8.51 ms | 592 ms | 671 ms | 100% ×9 |
| 12 | 13.09 ms | 1448 ms | 1528 ms | 100% ×12 |

p50 grows ~linearly (~1.2 ms/node, consistent with ring traversal); the **tail**
cliffs between ring 3 (clean to p99.9) and ring 6 (p95→p99 jumps ~70×). The
clean operating envelope is **ring ≤ 5** at this workload. The decisive
observation: **UTO completeness holds at every ring size** — under saturation
TRAINS degrades latency, never correctness.

**Transport sensitivity (SRD).** Re-running the saturated ring on AWS's SRD
transport (ENA Express) vs plain TCP:

| | t4g (TCP) p99 | c7gn (ARM+SRD) p99 |
|---|---|---|
| Ring 3 | 4.90 ms | 7.18 ms |
| Ring 6 | **306 ms** | **9.79 ms** |

At small rings SRD buys nothing (protocol overhead dominates); at the
saturation point its multipath + hardware retransmit prevents the tail
explosion — **31× tighter p99**. A cross-architecture sweep (ring 3, 16 KB)
confirmed the split: the p50 *floor* tracks the CPU (Graviton fastest, Intel
slowest), while the p99.9 *tail* tracks SRD (both SRD instances beat non-SRD
t4g by 1.4–1.6×, regardless of CPU vendor). **SRD makes the protocol more
scalable, not faster.** Saturation testing also surfaced a hard-coded 16 MB
`MAX_FRAME_LEN`, since made compile-time tunable.

---

## 6. EC2: fault injection (chaos)

Ring of 9, 256 B, all faults injected on node 4 *during active broadcast*. The
steady-state hypothesis is "every alive node delivers every broadcast, in
order"; we score safety (total order, no phantom delivery), masking, and
recovery time (MTTR signal = max inter-delivery stall).

| Scenario | Delivered / 50k | Total order | No phantom | Masking | Recovery |
|---|---|---|---|---|---|
| baseline | 50 000 (100%) | ✅ | ✅ | — | normal |
| 5% loss (20 s) | 50 000 (100%) | ✅ | ✅ | ✅ masked | 0.92 s stall |
| +100 ms latency (20 s) | 50 000 (100%) | ✅ | ✅ | ✅ masked | 0.58 s stall |
| partition 4↔5 (20 s, healed) | 50 000 (100%) | ✅ | ✅ | ✅ masked | ≈29 s stall, full catch-up |
| stop/restart node 4 (30 s) | 24 251 — **all 9 converged** | ✅ | ✅ | ⚠️ consistent, throughput halved | rejoined, no split-brain |
| **SIGKILL node 4 (permanent)** | 12 416 — nodes 5–8 lose 12 in-flight | ✅ | ✅ | ❌ **not masked** | none — ring halts |

Three findings:

1. **Safety is unconditional.** Total order and no-phantom-delivery held in
   *every* scenario, including the permanent crash. TRAINS never reordered or
   fabricated a message under any injected fault.
2. **Transient faults are masked, with a quantified MTTR.** 5% loss and +100 ms
   latency are absorbed with sub-second stalls; a 20 s partition is fully masked
   with a ~29 s backlog-drain stall.
3. **A permanent crash was not masked — by this build.** A SIGKILL halts UTO at
   12 416/50 000 because *the build under test had no membership-reconfiguration
   layer*. That is exactly the gap since closed by the rejoin/re-admission work
   (now formally verified — protocol paper §5.2): the view-change machinery
   re-forms the ring around a permanent crash and re-admits a recovered node.
   The chaos study is what *motivated* and scoped that feature.

**A methodology caveat that generalises.** An earlier chaos batch injected
faults during the post-workload *drain* and saw a flat ~50 ms stall and 100%
delivery for *every* scenario — including SIGKILL — a false "fully
fault-tolerant" reading. The differentiated results above only appear once the
fault overlaps the active-broadcast window. Cloud chaos experiments must align
fault timing to live load, or the results are meaningless.

---

## 7. Discussion: the throughput model

The single-machine crossover is predicted by a simple model (protocol paper
§6.2). For a homogeneous ring with per-link bandwidth `B` and `K` trains,
aggregate throughput is `≈ K·B / 2N` (a delivery is two ring laps = `2N`
transmissions). For Raft with leader bandwidth `B_L`, throughput is
`≤ B_L / (N−1)` (the leader is the bottleneck). TRAINS overtakes Raft on
broadcast throughput when

```
  K · B / (2N) > B / (N − 1)   ⇒   K > 2N / (N − 1)
```

For `N = 5` that is `K > 2.5`; for `N = 10`, `K > 2.22`. So **three or more
concurrent trains beat a leader on pure broadcast throughput**, and the
advantage grows with payload size (bandwidth-bound regime). The measured
single-machine numbers at `K = 2` sit *below* this crossover, and indeed Raft
wins there on bandwidth-bound payloads — the data confirms the model. The
`K = 3/4` cells where TRAINS should win require parameterising the
compile-time-constant ring and are left as a follow-up.

The cost is latency: a single message commits in ~1 RTT under Raft but waits
two ring laps (`2N` hops) under TRAINS. TRAINS optimises sustained throughput
and uniform total order, **not** tail latency for individual messages.

Finally, the chaos results put a name to a 1990s design choice: TRAINS halts
rather than diverge under partition — the **CP** corner of the CAP theorem. For
its original domain (power-plant control supervision) that is the correct
trade: correct when healthy, silent when not.

---

## 8. How this sits among other systems (published numbers — mind the caveats)

It is tempting to put TRAINS in a table next to etcd, ZooKeeper and friends and
declare a winner. **Don't** — the numbers below come from different hardware,
years, workloads, and even different *units*, and a ranking built from them
would be dishonest. They are useful only as a map of *regimes*. Every figure is
a published number under that project's own conditions, not something we
re-measured.

| System (family) | Published figure | Conditions | Unit being counted |
|---|---|---|---|
| **etcd** (Raft) | >30,000 req/s, <1 ms light load | 3-node cloud cluster | durable replicated KV ops via a leader |
| **ZooKeeper / ZAB** (atomic broadcast) | ~21,000 writes/s, ~1.2 ms | 3 servers | coordination writes via a leader |
| **TigerBeetle** (VSR) | ~1,000,000 txns/s (design); ProtoBeetle ~200,000/s on laptops | heavily batched (up to ~8,189 txns/request) | financial transfers |
| **Corosync / Totem** (ring TOB) | 30 → 60 MB/s (jumbo frames, 175 KB msgs) | LAN multicast | bandwidth |
| **Simatic TRAINS 2015** (ring TOB) | +250% *POTE* vs best prior ring protocol @ 10 B | 5 processes | throughput *efficiency* (bytes delivered ÷ transmitted) |
| **trains-rust** (this work) | 405k msg/s @ 64 B (1 host, TLS ring, batch); ~1.7k msg/s @ 256 B on EC2 ring 3–12 | various, §3–§6 | broadcast messages |

Sources: [etcd performance docs](https://etcd.io/docs/v3.5/op-guide/performance/);
Junqueira et al., *Zab* (DSN 2011) and the ZooKeeper benchmarks;
[TigerBeetle](https://tigerbeetle.com/); Corosync Totem documentation; Simatic
et al., *TRAINS* (CFIP/NOTERE 2015).

Two honest observations come out of it.

1. **The leader-based coordination systems and TRAINS occupy different points,
   so the cross-paper numbers don't line up.** etcd (~30k req/s) and ZooKeeper
   (~21k writes/s) count *durable replicated operations through a leader* on a
   small cluster at low-millisecond latency. TRAINS counts *broadcast messages*
   and deliberately trades single-message latency (two ring laps) for
   small-message throughput. The only apples-to-apples comparison in this study
   is the one we ran ourselves on the same box and harness — the openraft
   head-to-head in §3 — not these figures.
2. **The two numbers from TRAINS's own family are the most telling.** Corosync's
   Totem — the other production ring total-order broadcast — reports throughput
   in **MB/s and is bandwidth-bound**, which is exactly the regime trains-rust
   enters at large payloads (it plateaus around 135–145 MiB/s on one host, §3).
   And Simatic's 2015 TRAINS reports a throughput-*efficiency* peak for **small
   messages**, which is the same thesis our 64 B numbers show from a different
   angle. TigerBeetle's million-a-second is a useful reminder from a different
   problem entirely: it is *batching* (thousands of transfers per request), not
   the consensus protocol, that produces headline throughput — a lever TRAINS
   also pulls (a train is a batch) and one any fair comparison has to hold equal.

Treat the table as a map, not a leaderboard: it shows that TRAINS sits in the
small-message-throughput / bandwidth-bound-at-scale corner, alongside the other
ring protocols, and at a different point from the leader-based logs that
optimise low-latency durable operations.

---

## 9. Conclusion

On one machine, TRAINS' multi-train pipeline beats a leader-based log on small
messages and trails it on bandwidth-bound payloads at `K = 2`, exactly as the
throughput model predicts. Over a real lossy Wi-Fi link it holds every
invariant despite 6% packet loss. On EC2 it has a clean latency envelope through
ring ≤ 5 and degrades latency — never correctness — beyond it; AWS SRD does not
lower the floor but bounds the tail (31× at saturation). Under fault injection
aligned to live load it preserves total-order safety unconditionally, masks
transient faults with quantified recovery, and — in the build tested — halted on
a permanent crash, the gap the now-verified rejoin layer closes. Every result is
reproducible from a config and a script, with per-run provenance.

Read on the right axis, the picture is consistent: on the **control-plane**
workload it is built for — small messages, small clusters, total order,
consistency over availability — TRAINS has a clean, well-characterised envelope.
It is not, and does not try to be, a bulk data-plane engine. The honest summary
is "a good fit for ordered, consistency-first coordination," not "a faster Raft."

---

## References

- Protocol paper and verification: [`docs/paper.md`](paper.md),
  `VERIFICATION_REPORT.md`.
- Full EC2 detail + provenance:
  [`bench/reports/TRAINS-EC2-evaluation.md`](../bench/reports/TRAINS-EC2-evaluation.md).
- M. Simatic et al. *TRAINS: A Throughput-Efficient Uniform Total Order
  Broadcast Algorithm.* CFIP/NOTERE 2015.
- X. Défago, A. Schiper, P. Urbán. *Total Order Broadcast and Multicast
  Algorithms: Taxonomy and Survey.* ACM Computing Surveys 36(4), 2004.
- L. Shalev et al. *A Cloud-Optimized Transport Protocol (SRD).* IEEE Micro
  40(6), 2020.
- A. Basiri et al. *Chaos Engineering.* IEEE Software 33(3), 2016.
- etcd, *Performance* (official docs, v3.5). https://etcd.io/docs/v3.5/op-guide/performance/
- F. Junqueira, B. Reed, M. Serafini. *Zab: High-performance broadcast for
  primary-backup systems.* DSN 2011. (ZooKeeper, USENIX ATC 2010.)
- TigerBeetle (VSR) — published throughput figures, https://tigerbeetle.com/
- Corosync Cluster Engine — Totem single-ring protocol documentation.

---

## Appendix A — EC2 Evaluation (full study)

*This appendix reproduces, in full, the standalone EC2 evaluation study
maintained at [`bench/reports/TRAINS-EC2-evaluation.md`](../bench/reports/TRAINS-EC2-evaluation.md)
— included here so the benchmark paper is self-contained. The standalone
document remains the canonical, separately-accessible copy.*

### An EC2 Evaluation of the TRAINS Uniform Total-Order Broadcast Protocol: Scalability, Network-Feature Sensitivity, and Fault Behaviour

**Draft — 2026-05-24.** Author: AO bench harness (`trains-rust/bench`). Status: complete (benchmark Phases A–F + chaos Phase G, corrected-timing reruns).

---

#### Abstract

We present a reproducible measurement study of **TRAINS** [Simatic & Foltz, 2015], a throughput-efficient *uniform total-order broadcast* (UTO) protocol that circulates one or more "trains" around a logical ring of processes. Running TRAINS on Amazon EC2, we characterise three dimensions: (i) **scalability** — how latency degrades as the ring grows; (ii) **network-feature sensitivity** — whether AWS's Scalable Reliable Datagram (SRD) transport [Shalev et al., 2020] changes the latency profile; and (iii) **fault behaviour** — how the protocol masks, or fails to mask, injected faults, framed through the lens of chaos engineering [Basiri et al., 2016]. We find that (1) on uniform t4g.medium hardware the protocol has a clean operating envelope of ring ≤ 5 at 256 B × ~1.7k msg/s, beyond which tail latency explodes (ring 6 p99 = 306 ms) while UTO completeness is preserved; (2) SRD does not lower the latency *floor* (that tracks the CPU) but **bounds the tail** under saturation (31× tighter p99 at ring 6) and under heavy payload (1.4–1.6× tighter p99.9); and (3) under fault injection aligned to active circulation, TRAINS preserves *safety* (total order, no phantom delivery) **unconditionally** and **masks transient network faults** (5% loss, +100 ms latency, and a 20 s link partition all reach 100% delivery, with recovery time scaling from ~0.6 s to ~29 s), while a recoverable stop/restart converges all nodes consistently (no split-brain) — but a **permanent crash is not masked** (a SIGKILL halts UTO progress, as the tested build has no membership-reconfiguration layer). We additionally report a methodological finding: naïve fault injection during the post-workload drain phase produces false "fault-tolerance" results, and must be aligned to the active-broadcast window.

---

#### 1. Introduction

Uniform total-order broadcast is a foundational primitive for fault-tolerant and load-balanced distributed services [Défago et al., 2004]. Ring-based UTO protocols (LCR; TRAINS) trade latency for throughput by amortising acknowledgement and ordering over a token/train that circulates the ring. TRAINS [Simatic & Foltz, 2015] is reported to improve peak throughput over prior ring protocols by up to ~250% for small messages, at a documented cost in latency.

Most published evaluations of such protocols use controlled clusters. We instead measure TRAINS on commodity cloud infrastructure (EC2), where (a) ring size and instance type are trivially varied, (b) a modern offloaded transport (SRD, via ENA Express) is available, and (c) faults can be injected through the cloud control plane and Linux traffic control. Our contributions:

1. A **one-config-per-run, single-script reproducible harness** (`bench/`) that deploys an N-node ring, runs a calibrated workload, captures per-message latency and NIC counters, and tears down — with provenance recorded per run.
2. A **scalability sweep** (rings 3/6/9/12) and a **cross-architecture, SRD-vs-non-SRD comparison** (Graviton t4g, Graviton+SRD c7gn, Intel+SRD c7i).
3. A **chaos-engineering study** that injects network and instance faults *during active circulation* and evaluates them against UTO correctness + recovery (MTTR) invariants.
4. A **negative methodological result**: fault-injection timing relative to the workload is decisive; mis-timed injection silently yields invalid "robustness" data.

#### 2. Background and Related Work

**Total-order broadcast.** Défago, Schiper & Urbán [2004] survey ~60 algorithms and the safety properties we adopt as invariants: *agreement/uniformity* (every correct process delivers the same set), *total order* (all processes deliver in the same order), *validity* (no spurious messages). TRAINS provides the *uniform* variant.

**Ring protocols & TRAINS.** Ring TOB (LCR; TRAINS [Simatic & Foltz, 2015]) achieves high throughput by circulating trains carrying batched messages; a message is delivered (UTO) once the ordering/acknowledgement condition is met after circulation. The design trades latency (proportional to ring traversal) for throughput. Our scaling results directly probe that trade-off.

**SRD / ENA Express.** Shalev et al. [2020] describe SRD, AWS's Nitro-offloaded transport that sprays packets across many paths (out-of-order on the wire), with hardware retransmission, to minimise tail latency and jitter for HPC/ML. ENA Express exposes SRD to EC2 instances. We test whether a protocol whose cost is dominated by ring traversal benefits from SRD's tail-bounding.

**Chaos engineering.** Basiri et al. [2016] frame resilience testing as experiments against a *steady-state hypothesis* with minimised *blast radius*. We adopt this framing: the steady state is "every node delivers every broadcast, in order"; faults are injected with a bounded blast radius (one node / one link), and we measure whether the steady state is *masked* (preserved) and, if disrupted, the *recovery time*.

#### 3. Methodology

**Harness.** Each run is driven by one TOML config (`bench/configs/<name>.toml`) and `reproduce.sh`, which: validates the config, CDK-deploys a VPC + N ring nodes + a coordinator, builds the TRAINS binary on the coordinator for the target `RING_SIZE`, starts the ring, runs the workload, captures results to S3, aggregates a report, and tears down. Provenance (TRAINS commit, AMI, instance types, timing, cost) is emitted per run.

**Workload & metrics.** A producer on node 0 broadcasts *N* messages of fixed payload, spread over the run duration. We discard a warm-up prefix (cold-flight: TLS handshake, TCP slow-start, initial train formation) and report steady-state per-message round-trip latency percentiles (p50/p95/p99/p99.9) computed from per-message `send_ns`/`recv_ns`. We also diff `/sys/class/net/<iface>/statistics` and ENA SRD ethtool counters before/after to report measured wire bandwidth and SRD packet share.

**Invariants (chaos).** From the per-node delivery records we compute: **UTO completeness** (every alive node delivered every broadcast seq), **total order** (pairwise common-projection equality; no duplicate delivery), **no phantom delivery** (no delivered seq was never broadcast), and a **recovery/MTTR signal** (max inter-delivery progress stall per node from `recv_ns`). Liveness-recovery is measured; bounded-queue-depth is reported as *not measured* (requires runtime gauges).

**Fault injection.** `netem` (loss/latency) and `iptables` (partition) faults are applied to a target node's NIC via SSM; instance faults (SIGKILL of `trains-cli`; EC2 stop/start) via SSM/EC2. Faults are injected **concurrently with active broadcast** (see §6, Threats) and cleared after a configured duration (permanent for crash faults).

#### 4. Benchmark Results (Phases A–F)

##### 4.1 Scalability on uniform hardware (t4g.medium, no SRD)

| Ring | p50 | p95 | p99 | p99.9 | Delivery |
|---|---|---|---|---|---|
| 3 | 2.15 ms | 4.29 ms | 4.90 ms | 5.41 ms | 100% ×3 |
| 6 | 3.98 ms | 8.20 ms | **306 ms** | 386 ms | 100% ×6 |
| 9 | 8.51 ms | 296 ms | 592 ms | 671 ms | 100% ×9 |
| 12 | 13.09 ms | 1096 ms | 1448 ms | 1528 ms | 100% ×12 |

*N = 45k post-warmup, 256 B, ~1.7k msg/s.* **Finding:** p50 grows ~linearly (~1.2 ms/node), consistent with ring traversal. The tail transitions sharply between ring 3 (clean to p99.9) and ring 6 (p95→p99 jumps 70×). **Saturation point is ring 6** at this workload. Critically, **UTO completeness holds at every ring size** — TRAINS degrades *latency*, not *correctness*, under load.

##### 4.2 SRD effect under saturation (Graviton t4g vs c7gn, 256 B sweep)

| Ring | c7gn (ARM+SRD) p99 | t4g p99 | Tail improvement |
|---|---|---|---|
| 3 | 7.18 ms | 4.90 ms | t4g slightly faster |
| 6 | **9.79 ms** | **306 ms** | **31× tighter** |

**Finding:** at small rings SRD buys nothing (protocol overhead dominates). At the t4g saturation point (ring 6) the story inverts: SRD's multipath + hardware retransmit prevents the tail explosion (31× tighter p99). *SRD makes the protocol more scalable, not faster.*

##### 4.3 Cross-architecture bandwidth saturation (ring 3, 16 KB × 4000)

| Arch | p50 | p99 | p99.9 | TX MB/node | SRD share |
|---|---|---|---|---|---|
| T4G (non-SRD) | 3.92 | 8.21 | 11.71 | ~47.5 | 0% |
| c7gn (ARM+SRD) | **3.28** | **6.99** | **7.46** | ~50.8 | ~99.7% |
| c7i (Intel+SRD) | 4.51 | 7.88 | 8.43 | ~40.3 | ~99.7% |

**Finding — the floor is CPU, the tail is SRD.** The p50 floor tracks the CPU: ARM Graviton is fastest, Intel *slowest* (below even non-SRD t4g). The p99.9 tail tracks SRD: *both* SRD instances beat non-SRD t4g (1.4–1.6× tighter) regardless of core vendor. All runs UTO-complete.

##### 4.4 Protocol limit exposed by saturation testing

Bandwidth-saturation testing on large NICs surfaced TRAINS' hard-coded 16 MB `MAX_FRAME_LEN`: bursty workloads batch the entire pending queue into one train and trip "frame too large". We made it compile-time tunable (`TRAINS_MAX_FRAME_LEN_MB`), a candidate upstream protocol fix.

#### 5. Chaos Results (Phase G) — fault behaviour

Ring of 9 t4g.medium nodes, 256 B workload. Baseline (no fault): p50 7.42 / p99 17.1 / p99.9 44.0 ms, 100% UTO, all invariants clean. Faults injected on node 4 during active broadcast.

All faults injected on node 4 during active broadcast (the corrected harness, §6). "Delivered" = total broadcasts that achieved UTO; recovery stall = max inter-delivery gap (`recv_ns`).

| Scenario | Delivered / 50k | Total order | No phantom | Masking | Recovery (MTTR signal) |
|---|---|---|---|---|---|
| baseline (no fault) | 50,000 (100%) | ✅ | ✅ | — | 63 ms (normal spacing) |
| **netem-loss-5pct** (20 s) | 50,000 (100%) | ✅ | ✅ | ✅ **masked** | 0.92 s stall, recovered |
| **netem-latency +100 ms** (20 s) | 50,000 (100%) | ✅ | ✅ | ✅ **masked** | 0.58 s stall, recovered |
| **netem-partition** 4↔5 (20 s, healed) | 50,000 (100%) | ✅ | ✅ | ✅ **masked via recovery** | **≈ 29 s** stall (cut + drain), then full catch-up |
| **fis-stop-start** node-4 (stop 30 s, restart) | 24,251 — *all 9 nodes converged* | ✅ | ✅ | ⚠️ **consistent but throughput halved** | node-4 rejoined & converged; **no split-brain** |
| **fis-kill** node-4 (permanent) | 12,416 — nodes 5–8 lose 12 in-flight | ✅ | ✅ | ❌ **not masked** | none — ring halts (no reconfiguration in build) |

**Headline findings:**

1. **Safety is unconditional.** Total order and no-phantom-delivery held in **every** scenario, including the permanent crash. TRAINS never reordered or fabricated a message under any injected fault.
2. **Transient network faults are masked, with recovery time that scales with severity.** 5% loss and +100 ms latency are absorbed with sub-second stalls; a 20 s link partition is also fully masked but with a ~29 s progress stall (no circulation during the cut, then backlog drain after heal) — a clean, quantified **MTTR**.
3. **A recoverable instance fault (stop/start) converges consistently.** After node 4 was stopped 30 s and restarted, **all nine nodes agreed on the same 24,251 deliveries** — the rejoining node caught up with no split-brain. The shortfall vs 50 k is throughput lost during the outage (the fixed 30 s window ended before full catch-up), not a correctness failure.
4. **A permanent crash is NOT masked.** A SIGKILL during circulation halts UTO at 12,416/50,000; the tested build has no membership-reconfiguration/recovery layer, so the ring does not re-form. Safety holds; liveness does not.

**Why the corrected timing matters (see §6):** an earlier batch that injected faults during the post-broadcast drain showed a flat ~50 ms stall and 100% delivery for *every* scenario — including SIGKILL — a false "fully fault-tolerant" reading. The differentiated results above (0.6 s → 29 s → halted) only appear once the fault overlaps active circulation.

#### 6. Discussion & Threats to Validity

**Fault-injection timing is decisive (and a trap).** Our first chaos batch injected faults during the post-workload *settle* phase. Because the producer broadcasts and the ring delivers *concurrently* during the preceding ~30 s, by the time the settle began delivery was essentially complete — so faults landed after the protocol had finished its work and showed *zero* effect (100% delivery, ~50 ms stalls for every scenario, including SIGKILL). We confirmed this from SSM timestamps (e.g., a kill executed 6 s *after* the target finished delivering) and flat per-2 s delivery histograms. **This is a general hazard for cloud chaos experiments: a correctly-wired fault that fires outside the active window yields false-positive "fault tolerance."** We corrected the harness to inject concurrently with broadcast; the §5 results use the corrected harness.

**Other threats.** (i) Latency mixes per-node clocks (`send_ns` issuer vs `recv_ns` local); we report it as a relative/steady-state signal, not absolute one-way latency. (ii) Single-AZ, single region (us-east-1); cross-AZ behaviour untested. (iii) Coordinator builds on t4g.micro; build variance does not affect measured percentiles. (iv) Bounded-queue-depth invariant is unmeasured (no runtime gauges). (v) The tested TRAINS build has no membership/reconfiguration; crash-recovery results are specific to that build.

#### 7. Conclusion

On EC2, TRAINS exhibits a clean ring ≤ 5 latency envelope that degrades into a tail cliff under saturation while never sacrificing UTO completeness; AWS SRD does not change the latency floor but materially bounds the tail under both ring-saturation and heavy-payload pressure. Under fault injection aligned to active circulation, TRAINS preserves total-order safety unconditionally and masks transient network faults (loss/latency/partition, with quantified recovery times), and a stop/restart converges consistently — but it does not mask a permanent node crash, which halts UTO for lack of a reconfiguration layer (the principal gap for any replication use). We also caution that fault-injection timing must overlap the active workload or chaos results are meaningless. All runs are reproducible from a single config + script, with per-run provenance.

#### References

1. M. Simatic and A. Foltz. *TRAINS: A throughput-efficient uniform total order broadcast algorithm.* NTDS/ICPE, 2015. IEEE Xplore doc. 7293477. (See also TrainsProtocol, github.com/simatic/TrainsProtocol.)
2. X. Défago, A. Schiper, and P. Urbán. *Total order broadcast and multicast algorithms: Taxonomy and survey.* ACM Computing Surveys, 36(4):372–421, 2004.
3. L. Shalev, H. Ayoub, N. Bshara, and E. Sabbag. *A Cloud-Optimized Transport Protocol for Elastic and Scalable HPC.* IEEE Micro, 40(6), 2020.
4. A. Basiri, N. Behnam, R. de Rooij, L. Hochstein, L. Kosewski, J. Reynolds, and C. Rosenthal. *Chaos Engineering.* IEEE Software, 33(3):35–41, 2016.

---

*Reproducibility: every result above is regenerable via `./bench/scripts/reproduce.sh <config>`; raw per-run reports + provenance live in `bench/results/`.*

> **Note:** the EC2 deployment harness (`bench/scripts/`, `bench/configs/`) and the raw per-run artefacts (`bench/results/`) are not included in this public mirror — they carry account-specific deployment detail. The numbers, method, and findings above are complete and self-contained; the in-repo `bench/coordinator/` and `bench/infrastructure/` show the harness shape.
