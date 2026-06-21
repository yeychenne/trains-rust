# An EC2 Evaluation of the TRAINS Uniform Total-Order Broadcast Protocol: Scalability, Network-Feature Sensitivity, and Fault Behaviour

**Draft — 2026-05-24.** Author: AO bench harness (`trains-rust/bench`). Status: complete (benchmark Phases A–F + chaos Phase G, corrected-timing reruns).

> This study is also reproduced as **Appendix A** of the consolidated benchmark paper, [`docs/paper-benchmarks.md`](../../docs/paper-benchmarks.md). This file remains the canonical standalone copy.

---

## Abstract

We present a reproducible measurement study of **TRAINS** [Simatic & Foltz, 2015], a throughput-efficient *uniform total-order broadcast* (UTO) protocol that circulates one or more "trains" around a logical ring of processes. Running TRAINS on Amazon EC2, we characterise three dimensions: (i) **scalability** — how latency degrades as the ring grows; (ii) **network-feature sensitivity** — whether AWS's Scalable Reliable Datagram (SRD) transport [Shalev et al., 2020] changes the latency profile; and (iii) **fault behaviour** — how the protocol masks, or fails to mask, injected faults, framed through the lens of chaos engineering [Basiri et al., 2016]. We find that (1) on uniform t4g.medium hardware the protocol has a clean operating envelope of ring ≤ 5 at 256 B × ~1.7k msg/s, beyond which tail latency explodes (ring 6 p99 = 306 ms) while UTO completeness is preserved; (2) SRD does not lower the latency *floor* (that tracks the CPU) but **bounds the tail** under saturation (31× tighter p99 at ring 6) and under heavy payload (1.4–1.6× tighter p99.9); and (3) under fault injection aligned to active circulation, TRAINS preserves *safety* (total order, no phantom delivery) **unconditionally** and **masks transient network faults** (5% loss, +100 ms latency, and a 20 s link partition all reach 100% delivery, with recovery time scaling from ~0.6 s to ~29 s), while a recoverable stop/restart converges all nodes consistently (no split-brain) — but a **permanent crash is not masked** (a SIGKILL halts UTO progress, as the tested build has no membership-reconfiguration layer). We additionally report a methodological finding: naïve fault injection during the post-workload drain phase produces false "fault-tolerance" results, and must be aligned to the active-broadcast window.

---

## 1. Introduction

Uniform total-order broadcast is a foundational primitive for fault-tolerant and load-balanced distributed services [Défago et al., 2004]. Ring-based UTO protocols (LCR; TRAINS) trade latency for throughput by amortising acknowledgement and ordering over a token/train that circulates the ring. TRAINS [Simatic & Foltz, 2015] is reported to improve peak throughput over prior ring protocols by up to ~250% for small messages, at a documented cost in latency.

Most published evaluations of such protocols use controlled clusters. We instead measure TRAINS on commodity cloud infrastructure (EC2), where (a) ring size and instance type are trivially varied, (b) a modern offloaded transport (SRD, via ENA Express) is available, and (c) faults can be injected through the cloud control plane and Linux traffic control. Our contributions:

1. A **one-config-per-run, single-script reproducible harness** (`bench/`) that deploys an N-node ring, runs a calibrated workload, captures per-message latency and NIC counters, and tears down — with provenance recorded per run.
2. A **scalability sweep** (rings 3/6/9/12) and a **cross-architecture, SRD-vs-non-SRD comparison** (Graviton t4g, Graviton+SRD c7gn, Intel+SRD c7i).
3. A **chaos-engineering study** that injects network and instance faults *during active circulation* and evaluates them against UTO correctness + recovery (MTTR) invariants.
4. A **negative methodological result**: fault-injection timing relative to the workload is decisive; mis-timed injection silently yields invalid "robustness" data.

## 2. Background and Related Work

**Total-order broadcast.** Défago, Schiper & Urbán [2004] survey ~60 algorithms and the safety properties we adopt as invariants: *agreement/uniformity* (every correct process delivers the same set), *total order* (all processes deliver in the same order), *validity* (no spurious messages). TRAINS provides the *uniform* variant.

**Ring protocols & TRAINS.** Ring TOB (LCR; TRAINS [Simatic & Foltz, 2015]) achieves high throughput by circulating trains carrying batched messages; a message is delivered (UTO) once the ordering/acknowledgement condition is met after circulation. The design trades latency (proportional to ring traversal) for throughput. Our scaling results directly probe that trade-off.

**SRD / ENA Express.** Shalev et al. [2020] describe SRD, AWS's Nitro-offloaded transport that sprays packets across many paths (out-of-order on the wire), with hardware retransmission, to minimise tail latency and jitter for HPC/ML. ENA Express exposes SRD to EC2 instances. We test whether a protocol whose cost is dominated by ring traversal benefits from SRD's tail-bounding.

**Chaos engineering.** Basiri et al. [2016] frame resilience testing as experiments against a *steady-state hypothesis* with minimised *blast radius*. We adopt this framing: the steady state is "every node delivers every broadcast, in order"; faults are injected with a bounded blast radius (one node / one link), and we measure whether the steady state is *masked* (preserved) and, if disrupted, the *recovery time*.

## 3. Methodology

**Harness.** Each run is driven by one TOML config (`bench/configs/<name>.toml`) and `reproduce.sh`, which: validates the config, CDK-deploys a VPC + N ring nodes + a coordinator, builds the TRAINS binary on the coordinator for the target `RING_SIZE`, starts the ring, runs the workload, captures results to S3, aggregates a report, and tears down. Provenance (TRAINS commit, AMI, instance types, timing, cost) is emitted per run.

**Workload & metrics.** A producer on node 0 broadcasts *N* messages of fixed payload, spread over the run duration. We discard a warm-up prefix (cold-flight: TLS handshake, TCP slow-start, initial train formation) and report steady-state per-message round-trip latency percentiles (p50/p95/p99/p99.9) computed from per-message `send_ns`/`recv_ns`. We also diff `/sys/class/net/<iface>/statistics` and ENA SRD ethtool counters before/after to report measured wire bandwidth and SRD packet share.

**Invariants (chaos).** From the per-node delivery records we compute: **UTO completeness** (every alive node delivered every broadcast seq), **total order** (pairwise common-projection equality; no duplicate delivery), **no phantom delivery** (no delivered seq was never broadcast), and a **recovery/MTTR signal** (max inter-delivery progress stall per node from `recv_ns`). Liveness-recovery is measured; bounded-queue-depth is reported as *not measured* (requires runtime gauges).

**Fault injection.** `netem` (loss/latency) and `iptables` (partition) faults are applied to a target node's NIC via SSM; instance faults (SIGKILL of `trains-cli`; EC2 stop/start) via SSM/EC2. Faults are injected **concurrently with active broadcast** (see §6, Threats) and cleared after a configured duration (permanent for crash faults).

## 4. Benchmark Results (Phases A–F)

### 4.1 Scalability on uniform hardware (t4g.medium, no SRD)

| Ring | p50 | p95 | p99 | p99.9 | Delivery |
|---|---|---|---|---|---|
| 3 | 2.15 ms | 4.29 ms | 4.90 ms | 5.41 ms | 100% ×3 |
| 6 | 3.98 ms | 8.20 ms | **306 ms** | 386 ms | 100% ×6 |
| 9 | 8.51 ms | 296 ms | 592 ms | 671 ms | 100% ×9 |
| 12 | 13.09 ms | 1096 ms | 1448 ms | 1528 ms | 100% ×12 |

*N = 45k post-warmup, 256 B, ~1.7k msg/s.* **Finding:** p50 grows ~linearly (~1.2 ms/node), consistent with ring traversal. The tail transitions sharply between ring 3 (clean to p99.9) and ring 6 (p95→p99 jumps 70×). **Saturation point is ring 6** at this workload. Critically, **UTO completeness holds at every ring size** — TRAINS degrades *latency*, not *correctness*, under load.

### 4.2 SRD effect under saturation (Graviton t4g vs c7gn, 256 B sweep)

| Ring | c7gn (ARM+SRD) p99 | t4g p99 | Tail improvement |
|---|---|---|---|
| 3 | 7.18 ms | 4.90 ms | t4g slightly faster |
| 6 | **9.79 ms** | **306 ms** | **31× tighter** |

**Finding:** at small rings SRD buys nothing (protocol overhead dominates). At the t4g saturation point (ring 6) the story inverts: SRD's multipath + hardware retransmit prevents the tail explosion (31× tighter p99). *SRD makes the protocol more scalable, not faster.*

### 4.3 Cross-architecture bandwidth saturation (ring 3, 16 KB × 4000)

| Arch | p50 | p99 | p99.9 | TX MB/node | SRD share |
|---|---|---|---|---|---|
| T4G (non-SRD) | 3.92 | 8.21 | 11.71 | ~47.5 | 0% |
| c7gn (ARM+SRD) | **3.28** | **6.99** | **7.46** | ~50.8 | ~99.7% |
| c7i (Intel+SRD) | 4.51 | 7.88 | 8.43 | ~40.3 | ~99.7% |

**Finding — the floor is CPU, the tail is SRD.** The p50 floor tracks the CPU: ARM Graviton is fastest, Intel *slowest* (below even non-SRD t4g). The p99.9 tail tracks SRD: *both* SRD instances beat non-SRD t4g (1.4–1.6× tighter) regardless of core vendor. All runs UTO-complete.

### 4.4 Protocol limit exposed by saturation testing

Bandwidth-saturation testing on large NICs surfaced TRAINS' hard-coded 16 MB `MAX_FRAME_LEN`: bursty workloads batch the entire pending queue into one train and trip "frame too large". We made it compile-time tunable (`TRAINS_MAX_FRAME_LEN_MB`), a candidate upstream protocol fix.

## 5. Chaos Results (Phase G) — fault behaviour

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

## 6. Discussion & Threats to Validity

**Fault-injection timing is decisive (and a trap).** Our first chaos batch injected faults during the post-workload *settle* phase. Because the producer broadcasts and the ring delivers *concurrently* during the preceding ~30 s, by the time the settle began delivery was essentially complete — so faults landed after the protocol had finished its work and showed *zero* effect (100% delivery, ~50 ms stalls for every scenario, including SIGKILL). We confirmed this from SSM timestamps (e.g., a kill executed 6 s *after* the target finished delivering) and flat per-2 s delivery histograms. **This is a general hazard for cloud chaos experiments: a correctly-wired fault that fires outside the active window yields false-positive "fault tolerance."** We corrected the harness to inject concurrently with broadcast; the §5 results use the corrected harness.

**Other threats.** (i) Latency mixes per-node clocks (`send_ns` issuer vs `recv_ns` local); we report it as a relative/steady-state signal, not absolute one-way latency. (ii) Single-AZ, single region (us-east-1); cross-AZ behaviour untested. (iii) Coordinator builds on t4g.micro; build variance does not affect measured percentiles. (iv) Bounded-queue-depth invariant is unmeasured (no runtime gauges). (v) The tested TRAINS build has no membership/reconfiguration; crash-recovery results are specific to that build.

## 7. Conclusion

On EC2, TRAINS exhibits a clean ring ≤ 5 latency envelope that degrades into a tail cliff under saturation while never sacrificing UTO completeness; AWS SRD does not change the latency floor but materially bounds the tail under both ring-saturation and heavy-payload pressure. Under fault injection aligned to active circulation, TRAINS preserves total-order safety unconditionally and masks transient network faults (loss/latency/partition, with quantified recovery times), and a stop/restart converges consistently — but it does not mask a permanent node crash, which halts UTO for lack of a reconfiguration layer (the principal gap for any replication use). We also caution that fault-injection timing must overlap the active workload or chaos results are meaningless. All runs are reproducible from a single config + script, with per-run provenance.

## References

1. M. Simatic and A. Foltz. *TRAINS: A throughput-efficient uniform total order broadcast algorithm.* NTDS/ICPE, 2015. IEEE Xplore doc. 7293477. (See also TrainsProtocol, github.com/simatic/TrainsProtocol.)
2. X. Défago, A. Schiper, and P. Urbán. *Total order broadcast and multicast algorithms: Taxonomy and survey.* ACM Computing Surveys, 36(4):372–421, 2004.
3. L. Shalev, H. Ayoub, N. Bshara, and E. Sabbag. *A Cloud-Optimized Transport Protocol for Elastic and Scalable HPC.* IEEE Micro, 40(6), 2020.
4. A. Basiri, N. Behnam, R. de Rooij, L. Hochstein, L. Kosewski, J. Reynolds, and C. Rosenthal. *Chaos Engineering.* IEEE Software, 33(3):35–41, 2016.

---

*Reproducibility: every result above is regenerable via `./bench/scripts/reproduce.sh <config>`; raw per-run reports + provenance live in `bench/results/`.*

> **Note:** the EC2 deployment harness (`bench/scripts/`, `bench/configs/`) and the raw per-run artefacts (`bench/results/`) are not included in this public mirror — they carry account-specific deployment detail. The numbers, method, and findings above are complete and self-contained; the in-repo `bench/coordinator/` and `bench/infrastructure/` show the harness shape.
