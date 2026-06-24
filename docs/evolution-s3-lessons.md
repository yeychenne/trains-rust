# Looking forward: S3 lessons for TRAINS and trains-valkey

AWS S3's evolution toward strong consistency is a useful comparison point for
TRAINS, but not because TRAINS should copy S3's internal architecture. S3 solves
object-storage metadata, indexing, caching, placement, durability, repair, and
availability at a scale far beyond the intended scope of this project. The
transferable lesson is narrower and more valuable: correctness became an
engineering discipline, not a one-time protocol decision.

The strongest conclusion is therefore conservative. TRAINS should keep its
consistency-first total-order core. `trains-valkey` should keep using that core
to protect acknowledged writes. The evolution path is to surround the verified
ordering primitive with the operational habits that make a replicated system
remain trustworthy over time: continuous verification, crash testing, replica
audit, repair, clear failure-domain assumptions, and observable durability.

## What S3 suggests

Public accounts of S3's internals describe a move from eventual consistency to
strong consistency using replicated journals, careful cache-coherency protocols,
failure allowance, formal methods, crash-consistency validation, and continuous
repair. The important point is not the exact journal structure. It is that S3's
team treats correctness as a live system property:

- models and implementation are checked together;
- executable reference models and property tests catch implementation drift;
- crash consistency is tested as part of normal development;
- repair and audit are permanent system responsibilities;
- observability tells operators whether durability is actually holding;
- correlated failures are treated as the real enemy, not just single-node loss.

That maps cleanly to the next stage of this project. `trains-rust` already has a
small protocol kernel, a TLA+ model, differential testing, fuzzing, crash
injection, and runtime trace validation. `trains-valkey` already applies that
kernel to a practical data-system boundary: preserving acknowledged Redis/Valkey
writes through failover, rejoin, and re-admission. The opportunity is to make the
surrounding product as disciplined as the core.

## Direction for trains-rust

`trains-rust` should not borrow S3's replicated-journal design wholesale.
TRAINS already is the ordered broadcast primitive. Replacing it with a
leader-based or S3-like journal would discard the project's small-kernel
advantage, its lineage, and its proof investment.

The useful borrow is the verification workflow:

- Treat the Rust implementation, reference model, TLA+/Apalache model, and
  protocol tests as one change surface.
- Gate protocol changes on fast checks in PRs, and run deeper model-checking
  configurations nightly or before release.
- Expand properties around re-admit view changes, view fencing, interrupted
  snapshot/tail transfer, duplicate or replayed frames, and stale recovered
  members.
- Keep the pure protocol kernel boundary strict so the core remains amenable to
  model checking, differential testing, and bounded verification.
- Track operational protocol metrics, including view-change latency, re-admit
  duration, undelivered-train age, and failed repair attempts.

The CP contract should remain explicit. TRAINS should halt rather than create
conflicting histories. Gossip, SWIM-style health, or anti-entropy can be useful
around the protocol, but they must not become an eventually consistent write
path.

## Direction for trains-valkey

`trains-valkey` is where the S3 lesson becomes most operational. Preserving an
acked write is not only a write-path property; it is also an audit-and-repair
property. A deployed system needs to keep proving to itself that every survivor
has the same durable effects and that a returning node has caught up from a
contiguous, canonical history.

The next layer should be a replica auditor:

- compare delivered indexes across replicas;
- compare per-origin dedup watermarks and recent-id windows;
- compute bounded keyspace digests or sampled digests from Valkey;
- validate snapshot plus delivered-tail continuity;
- detect stale local engines after restart;
- trigger state transfer or operator-visible quarantine on mismatch.

That auditor should remain outside the write-ordering path. It can use
anti-entropy and periodic probes because its job is detection and repair, not
deciding the order of acknowledged writes.

The repair path should also become explicit:

- choose a canonical survivor for state transfer;
- import a snapshot and contiguous tail from the same source;
- verify post-import delivered index and digest;
- only then allow passive catch-up or active re-admit;
- record the repair source, target, duration, and result.

This gives `trains-valkey` a smaller version of S3's permanent repair habit
without importing S3's service decomposition or storage-tier complexity.

## What not to borrow

Several S3 lessons do not transfer directly:

- Do not weaken the CP contract to maintain apparent write availability during
  partition.
- Do not put gossip or anti-entropy on the acknowledged-write ordering path.
- Do not add S3-scale microservice decomposition to a small proxy and protocol
  core.
- Do not serve writes through a node that has not completed catch-up and, when
  applicable, ordered re-admission.
- Do not treat cache coherence as a substitute for deterministic ordered apply.

The product promise should stay simple: acknowledged writes survive, membership
changes are ordered, recovery is conservative, and the system stops rather than
inventing conflicting histories.

## Evolution backlog

1. Add a `trains-valkey` replica-auditor design note covering delivered-index
   checks, dedup watermark comparison, keyspace digests, and repair triggers.
2. Add a fast/deep/pre-release verification policy for `trains-rust`, with fast
   PR checks and scheduled deeper model-checking runs.
3. Add chaos scenarios for interrupted state transfer, stale backend restart,
   duplicate/replayed ring frames, and correlated node loss.
4. Add durability and repair metrics: acked writes preserved, replica lag, last
   successful audit, last repair source, divergence detections, rejoin duration,
   and re-admit duration.
5. Add a documented repair runbook: canonical survivor selection, snapshot/tail
   import, verification, quarantine, and promotion.
6. Keep binary-distribution and deployment hardening on the roadmap: signed
   bench artifacts, S3 bucket hardening, view-change frame authorization, and
   long-term key revocation.

The target is verified ordering, observable durability, automatic repair, and
boring operations. The project already shows that the ordering core can be small
and trustworthy. The next proof point is that a deployed system can remain
trustworthy through restarts, stale disks, partial transfers, operator mistakes,
artifact swaps, and correlated infrastructure failures.
