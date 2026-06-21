# TRAINS — Architecture and Algorithm Diagrams

This page is the visual companion to [paper.md](paper.md) and [blog.md](blog.md).

## At a glance

![TRAINS two-lap uniform total-order broadcast on a ring](diagrams/trains-ring.svg)

The whole protocol in one picture: a leaderless ring, a **train** circulating
twice — **lap 1** gathers each node's messages and acks until the train is
`FULL_ACK`, **lap 2** propagates the frozen train so every node delivers the
identical batch in `(clock, issuer)` order. Delivering only after the train
closes the ring is what makes the order *uniform*. The detailed Mermaid diagrams
below (which render natively on GitHub) break down each piece.

---

## 1. Ring topology

```mermaid
flowchart LR
    subgraph Ring [Unidirectional ring of N processes]
        direction LR
        N0((Node 0)) --> N1((Node 1)) --> N2((Node 2)) --> N0
    end

    classDef issuer fill:#e7f5ff,stroke:#1971c2,color:#1971c2,stroke-width:2px
    classDef nonissuer fill:#fff,stroke:#aaa,color:#666

    class N0,N1 issuer
    class N2 nonissuer
```

With `RING_SIZE = 3` and `NumTrains = 2`, two processes are *issuers*
(blue) — they own a train slot. The third forwards and acks but never
issues.

---

## 2. Train data flow — one full cycle

```mermaid
sequenceDiagram
    autonumber
    participant N0 as Node 0 (issuer)
    participant N1 as Node 1
    participant N2 as Node 2

    Note over N0: issue train T<br/>clock=1, acks=001, msgs=∅

    rect rgb(231, 245, 255)
    Note over N0,N2: LAP 1 — collect acks and payloads
    N0->>N1: T(clock=1, acks=001, msgs=∅)
    Note over N1: load pending<br/>add ack
    N1->>N2: T(clock=1, acks=011, msgs=[m1,m2])
    Note over N2: load pending<br/>add ack → FULL_ACK
    N2->>N0: T(clock=1, acks=111, msgs=[m1,m2,m3])
    Note over N0: deliver locally<br/>(first node to see FULL_ACK)
    end

    rect rgb(255, 243, 191)
    Note over N0,N2: LAP 2 — propagate FULL_ACK so others deliver
    N0->>N1: T unchanged (acks=111)
    Note over N1: replay → deliver
    N1->>N2: T unchanged
    Note over N2: replay → deliver
    N2->>N0: T returning, key in doneKeys
    Note over N0: recycle: clock=2, msgs=∅
    end
```

The two-lap structure is load-bearing for `ConsistentDelivery`.
Lap 1 builds the train; Lap 2 distributes the *frozen* version of it
so every node delivers identical content.

---

## 3. State machine — one TrainsNode

![TrainsNode step() state machine](diagrams/node-state-machine.svg)

*The Mermaid source below renders the same structure natively on GitHub.*

```mermaid
stateDiagram-v2
    [*] --> Idle

    Idle --> Idle: AppBroadcast<br/>(append to pending)
    Idle --> Idle: Tick

    Idle --> ReceivedTrain: TrainReceived

    state ReceivedTrain {
        [*] --> CheckClock

        CheckClock --> Replay: prev_seen >= clock<br/>(lap-2 propagation)
        CheckClock --> FirstSight: prev_seen < clock<br/>(lap-1)
        CheckClock --> GapDetected: clock > prev+1<br/>(crash suspected)

        FirstSight --> LoadAndAck: load pending<br/>add ack
        Replay --> AttemptDeliver: train is closed
        GapDetected --> EmitDeclareCrash
        EmitDeclareCrash --> FirstSight

        LoadAndAck --> AttemptDeliver: AllPriorDelivered?
        AttemptDeliver --> ParkUntilDeliverable: NO
        AttemptDeliver --> Deliver: YES
        ParkUntilDeliverable --> Forward
        Deliver --> RecordKey
        RecordKey --> Forward
    }

    ReceivedTrain --> Idle: ForwardTrain emitted
```

Each `step(input) → Vec<output>` call traces a path through this
machine. The kernel is purely functional — no I/O, no allocation
beyond returned outputs. This is what makes the verification stack
tractable.

---

## 4. Multiple concurrent trains (NumTrains = 2)

![Two trains pipelining around the ring](diagrams/multi-train-pipeline.svg)

```mermaid
flowchart LR
    subgraph t0 [Time t]
        direction LR
        N0a((N0)) -- "T_A clock=1" --> N1a((N1))
        N1a((N1)) -- "T_B clock=1" --> N2a((N2))
        N2a((N2)) -.- N0a
    end

    subgraph t1 [Time t+1]
        direction LR
        N0b((N0)) -.- N1b((N1))
        N1b((N1)) -- "T_A clock=1<br/>acks=011" --> N2b((N2))
        N2b((N2)) -- "T_B clock=1<br/>acks=110" --> N0b
    end

    subgraph t2 [Time t+2]
        direction LR
        N0c((N0)) -- "T_A FULL_ACK<br/>(propagating)" --> N1c((N1))
        N1c((N1)) -.- N2c((N2))
        N2c((N2)) -- "T_B FULL_ACK<br/>(propagating)" --> N0c
    end

    t0 --> t1 --> t2
```

Two trains pipeline around the ring. With `NumTrains = K` the
aggregate throughput scales as `K × (per-train throughput)`, up to the
weakest link's bandwidth.

---

## 5. Verification stack

![The four-layer verification stack](diagrams/verification-stack.svg)

```mermaid
flowchart TD
    spec["TLA+ specification<br/>verification/tla/TRAINS.tla"]:::spec
    refmpl["Reference impl<br/>verification/reference/"]:::pure
    impl["Production impl<br/>crates/trains-core/"]:::pure
    net["TLS transport<br/>crates/trains-net/"]:::async
    cli["CLI demo<br/>crates/trains-cli/"]:::async

    tlc["TLC<br/>1.09M states / 25 s<br/>6 invariants + liveness"]:::ok
    proptest["PropTest fuzz<br/>256 schedules + crash"]:::ok
    drt["Differential RT<br/>384 cases (vs reference)"]:::ok
    kani["Kani / CBMC<br/>8 leaf harnesses, 0.23 s"]:::ok
    demo["Live 3-node TLS demo"]:::ok

    spec --> tlc
    spec -. derived from .-> impl
    spec -. derived from .-> refmpl

    impl --> proptest
    impl --> kani
    impl <--> drt
    refmpl <--> drt
    impl --> net
    net --> cli
    cli --> demo

    classDef spec fill:#fff3bf,stroke:#f59f00,color:#5c3a00
    classDef pure fill:#d3f9d8,stroke:#2f9e44,color:#0d3617
    classDef async fill:#a5d8ff,stroke:#1971c2,color:#0c4675
    classDef ok fill:#dee2e6,stroke:#495057,color:#212529
```

Each layer catches a different bug class:

| Layer | Catches |
|------|------|
| TLC | Spec-level race conditions, bad invariants, type errors |
| PropTest fuzz | Schedule-sensitive bugs the example tests miss |
| DRT | Production vs reference divergences (incl. dedupe / boundary cases) |
| Kani | Arithmetic / panic / overflow on leaf functions |

All four are complementary — a bug in one is rarely caught by another.

---

## 6. Workspace architecture

![Workspace crates and the pure/impure split](diagrams/workspace-architecture.svg)

```mermaid
flowchart TD
    cli["trains-cli<br/><b>I/O + binary</b>"]:::async
    ao["trains-ao<br/>AO adapter"]:::async
    net["trains-net<br/>TLS ring transport"]:::async

    core["trains-core<br/><b>pure protocol kernel</b><br/>step(input) → Vec(output)"]:::pure
    refmpl["trains-reference<br/>clarity-first reference"]:::pure
    drt["trains-drt<br/>differential harness"]:::test

    cli --> net
    cli --> core
    ao --> core
    net --> core
    drt --> core
    drt --> refmpl
    refmpl --> core

    classDef pure fill:#d3f9d8,stroke:#2f9e44,color:#0d3617
    classDef async fill:#a5d8ff,stroke:#1971c2,color:#0c4675
    classDef test fill:#fff3bf,stroke:#f59f00,color:#5c3a00
```

The pure / impure split is the most important architectural decision.
`trains-core` has zero `tokio`, zero syscalls, zero non-deterministic
state. It is the *only* crate the verification stack targets.

---

## 7. Comparison: Raft vs TRAINS critical path

![Raft leader bottleneck vs leaderless TRAINS ring](diagrams/raft-vs-trains.svg)

*The two Mermaid sequence diagrams below show the per-message critical path in detail.*

### Raft (steady-state replication)

```mermaid
sequenceDiagram
    participant Cli as Client
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2

    Cli->>L: append(msg)
    par
        L->>F1: AppendEntries(msg)
        L->>F2: AppendEntries(msg)
    end
    par
        F1-->>L: ack
        F2-->>L: ack
    end
    L-->>Cli: committed (1 RTT)
    par
        L->>F1: AppendEntries(commitIndex++)
        L->>F2: AppendEntries(commitIndex++)
    end
```

- Leader is the bottleneck.
- Per-decision: O(N) messages, ≈ 1 RTT critical path.
- Survives ⌊(N−1)/2⌋ crashes with automatic recovery.

### TRAINS (UTO mode)

```mermaid
sequenceDiagram
    participant Cli as Client
    participant N0 as Node 0
    participant N1 as Node 1
    participant N2 as Node 2

    Cli->>N0: broadcast(msg)
    Note over N0: appended to pending[0]<br/>(no extra RTT)
    Note over N0: msg rides next train slot

    rect rgb(231, 245, 255)
    Note over N0,N2: lap 1 — gather
    N0->>N1: T (msg piggybacked)
    N1->>N2: T
    N2->>N0: T FULL_ACK
    Note over N0: deliver
    end

    rect rgb(255, 243, 191)
    Note over N0,N2: lap 2 — propagate
    N0->>N1: T unchanged
    Note over N1: deliver
    N1->>N2: T
    Note over N2: deliver
    N2->>N0: T returning
    end

    Note over N0: recycle slot, clock++
```

- No leader; every node forwards equal load.
- Per-message: amortised O(N) across batch; 2 ring laps until delivery.
- UTO mode halts on **any** crash (the strongest safety mode).

---

## 8. The bug TLC found

```mermaid
sequenceDiagram
    participant N0 as Node 0
    participant N1 as Node 1

    Note over N0,N1: Initial state: doneKeys[0]={(1,0)}, doneKeys[1]={(1,0)}<br/>slot1 (issuer 0) at clock=1<br/>slot2 (issuer 1) at clock=1

    N0-->>N0: ProcessTrain on slot 2 → slot2 reaches (2, 1) FULL_ACK
    Note over N0: AllPriorDelivered(0, (2,1))?<br/>slot1.current_key = (1,0) ∈ doneKeys[0] ✓<br/>slot2.current_key = (2,1) ✓ (this train)<br/>--> TRUE → deliver (2,1) [m2]

    Note over N0: delivered[0] = ⟨m1, m2⟩

    N1-->>N1: slot1 advances to (2, 0)<br/>via RecycleTrain
    N1-->>N1: ProcessTrain on slot 1 → slot1 reaches (2, 0) FULL_ACK
    Note over N1: AllPriorDelivered(1, (2,0))?<br/>slot1.current_key = (2,0) (this train)<br/>slot2.current_key = (2,1) — NOT smaller<br/>--> TRUE → deliver (2,0) [m3]

    Note over N1: delivered[1] = ⟨m1, m3⟩

    Note over N0,N1: ConsistentDelivery VIOLATED:<br/>⟨m1, m2⟩ vs ⟨m1, m3⟩<br/>neither is a prefix of the other.
```

The fix introduces a global `issuedKeys` set + an `Issuers`
clock-catchup precondition so the unsafe interleaving is no longer
enabled. See `paper.md` §4.4.

---

## 9. Throughput model — TRAINS vs Raft (qualitative)

![Throughput vs N — TRAINS scales with trains, Raft is leader-bound](diagrams/throughput-model.svg)

```mermaid
xychart-beta
    title "Aggregate throughput vs N (illustrative)"
    x-axis "Number of processes (N)" [3, 5, 7, 9, 11, 13]
    y-axis "Throughput (msg/s, normalised)"
    line "Raft (leader-bound)" [100, 80, 65, 55, 48, 42]
    line "TRAINS (K=4 trains)" [60, 100, 130, 155, 175, 195]
    line "TRAINS (K=2 trains)" [40, 60, 75, 87, 95, 105]
```

*Illustrative — actual numbers depend on hardware, network, and message
size. The qualitative shape is the point: TRAINS scales with the number
of concurrent trains, Raft is bottlenecked at the leader.*

The crossover between Raft and TRAINS lies at `K > 2N / (N − 1)`
(see paper.md §6.2). For N = 5, three or more concurrent trains beat
Raft on broadcast throughput.

---

## 10. Group membership — exclude, catch up, re-admit

![Crash → exclude, v2 passive catch-up, v3 virtually-synchronous re-admit](diagrams/rejoin-readmit.svg)

The node lifecycle the original protocol lacked. A confirmed crash triggers a
`Reconfigure` view change (the survivors adopt the most-advanced log and run at
N−1); the recovered node first rejoins as a **passive read-replica (v2)** that
catches up from a survivor's snapshot + delivered-effect tail; then a
**virtually-synchronous re-admit view change (v3)** returns it as a full acking
member, restoring N-redundancy. TLC checks `ConsistentDelivery` survives the
membership change (6.28 M states) — order holds as the view both shrinks and
grows. See [`WHITEPAPER-rejoin-and-readmission-2026-06-16.md`](WHITEPAPER-rejoin-and-readmission-2026-06-16.md)
and `paper.md` §5.2.
