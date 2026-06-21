# Lineage and the TRAIN protocol — a code-oriented note

This note traces where TRAINS comes from, explains the train/ring mechanism in
engineer-friendly pseudo-code, and maps it onto the abstractions a reader who
knows Raft/Paxos already has.

---

## 1. Lineage

TRAINS has an unusually long arc — from a 1980s European research project to a
2026 mechanically-verified reimplementation.

### 1.1 Timeline

- **~1986–1989 — IOLE / RACE (European Commission).** The RACE programme
  (Research and development in Advanced Communications technologies in Europe)
  set out to build a fault-tolerant, hot-upgradable software environment for
  high-speed telecom systems — live update in production, automatic
  resynchronisation after failure. **Flaviu Cristian advised the project**, and
  his atomic-broadcast and group-membership papers — then circulating as
  photocopies — were the central academic reference.
- **~1989–1993 — Cegelec / Alcatel-Alsthom Recherche (Massy-Marcoussis).** The
  data-train protocol was industrialised for **process-control supervision**
  (Cegelec P3200 power-plant control systems), with primary/backup replicated
  objects written in Objective-C. This produced the two patents below.
- **2012–2016 — academic revival.** Michel Simatic (a co-inventor) returned to
  the protocol: a 2012 CNAM doctoral thesis, the *TRAINS* paper (CFIP/NOTERE
  2015), and BBOBB (DSN 2016), with an open-source implementation
  ([simatic/TrainsProtocol](https://github.com/simatic/TrainsProtocol)).
- **2026 — this repository.** A from-the-source Rust reimplementation, given a
  TLA+/Apalache/Ivy/Kani verification stack and extended with the one capability
  the original patents lacked: online node rejoin/re-admission.

### 1.2 Foundations: Flaviu Cristian

The conceptual foundation is **Flaviu Cristian's** work on fault-tolerant
broadcast — and, as above, the link is not just bibliographic: Cristian advised
the original project. By the project's own account, the **circulating-"train"
idea itself came from Cristian** — it emerged from his advice to the team, not
from a published paper, and Eychenne & Simatic turned it into the concrete ring
protocol below. (Cristian did *not* coin a "TRAIN" acronym; the name traces to
this work, the idea to him.) Two of his published contributions anchor the
design:

- **Atomic broadcast.** Cristian, Aghili, Strong & Dolev, *Atomic Broadcast:
  From Simple Message Diffusion to Byzantine Agreement* (FTCS-15, 1985;
  revised 1994; *Information and Computation* 118(1):158–179, 1995). This is
  the paper that frames atomic broadcast as the way to implement **synchronous
  replicated storage** — "a distributed storage that displays the same
  contents at every correct processor as of any clock time" — and derives a
  family of protocols tolerant of progressively harder failure classes
  (omission → timing → authentication-detectable Byzantine). The total-order
  delivery properties TRAINS preserves are the ones formalised in this line of
  work.
- **Processor-group membership.** Cristian, *Reaching Agreement on
  Processor-Group Membership in Synchronous Distributed Systems*, *Distributed
  Computing* 4:175–187 (1991). All correct processors compute the **same
  sequence of agreed views** (view 0, view 1, …), with bounded failure-detection
  and join delays. This is the membership-view model that any reconfiguration-
  capable total-order broadcast — TRAINS's online rejoin/re-admission included
  — has to implement.

### 1.3 Industrialisation and the patents

The **data-train mechanism itself** was the industrial realisation of Cristian's
atomic broadcast, reduced to practice and patented:

- **US 5,483,520 A** — *Method of broadcasting data by means of a data train* —
  inventors **Yves Eychenne and Michel Simatic**, assignee **Cegelec SA**;
  filed 1994-10-20, granted 1996-01-09. The patent describes a "train" of data
  circulating a looped network ("ring"); each node recovers, removes, and
  writes data into "cars" attached to the train, increments a header counter,
  and forwards it — including **fault recovery** that detects obsolete trains
  and establishes a new ring path when a node fails.
- **US 5,488,723 A** — *Software system having replicated objects and using
  dynamic messaging* — Baradel, Eychenne & Kohen, Cegelec SA, 1996 — built
  primary/backup replicated objects on top of the train broadcast.

Both patents have **expired and are in the public domain** (US 5,483,520 A
~2014; US 5,488,723 A in 2013), so the techniques can be implemented and
published freely. The academic record of the work is three papers: *Exploiting
late binding in object messaging* (ACM, 1992); *The use of object groups to
implement dependability in a process control supervision system* (FTCS-23,
Toulouse, 1994); and *Fault-tolerance and on-line maintainability in a process
control supervision system* (*Distributed System Engineering* 2, 1995).

### 1.4 Formal methods, then and now

TRAINS was verified with the formal tools of its day. The team modelled the
protocols with **coloured Petri nets** using a CNAM tool (from Gérard
Berthelot's group at CEDRIC), and **adapted Alcatel's internal protocol
code-generator** to emit the train logic — early model-driven and
formal-methods practice that is a direct ancestor of the TLA+/Apalache/Kani
stack this repository uses today. The thread is the same across thirty years:
*don't ship a broadcast protocol you have only tested.*

### 1.5 CAP before it had a name

In testing, TRAINS **halted** when the network lost coherence — a partition or
loss of quorum. In 1992 there was no vocabulary for that; the vocabulary arrived
in 2000 with Brewer's **CAP theorem** (under a partition you cannot have both
consistency and availability). TRAINS had already chosen **CP** — consistency
and partition-tolerance — the same call **etcd** makes today: for a power-plant
controller, "available" means "correct when healthy, silent when not." The ring
halting on crash, and recovering only via an agreed view change, is this choice
showing through (§3, and the protocol's UTO mode).

### 1.6 Where it sits in the taxonomy

Défago, Schiper & Urbán, *Total Order Broadcast
and Multicast Algorithms: Taxonomy and Survey* (*ACM Computing Surveys*
36(4):372–421, 2004), classifies total-order broadcast into fixed-sequencer,
moving-sequencer, **privilege-based**, communication-history, and
destinations-agreement families. A circulating train is a **privilege-based**
algorithm: the right to broadcast-and-order travels around the ring as a token.
The survey catalogues a "Train" protocol in exactly this class.

---

## 2. The TRAIN mechanism, in pseudo-code

A train is a token that carries a batch of messages and a record of who has
processed it. With `K` concurrent trains (one per *issuer* slot) you get `K`
independent privileges circulating at once — that is where the throughput comes
from.

```text
Train = {
    issuer   : ProcId          # which of the K slots this train is
    clock    : Tick            # strictly increasing per issuer  -> (clock, issuer) is a total order key
    payloads : Set(Message)    # messages picked up this lap
    acks     : Set(ProcId)     # who has seen+processed this train instance
}
```

Each node runs the same loop — there is **no leader**; the privilege is the
train in your hands:

```text
on receive(train) from predecessor:
    # 1. apply what the ring has agreed on (see delivery rule below)
    deliver_ready(train)

    # 2. pick up my pending local broadcasts onto a train I issue / forward
    if i_own_slot(train.issuer):
        train.payloads += drain(my_outbox)
        train.clock    += 1                 # advance this slot's logical clock

    # 3. record that I processed this instance, then pass the privilege on
    train.acks += { me }
    send(train) to successor

broadcast(m):                                # the TO-broadcast API, sender side
    my_outbox.append(m)                      # delivered when a train carries it full circle
```

**Delivery (the two-lap rule).** A message is not deliverable the moment it is
seen — that would not be *uniform*. It becomes deliverable only once its train
has gone around far enough that every node is guaranteed to also hold it, i.e.
`train.acks` has closed the ring for that `(issuer, clock)`. Messages are then
delivered in `(clock, issuer)` lexicographic order, identically at every node:

```text
deliver_ready(train):
    for (issuer, clock, m) in sorted_by_key(stable_entries(train)):
        if ring_closed(issuer, clock):       # uniform: all nodes hold it
            upcall deliver(m)                 # same order on every node
```

`ring_closed` is the subtle part — getting the "has everyone prior been
delivered?" predicate right is precisely where mechanical verification earned
its keep (see `VERIFICATION_REPORT.md`; the `AllPriorDelivered` fix in
`docs/paper.md` §4.4).

**Membership / view change (online rejoin).** When a node crashes the ring
halts (strongest, uniform mode). Recovery installs a **new agreed view** — the
direct descendant of Cristian's membership-view idea — and re-anchors train
circulation on it:

```text
on suspect(p):                               # failure detector fires
    propose view' = view \ {p}               # everyone converges on the same view'
    install(view')                            # ordered transition; ring re-stitched around p
    resume train circulation on view'

on rejoin(p):                                # the new part this repo adds
    state_transfer(p)                        # passive catch-up: p replays the delivered log
    when p is caught up:
        propose view'' = view ∪ {p}          # virtually-synchronous re-admit
        install(view'')                       # p returns as a full acking member
```

The re-admit is an *ordered membership transition executed inside the same
verified view-change machinery*, and TLC re-verifies that uniform total order
survives it (`ConsistentDelivery`, 6.28 M states).

---

## 3. Mapping to modern abstractions

| TRAINS concept | Modern equivalent |
|---|---|
| `broadcast(m)` / `deliver(m)` upcall | The **total-order broadcast (atomic broadcast) API** — the primitive under state-machine replication [Schneider 1990]. Raft's "append to the replicated log" + "apply committed entry" is the same API with a leader in the middle. |
| Circulating train = the privilege to order | **Privilege-based / token** ordering (Défago class). Contrast Raft/Paxos, which are **leader/sequencer-based**: one process orders everything. TRAINS spreads ordering work symmetrically; `K` trains = `K` privileges in flight. |
| `(clock, issuer)` lexicographic key | The **global sequence number** of a TO-broadcast. In multi-Paxos/Raft it is the log index assigned by the leader; here it is assigned by whoever holds the slot, made total by the issuer tiebreak. |
| Two-lap "ring_closed" before delivery | **Uniform agreement / commit point.** Raft commits once a quorum has the entry; TRAINS "commits" once the train has closed the ring (all `N`, not a quorum — the price of the symmetric ring and the source of its uniformity). |
| Agreed views `view0, view1, …` | **Group membership / virtual synchrony** [Cristian 1991; Birman's Isis]. The modern consensus analogue is **Raft joint consensus / Paxos reconfiguration**: membership changes are themselves ordered events in the log so all replicas switch configuration at the same logical point. |
| Online rejoin = state transfer → re-admit | **Snapshot install + catch-up + add-server** in Raft; **state transfer** in virtual-synchrony stacks. The replica replays the delivered log, then a membership transition adds it back to the voting/acking set. |
| Ring halts on crash until view change | The CP-side of the failure model: like Raft refusing to commit without a quorum, TRAINS refuses to deliver without the full ring — availability is restored by reconfiguration, not by weakening the safety property. |

**One-sentence summary for a Raft reader:** TRAINS is a leaderless,
token-passing total-order broadcast where the replicated-log index is carried
by a circulating "train" instead of assigned by a leader; membership changes
are ordered view installations (Cristian-style), and the online rejoin is the
direct analogue of Raft's snapshot-install + add-server, re-verified to
preserve uniform total order.

---

*Primary sources:* US 5,483,520 A (Google Patents); Cristian et al., *Atomic
Broadcast* (Inf. Comput. 1995); Cristian, *Processor-Group Membership* (Distrib.
Comput. 1991); Défago, Schiper & Urbán, *Taxonomy and Survey* (ACM Comput.
Surv. 2004); Simatic et al., *TRAINS* (CFIP/NOTERE 2015). Full citations in
`docs/paper.md` §References.
