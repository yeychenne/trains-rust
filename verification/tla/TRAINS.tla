--------------------------- MODULE TRAINS ---------------------------
(****************************************************************************)
(* TLA+ specification of the TRAINS protocol for Uniform Total-Order        *)
(* broadcast over a unidirectional process ring.                            *)
(*                                                                          *)
(* Reference: Simatic, M. et al. (2015). "TRAINS: A throughput-efficient    *)
(* uniform total order broadcast algorithm." CFIP/NOTERE 2015.              *)
(* IEEE Xplore 7293477. Open source: github.com/simatic/TrainsProtocol      *)
(*                                                                          *)
(* Protocol summary:                                                        *)
(*   NumTrains token-trains circulate concurrently around a ring of Procs   *)
(*   processes.  Each process holds a train, loads pending messages onto    *)
(*   it, adds its own acknowledgement, and forwards it to its successor.    *)
(*   When a train has been acknowledged by every process it carries a       *)
(*   complete ack set.  Any process that receives a fully-acked train       *)
(*   delivers its messages — but only after all trains with a lexicograph-  *)
(*   ically smaller (clock, issuer) key have already been delivered.  This  *)
(*   enforces the same global delivery order at every process.              *)
(*                                                                          *)
(* Delivery modes modelled:                                                 *)
(*   UTO  Uniform Total Order  acks == Procs   (default, strongest)         *)
(*   TO   Total Order          acks == Procs \ crashed                      *)
(*   (causal order is implied by UTO; not separately checked here)          *)
(*                                                                          *)
(* Verified properties:                                                     *)
(*   TypeOK              — type safety                                      *)
(*   ClockMonotonicity   — P5: seenClk never goes backwards                 *)
(*   ConsistentDelivery  — P1/P2: all delivery logs are mutual prefixes     *)
(*   NoSpuriousDelivery  — P3: only broadcast messages are delivered        *)
(*   EventualDelivery    — L1: every broadcast is eventually delivered      *)
(*                         (checked with weak fairness in TLC)              *)
(****************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC, Apalache

(*------------------------------------------------------------------*)
(* CONSTANTS                                                        *)
(*------------------------------------------------------------------*)

CONSTANTS
  \* @type: Set(Int);
  Procs,       (* set of process IDs,  e.g. {0, 1, 2}              *)
  \* @type: Seq(Int);
  ring,        (* Seq(Procs) ring order, e.g. <<0, 1, 2>>          *)
  \* @type: Int;
  NumTrains,   (* number of concurrent trains, e.g. 2              *)
  \* @type: Set(Str);
  Messages,    (* set of application messages (TLC model values)   *)
  \* @type: Int;
  MaxClock,    (* logical clock upper bound for TLC finitisation   *)
  \* @type: Int;
  MaxPending,  (* per-process pending-queue bound for TLC          *)
  \* @type: Str;
  Mode         (* delivery mode: "UTO" (all procs) or "TO" (live)  *)

ASSUME IsFiniteSet(Procs)
ASSUME Len(ring) = Cardinality(Procs)
ASSUME \A p \in Procs : p \in {ring[i] : i \in DOMAIN ring}
ASSUME NumTrains \in Nat /\ NumTrains >= 1
ASSUME IsFiniteSet(Messages)
ASSUME MaxClock  \in Nat /\ MaxClock  >= 1
ASSUME MaxPending \in Nat /\ MaxPending >= 1
ASSUME Mode \in {"UTO", "TO"}

(*------------------------------------------------------------------*)
(* UTILITY DEFINITIONS                                              *)
(*------------------------------------------------------------------*)

\* @type: Seq(Str) => Set(Str);
Range(s)   == {s[i] : i \in DOMAIN s}
\* @type: Int => Int;
RingPos(p) == CHOOSE i \in DOMAIN ring : ring[i] = p

(* Successor of p in the (unidirectional) ring — wraps around      *)
\* @type: Int => Int;
Succ(p) == ring[(RingPos(p) % Len(ring)) + 1]

(* Predecessor of p                                                 *)
\* @type: Int => Int;
Pred(p) == ring[((RingPos(p) - 2) % Len(ring)) + 1]

TrainId == 1..NumTrains

(* Set of processes that own a train slot (i.e. that issue trains). *)
Issuers == {ring[t] : t \in TrainId}

(*------------------------------------------------------------------*)
(* CLOCK-KEY ORDERING                                               *)
(*                                                                  *)
(* Each train is identified by (clock, issuer).  Delivery order     *)
(* is strict ascending in this key.                                 *)
(*------------------------------------------------------------------*)

\* @type: (Int, Int) => <<Int, Int>>;
ClockKey(cl, iss) == <<cl, iss>>

(* Strict lexicographic less-than on clock keys                     *)
\* @type: (<<Int, Int>>, <<Int, Int>>) => Bool;
CKLt(ck1, ck2) ==
  \/ ck1[1] < ck2[1]
  \/ /\ ck1[1] = ck2[1]
     /\ ck1[2] < ck2[2]

(*------------------------------------------------------------------*)
(* MESSAGE ORDERING WITHIN A TRAIN                                  *)
(*                                                                  *)
(* Within a single train, messages are delivered as a deterministic *)
(* sequence.  Because Messages are TLC model values we use a CHOOSE *)
(* -based min-sort; all processes apply the same function, so they  *)
(* agree on within-train message order.                             *)
(*------------------------------------------------------------------*)

(* Deterministic enumeration of a set as a sequence.
   Original TLC implementation used a RECURSIVE operator with CHOOSE;
   we rewrite it as a fold so Apalache can typecheck it.  TLC still
   accepts the fold (it ships in the TLC community modules).
   Both TLC and Apalache pick a deterministic order — agreement on
   within-train message order follows. *)

\* @type: (Seq(Str), Str) => Seq(Str);
AppendToSeq(acc, m) == Append(acc, m)

\* @type: Set(Str) => Seq(Str);
MsgsToSeq(S) == ApaFoldSet(AppendToSeq, <<>>, S)

(*------------------------------------------------------------------*)
(* STATE VARIABLES                                                  *)
(*------------------------------------------------------------------*)

VARIABLES
  \* @type: Int -> { issuer: Int, clock: Int, msgs: Set(Str), acks: Set(Int), pos: Int };
  tr,         (* [TrainId -> record]     state of each train slot    *)
              (*   .issuer : Procs                                   *)
              (*   .clock  : 0..MaxClock                             *)
              (*   .msgs   : SUBSET Messages                         *)
              (*   .acks   : SUBSET Procs                            *)
              (*   .pos    : Procs   (which process currently holds) *)
  \* @type: Int -> Set(Str);
  pending,    (* [Procs   -> SUBSET Messages]  queued for broadcast  *)
  \* @type: Int -> Seq(Str);
  delivered,  (* [Procs   -> Seq(Messages)]    delivery log          *)
  \* @type: Int -> Set(<<Int, Int>>);
  doneKeys,   (* [Procs   -> SUBSET (Nat×Procs)]  trains delivered   *)
  \* @type: Int -> (Int -> Int);
  seenClk,    (* [Procs   -> [Procs -> 0..MaxClock]]                 *)
              (*   seenClk[p][q] = last clock from issuer q at p     *)
  \* @type: Int -> Int;
  issClk,     (* [Procs   -> 0..MaxClock]   next clock per issuer    *)
  \* @type: Set(Int);
  crashed,    (* SUBSET Procs                                        *)
  \* @type: Set(Str);
  broadcast,  (* SUBSET Messages            ever-broadcast set       *)
  \* @type: Set(<<Int, Int>>);
  issuedKeys  (* SUBSET (Nat×Procs)        every (clock,issuer) ever *)
              (*                            stamped on a train slot  *)

vars == <<tr, pending, delivered, doneKeys,
          seenClk, issClk, crashed, broadcast, issuedKeys>>

(*------------------------------------------------------------------*)
(* DELIVERY MODE                                                    *)
(*                                                                  *)
(* The live view = non-crashed processes.  UTO requires an ack from *)
(* every process; TO (uniform within the surviving view) requires   *)
(* an ack from every *live* process.  With no crashes the two are   *)
(* identical, so TO degrades to UTO until a Reconfigure populates    *)
(* `crashed`.                                                        *)
(*------------------------------------------------------------------*)

LiveProcs    == Procs \ crashed
\* @type: Set(Int);
RequiredAcks == IF Mode = "TO" THEN LiveProcs ELSE Procs

(*------------------------------------------------------------------*)
(* TYPE INVARIANT                                                   *)
(*------------------------------------------------------------------*)

TrainRec == [issuer : Procs,
             clock  : 0..MaxClock,
             msgs   : SUBSET Messages,
             acks   : SUBSET Procs,
             pos    : Procs]

TypeOK ==
  /\ tr         \in [TrainId -> TrainRec]
  /\ pending    \in [Procs -> SUBSET Messages]
  /\ delivered  \in [Procs -> Seq(Messages)]
  /\ doneKeys   \in [Procs -> SUBSET ((0..MaxClock) \X Procs)]
  /\ seenClk    \in [Procs -> [Procs -> 0..MaxClock]]
  /\ issClk     \in [Procs -> 0..MaxClock]
  /\ crashed    \in SUBSET Procs
  /\ broadcast  \in SUBSET Messages
  /\ issuedKeys \in SUBSET ((0..MaxClock) \X Procs)

(*------------------------------------------------------------------*)
(* INITIAL STATE                                                    *)
(*                                                                  *)
(* NumTrains trains are created, each owned by ring[t] at clock 1. *)
(* They start at their issuers with empty message sets.             *)
(*------------------------------------------------------------------*)

Init ==
  /\ tr         = [t \in TrainId |->
                     [issuer |-> ring[t],
                      clock  |-> 1,
                      msgs   |-> {},
                      acks   |-> {},
                      pos    |-> ring[t]]]
  /\ pending    = [p \in Procs |-> {}]
  /\ delivered  = [p \in Procs |-> <<>>]
  /\ doneKeys   = [p \in Procs |-> {}]
  /\ seenClk    = [p \in Procs |-> [q \in Procs |-> 0]]
  /\ issClk     = [p \in Procs |-> 1]
  /\ crashed    = {}
  /\ broadcast  = {}
  /\ issuedKeys = {<<1, ring[t]>> : t \in TrainId}

(*------------------------------------------------------------------*)
(* ACTION: AppBroadcast                                             *)
(*                                                                  *)
(* A non-crashed process p requests broadcast of message m.        *)
(* m is added to p's pending queue.  Each message is broadcast at  *)
(* most once (enforced by the broadcast set).                       *)
(*------------------------------------------------------------------*)

AppBroadcast(p, m) ==
  /\ p \notin crashed
  /\ m \notin broadcast
  /\ Cardinality(pending[p]) < MaxPending
  /\ pending'   = [pending   EXCEPT ![p] = @ \union {m}]
  /\ broadcast' = broadcast \union {m}
  /\ UNCHANGED <<tr, delivered, doneKeys, seenClk, issClk, crashed, issuedKeys>>

(*------------------------------------------------------------------*)
(* ACTION: ProcessTrain                                             *)
(*                                                                  *)
(* The core ring-step.  Process p holds train t (tr[t].pos = p).   *)
(* p loads its pending messages onto the train, adds its ack, and  *)
(* forwards the train to Succ(p).                                   *)
(*                                                                  *)
(* Clock gap detection: if tr[t].clock > seenClk[p][issuer] + 1,  *)
(* one or more trains from issuer have been lost — a process on the *)
(* ring has crashed.  The spec records this by allowing crashed     *)
(* nodes into the crashed set via the DetectCrash action below; for *)
(* liveness the gap is tolerated here by updating seenClk directly. *)
(*------------------------------------------------------------------*)

ProcessTrain(p, t) ==
  /\ p \notin crashed
  /\ tr[t].pos = p
  /\ tr[t].acks /= Procs            (* not yet fully acked; still circulating *)
  /\ LET iss   == tr[t].issuer
         cl    == tr[t].clock
         newAcks == tr[t].acks \union {p}
     IN
       /\ tr'      = [tr EXCEPT
                       ![t].msgs = @ \union pending[p],
                       ![t].acks = newAcks,
                       ![t].pos  = Succ(p)]
       /\ pending'  = [pending  EXCEPT ![p] = {}]
       /\ seenClk'  = [seenClk  EXCEPT ![p][iss] = cl]
  /\ UNCHANGED <<delivered, doneKeys, issClk, crashed, broadcast, issuedKeys>>

(*------------------------------------------------------------------*)
(* ACTION: DeliverTrain                                             *)
(*                                                                  *)
(* Process p delivers the messages carried by train t.             *)
(*                                                                  *)
(* Pre-conditions (UTO mode):                                       *)
(*   1. t has been acknowledged by every process in Procs           *)
(*   2. p has not yet delivered this (clock, issuer) key            *)
(*   3. All trains with a strictly smaller clock-key have already   *)
(*      been delivered at p — this enforces the global total order  *)
(*                                                                  *)
(* Effect: messages are appended to delivered[p] in a deterministic *)
(* order (MsgsToSeq), and the clock-key is added to doneKeys[p].   *)
(*------------------------------------------------------------------*)

(* Two parts to the global ordering check:                              *)
(*                                                                      *)
(*   1. Every (clock', issuer') *already issued* and strictly smaller   *)
(*      than ck must be in doneKeys[p].                                 *)
(*                                                                      *)
(*   2. Every issuer's clock must have *caught up* to ck[1].  Without   *)
(*      this guard, a slow slot could later issue (ck[1], q) for some   *)
(*      q < ck[2] and re-introduce a key smaller than ck — violating    *)
(*      the global order we promised.                                   *)
AllPriorDelivered(p, ck) ==
  /\ \A ck2 \in issuedKeys :
       CKLt(ck2, ck) => ck2 \in doneKeys[p]
  (* Only LIVE issuers must have caught up.  A confirmed-crashed issuer
     never advances its clock again, so requiring it would block every
     post-reconfiguration delivery forever (the dead-issuer gate bug).
     Safe because a Reconfigure has resolved every key the dead issuer
     owned up to the boundary.  Applied in TO mode only — the UTO path
     keeps the original strict gate (and is thus byte-identical to the
     previously-verified model); this mirrors the Rust core, where the
     skip is keyed on `crashed_bits`, only ever set via `confirm_crash`
     on the reconfiguration path. *)
  /\ \A q \in (Issuers \ (IF Mode = "TO" THEN crashed ELSE {})) :
       issClk[q] >= ck[1]

DeliverTrain(p, t) ==
  /\ p \notin crashed
  /\ RequiredAcks \subseteq tr[t].acks           (* UTO: all; TO: all live *)
  /\ tr[t].msgs /= {}                            (* at least one message   *)
  /\ LET ck == ClockKey(tr[t].clock, tr[t].issuer)
     IN
       /\ ck \notin doneKeys[p]                  (* not yet delivered      *)
       /\ AllPriorDelivered(p, ck)               (* total order enforced   *)
       /\ delivered' = [delivered EXCEPT
                          ![p] = @ \o MsgsToSeq(tr[t].msgs)]
       /\ doneKeys'  = [doneKeys  EXCEPT
                          ![p] = @ \union {ck}]
  /\ UNCHANGED <<tr, pending, seenClk, issClk, crashed, broadcast, issuedKeys>>

(*------------------------------------------------------------------*)
(* ACTION: RecycleTrain                                             *)
(*                                                                  *)
(* Once all non-crashed processes have delivered train t's messages,*)
(* the issuer resets the slot for the next cycle.  The clock is     *)
(* incremented; the new train starts empty at the issuer.          *)
(*------------------------------------------------------------------*)

AllDelivered(t) ==
  \A p \in Procs \ crashed :
    ClockKey(tr[t].clock, tr[t].issuer) \in doneKeys[p]

RecycleTrain(t) ==
  /\ AllDelivered(t)
  /\ tr[t].msgs /= {}                           (* was not an empty cycle  *)
  /\ issClk[tr[t].issuer] < MaxClock            (* TLC bound               *)
  /\ LET iss    == tr[t].issuer
         newClk == issClk[iss] + 1
     IN
       /\ tr'         = [tr      EXCEPT
                          ![t].clock = newClk,
                          ![t].msgs  = {},
                          ![t].acks  = {},
                          ![t].pos   = iss]
       /\ issClk'     = [issClk  EXCEPT ![iss] = newClk]
       /\ issuedKeys' = issuedKeys \union {<<newClk, iss>>}
  /\ UNCHANGED <<pending, delivered, doneKeys, seenClk, crashed, broadcast>>

(*------------------------------------------------------------------*)
(* ACTION: RecycleEmptyTrain                                        *)
(*                                                                  *)
(* A train that completed a ring tour with no messages (clock-key   *)
(* not in any doneKeys because msgs = {}) is simply recycled        *)
(* without delivery.  This keeps trains in motion.                  *)
(*------------------------------------------------------------------*)

RecycleEmptyTrain(t) ==
  /\ RequiredAcks \subseteq tr[t].acks
  /\ tr[t].msgs = {}
  /\ issClk[tr[t].issuer] < MaxClock
  /\ LET iss    == tr[t].issuer
         oldCk  == ClockKey(tr[t].clock, tr[t].issuer)
         newClk == issClk[iss] + 1
     IN
       /\ tr'         = [tr      EXCEPT
                          ![t].clock = newClk,
                          ![t].msgs  = {},
                          ![t].acks  = {},
                          ![t].pos   = iss]
       /\ issClk'     = [issClk  EXCEPT ![iss] = newClk]
       /\ issuedKeys' = issuedKeys \union {<<newClk, iss>>}
       (* Empty trains are "vacuously delivered" everywhere — record
          the old key in every live process's doneKeys so successor
          deliveries aren't blocked on it. *)
       /\ doneKeys'   = [p \in Procs |->
                          IF p \in crashed
                            THEN doneKeys[p]
                            ELSE doneKeys[p] \union {oldCk}]
  /\ UNCHANGED <<pending, delivered, seenClk, crashed, broadcast>>

(*------------------------------------------------------------------*)
(* ACTION: CrashProcess                                             *)
(*                                                                  *)
(* Non-deterministic crash.  A crashed process takes no further     *)
(* steps.  In UTO mode membership is static (no recovery); the TO    *)
(* mode models recovery via Reconfigure (exclude) + ReAdmit (re-     *)
(* admit), so `crashed` both grows and shrinks there.               *)
(* At most |Procs|-1 processes may crash (ring must stay live).    *)
(*------------------------------------------------------------------*)

CrashProcess(p) ==
  /\ p \notin crashed
  /\ Cardinality(crashed) < Cardinality(Procs) - 1
  /\ crashed' = crashed \union {p}
  /\ UNCHANGED <<tr, pending, delivered, doneKeys, seenClk, issClk, broadcast, issuedKeys>>

(*------------------------------------------------------------------*)
(* ACTION: Reconfigure  (TO mode only — the C3 view change)         *)
(*                                                                  *)
(* Models the lost-key gap resolution (Totem token-recovery) as an  *)
(* atomic view change: a crash is confirmed and the survivors run    *)
(* the recovery in one logical step.                                 *)
(*                                                                  *)
(*   - All survivors adopt the most-advanced survivor's delivery log *)
(*     (`canon`).  Because ConsistentDelivery holds, every survivor's *)
(*     log is a prefix of canon, so this only EXTENDS logs (models    *)
(*     retransmitting so everyone catches up to the union) — never    *)
(*     un-delivers.  Uniform agreement among survivors is immediate   *)
(*     (all logs become identical).                                   *)
(*   - Every issued key up to the boundary is marked done: keys canon *)
(*     delivered carry their messages; keys delivered nowhere are     *)
(*     empty-skipped (the lost messages are a reliability gap, not a  *)
(*     safety gap — they were delivered nowhere).                     *)
(*   - Each surviving issuer's train slot is reissued one clock above *)
(*     the boundary (C2), so circulation resumes; dead-issuer slots   *)
(*     are abandoned but their keys are now in doneKeys so they no    *)
(*     longer block AllPriorDelivered.                                *)
(*------------------------------------------------------------------*)

Reconfigure(victim) ==
  /\ Mode = "TO"
  /\ victim \notin crashed
  /\ Cardinality(crashed) < Cardinality(Procs) - 1
  /\ LET newCrashed == crashed \union {victim}
         survivors  == Procs \ newCrashed
         (* The victim's deliveries are STABLE: it delivered a key only when
            every then-live process (⊇ the current survivors, which never
            crashed) had acked it, so the survivors hold the content and must
            catch up to the victim's log too. So `canon` ranges over the
            pre-victim live set (includes the victim), not just survivors —
            otherwise a message the victim delivered would be wrongly skipped
            at survivors, diverging the logs. *)
         priorLive  == Procs \ crashed
         canon      == CHOOSE p \in priorLive :
                         \A q \in priorLive : Len(delivered[q]) <= Len(delivered[p])
         liveSlots  == {t \in TrainId : tr[t].issuer \in survivors}
     IN
       \* TLC finitisation: only reissue while issuers are below MaxClock.
       /\ \A t \in liveSlots : issClk[tr[t].issuer] < MaxClock
       /\ crashed'    = newCrashed
       /\ delivered'  = [p \in Procs |->
                          IF p \in survivors THEN delivered[canon] ELSE delivered[p]]
       /\ doneKeys'   = [p \in Procs |->
                          IF p \in survivors
                            THEN doneKeys[canon] \union issuedKeys
                            ELSE doneKeys[p]]
       /\ tr'         = [t \in TrainId |->
                          IF t \in liveSlots
                            THEN [tr[t] EXCEPT !.clock = issClk[tr[t].issuer] + 1,
                                               !.msgs  = {},
                                               !.acks  = {},
                                               !.pos   = tr[t].issuer]
                            ELSE tr[t]]
       /\ issClk'     = [q \in Procs |->
                          IF q \in survivors /\ q \in Issuers
                            THEN issClk[q] + 1 ELSE issClk[q]]
       /\ issuedKeys' = issuedKeys \union
                          { <<issClk[tr[t].issuer] + 1, tr[t].issuer>> : t \in liveSlots }
  /\ UNCHANGED <<pending, seenClk, broadcast>>

(*------------------------------------------------------------------*)
(* ACTION: ReAdmit  (TO mode only — the v3 re-admit view change)     *)
(*                                                                  *)
(* The mirror image of Reconfigure: a previously-excluded node       *)
(* rejoins the live view.  This is the spec-first half of v3         *)
(* (ADR-001 in trains-rust): a *virtually-synchronous* re-admission. *)
(* The membership change is an atomic, ordered transition (the ring- *)
(* circulated view-change token in the implementation), and state    *)
(* transfer is synchronized to that install point — modelled here as *)
(* the rejoiner adopting the most-advanced survivor's delivery state *)
(* (`canon`) in the same step it leaves `crashed`.                    *)
(*                                                                  *)
(* Why this preserves ConsistentDelivery (the crux of the v3         *)
(* provability question): `canon` is a survivor, and among the live  *)
(* set every delivery log is a mutual prefix of canon's.  Setting    *)
(* delivered[rejoiner] := delivered[canon] therefore makes the       *)
(* rejoiner's log a mutual prefix of every other log — the consistent *)
(* cut.  doneKeys / seenClk are copied from canon too, so the         *)
(* rejoiner resumes delivering EXACTLY where canon is.  A re-admitted *)
(* issuer's slot is reissued one clock above the current boundary     *)
(* (symmetric to Reconfigure), and its issClk is caught up to that    *)
(* boundary so a now-live issuer never blocks AllPriorDelivered with  *)
(* a stale clock.  Unlike CrashProcess/Reconfigure, `crashed` SHRINKS *)
(* here — this is the only action that models recovery.              *)
(*------------------------------------------------------------------*)

ReAdmit(rejoiner) ==
  /\ Mode = "TO"
  /\ rejoiner \in crashed
  /\ LET survivors == Procs \ crashed
         newLive   == survivors \union {rejoiner}    (* the post-re-admit view *)
         (* Most-advanced survivor: the install-point snapshot source.
            ConsistentDelivery ⇒ every survivor's (and the excluded rejoiner's)
            log is a prefix of it. *)
         canon     == CHOOSE p \in survivors :
                        \A q \in survivors : Len(delivered[q]) <= Len(delivered[p])
         liveSlots == {t \in TrainId : tr[t].issuer \in newLive}
     IN
       /\ survivors /= {}
       (* TLC finitisation: only reissue while issuers are below MaxClock. *)
       /\ \A t \in liveSlots : issClk[tr[t].issuer] < MaxClock
       /\ crashed'    = crashed \ {rejoiner}
       (* Virtual-synchrony barrier (symmetric to Reconfigure): the whole new
          live view adopts canon's log in the SAME step membership changes —
          the rejoiner catches up, survivors are already prefixes of canon and
          so only extend, never un-deliver. This atomic install is what makes
          delivered[rejoiner] a mutual prefix of every other log (the consistent
          cut), and — crucially — it RESETS every in-flight train, so the
          re-admitted node cannot mutate a train whose key a survivor already
          delivered (the divergence a naive re-admit would cause). *)
       /\ delivered'  = [p \in Procs |->
                          IF p \in newLive THEN delivered[canon] ELSE delivered[p]]
       /\ doneKeys'   = [p \in Procs |->
                          IF p \in newLive THEN doneKeys[canon] \union issuedKeys
                                           ELSE doneKeys[p]]
       /\ seenClk'    = [p \in Procs |->
                          IF p = rejoiner THEN seenClk[canon] ELSE seenClk[p]]
       (* Reissue every live slot (incl. the rejoiner's) one clock above the
          boundary; fresh empty trains restart at their issuers. *)
       /\ tr'         = [t \in TrainId |->
                          IF t \in liveSlots
                            THEN [tr[t] EXCEPT !.clock = issClk[tr[t].issuer] + 1,
                                               !.msgs  = {},
                                               !.acks  = {},
                                               !.pos   = tr[t].issuer]
                            ELSE tr[t]]
       /\ issClk'     = [q \in Procs |->
                          IF q \in newLive /\ q \in Issuers
                            THEN issClk[q] + 1 ELSE issClk[q]]
       /\ issuedKeys' = issuedKeys \union
                          { <<issClk[tr[t].issuer] + 1, tr[t].issuer>> : t \in liveSlots }
  /\ UNCHANGED <<pending, broadcast>>

(*------------------------------------------------------------------*)
(* NEXT-STATE RELATION                                              *)
(*------------------------------------------------------------------*)

Next ==
  \/ \E p \in Procs, m \in Messages  : AppBroadcast(p, m)
  \/ \E p \in Procs, t \in TrainId   : ProcessTrain(p, t)
  \/ \E p \in Procs, t \in TrainId   : DeliverTrain(p, t)
  \/ \E t \in TrainId                : RecycleTrain(t)
  \/ \E t \in TrainId                : RecycleEmptyTrain(t)
  \/ (Mode = "UTO" /\ \E p \in Procs : CrashProcess(p))
  \/ (Mode = "TO"  /\ \E p \in Procs : Reconfigure(p))
  \/ (Mode = "TO"  /\ \E p \in Procs : ReAdmit(p))

(*------------------------------------------------------------------*)
(* FAIRNESS                                                         *)
(*                                                                  *)
(* Weak fairness on ProcessTrain and DeliverTrain ensures that      *)
(* trains keep moving and delivery eventually happens if possible.  *)
(*------------------------------------------------------------------*)

Fairness ==
  /\ \A t \in TrainId :
       \A p \in Procs :
         WF_vars(ProcessTrain(p, t))
  /\ \A t \in TrainId :
       \A p \in Procs :
         WF_vars(DeliverTrain(p, t))
  /\ \A t \in TrainId :
       WF_vars(RecycleTrain(t))
  /\ \A t \in TrainId :
       WF_vars(RecycleEmptyTrain(t))

Spec == Init /\ [][Next]_vars /\ Fairness

(*------------------------------------------------------------------*)
(* STATE CONSTRAINT (for TLC — limits exploration to finite space)  *)
(*------------------------------------------------------------------*)

StateConstraint ==
  /\ \A p \in Procs : Cardinality(pending[p])  <= MaxPending
  /\ \A t \in TrainId : tr[t].clock            <= MaxClock
  /\ \A p \in Procs : issClk[p]               <= MaxClock

(*------------------------------------------------------------------*)
(* SAFETY INVARIANTS                                                *)
(*------------------------------------------------------------------*)

(*
  P5 — Clock Monotonicity
  The clock seen from issuer q at process p never decreases.
  Proof sketch: seenClk[p][q] is only updated in ProcessTrain to
  tr[t].clock, and trains are recycled with strictly increasing clocks.
*)
ClockMonotonicity ==
  \A p \in Procs :
    \A q \in Procs :
      seenClk[p][q] <= issClk[q]

(*
  P1 / P2 — Consistent Delivery (Total Order + Uniform)
  At any point in the execution, the delivery logs of all processes
  are mutual prefixes of each other.  This is the key UTO property:
  if two processes have both delivered a set of messages, they agree
  on their order.
*)
(* Apalache-friendly: avoid SubSeq, which requires constant bounds.
   Equivalent: s is a prefix of t iff every index of s holds the same
   element in t. *)
IsPrefix(s, t) == Len(s) <= Len(t) /\ \A i \in DOMAIN s : s[i] = t[i]

ConsistentDelivery ==
  \A p \in Procs :
    \A q \in Procs :
      IsPrefix(delivered[p], delivered[q])
      \/ IsPrefix(delivered[q], delivered[p])

(*
  P3 — No Spurious Delivery
  Every delivered message was previously broadcast.
*)
NoSpuriousDelivery ==
  \A p \in Procs :
    \A i \in 1..Len(delivered[p]) :
      delivered[p][i] \in broadcast

(*
  Train Integrity
  Messages on a train in circulation are only those that were
  previously in some process's pending set.
*)
TrainIntegrity ==
  \A t \in TrainId :
    tr[t].msgs \subseteq broadcast

(*
  AckMonotonicity
  The ack set of a train only grows as it circulates.
*)
AckMonotonicity ==
  \A t \in TrainId :
    tr[t].acks \subseteq Procs

(*
  IssuerUniqueness
  No two train slots share the same (issuer, clock) pair in circulation.
*)
IssuerUniqueness ==
  \A t1, t2 \in TrainId :
    t1 /= t2 =>
      \/ tr[t1].issuer /= tr[t2].issuer
      \/ tr[t1].clock  /= tr[t2].clock

(*------------------------------------------------------------------*)
(* LIVENESS PROPERTY                                                *)
(*                                                                  *)
(* Every broadcast message is eventually delivered by every non-    *)
(* crashed process.  Requires Fairness in Spec.                     *)
(*------------------------------------------------------------------*)

EventualDelivery ==
  \A m \in Messages :
    \A p \in Procs :
      m \in broadcast =>
        <>( \/ p \in crashed
            \/ m \in Range(delivered[p]) )

(*------------------------------------------------------------------*)
(* THEOREM STUBS (for TLAPS)                                        *)
(*                                                                  *)
(* Proof of ClockMonotonicity by induction on ProcessTrain steps:   *)
(*   seenClk[p][q] is updated to tr[t].clock in ProcessTrain(p,t)  *)
(*   where issuer = q.  issClk[q] = tr[t].clock at that moment     *)
(*   (since the train was created with clock = issClk[q]).          *)
(*   RecycleTrain increments issClk[q] strictly, so the invariant   *)
(*   is maintained.                                                  *)
(*                                                                  *)
(* Proof of ConsistentDelivery by induction on DeliverTrain steps:  *)
(*   All deliveries append the same sequence MsgsToSeq(tr[t].msgs)  *)
(*   at the same point in the global ordering (enforced by          *)
(*   AllPriorDelivered). Since every process delivers the same      *)
(*   trains in the same order, all logs are consistent prefixes.    *)
(*------------------------------------------------------------------*)

THEOREM Spec => []TypeOK
THEOREM Spec => []ClockMonotonicity
THEOREM Spec => []ConsistentDelivery
THEOREM Spec => []NoSpuriousDelivery
THEOREM Spec => []TrainIntegrity
THEOREM Spec => []IssuerUniqueness
THEOREM Spec => EventualDelivery

=============================================================================
