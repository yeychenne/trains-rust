--------------------------- MODULE TRAINS_MC ---------------------------
(*
  Model-checking companion for TRAINS.tla.

  Provides:
    - ring_value: concrete tuple for the `ring` constant
                  (TLC config files cannot inline tuple literals)
    - MessageSymmetry: permutation set for TLC symmetry reduction

  Usage:
      java -cp tla2tools.jar tlc2.TLC TRAINS_MC.tla \
           -config TRAINS_MC.cfg -workers auto -noGenerateSpecTE
*)

EXTENDS TRAINS

(*--------------------------------------------------------------------*)
(* Concrete value substituted into the `ring` constant via TLC's      *)
(* `CONSTANT ring <- ring_value` config syntax.                       *)
(*--------------------------------------------------------------------*)
\* @type: Seq(Int);
ring_value    == <<0, 1, 2>>
\* @type: Seq(Int);
ring_value_n4 == <<0, 1, 2, 3>>

(*--------------------------------------------------------------------*)
(* Symmetry set: TLC treats all permutations of Messages as           *)
(* equivalent states.  Valid because the protocol uses messages as    *)
(* opaque identifiers — no message has a distinguished role.          *)
(* 3 messages → 6× state reduction;  4 messages → 24× reduction.      *)
(*--------------------------------------------------------------------*)
MessageSymmetry == Permutations(Messages)

(*--------------------------------------------------------------------*)
(* Apalache `ConstInit`: assigns concrete values to all CONSTANTS so  *)
(* the symbolic backend doesn't need TLC's separate .cfg file.        *)
(*                                                                     *)
(* Mirrors TRAINS_MC.cfg's Procs={0,1,2}, NumTrains=2, etc.            *)
(*--------------------------------------------------------------------*)
ConstInit ==
  /\ Procs      = {0, 1, 2}
  /\ ring       = <<0, 1, 2>>
  /\ NumTrains  = 2
  /\ Messages   = {"m1", "m2", "m3"}
  /\ MaxClock   = 4
  /\ MaxPending = 2
  /\ Mode       = "UTO"

(*--------------------------------------------------------------------*)
(* TO-mode ConstInit: identical to ConstInit but Mode = "TO", which   *)
(* enables the membership view-change actions (Reconfigure exclude +   *)
(* ReAdmit re-admit) in Next.  Used to extend the Apalache bounded     *)
(* symbolic check to dynamic membership — previously TLC-only.         *)
(* Run:                                                                *)
(*   apalache-mc check --init=Init --inv=ConsistentDelivery \          *)
(*     --cinit=ConstInitTO --length=N TRAINS_MC.tla                    *)
(*--------------------------------------------------------------------*)
ConstInitTO ==
  /\ Procs      = {0, 1, 2}
  /\ ring       = <<0, 1, 2>>
  /\ NumTrains  = 2
  /\ Messages   = {"m1", "m2", "m3"}
  /\ MaxClock   = 4
  /\ MaxPending = 2
  /\ Mode       = "TO"

(* The four non-ConsistentDelivery safety invariants, conjoined so a       *)
(* single Apalache run amortises the (dominant) symbolic exploration cost   *)
(* across all of them.  ConsistentDelivery is checked on its own (it is the *)
(* most expensive checker).                                                 *)
\* @type: () => Bool;
OtherSafetyTO ==
  /\ ClockMonotonicity
  /\ NoSpuriousDelivery
  /\ TrainIntegrity
  /\ IssuerUniqueness

(*--------------------------------------------------------------------*)
(* LIVENESS FOR DYNAMIC MEMBERSHIP                                    *)
(*                                                                    *)
(* TRAINS.tla's `Fairness` predicate covers the core protocol actions *)
(* (ProcessTrain, DeliverTrain, RecycleTrain, RecycleEmptyTrain).     *)
(* It does NOT include the v3 membership actions.                     *)
(*                                                                    *)
(* For dynamic-membership liveness we add Strong Fairness on ReAdmit  *)
(* — SF rather than WF because between view-change steps, ReAdmit can *)
(* be transiently disabled (e.g., a survivor's local clock catches up *)
(* to MaxClock briefly).  SF says: if ReAdmit(p) is INFINITELY often  *)
(* enabled, it eventually fires.                                       *)
(*                                                                    *)
(* We DO NOT add fairness on Reconfigure — that would force crashes   *)
(* to happen, which is environmental and adversarial.                  *)
(*                                                                    *)
(* The spec extension is local to this MC file: we leave the master    *)
(* `Spec` in TRAINS.tla untouched and define `SpecTOLiveness` here.    *)
(*--------------------------------------------------------------------*)

MembershipFairness ==
  \A p \in Procs : SF_vars(ReAdmit(p))

SpecTOLiveness ==
  Init /\ [][Next]_vars /\ Fairness /\ MembershipFairness

(*--------------------------------------------------------------------*)
(* MEMBERSHIP LIVENESS PROPERTIES                                     *)
(*                                                                    *)
(* P1 — EventualReAdmit: every crashed process is eventually re-     *)
(* admitted.  "If p is ever in crashed, p eventually leaves crashed." *)
(* This is the canonical "recovery is not stuck" property.            *)
(*                                                                    *)
(* P2 — BoundedDowntime: at most one process can be in crashed at any *)
(* time, eventually.  Stronger than P1 and only meaningful if         *)
(* Reconfigure cannot fire faster than ReAdmit catches up; included   *)
(* as a candidate for runs where it is interesting.                    *)
(*--------------------------------------------------------------------*)

(* The model is "exhausted" when every live issuer has used its full      *)
(* clock budget — the TLC finitisation explicitly bounds issClk by         *)
(* MaxClock to keep the state space finite, so ReAdmit's precondition      *)
(* `issClk[tr[t].issuer] < MaxClock` legitimately deactivates re-admit     *)
(* once the model runs out of clock room.  This is a property of the       *)
(* finite model, not of the protocol.                                       *)
ModelClockExhausted ==
  \A q \in Issuers : issClk[q] >= MaxClock

(* P1 — EventualReAdmit (model-bounded): every crashed process is          *)
(* eventually re-admitted, OR the finite model has exhausted its clock     *)
(* budget.  The interesting claim — and the only one TLC can soundly       *)
(* verify on a bounded model — is that the *protocol* never starves a       *)
(* recovery; the model bound, when hit, is acknowledged in the disjunct.   *)
EventualReAdmit ==
  \A p \in Procs :
    (p \in crashed) ~> (p \notin crashed \/ ModelClockExhausted)

BoundedDowntime ==
  <>[](Cardinality(crashed) <= 1)

=============================================================================
