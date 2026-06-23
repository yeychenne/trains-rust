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

=============================================================================
