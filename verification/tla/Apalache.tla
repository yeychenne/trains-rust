--------------------------- MODULE Apalache ---------------------------
(*
  TLC-compatible stub of Apalache's standard library.

  Apalache (apalache.jar) ships its own real Apalache.tla and rewrites
  these operators to native SMT/Z3 encodings.  When TRAINS.tla is
  loaded by TLC (tla2tools.jar), this file provides equivalent TLC-
  executable definitions.

  Only the operators TRAINS.tla actually uses are defined here.
*)

EXTENDS Naturals, Sequences, FiniteSets

(* Fold an operator over a finite set.  Order of folding is not
   specified — caller must use a commutative operator if order is
   significant.  TLC executes this recursively via CHOOSE; Apalache
   replaces it with a native fold. *)
RECURSIVE ApaFoldSet(_, _, _)
\* @type: ((a, b) => a, a, Set(b)) => a;
ApaFoldSet(Op(_, _), v, S) ==
  IF S = {}
    THEN v
    ELSE LET x == CHOOSE x \in S : TRUE
         IN  ApaFoldSet(Op, Op(v, x), S \ {x})

=============================================================================
