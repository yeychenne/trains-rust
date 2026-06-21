"""Unit tests for bench/coordinator/invariants.py.

Pure functions over synthetic per-node delivery maps — no AWS, no
wall-clock. Covers the three computable invariants (UTO completeness,
total order, no phantom) plus the honest `not_measured` reporting for
liveness + bounded-queue.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import invariants  # noqa: E402


def _dels(*seqs: int) -> list[dict]:
    """Build delivery records (seq + dummy send_ns) in delivery order."""
    return [{"seq": s, "send_ns": 1000 + s} for s in seqs]


class TestUtoCompleteness(unittest.TestCase):
    def test_all_nodes_complete_passes(self):
        pnd = {0: _dels(1, 2, 3), 1: _dels(1, 2, 3), 2: _dels(1, 2, 3)}
        r = invariants.check_uto_completeness(pnd, broadcast_seqs={1, 2, 3})
        self.assertTrue(r.passed)

    def test_one_node_missing_fails(self):
        pnd = {0: _dels(1, 2, 3), 1: _dels(1, 3), 2: _dels(1, 2, 3)}
        r = invariants.check_uto_completeness(pnd, broadcast_seqs={1, 2, 3})
        self.assertFalse(r.passed)
        self.assertIn("node 1", r.detail)

    def test_killed_node_excluded_via_alive_set(self):
        # Node 1 was killed mid-run; excluding it, the rest are complete.
        pnd = {0: _dels(1, 2, 3), 1: _dels(1), 2: _dels(1, 2, 3)}
        r = invariants.check_uto_completeness(
            pnd, broadcast_seqs={1, 2, 3}, alive_nodes={0, 2},
        )
        self.assertTrue(r.passed)

    def test_no_broadcast_seqs_is_not_measured(self):
        pnd = {0: _dels(1), 1: _dels(1)}
        r = invariants.check_uto_completeness(pnd, broadcast_seqs=set())
        self.assertIsNone(r.passed)


class TestTotalOrder(unittest.TestCase):
    def test_identical_order_passes(self):
        pnd = {0: _dels(1, 2, 3), 1: _dels(1, 2, 3)}
        r = invariants.check_total_order(pnd)
        self.assertTrue(r.passed)

    def test_partial_overlap_consistent_passes(self):
        # Node 1 missing seq 2, but the common projection (1,3) matches.
        pnd = {0: _dels(1, 2, 3), 1: _dels(1, 3)}
        r = invariants.check_total_order(pnd)
        self.assertTrue(r.passed)

    def test_order_mismatch_fails(self):
        pnd = {0: _dels(1, 2, 3), 1: _dels(1, 3, 2)}
        r = invariants.check_total_order(pnd)
        self.assertFalse(r.passed)
        self.assertIn("order mismatch", r.detail)

    def test_duplicate_delivery_fails(self):
        pnd = {0: _dels(1, 2, 2, 3), 1: _dels(1, 2, 3)}
        r = invariants.check_total_order(pnd)
        self.assertFalse(r.passed)
        self.assertIn("duplicate", r.detail)


class TestNoPhantom(unittest.TestCase):
    def test_clean_passes(self):
        pnd = {0: _dels(1, 2, 3), 1: _dels(1, 2, 3)}
        r = invariants.check_no_phantom(pnd, broadcast_seqs={1, 2, 3})
        self.assertTrue(r.passed)

    def test_phantom_seq_fails(self):
        pnd = {0: _dels(1, 2, 3, 99), 1: _dels(1, 2, 3)}
        r = invariants.check_no_phantom(pnd, broadcast_seqs={1, 2, 3})
        self.assertFalse(r.passed)
        self.assertIn("phantom", r.detail)

    def test_no_broadcast_seqs_is_not_measured(self):
        pnd = {0: _dels(1)}
        r = invariants.check_no_phantom(pnd, broadcast_seqs=set())
        self.assertIsNone(r.passed)


class TestCheckAll(unittest.TestCase):
    def test_returns_all_five_invariants(self):
        pnd = {0: _dels(1, 2), 1: _dels(1, 2)}
        results = invariants.check_all(pnd, broadcast_seqs={1, 2})
        self.assertEqual(
            set(results),
            {"uto_completeness", "total_order", "no_phantom",
             "liveness_recovery", "bounded_queue_depth"},
        )

    def test_liveness_and_queue_are_not_measured(self):
        pnd = {0: _dels(1), 1: _dels(1)}
        results = invariants.check_all(pnd, broadcast_seqs={1})
        self.assertIsNone(results["liveness_recovery"].passed)
        self.assertIsNone(results["bounded_queue_depth"].passed)

    def test_default_broadcast_seqs_is_union(self):
        # No broadcast_seqs given → union of delivered = {1,2}; all nodes
        # have both → completeness passes on the weaker ground truth.
        pnd = {0: _dels(1, 2), 1: _dels(1, 2)}
        results = invariants.check_all(pnd)
        self.assertTrue(results["uto_completeness"].passed)
        self.assertTrue(results["no_phantom"].passed)

    def test_clean_run_all_computable_pass(self):
        pnd = {0: _dels(1, 2, 3), 1: _dels(1, 2, 3), 2: _dels(1, 2, 3)}
        results = invariants.check_all(pnd, broadcast_seqs={1, 2, 3})
        self.assertTrue(results["uto_completeness"].passed)
        self.assertTrue(results["total_order"].passed)
        self.assertTrue(results["no_phantom"].passed)


def _dels_recv(pairs: list[tuple[int, int]]) -> list[dict]:
    """pairs of (seq, recv_ns)."""
    return [{"seq": s, "send_ns": 1000 + s, "recv_ns": r} for s, r in pairs]


class TestRecovery(unittest.TestCase):
    def test_max_progress_stall_detects_gap(self):
        # node 1: a ~2 s gap between delivery 1 and 2.
        pnd = {1: _dels_recv([(1, 0), (2, 2_000_000_000), (3, 2_001_000_000)])}
        stalls = invariants.max_progress_stall(pnd)
        self.assertEqual(round(stalls[1]), 2000)  # ms

    def test_no_recv_ns_yields_no_stalls(self):
        pnd = {0: _dels(1, 2, 3)}  # _dels has no recv_ns
        self.assertEqual(invariants.max_progress_stall(pnd), {})

    def test_dead_node_excluded_from_stalls(self):
        pnd = {
            0: _dels_recv([(1, 0), (2, 1_000_000)]),
            4: _dels_recv([(1, 0), (2, 9_000_000_000)]),  # killed node, huge gap
        }
        stalls = invariants.max_progress_stall(pnd, alive_nodes={0})
        self.assertNotIn(4, stalls)

    def test_check_all_liveness_measured_with_recv_ns(self):
        pnd = {
            0: _dels_recv([(1, 0), (2, 1_000_000)]),
            1: _dels_recv([(1, 0), (2, 1_000_000)]),
        }
        results = invariants.check_all(pnd, broadcast_seqs={1, 2})
        self.assertIs(results["liveness_recovery"].passed, True)
        self.assertIn("stall", results["liveness_recovery"].detail)

    def test_check_all_liveness_not_measured_without_recv_ns(self):
        pnd = {0: _dels(1, 2), 1: _dels(1, 2)}
        results = invariants.check_all(pnd, broadcast_seqs={1, 2})
        self.assertIsNone(results["liveness_recovery"].passed)


if __name__ == "__main__":
    unittest.main()
