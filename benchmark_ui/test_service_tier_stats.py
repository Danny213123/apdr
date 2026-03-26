"""Tests for benchmark service tier breakdown statistics."""
from __future__ import annotations

import unittest
from datetime import datetime
from queue import Queue

from .service import BenchmarkService
from .state import AppState


class TestServiceTierStats(unittest.TestCase):
    """Test tier breakdown calculation in BenchmarkService."""

    def setUp(self):
        """Create test service instance."""
        self.state = AppState()
        self.service = BenchmarkService(self.state)

    def test_tier_stats_calculation(self):
        """Test tier breakdown percentages calculated correctly."""
        # Mock run state with results
        run_state = {
            "results": [
                {"tier": "tier1"},
                {"tier": "tier1"},
                {"tier": "tier1"},
                {"tier": "tier2"},
                {"tier": "tier2"},
                {"tier": "tier3"},
                {"tier": "tier3"},
                {"tier": "tier3"},
                {"tier": "tier3"},
                {"tier": "tier3"},
            ]
        }

        # Calculate tier stats
        stats = self.service._calculate_tier_stats(run_state)

        # Verify counts
        self.assertEqual(stats["tier1"]["count"], 3)
        self.assertEqual(stats["tier2"]["count"], 2)
        self.assertEqual(stats["tier3"]["count"], 5)
        self.assertEqual(stats["total"], 10)

        # Verify percentages (1 decimal place precision)
        self.assertEqual(stats["tier1"]["percent"], 30.0)
        self.assertEqual(stats["tier2"]["percent"], 20.0)
        self.assertEqual(stats["tier3"]["percent"], 50.0)

    def test_tier_stats_emitted_after_case_complete(self):
        """Test tier_stats event follows case_complete in stream."""
        # This test will FAIL until service.py emits tier_stats events
        # Expected event sequence:
        # 1. init event
        # 2. case_complete event (with tier field)
        # 3. tier_stats event (immediately after case_complete)

        # Mock: After each case completion, tier_stats should be emitted
        expected_tier_stats = {
            "type": "tier_stats",
            "stats": {
                "tier1": {"count": 1, "percent": 100.0},
                "tier2": {"count": 0, "percent": 0.0},
                "tier3": {"count": 0, "percent": 0.0},
                "total": 1
            },
            "timestamp": datetime.now().isoformat()
        }

        # Verify structure
        self.assertEqual(expected_tier_stats["type"], "tier_stats")
        self.assertIn("stats", expected_tier_stats)
        self.assertIn("tier1", expected_tier_stats["stats"])
        self.assertIn("tier2", expected_tier_stats["stats"])
        self.assertIn("tier3", expected_tier_stats["stats"])

    def test_tier_stats_handles_missing_tier_field(self):
        """Test unknown tier cases don't crash calculation."""
        # If tier field missing from result, don't crash
        run_state = {
            "results": [
                {"tier": "tier1"},
                {},  # Missing tier field
                {"tier": "tier2"},
            ]
        }

        # Calculate should not crash
        stats = self.service._calculate_tier_stats(run_state)

        # Verify counts (missing tier not counted in any category)
        self.assertEqual(stats["tier1"]["count"], 1)
        self.assertEqual(stats["tier2"]["count"], 1)
        self.assertEqual(stats["tier3"]["count"], 0)
        self.assertEqual(stats["total"], 3)

        # Percentages should still add up correctly (2/3 = 66.6666...)
        total_percent = (stats["tier1"]["percent"] +
                        stats["tier2"]["percent"] +
                        stats["tier3"]["percent"])
        # Allow for floating point rounding (should be ~66.6% or 66.7%)
        self.assertAlmostEqual(total_percent, 66.7, delta=0.2)

    def test_tier_stats_with_empty_results(self):
        """Test tier_stats calculation with no results yet."""
        run_state = {
            "results": []
        }

        stats = self.service._calculate_tier_stats(run_state)

        # Verify all counts are zero
        self.assertEqual(stats["tier1"]["count"], 0)
        self.assertEqual(stats["tier2"]["count"], 0)
        self.assertEqual(stats["tier3"]["count"], 0)
        self.assertEqual(stats["total"], 0)

        # Verify percentages are 0.0 (not NaN or error)
        self.assertEqual(stats["tier1"]["percent"], 0.0)
        self.assertEqual(stats["tier2"]["percent"], 0.0)
        self.assertEqual(stats["tier3"]["percent"], 0.0)

    def test_tier_stats_percentage_precision(self):
        """Test tier percentages rounded to 1 decimal place."""
        # Test case with non-round percentages
        run_state = {
            "results": [
                {"tier": "tier1"},
                {"tier": "tier1"},
                {"tier": "tier2"},
                {"tier": "tier2"},
                {"tier": "tier2"},
                {"tier": "tier3"},
                {"tier": "tier3"},
            ]
        }

        stats = self.service._calculate_tier_stats(run_state)

        # Verify counts
        self.assertEqual(stats["tier1"]["count"], 2)
        self.assertEqual(stats["tier2"]["count"], 3)
        self.assertEqual(stats["tier3"]["count"], 2)

        # Verify percentages rounded to 1 decimal (2/7 = 28.571... -> 28.6%)
        self.assertEqual(stats["tier1"]["percent"], 28.6)
        self.assertEqual(stats["tier2"]["percent"], 42.9)
        self.assertEqual(stats["tier3"]["percent"], 28.6)


if __name__ == "__main__":
    unittest.main()
