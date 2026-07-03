from __future__ import annotations

import unittest

from dhan_engine.domain.market.five_minute_zone import FiveMinuteZoneTracker


class FiveMinuteZoneTrackerTests(unittest.TestCase):
    def test_observes_150_seconds_and_emits_once_after_confirmation(self):
        tracker = FiveMinuteZoneTracker(
            cycle_sec=300,
            observe_sec=150,
            confirm_sec=10,
            entry_window_sec=30,
            middle_zone_ratio=0.25,
            strong_zone_ratio=0.65,
        )
        cycle_start = 1_800.0
        self.assertIsNone(tracker.update("SBIN", 100.0, cycle_start))
        for second in range(1, 160):
            price = 100.0 + (second * 0.01)
            self.assertIsNone(tracker.update("SBIN", price, cycle_start + second))

        decision = tracker.update("SBIN", 101.60, cycle_start + 160)
        self.assertIsNotNone(decision)
        self.assertEqual(decision.direction, "POSITIVE")
        self.assertEqual(decision.zone, "STRONG")
        self.assertEqual(decision.cycle_end, cycle_start + 300)
        self.assertIsNone(tracker.update("SBIN", 101.70, cycle_start + 161))

    def test_starting_after_observation_cannot_fabricate_history(self):
        tracker = FiveMinuteZoneTracker()
        self.assertIsNone(tracker.update("RELIANCE", 100.0, 1_800.0 + 170.0))
        self.assertIsNone(tracker.update("RELIANCE", 101.0, 1_800.0 + 171.0))

    def test_negative_future_zone_is_classified(self):
        tracker = FiveMinuteZoneTracker()
        cycle_start = 2_100.0
        tracker.update("NIFTY_STRADDLE", 50.0, cycle_start)
        for second in range(1, 160):
            tracker.update("NIFTY_STRADDLE", 50.0 - (second * 0.01), cycle_start + second)
        decision = tracker.update("NIFTY_STRADDLE", 48.40, cycle_start + 160)
        self.assertIsNotNone(decision)
        self.assertEqual(decision.direction, "NEGATIVE")
        self.assertEqual(decision.zone, "STRONG")


if __name__ == "__main__":
    unittest.main()
