from __future__ import annotations

import unittest

from dhan_engine.domain.market.full_depth_microstructure import BookSnapshot
from dhan_engine.domain.market.liquidity_event_state import (
    LiquidityEventEvidence,
    LiquidityEventTracker,
    adaptive_horizon_seconds,
)


def _book(*, ask_qty: int, received_mono: float) -> BookSnapshot:
    bids = [(100.0 - index * 0.05, 1000, 10) for index in range(200)]
    asks = [
        (100.05 + index * 0.05, ask_qty if index == 0 else 1000, 10)
        for index in range(200)
    ]
    return BookSnapshot.build(
        123,
        "NIFTY_FUT",
        bids,
        asks,
        received_ts=1_700_000_000.0 + received_mono,
        received_mono=received_mono,
    )


class LiquidityEventStateTest(unittest.TestCase):
    def test_trade_volume_caps_consumption_and_preserves_cancellation(self):
        tracker = LiquidityEventTracker()
        tracker.update(
            _book(ask_qty=1000, received_mono=1.0),
            {"ltp": 100.05, "ltq": 0, "volume": 10_000, "received_ns": 1},
        )
        evidence = tracker.update(
            _book(ask_qty=700, received_mono=2.0),
            {"ltp": 100.05, "ltq": 100, "volume": 10_100, "received_ns": 2},
        )
        self.assertEqual(evidence.aggressive_buy_qty, 100)
        self.assertEqual(evidence.ask_consumed_qty, 100)
        self.assertEqual(evidence.ask_cancelled_qty, 200)
        self.assertGreater(evidence.score, 0.0)

    def test_adaptive_horizon_contracts_when_volatility_increases(self):
        stable = LiquidityEventEvidence(
            persistence=0.9,
            evidence_quality=0.9,
            volatility_bps_sec=0.1,
        )
        volatile = LiquidityEventEvidence(
            persistence=0.9,
            evidence_quality=0.9,
            volatility_bps_sec=4.0,
        )
        stable_horizon = adaptive_horizon_seconds(
            stable, minimum=10, maximum=60, profile="scalp"
        )
        volatile_horizon = adaptive_horizon_seconds(
            volatile, minimum=10, maximum=60, profile="scalp"
        )
        self.assertGreater(stable_horizon, volatile_horizon)
        self.assertGreaterEqual(volatile_horizon, 10)
        self.assertLessEqual(stable_horizon, 60)


if __name__ == "__main__":
    unittest.main()
