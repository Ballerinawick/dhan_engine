import io
import json
import time
import unittest
from datetime import time as market_time
from types import SimpleNamespace

from dhan_engine.application.deeplob.percentage_reversal import (
    HistoricalReversalPrior,
    PercentageReversalRuntime,
    PercentageReversalSettings,
)
from dhan_engine.application.deeplob.post_market_analysis import (
    PostMarketAnalysisRuntime,
    PostMarketAnalysisSettings,
)
from dhan_engine.domain.market.liquidity_event_state import LiquidityEventEvidence


class _MemoryS3:
    def __init__(self, payload=None):
        self.payload = payload

    def get_object(self, **_kwargs):
        if self.payload is None:
            raise KeyError("missing")
        return {"Body": io.BytesIO(json.dumps(self.payload).encode())}


def _settings(**overrides):
    values = {
        "enabled": True,
        "sample_interval_ms": 100,
        "sequence_length": 12,
        "signal_interval_sec": 0.5,
        "minimum_turn_score": 0.58,
        "minimum_event_quality": 0.30,
        "minimum_adverse_bps": 1.5,
        "stale_after_sec": 5.0,
        "queue_size": 16,
        "horizons_sec": (15, 30, 60),
        "s3_bucket": "",
        "prior_key": "",
    }
    values.update(overrides)
    return PercentageReversalSettings(**values)


def _composite(mid, direction):
    features = SimpleNamespace(
        mid=mid,
        spread_bps=1.0,
        pressure_score=0.6 * direction,
        imbalance_5=0.5 * direction,
        depth_consensus=0.45 * direction,
        imbalance_200=0.45 * direction,
        weighted_imbalance_200=0.50 * direction,
        order_imbalance_200=0.40 * direction,
        depth_flow_200=0.55 * direction,
        bid_replenishment=0.6 if direction > 0 else 0.0,
        ask_replenishment=0.0 if direction > 0 else 0.6,
        bid_depletion=0.0 if direction > 0 else 0.6,
        ask_depletion=0.6 if direction > 0 else 0.0,
    )
    evidence = LiquidityEventEvidence(
        score=0.6 * direction,
        persistence=0.8,
        evidence_quality=0.9,
        volatility_bps_sec=1.0,
    )
    return SimpleNamespace(features=features, event_evidence=evidence)


class PercentageReversalTest(unittest.TestCase):
    def test_down_then_up_transition_emits_paper_ce_signal(self):
        predictions = []
        settings = _settings()
        prior = HistoricalReversalPrior(settings)
        runtime = PercentageReversalRuntime(
            settings,
            prediction_sink=lambda **payload: predictions.append(payload),
            historical_prior=prior,
        )
        runtime.start_worker()
        base = time.monotonic()
        mids = [100.0, 99.9, 99.8, 99.7, 99.6, 99.5]
        mids += [99.55, 99.65, 99.75, 99.85, 99.95, 100.05]
        for index, mid in enumerate(mids):
            direction = -1 if index < 6 else 1
            snapshot = SimpleNamespace(received_mono=base + index * 0.11)
            runtime.on_book("NIFTY_FUT", snapshot, _composite(mid, direction))
        deadline = time.monotonic() + 2.0
        while not predictions and time.monotonic() < deadline:
            time.sleep(0.02)
        runtime.close_worker()

        self.assertTrue(predictions)
        self.assertEqual(predictions[-1]["paper_action"], "BUY_CE")
        metadata = predictions[-1]["signal_metadata"]
        self.assertEqual(metadata["profile"], "reversal")
        self.assertEqual(metadata["reversal_direction"], "BULLISH")
        self.assertLess(metadata["prior_move_bps"], 0)
        self.assertGreater(metadata["confirmation_move_bps"], 0)
        self.assertIn("depth_before_pct", metadata)
        self.assertIn("depth_after_pct", metadata)
        self.assertGreater(metadata["range_position_pct"], 50.0)

    def test_historical_prior_loads_clamped_probabilities(self):
        settings = _settings(s3_bucket="bucket", prior_key="prior.json")
        prior = HistoricalReversalPrior(
            settings,
            s3_client=_MemoryS3(
                {
                    "sample_count": 12,
                    "bullish_probability": 1.4,
                    "bearish_probability": -0.2,
                }
            ),
        )
        prior.load()
        self.assertEqual(prior.sample_count, 12)
        self.assertEqual(prior.probability(1), 1.0)
        self.assertEqual(prior.probability(-1), 0.0)

    def test_prior_merges_previous_sessions_but_not_same_day_rerun(self):
        settings = PostMarketAnalysisSettings(
            enabled=True,
            bucket="bucket",
            market_prefix="data",
            trade_prefix="trades",
            report_prefix="analysis/deeplob",
            run_after=market_time(15, 35),
            sideways_bps=5.0,
        )
        previous = {
            "trade_date": "2026-08-20",
            "session_count": 2,
            "bullish_samples": 10,
            "bearish_samples": 5,
            "bullish_correct": 7,
            "bearish_correct": 2,
            "bullish_probability": 0.7,
            "bearish_probability": 0.4,
        }
        runtime = PostMarketAnalysisRuntime(settings, s3_client=_MemoryS3(previous))
        current = {
            "sample_count": 5,
            "bullish_samples": 2,
            "bearish_samples": 3,
            "bullish_correct": 1,
            "bearish_correct": 2,
            "bullish_probability": 0.5,
            "bearish_probability": 0.8,
            "lookback_sec": 30,
            "horizon_sec": 60,
            "minimum_move_bps": 1.5,
        }
        merged = runtime._merge_reversal_prior("2026-08-21", current)
        self.assertEqual(merged["session_count"], 3)
        self.assertEqual(merged["sample_count"], 20)
        self.assertEqual(merged["bullish_correct"], 8)
        self.assertEqual(merged["bearish_correct"], 4)
        self.assertAlmostEqual(merged["bullish_probability"], 0.642857)
        self.assertAlmostEqual(merged["bearish_probability"], 0.5)

        previous["trade_date"] = "2026-08-21"
        same_day = runtime._merge_reversal_prior("2026-08-21", current)
        self.assertEqual(same_day["session_count"], 1)
        self.assertEqual(same_day["sample_count"], 5)


if __name__ == "__main__":
    unittest.main()
