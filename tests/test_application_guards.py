import unittest

from dhan_engine.application.market_health import PairFreshness, age_from_ts, format_age
from dhan_engine.application.risk_manager import (
    EntryGateConfig,
    ScaleInGateConfig,
    evaluate_entry_quality,
    evaluate_scale_in_quality,
)


class MarketHealthTests(unittest.TestCase):
    def test_pair_freshness_requires_both_legs(self):
        self.assertTrue(PairFreshness(ce_age=2.0, pe_age=3.0, max_age_sec=5.0).is_fresh)
        self.assertFalse(PairFreshness(ce_age=None, pe_age=3.0, max_age_sec=5.0).is_fresh)
        self.assertFalse(PairFreshness(ce_age=2.0, pe_age=6.0, max_age_sec=5.0).is_fresh)

    def test_age_formatting(self):
        self.assertIsNone(age_from_ts(0.0, 100.0))
        self.assertEqual(age_from_ts(90.0, 100.0), 10.0)
        self.assertEqual(format_age(None), "missing")
        self.assertEqual(format_age(2.4), "2.4s")


class RiskManagerTests(unittest.TestCase):
    def test_entry_quality_blocks_weak_edge(self):
        decision = evaluate_entry_quality(
            stats={"dynamic_support_score": 0.4, "dynamic_risk_score": 0.2, "dynamic_edge": 0.2},
            ltp=100.0,
            lot_size=65,
            fee=60.0,
            phase="RECOVERY",
            config=EntryGateConfig(
                min_support_score=0.55,
                max_risk_score=0.45,
                min_dynamic_edge=0.15,
                min_expected_net_rupees=120.0,
                min_expected_move_pct=0.75,
            ),
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "WEAK_ENTRY_EDGE")

    def test_entry_quality_blocks_fee_weak_opportunity(self):
        decision = evaluate_entry_quality(
            stats={
                "dynamic_support_score": 0.8,
                "dynamic_risk_score": 0.1,
                "dynamic_edge": 0.7,
                "last_5_delta": 0.0,
                "recent_high": 100.0,
            },
            ltp=100.0,
            lot_size=65,
            fee=60.0,
            phase="RECOVERY",
            config=EntryGateConfig(
                min_support_score=0.55,
                max_risk_score=0.45,
                min_dynamic_edge=0.15,
                min_expected_net_rupees=120.0,
                min_expected_move_pct=0.75,
            ),
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "EXPECTED_NET_BELOW_FEES")

    def test_scale_in_requires_green_fresh_strong_same_trade(self):
        config = ScaleInGateConfig(
            enabled=True,
            max_lots=2,
            min_profit_pct=0.8,
            min_support_score=0.7,
            max_risk_score=0.25,
            min_edge=0.45,
            cooldown_sec=30.0,
            fresh_ltp_max_age_sec=5.0,
        )
        position = {"lots": 1, "entry": 100.0, "last_add_ts": 50.0}
        stats = {"dynamic_support_score": 0.8, "dynamic_risk_score": 0.1, "dynamic_edge": 0.7}

        allowed = evaluate_scale_in_quality(
            position=position,
            ltp=101.0,
            stats=stats,
            ltp_age=1.0,
            now_ts=100.0,
            config=config,
        )
        self.assertTrue(allowed.allowed)

        stale = evaluate_scale_in_quality(
            position=position,
            ltp=101.0,
            stats=stats,
            ltp_age=10.0,
            now_ts=100.0,
            config=config,
        )
        self.assertFalse(stale.allowed)
        self.assertEqual(stale.reason, "SCALE_IN_STALE_LTP")


if __name__ == "__main__":
    unittest.main()

