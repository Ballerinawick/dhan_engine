import os
import tempfile
import unittest

import pandas as pd

from dhan_engine.domain.commodities.percent_engine import PercentNormalizedCommodityEngine
from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
from dhan_engine.simulations.commodity_paper_portfolio import CommodityPaperPortfolio


class CommodityPercentEngineTests(unittest.TestCase):
    @staticmethod
    def _strong_features(ltp: float) -> dict:
        return {
            "intraday_return_pct": 0.45,
            "ltp_vs_avg_pct": 0.18,
            "day_position": 0.70,
            "depth_imbalance_5": 0.45,
            "market_queue_imbalance": 0.25,
            "spread_pct": 0.04,
            "clean_trade_score": 0.75,
            "spoof_risk": 0.10,
            "top_depth_imbalance": 0.35,
            "recovery_score": 0.45,
            "exhaustion_score": 0.05,
            "ltp": ltp,
        }

    def test_rising_commodity_structure_produces_entry(self):
        engine = PercentNormalizedCommodityEngine(min_samples=8)
        signal = None
        for index in range(35):
            price = 72000.0 * (1.0 + (index * 0.00018))
            signal = engine.on_tick(
                "GOLD",
                price,
                self._strong_features(price),
                100.0 + index,
                in_position=False,
            )
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "ENTRY")
        self.assertGreaterEqual(signal.score, 68.0)

    def test_clean_commodity_scalp_can_enter_below_old_profile_threshold(self):
        engine = PercentNormalizedCommodityEngine(min_samples=8)
        signal = None
        features = {
            "intraday_return_pct": 0.05,
            "ltp_vs_avg_pct": 0.02,
            "day_position": 0.58,
            "depth_imbalance_5": 0.28,
            "market_queue_imbalance": 0.20,
            "spread_pct": 0.03,
            "clean_trade_score": 0.78,
            "spoof_risk": 0.0,
            "top_depth_imbalance": 0.15,
            "recovery_score": 0.20,
            "exhaustion_score": 0.02,
        }
        for index in range(18):
            price = 6700.0 * (1.0 + (index * 0.00008))
            signal = engine.on_tick("CRUDEOIL", price, features, 100.0 + index, in_position=False)

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "ENTRY")
        self.assertEqual(signal.reason, "COMMODITY_SCALP_MOMENTUM_ALIGNMENT")
        self.assertLess(signal.score, 72.0)

    def test_pullback_reclaim_allows_weak_ret5_with_confirmed_structure(self):
        engine = PercentNormalizedCommodityEngine(min_samples=8)
        signal = None
        features = {
            "intraday_return_pct": 0.25,
            "ltp_vs_avg_pct": 0.06,
            "day_position": 0.65,
            "depth_imbalance_5": 0.50,
            "market_queue_imbalance": 0.30,
            "spread_pct": 0.03,
            "clean_trade_score": 0.85,
            "spoof_risk": 0.02,
            "top_depth_imbalance": 0.30,
            "recovery_score": 0.40,
            "exhaustion_score": 0.05,
        }
        prices = [72000.0, 72010.0, 72020.0, 72030.0, 72040.0, 72055.0, 72048.0, 72044.0, 72042.0, 72046.0]
        for index, price in enumerate(prices):
            signal = engine.on_tick("GOLD", price, features, 100.0 + index, in_position=False)
        signal = engine.on_tick("GOLD", 72049.0, features, 110.0, in_position=False)

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "ENTRY")
        self.assertEqual(signal.reason, "COMMODITY_SCALP_PULLBACK_RECLAIM")
        self.assertLess(signal.features["return_5s_pct"], signal.features["profile_ret5_min"])

    def test_symbols_keep_independent_histories(self):
        engine = PercentNormalizedCommodityEngine(min_samples=3)
        engine.on_tick("GOLD", 72000.0, self._strong_features(72000.0), 1.0, in_position=False)
        engine.on_tick("GOLD", 72050.0, self._strong_features(72050.0), 2.0, in_position=False)
        engine.on_tick("CRUDEOIL", 6500.0, self._strong_features(6500.0), 2.0, in_position=False)
        self.assertEqual(len(engine.history["GOLD"]), 2)
        self.assertEqual(len(engine.history["CRUDEOIL"]), 1)

    def test_rejection_reason_names_first_failed_gate(self):
        engine = PercentNormalizedCommodityEngine(min_samples=3)
        signal = None
        weak = dict(self._strong_features(6700.0))
        weak["clean_trade_score"] = 0.10
        for index in range(12):
            price = 6700.0 * (1.0 + (index * 0.00020))
            signal = engine.on_tick(
                "CRUDEOIL",
                price,
                weak,
                100.0 + index,
                in_position=False,
            )
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "HOLD")
        self.assertIn(signal.reason, {"CLEAN_TRADE_WEAK", "SCORE_BELOW_PROFILE"})


class CommodityPaperPortfolioTests(unittest.TestCase):
    def test_multi_commodity_positions_and_net_pnl(self):
        portfolio = CommodityPaperPortfolio(
            capital=500000.0,
            notional_per_trade=75000.0,
            max_positions=2,
            round_trip_fee=60.0,
        )
        self.assertTrue(portfolio.enter(1, "GOLD", "GOLD-05Dec2026-FUT", 72000.0, 75.0, now=10.0))
        self.assertTrue(portfolio.enter(2, "CRUDEOIL", "CRUDEOIL-18Dec2026-FUT", 6500.0, 78.0, now=11.0))
        self.assertFalse(portfolio.enter(3, "NATURALGAS", "NATURALGAS-24Dec2026-FUT", 280.0, 80.0, now=12.0))
        trade = portfolio.exit(2, 6530.0, "TEST", now=20.0)
        self.assertIsNotNone(trade)
        self.assertAlmostEqual(trade["net_pnl"], 270.0)
        self.assertEqual(len(portfolio.positions), 1)


class CommodityInstrumentResolutionTests(unittest.TestCase):
    def test_nearest_active_mcx_future_resolution(self):
        frame = pd.DataFrame(
            [
                {
                    "SEM_EXM_EXCH_ID": "MCX", "SEM_SEGMENT": "M",
                    "SEM_SMST_SECURITY_ID": "510463", "SEM_INSTRUMENT_NAME": "FUTCOM",
                    "SEM_TRADING_SYMBOL": "GOLDTEN-30Jun2026-FUT", "SEM_CUSTOM_SYMBOL": "GOLDTEN JUN FUT",
                    "SEM_OPTION_TYPE": "NA", "SEM_STRIKE_PRICE": "0",
                    "SEM_LOT_UNITS": "1", "SEM_EXPIRY_DATE": "2026-06-30",
                    "SM_SYMBOL_NAME": "GOLDTEN",
                },
                {
                    "SEM_EXM_EXCH_ID": "MCX", "SEM_SEGMENT": "M",
                    "SEM_SMST_SECURITY_ID": "445003", "SEM_INSTRUMENT_NAME": "FUTCOM",
                    "SEM_TRADING_SYMBOL": "GOLD-05Dec2026-FUT", "SEM_CUSTOM_SYMBOL": "GOLD DEC FUT",
                    "SEM_OPTION_TYPE": "NA", "SEM_STRIKE_PRICE": "0",
                    "SEM_LOT_UNITS": "1", "SEM_EXPIRY_DATE": "2026-12-05",
                    "SM_SYMBOL_NAME": "GOLD",
                },
                {
                    "SEM_EXM_EXCH_ID": "MCX", "SEM_SEGMENT": "M",
                    "SEM_SMST_SECURITY_ID": "445004", "SEM_INSTRUMENT_NAME": "FUTCOM",
                    "SEM_TRADING_SYMBOL": "GOLD-05Feb2027-FUT", "SEM_CUSTOM_SYMBOL": "GOLD FEB FUT",
                    "SEM_OPTION_TYPE": "NA", "SEM_STRIKE_PRICE": "0",
                    "SEM_LOT_UNITS": "1", "SEM_EXPIRY_DATE": "2027-02-05",
                    "SM_SYMBOL_NAME": "GOLD",
                },
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "master.csv")
            frame.to_csv(path, index=False)
            master = InstrumentMaster(path, debug=False)
            instrument = master.get_nearest_commodity_future("GOLD")
        self.assertEqual(instrument["security_id"], 445003)
        self.assertEqual(instrument["exchange_segment"], "MCX_COMM")
        self.assertEqual(instrument["symbol"], "GOLD")


if __name__ == "__main__":
    unittest.main()
