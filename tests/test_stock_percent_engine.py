import os
import sys
import tempfile
import unittest
from collections import defaultdict
from types import SimpleNamespace

import pandas as pd

sys.modules.setdefault("websocket", SimpleNamespace(WebSocketApp=object))
sys.modules.setdefault("requests", SimpleNamespace(Session=object, get=lambda *_, **__: None, post=lambda *_, **__: None))

from dhan_engine.application.stocks.paper_runtime import StockPaperRuntime
from dhan_engine.domain.stocks.percent_engine import PercentNormalizedStockEngine
from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
from dhan_engine.simulations.stock_paper_portfolio import StockPaperPortfolio, StockPaperPosition


class StockPercentEngineTests(unittest.TestCase):
    @staticmethod
    def _strong_features(ltp: float) -> dict:
        return {
            "intraday_return_pct": 0.8,
            "ltp_vs_avg_pct": 0.25,
            "day_position": 0.75,
            "depth_imbalance_5": 0.55,
            "market_queue_imbalance": 0.35,
            "spread_pct": 0.03,
            "clean_trade_score": 0.85,
            "spoof_risk": 0.10,
            "ltp": ltp,
        }

    def test_rising_percentage_structure_produces_entry(self):
        engine = PercentNormalizedStockEngine(min_samples=10)
        signal = None
        for index in range(35):
            price = 1000.0 * (1.0 + (index * 0.00025))
            signal = engine.on_tick(
                "RELIANCE", price, self._strong_features(price), 100.0 + index,
                in_position=False,
            )
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "ENTRY")
        self.assertGreaterEqual(signal.score, 72.0)

    def test_clean_scalp_structure_can_enter_below_old_swing_threshold(self):
        engine = PercentNormalizedStockEngine(min_samples=8)
        signal = None
        features = {
            "intraday_return_pct": 0.05,
            "ltp_vs_avg_pct": -0.05,
            "day_position": 0.55,
            "depth_imbalance_5": 0.20,
            "market_queue_imbalance": 0.12,
            "spread_pct": 0.03,
            "clean_trade_score": 0.70,
            "spoof_risk": 0.05,
        }
        for index in range(18):
            price = 1000.0 * (1.0 + (index * 0.00008))
            signal = engine.on_tick("SBIN", price, features, 100.0 + index, in_position=False)

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "ENTRY")
        self.assertEqual(signal.reason, "STOCK_SCALP_MOMENTUM_ALIGNMENT")
        self.assertLess(signal.score, 72.0)

    def test_symbols_keep_independent_histories(self):
        engine = PercentNormalizedStockEngine(min_samples=3)
        engine.on_tick("SBIN", 800.0, self._strong_features(800.0), 1.0, in_position=False)
        engine.on_tick("SBIN", 801.0, self._strong_features(801.0), 2.0, in_position=False)
        engine.on_tick("HDFCBANK", 1700.0, self._strong_features(1700.0), 2.0, in_position=False)
        self.assertEqual(len(engine.history["SBIN"]), 2)
        self.assertEqual(len(engine.history["HDFCBANK"]), 1)


class StockPaperPortfolioTests(unittest.TestCase):
    def test_multi_stock_positions_and_net_pnl(self):
        portfolio = StockPaperPortfolio(
            capital=500000.0,
            notional_per_trade=75000.0,
            max_positions=2,
            round_trip_fee=40.0,
        )
        self.assertTrue(portfolio.enter(1, "SBIN", 750.0, 80.0, now=10.0))
        self.assertTrue(portfolio.enter(2, "HDFCBANK", 1500.0, 82.0, now=11.0))
        self.assertFalse(portfolio.enter(3, "RELIANCE", 3000.0, 85.0, now=12.0))
        trade = portfolio.exit(1, 755.0, "TEST", now=20.0)
        self.assertIsNotNone(trade)
        self.assertAlmostEqual(trade["net_pnl"], 460.0)
        self.assertEqual(len(portfolio.positions), 1)

    def test_score_breakdown_exit_requires_hold_confirmation_and_fee_edge(self):
        runtime = StockPaperRuntime.__new__(StockPaperRuntime)
        runtime.settings = SimpleNamespace(
            stop_loss_pct=0.35,
            take_profit_pct=0.80,
            trail_arm_pct=0.40,
            trail_giveback_pct=0.25,
            max_hold_sec=900.0,
            score_exit_min_hold_sec=25.0,
            score_exit_confirmations=2,
            score_exit_fee_guard_sec=90.0,
            score_exit_min_adverse_fee_ratio=0.75,
            round_trip_fee=40.0,
            heartbeat_sec=0.0,
        )
        runtime.score_exit_weak_count = defaultdict(int)
        runtime.last_score_exit_guard_log_ts = defaultdict(float)
        position = StockPaperPosition(
            secid=1,
            symbol="HDFCBANK",
            qty=94,
            entry=796.00,
            entry_ts=100.0,
            last_ltp=795.95,
            last_tick_ts=108.0,
            peak_ltp=796.00,
            entry_score=45.0,
        )
        early_signal = SimpleNamespace(action="EXIT", reason="PERCENT_SCORE_BREAKDOWN", ltp=795.95, score=35.0)
        self.assertIsNone(runtime._position_exit_reason(position, early_signal, 108.0))

        position.entry_ts = 70.0
        self.assertIsNone(runtime._position_exit_reason(position, early_signal, 108.0))
        self.assertIsNone(runtime._position_exit_reason(position, early_signal, 109.0))

        real_adverse_signal = SimpleNamespace(action="EXIT", reason="PERCENT_SCORE_BREAKDOWN", ltp=795.50, score=34.0)
        self.assertEqual(
            runtime._position_exit_reason(position, real_adverse_signal, 110.0),
            "PERCENT_SCORE_BREAKDOWN_CONFIRMED",
        )


class EquityInstrumentResolutionTests(unittest.TestCase):
    def test_exact_nse_equity_resolution(self):
        frame = pd.DataFrame(
            [
                {
                    "SEM_EXM_EXCH_ID": "NSE", "SEM_SEGMENT": "E",
                    "SEM_SMST_SECURITY_ID": "1333", "SEM_INSTRUMENT_NAME": "EQUITY",
                    "SEM_TRADING_SYMBOL": "HDFCBANK", "SEM_CUSTOM_SYMBOL": "HDFC BANK",
                    "SEM_OPTION_TYPE": "NA", "SEM_STRIKE_PRICE": "0",
                    "SEM_LOT_UNITS": "1", "SEM_EXPIRY_DATE": "",
                },
                {
                    "SEM_EXM_EXCH_ID": "NSE", "SEM_SEGMENT": "E",
                    "SEM_SMST_SECURITY_ID": "2885", "SEM_INSTRUMENT_NAME": "EQUITY",
                    "SEM_TRADING_SYMBOL": "RELIANCE", "SEM_CUSTOM_SYMBOL": "RELIANCE",
                    "SEM_OPTION_TYPE": "NA", "SEM_STRIKE_PRICE": "0",
                    "SEM_LOT_UNITS": "1", "SEM_EXPIRY_DATE": "",
                },
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "master.csv")
            frame.to_csv(path, index=False)
            master = InstrumentMaster(path, debug=False)
            instrument = master.get_equity("HDFCBANK")
        self.assertEqual(instrument["security_id"], 1333)
        self.assertEqual(instrument["exchange_segment"], "NSE_EQ")


if __name__ == "__main__":
    unittest.main()
