import os
import sys
import tempfile
import unittest
from collections import defaultdict
from types import SimpleNamespace

import pandas as pd

sys.modules.setdefault("websocket", SimpleNamespace(WebSocketApp=object))
sys.modules.setdefault(
    "requests",
    SimpleNamespace(
        Session=object,
        HTTPError=Exception,
        get=lambda *_, **__: None,
        post=lambda *_, **__: None,
    ),
)

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
            "ltp_vs_avg_pct": 0.03,
            "day_position": 0.55,
            "depth_imbalance_5": 0.20,
            "top_depth_imbalance": 0.18,
            "market_queue_imbalance": 0.16,
            "pressure_score": 0.10,
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

    def test_support_watch_without_reclaim_blocks_reliance_style_tick(self):
        engine = PercentNormalizedStockEngine(min_samples=3)
        signal = None
        features = {
            "intraday_return_pct": -1.071,
            "ltp_vs_avg_pct": -0.226,
            "day_position": 0.1716,
            "depth_imbalance_5": 0.593,
            "top_depth_imbalance": -0.906,
            "market_queue_imbalance": -0.103,
            "pressure_score": -0.101,
            "spread_pct": 0.0077,
            "clean_trade_score": 0.951,
            "spoof_risk": 0.0,
            "buy_sell_qty_ratio": 0.814,
        }
        for index in range(8):
            signal = engine.on_tick("RELIANCE", 1292.9 + (index * 0.02), features, 100.0 + index, in_position=False)

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "HOLD")
        self.assertEqual(signal.reason, "SUPPORT_WATCH_NO_RECLAIM")
        self.assertEqual(signal.features["support_watch"], 1.0)
        self.assertEqual(signal.features["long_entry_ready"], 0.0)

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

    def test_adaptive_exit_locks_profit_after_fee_aware_giveback(self):
        runtime = StockPaperRuntime.__new__(StockPaperRuntime)
        runtime.settings = SimpleNamespace(
            stop_loss_pct=0.35,
            take_profit_pct=0.80,
            trail_arm_pct=0.80,
            trail_giveback_pct=0.50,
            max_hold_sec=900.0,
            round_trip_fee=40.0,
            adaptive_exit_enabled=True,
            profit_lock_min_hold_sec=90.0,
            profit_lock_min_fee_multiple=1.60,
            profit_lock_giveback_fee_multiple=0.80,
            dead_trade_sec=360.0,
            dead_trade_fee_ratio=0.85,
            dead_trade_max_score=45.0,
            dead_trade_min_net_fee_multiple=0.50,
        )
        runtime.score_exit_weak_count = defaultdict(int)
        position = StockPaperPosition(
            secid=1,
            symbol="SBIN",
            qty=72,
            entry=1000.0,
            entry_ts=100.0,
            last_ltp=1001.0,
            last_tick_ts=200.0,
            peak_ltp=1001.8,
            entry_score=60.0,
        )
        signal = SimpleNamespace(
            action="HOLD",
            reason="POSITION_HELD",
            ltp=1001.0,
            score=52.0,
            features={"orderflow_score": 55.0, "scalp_confidence": 56.0, "return_5s_pct": 0.01, "return_30s_pct": 0.03},
        )

        self.assertEqual(
            runtime._position_exit_reason(position, signal, 220.0),
            "STOCK_ADAPTIVE_PROFIT_LOCK",
        )

    def test_adaptive_exit_cuts_dead_stock_scalp_before_max_hold(self):
        runtime = StockPaperRuntime.__new__(StockPaperRuntime)
        runtime.settings = SimpleNamespace(
            stop_loss_pct=0.35,
            take_profit_pct=0.80,
            trail_arm_pct=0.40,
            trail_giveback_pct=0.25,
            max_hold_sec=900.0,
            round_trip_fee=40.0,
            adaptive_exit_enabled=True,
            dead_trade_sec=360.0,
            dead_trade_fee_ratio=0.85,
            dead_trade_max_score=45.0,
            dead_trade_min_net_fee_multiple=0.50,
            profit_lock_min_hold_sec=90.0,
            profit_lock_min_fee_multiple=1.60,
            profit_lock_giveback_fee_multiple=0.80,
        )
        runtime.score_exit_weak_count = defaultdict(int)
        position = StockPaperPosition(
            secid=1,
            symbol="SBIN",
            qty=72,
            entry=1000.0,
            entry_ts=100.0,
            last_ltp=999.90,
            last_tick_ts=500.0,
            peak_ltp=1000.10,
            entry_score=60.0,
        )
        signal = SimpleNamespace(
            action="HOLD",
            reason="POSITION_HELD",
            ltp=999.90,
            score=42.0,
            features={"orderflow_score": 44.0, "scalp_confidence": 40.0, "return_5s_pct": -0.01, "return_30s_pct": -0.02},
        )

        self.assertEqual(
            runtime._position_exit_reason(position, signal, 470.0),
            "STOCK_ADAPTIVE_DEAD_SCALP_EXIT",
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
