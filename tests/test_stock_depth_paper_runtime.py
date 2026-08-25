from datetime import datetime, time as clock_time
from types import SimpleNamespace
from unittest import TestCase

from dhan_engine.application.stocks.depth_paper_runtime import (
    IST,
    StockDepthPaperExecutor,
    StockDepthPaperSettings,
    StockProfile,
)
from dhan_engine.domain.stocks.equity_charges import NseIntradayChargeCalculator
from dhan_engine.simulations.stock_paper_portfolio import StockPaperPortfolio


class RecordingSink:
    def __init__(self):
        self.trades = []

    def record(self, trade):
        self.trades.append(dict(trade))
        return True


class StockDepthPaperExecutorTests(TestCase):
    def setUp(self):
        self.now = datetime(2026, 8, 25, 10, 0, tzinfo=IST).timestamp()
        self.settings = StockDepthPaperSettings(
            client_id="client",
            access_token="token",
            csv_file="master.csv",
            symbols=("RELIANCE",),
            capital=500_000.0,
            notional_per_trade=10_000.0,
            max_positions=1,
            leverage=5.0,
            fixed_qty=100,
            entry_confirmations=2,
            exit_confirmations=2,
            uncertain_exit_confirmations=3,
            min_confidence=0.65,
            min_edge_strength=0.08,
            min_forecast_reliability=0.35,
            max_quote_age_sec=2.0,
            max_future_spread_bps=25.0,
            max_cash_spread_bps=20.0,
            cash_beta=1.0,
            min_cost_multiple=1.5,
            market_start=clock_time(9, 15),
            entry_cutoff=clock_time(15, 10),
            market_end=clock_time(15, 15),
            health_interval_sec=30.0,
            trade_s3_bucket="bucket",
            trade_s3_prefix="paper-trades/stock-depth",
            trade_s3_queue_size=32,
        )
        self.portfolio = StockPaperPortfolio(
            capital=self.settings.capital,
            notional_per_trade=self.settings.notional_per_trade,
            max_positions=self.settings.max_positions,
            round_trip_fee=0.0,
            charge_calculator=NseIntradayChargeCalculator(),
            leverage=self.settings.leverage,
        )
        self.sink = RecordingSink()
        self.executor = StockDepthPaperExecutor(
            self.settings,
            self.portfolio,
            self.sink,
            clock_fn=lambda: self.now,
        )
        self.profile = StockProfile(
            root="RELIANCE",
            future_tag="RELIANCE_FUT",
            future_secid=101,
            future_symbol="RELIANCE-AUG2026-FUT",
            future_expiry="2026-08-25",
            cash_tag="RELIANCE_EQ",
            cash_secid=201,
            cash_symbol="RELIANCE",
        )
        self.executor.register(self.profile)
        self.executor.on_cash_quote(201, 100.0, 99.95, 100.05, self.now)
        self.composite = SimpleNamespace(
            book=SimpleNamespace(name="RELIANCE_FUT"),
            full_quote={"ltp": 100.0},
        )

    def predict(self, action, *, expected_bps=100.0, edge_active=True):
        self.executor.on_prediction(
            paper_action=action,
            confidence=0.90,
            composite=self.composite,
            probability_down=0.05,
            probability_flat=0.05,
            probability_up=0.90,
            model_version="premodel-test",
            horizon_sec=60,
            signal_metadata={
                "edge_active": edge_active,
                "edge_strength": 0.50 if edge_active else 0.0,
                "forecast_reliability": 0.90,
                "expected_future_move_bps": expected_bps,
                "ltp_now": 100.0,
            },
        )

    def test_confirmed_future_state_trades_cash_and_opposite_state_exits(self):
        self.predict("BUY_CE")
        self.assertEqual(self.portfolio.positions, {})
        self.predict("BUY_CE")
        self.assertEqual(self.portfolio.positions[201].side, "LONG")
        self.assertEqual(self.sink.trades, [])

        self.executor.on_cash_quote(201, 101.0, 100.95, 101.05, self.now + 1)
        self.now += 1
        self.predict("BUY_PE")
        self.assertIn(201, self.portfolio.positions)
        self.predict("BUY_PE")

        self.assertNotIn(201, self.portfolio.positions)
        self.assertEqual(len(self.sink.trades), 1)
        trade = self.sink.trades[0]
        self.assertEqual(trade["side"], "LONG")
        self.assertEqual(trade["profile"], "stock_depth_v1")
        self.assertEqual(trade["index"], "STOCKS")
        self.assertEqual(trade["reason"], "FUTURE_STATE_OPPOSITE:SHORT")
        self.assertGreater(trade["net_pnl"], 0.0)

        # Reversal is deliberately deferred until a subsequent prediction.
        self.predict("BUY_PE")
        self.assertEqual(self.portfolio.positions[201].side, "SHORT")

    def test_cost_gate_blocks_low_value_signal(self):
        self.predict("BUY_CE", expected_bps=0.01)
        self.predict("BUY_CE", expected_bps=0.01)
        self.assertEqual(self.portfolio.positions, {})
        self.assertGreater(
            self.executor.health()["blocked"]["EXPECTED_GROSS_BELOW_COST_BUFFER"],
            0,
        )

    def test_stale_cash_quote_flattens_open_position_and_queues_summary(self):
        self.predict("BUY_CE")
        self.predict("BUY_CE")
        self.assertIn(201, self.portfolio.positions)

        self.now += self.settings.max_quote_age_sec + 1.0
        self.executor.heartbeat()

        self.assertNotIn(201, self.portfolio.positions)
        self.assertEqual(len(self.sink.trades), 1)
        self.assertEqual(self.sink.trades[0]["reason"], "STALE_CASH_MARKET_DATA")

    def test_stale_future_depth_signal_flattens_while_cash_quote_is_fresh(self):
        self.predict("BUY_CE")
        self.predict("BUY_CE")
        self.assertIn(201, self.portfolio.positions)

        self.now += self.settings.max_quote_age_sec + 1.0
        self.executor.on_cash_quote(201, 100.5, 100.45, 100.55, self.now)
        self.executor.heartbeat()

        self.assertNotIn(201, self.portfolio.positions)
        self.assertEqual(len(self.sink.trades), 1)
        self.assertEqual(
            self.sink.trades[0]["reason"], "STALE_FUTURE_DEPTH_SIGNAL"
        )
