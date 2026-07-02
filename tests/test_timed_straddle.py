from __future__ import annotations

import unittest
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from dhan_engine.application.experiments.timed_straddle import (
    LegQuote,
    TimedStraddleBook,
    TimedStraddleRuntime,
    TimedStraddleSettings,
)


def settings(**overrides):
    values = {
        "client_id": "test",
        "access_token": "test",
        "lot_size_override": 25,
        "round_trip_cost": 160.0,
        "profit_target_net": 100.0,
        "hold_sec": 180.0,
    }
    values.update(overrides)
    return TimedStraddleSettings(**values)


def selection(atm=25000.0, ids=(1, 2, 3, 4)):
    return {
        "strike": atm,
        "upper_strike": atm + 50,
        "lower_strike": atm - 50,
        "wing_width": 50.0,
        "expiry": "2026-07-07",
        "underlying_ltp": atm + 10,
        "long_ce_secid": ids[0],
        "long_pe_secid": ids[1],
        "short_ce_secid": ids[2],
        "short_pe_secid": ids[3],
        "lot_size": 25,
    }


def quote(secid, side, bid, ask, ts=100.0):
    return LegQuote(secid, side, (bid + ask) / 2.0, bid, ask, ts)


def entry_legs():
    return (
        quote(1, "CE", 99, 100),
        quote(2, "PE", 99, 100),
        quote(3, "CE", 79, 80),
        quote(4, "PE", 79, 80),
    )


class TimedStraddleBookTests(unittest.TestCase):
    def test_four_leg_executable_prices_and_costs(self):
        book = TimedStraddleBook(settings())
        book.open(cycle=1, selection=selection(), long_ce=entry_legs()[0], long_pe=entry_legs()[1], short_ce=entry_legs()[2], short_pe=entry_legs()[3], now=100)
        summary = book.close(
            quote(1, "CE", 110, 111),
            quote(2, "PE", 90, 91),
            quote(3, "CE", 88, 89),
            quote(4, "PE", 70, 71),
            now=160,
            reason="TEST",
        )
        self.assertEqual(summary["long_ce_pnl"], 250.0)
        self.assertEqual(summary["long_pe_pnl"], -250.0)
        self.assertEqual(summary["short_ce_pnl"], -250.0)
        self.assertEqual(summary["short_pe_pnl"], 200.0)
        self.assertEqual(summary["gross_pnl"], -50.0)
        self.assertEqual(summary["net_pnl"], -210.0)

    def test_calculates_debit_and_capped_max_profit(self):
        book = TimedStraddleBook(settings())
        position = book.open(cycle=1, selection=selection(), long_ce=entry_legs()[0], long_pe=entry_legs()[1], short_ce=entry_legs()[2], short_pe=entry_legs()[3], now=100)
        self.assertEqual(position.net_debit_points, 42.0)
        self.assertEqual(position.max_profit_net, 40.0)

    def test_timeout_reason_is_duration_neutral(self):
        book = TimedStraddleBook(settings(hold_sec=120))
        legs = entry_legs()
        book.open(cycle=1, selection=selection(), long_ce=legs[0], long_pe=legs[1], short_ce=legs[2], short_pe=legs[3], now=100)
        self.assertIsNone(book.exit_reason(*legs, now=219.9))
        self.assertEqual(book.exit_reason(*legs, now=220), "TIMED_HOLD_TIMEOUT")

    def test_force_close_overrides_profit(self):
        book = TimedStraddleBook(settings())
        legs = entry_legs()
        book.open(cycle=1, selection=selection(), long_ce=legs[0], long_pe=legs[1], short_ce=legs[2], short_pe=legs[3], now=100)
        self.assertEqual(book.exit_reason(*legs, now=101, force_close=True), "MARKET_FORCE_CLOSE")


class FakeSelector:
    def __init__(self):
        self.value = selection()

    def select_atm_reverse_iron_fly(self, index, wing_steps):
        return dict(self.value)


class FakeStream:
    def __init__(self):
        self.subscriptions = []
        self.reconnections = []

    def replace_subscriptions(self, subscriptions, reason=""):
        self.subscriptions.append((list(subscriptions), reason))

    def reconnect_for_subscriptions(self, subscriptions, reason=""):
        self.reconnections.append((list(subscriptions), reason))


class FakeSink:
    def __init__(self):
        self.records = []
        self.portfolios = []

    def record(self, section, payload):
        self.records.append((section, payload))
        return True

    def record_portfolio(self, section, payload):
        self.portfolios.append((section, payload))
        return True


def ist_ts(hour, minute, second=0):
    return datetime(2026, 7, 1, hour, minute, second, tzinfo=ZoneInfo("Asia/Kolkata")).timestamp()


class TimedStraddleRuntimeTests(unittest.TestCase):
    def make_runtime(self, **setting_overrides):
        cfg = settings(**setting_overrides)
        sink, stream, selector = FakeSink(), FakeStream(), FakeSelector()
        runtime = TimedStraddleRuntime(cfg, None, selector, stream, sink=sink)
        return runtime, stream, sink, selector

    @staticmethod
    def depth(bid, ask):
        return SimpleNamespace(features={"best_bid": bid, "best_ask": ask}, bid_price=[], ask_price=[])

    def feed_structure(self, runtime, now, values=(100, 100, 70, 70), ids=(1, 2, 3, 4)):
        runtime.clock = lambda: now
        tags = ("NIFTY_LONG_CE", "NIFTY_LONG_PE", "NIFTY_SHORT_CE", "NIFTY_SHORT_PE")
        for secid, tag, value in zip(ids, tags, values):
            runtime.on_quote(secid, tag, value, self.depth(value - 1, value))

    def test_selects_four_legs_and_enters_only_when_max_profit_can_hit_target(self):
        runtime, stream, _, _ = self.make_runtime(round_trip_cost=80, profit_target_net=100)
        now = ist_ts(10, 0)
        runtime.step(now)
        self.assertEqual(len(stream.subscriptions[0][0]), 4)
        self.feed_structure(runtime, now + 1, values=(100, 100, 90, 90))
        runtime.step(now + 1)
        self.assertIsNotNone(runtime.book.position)
        self.assertEqual(runtime.cycle_count, 1)

    def test_blocks_mathematically_insufficient_structure(self):
        runtime, _, _, _ = self.make_runtime(round_trip_cost=160, profit_target_net=100)
        now = ist_ts(10, 0)
        runtime.step(now)
        self.feed_structure(runtime, now + 1, values=(100, 100, 80, 80))
        runtime.step(now + 1)
        self.assertIsNone(runtime.book.position)
        self.assertEqual(runtime.cycle_count, 0)

    def test_contract_change_reconnects_stream(self):
        runtime, stream, _, selector = self.make_runtime()
        now = ist_ts(10, 0)
        runtime.step(now)
        runtime.selection = None
        selector.value = selection(atm=25050, ids=(5, 6, 7, 8))
        runtime.step(now + 20)
        self.assertEqual(stream.reconnections[-1][1], "timed_straddle_contract_change")

    def test_missing_quotes_trigger_recovery(self):
        runtime, stream, _, _ = self.make_runtime()
        now = ist_ts(10, 0)
        runtime.step(now)
        runtime.step(now + 6)
        self.assertEqual(stream.reconnections[-1][1], "timed_straddle_missing_quotes")

    def test_no_new_entry_at_or_after_1525(self):
        runtime, stream, _, _ = self.make_runtime()
        runtime.step(ist_ts(15, 25))
        self.assertEqual(stream.subscriptions, [])

    def test_three_consecutive_losses_halt_new_cycles(self):
        runtime, _, _, _ = self.make_runtime(
            round_trip_cost=0,
            profit_target_net=0,
            hold_sec=1,
            max_consecutive_losses=3,
            daily_loss_limit=10000,
        )
        now = ist_ts(10, 0)
        for cycle in range(3):
            cycle_start = now + cycle * 10
            runtime.step(cycle_start)
            self.feed_structure(runtime, cycle_start + 1, values=(100, 100, 80, 80))
            runtime.step(cycle_start + 1)
            self.feed_structure(runtime, cycle_start + 3, values=(95, 95, 85, 85))
            runtime.step(cycle_start + 3)

        self.assertTrue(runtime.risk_halted)
        self.assertEqual(runtime.consecutive_losses, 3)
        runtime.step(now + 40)
        self.assertEqual(runtime.cycle_count, 3)

    def test_win_resets_consecutive_loss_counter(self):
        runtime, _, _, _ = self.make_runtime()
        runtime.consecutive_losses = 2
        runtime.selection = selection()
        runtime.book.open(
            cycle=1,
            selection=selection(),
            long_ce=entry_legs()[0],
            long_pe=entry_legs()[1],
            short_ce=entry_legs()[2],
            short_pe=entry_legs()[3],
            now=ist_ts(10, 0),
        )
        now = ist_ts(10, 4)
        self.feed_structure(runtime, now, values=(120, 100, 80, 80))
        runtime.step(now)
        self.assertEqual(runtime.consecutive_losses, 0)


if __name__ == "__main__":
    unittest.main()
