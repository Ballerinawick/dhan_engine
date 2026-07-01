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
        "round_trip_cost": 80.0,
        "profit_target_net": 100.0,
        "hold_sec": 300.0,
    }
    values.update(overrides)
    return TimedStraddleSettings(**values)


def quote(secid, side, bid, ask, ts=100.0):
    return LegQuote(secid, side, (bid + ask) / 2.0, bid, ask, ts)


class TimedStraddleBookTests(unittest.TestCase):
    def setUp(self):
        self.selection = {"strike": 25000.0, "expiry": "2026-07-07", "lot_size": 25}

    def test_uses_ask_for_entry_bid_for_exit_and_deducts_cost(self):
        book = TimedStraddleBook(settings())
        book.open(
            cycle=1,
            selection=self.selection,
            ce=quote(1, "CE", 99.0, 100.0),
            pe=quote(2, "PE", 109.0, 110.0),
            now=100.0,
        )
        summary = book.close(
            quote(1, "CE", 104.0, 105.0),
            quote(2, "PE", 113.0, 114.0),
            160.0,
            "TEST",
        )
        self.assertEqual(summary["gross_pnl"], 175.0)
        self.assertEqual(summary["net_pnl"], 95.0)
        self.assertEqual(summary["fees"], 80.0)

    def test_exits_early_only_on_net_profit_target(self):
        book = TimedStraddleBook(settings(profit_target_net=100.0))
        book.open(cycle=1, selection=self.selection, ce=quote(1, "CE", 99, 100), pe=quote(2, "PE", 99, 100), now=100)
        self.assertIsNone(book.exit_reason(quote(1, "CE", 103, 104), quote(2, "PE", 103, 104), 120))
        self.assertEqual(book.exit_reason(quote(1, "CE", 104, 105), quote(2, "PE", 104, 105), 121), "NET_PROFIT_TARGET")

    def test_forces_timeout_at_five_minutes_even_when_negative(self):
        book = TimedStraddleBook(settings())
        book.open(cycle=1, selection=self.selection, ce=quote(1, "CE", 99, 100), pe=quote(2, "PE", 99, 100), now=100)
        self.assertIsNone(book.exit_reason(quote(1, "CE", 98, 99), quote(2, "PE", 98, 99), 399.9))
        self.assertEqual(book.exit_reason(quote(1, "CE", 98, 99), quote(2, "PE", 98, 99), 400), "FIVE_MINUTE_TIMEOUT")

    def test_force_close_overrides_other_exit_reasons(self):
        book = TimedStraddleBook(settings())
        book.open(cycle=1, selection=self.selection, ce=quote(1, "CE", 99, 100), pe=quote(2, "PE", 99, 100), now=100)
        self.assertEqual(book.exit_reason(quote(1, "CE", 110, 111), quote(2, "PE", 110, 111), 101, force_close=True), "MARKET_FORCE_CLOSE")


class FakeSelector:
    def select_atm_pair(self, index):
        return {
            "index": index,
            "strike": 25000.0,
            "expiry": "2026-07-07",
            "ce_secid": 1,
            "pe_secid": 2,
            "lot_size": 25,
        }


class FakeStream:
    def __init__(self):
        self.subscriptions = []

    def replace_subscriptions(self, subscriptions, reason=""):
        self.subscriptions.append((list(subscriptions), reason))


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
        sink = FakeSink()
        stream = FakeStream()
        runtime = TimedStraddleRuntime(cfg, None, FakeSelector(), stream, sink=sink)
        return runtime, stream, sink

    @staticmethod
    def depth(bid, ask):
        return SimpleNamespace(features={"best_bid": bid, "best_ask": ask}, bid_price=[], ask_price=[])

    def test_deployment_during_session_selects_then_enters_when_both_quotes_arrive(self):
        runtime, stream, _ = self.make_runtime()
        now = ist_ts(10, 0)
        runtime.step(now)
        self.assertEqual(len(stream.subscriptions), 1)
        runtime.clock = lambda: now + 1
        runtime.on_quote(1, "NIFTY_CE", 100, self.depth(99, 100))
        runtime.on_quote(2, "NIFTY_PE", 110, self.depth(109, 110))
        runtime.step(now + 1)
        self.assertIsNotNone(runtime.book.position)
        self.assertEqual(runtime.cycle_count, 1)

    def test_timeout_closes_records_and_next_step_starts_next_cycle(self):
        runtime, stream, sink = self.make_runtime()
        now = ist_ts(10, 0)
        runtime.step(now)
        runtime.clock = lambda: now + 1
        runtime.on_quote(1, "NIFTY_CE", 100, self.depth(99, 100))
        runtime.on_quote(2, "NIFTY_PE", 100, self.depth(99, 100))
        runtime.step(now + 1)
        runtime.clock = lambda: now + 301
        runtime.on_quote(1, "NIFTY_CE", 98, self.depth(98, 99))
        runtime.on_quote(2, "NIFTY_PE", 98, self.depth(98, 99))
        runtime.step(now + 301)
        self.assertIsNone(runtime.book.position)
        self.assertEqual(sink.records[0][1]["exit_reason"], "FIVE_MINUTE_TIMEOUT")
        runtime.step(now + 302)
        self.assertEqual(len(stream.subscriptions), 2)

    def test_no_new_entry_at_or_after_1525(self):
        runtime, stream, _ = self.make_runtime()
        runtime.step(ist_ts(15, 25))
        self.assertEqual(stream.subscriptions, [])

    def test_max_cycle_guard_is_entry_count_not_signal_count(self):
        runtime, stream, _ = self.make_runtime(max_cycles=1)
        runtime.cycle_count = 1
        runtime.step(ist_ts(10, 0))
        self.assertEqual(stream.subscriptions, [])


if __name__ == "__main__":
    unittest.main()
