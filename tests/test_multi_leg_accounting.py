from __future__ import annotations

import math

import pytest

from dhan_engine.domain.accounting.engine import StrategyAccountingBook
from dhan_engine.domain.accounting.models import (
    InstrumentIdentity,
    LegSpec,
    OptionType,
    TradeSide,
)


def _quote(bid: float, ask: float, ts: int) -> dict:
    return {"bid": bid, "ask": ask, "timestamp_ns": ts}


def _leg(leg_id: str, *, secid: int, side: TradeSide, quantity: int, strategy: str = "s1", strike: float = 100.0, expiry: str = "2026-12-31") -> LegSpec:
    return LegSpec(
        strategy_instance_id=strategy,
        leg_id=leg_id,
        instrument=InstrumentIdentity(security_id=secid, exchange_segment="NSE_FNO", symbol=f"OPT{secid}"),
        option_type=OptionType.CALL,
        strike=strike,
        expiry=expiry,
        side=side,
        quantity=quantity,
    )


def test_initialization_creates_first_authoritative_snapshot_and_negative_net_pnl():
    book = StrategyAccountingBook(strategy_instance_id="s1")
    long_ce = _leg("long_ce", secid=101, side=TradeSide.BUY, quantity=2)
    short_pe = _leg("short_pe", secid=102, side=TradeSide.SELL, quantity=3)
    book.add_leg(long_ce)
    book.add_leg(short_pe)

    init = book.initialize(
        {
            "long_ce": _quote(94.0, 96.0, 1_000),
            "short_pe": _quote(104.0, 106.0, 1_000),
        },
        timestamp_ns=1_000,
        entry_fees=25.0,
    )

    assert init is not None
    assert book.lifecycle == "INITIALIZED"
    assert book.snapshot().net_pnl < 0
    assert book.snapshot().gross_realized_pnl == 0.0
    assert book.snapshot().gross_unrealized_pnl != 0.0
    assert book.snapshot().mfe == 0.0
    assert book.snapshot().mae <= 0.0


def test_short_leg_entry_and_mark_pricing():
    book = StrategyAccountingBook(strategy_instance_id="s2")
    short_leg = _leg("short_leg", secid=202, side=TradeSide.SELL, quantity=4, strategy="s2")
    book.add_leg(short_leg)

    init = book.initialize({"short_leg": _quote(100.0, 110.0, 2_000)}, timestamp_ns=2_000, entry_fees=10.0)
    assert init is not None
    assert book.snapshot().leg_snapshots["short_leg"].entry_price == 100.0
    assert book.snapshot().leg_snapshots["short_leg"].current_mark == 110.0

    mark = book.mark({"short_leg": _quote(90.0, 120.0, 3_000)}, timestamp_ns=3_000)
    assert mark is not None
    assert mark.leg_snapshots["short_leg"].gross_unrealized_pnl == (100.0 - 120.0) * 4.0
    assert mark.leg_snapshots["short_leg"].current_mark == 120.0


def test_long_and_short_cash_flows_are_signed_executable_values():
    book = StrategyAccountingBook(strategy_instance_id="cash")
    long_leg = _leg("long", secid=203, side=TradeSide.BUY, quantity=2, strategy="cash")
    short_leg = _leg("short", secid=204, side=TradeSide.SELL, quantity=3, strategy="cash")
    book.add_leg(long_leg)
    book.add_leg(short_leg)
    initial = book.initialize(
        {"long": _quote(9.0, 10.0, 2_500), "short": _quote(20.0, 21.0, 2_500)},
        timestamp_ns=2_500,
    )
    assert initial is not None
    assert initial.entry_cash_flow == -20.0 + 60.0
    assert initial.leg_snapshots["long"].executable_liquidation_value == 18.0
    assert initial.leg_snapshots["short"].executable_liquidation_value == -63.0

    closed = book.close(
        {"long": _quote(12.0, 13.0, 2_600), "short": _quote(15.0, 16.0, 2_600)},
        timestamp_ns=2_600,
    )
    assert closed is not None
    assert closed.exit_cash_flow == 24.0 - 48.0
    assert closed.gross_realized_pnl == (12.0 - 10.0) * 2.0 + (20.0 - 16.0) * 3.0
    assert closed.gross_unrealized_pnl == 0.0
    assert closed.liquidation_value == 0.0


def test_strategy_mfe_mae_comes_from_combined_net_trajectory_and_not_summed_leg_values():
    book = StrategyAccountingBook(strategy_instance_id="s3")
    long_ce = _leg("long_ce", secid=301, side=TradeSide.BUY, quantity=2, strategy="s3")
    short_ce = _leg("short_ce", secid=302, side=TradeSide.SELL, quantity=2, strategy="s3")
    book.add_leg(long_ce)
    book.add_leg(short_ce)

    book.initialize(
        {
            "long_ce": _quote(100.0, 100.0, 5_000),
            "short_ce": _quote(100.0, 100.0, 5_000),
        },
        timestamp_ns=5_000,
        entry_fees=10.0,
    )

    book.mark({"long_ce": _quote(99.0, 101.0, 6_000), "short_ce": _quote(99.0, 101.0, 6_000)}, timestamp_ns=6_000)
    book.mark({"long_ce": _quote(105.0, 106.0, 7_000), "short_ce": _quote(95.0, 96.0, 7_000)}, timestamp_ns=7_000)

    snapshot = book.snapshot()
    assert snapshot.mfe >= 0.0
    assert snapshot.mae <= 0.0
    assert snapshot.net_pnl == snapshot.gross_realized_pnl + snapshot.gross_unrealized_pnl - snapshot.fees
    assert snapshot.mfe >= 0.0


def test_partial_and_full_strategy_closure_allocate_fees_deterministically_and_preserve_exclusive_pnl():
    book = StrategyAccountingBook(strategy_instance_id="s4")
    long_a = _leg("long_a", secid=401, side=TradeSide.BUY, quantity=2, strategy="s4")
    long_b = _leg("long_b", secid=402, side=TradeSide.BUY, quantity=4, strategy="s4")
    book.add_leg(long_a)
    book.add_leg(long_b)

    book.initialize(
        {
            "long_a": _quote(95.0, 100.0, 10_000),
            "long_b": _quote(90.0, 95.0, 10_000),
        },
        timestamp_ns=10_000,
        entry_fees=30.0,
    )

    partial = book.close_legs(
        ["long_a"],
        {"long_a": _quote(98.0, 100.0, 11_000), "long_b": _quote(88.0, 90.0, 11_000)},
        timestamp_ns=11_000,
        exit_fees=4.0,
    )
    assert partial is not None
    assert book.snapshot().strategy_lifecycle == "PARTIALLY_CLOSED"
    assert book.snapshot().leg_snapshots["long_a"].lifecycle == "CLOSED"
    assert book.snapshot().leg_snapshots["long_b"].lifecycle == "OPEN"
    assert book.snapshot().fees == 34.0

    final = book.close({"long_b": _quote(100.0, 101.0, 12_000)}, timestamp_ns=12_000, exit_fees=6.0)
    assert final is not None
    assert book.snapshot().strategy_lifecycle == "CLOSED"
    assert book.snapshot().leg_snapshots["long_b"].lifecycle == "CLOSED"
    assert book.snapshot().gross_realized_pnl >= 0.0
    assert book.snapshot().gross_unrealized_pnl == 0.0


def test_rejected_marks_do_not_mutate_state_and_failed_operations_have_no_side_effects():
    book = StrategyAccountingBook(strategy_instance_id="s5")
    leg = _leg("leg", secid=501, side=TradeSide.BUY, quantity=1, strategy="s5")
    book.add_leg(leg)
    book.initialize({"leg": _quote(90.0, 100.0, 20_000)}, timestamp_ns=20_000, entry_fees=5.0)

    rejected = book.mark({"leg": _quote(0.0, 0.0, 21_000)}, timestamp_ns=21_000)
    assert rejected is None
    assert book.snapshot().valuation_state == "VALID"
    assert book.snapshot().leg_snapshots["leg"].current_mark == 90.0

    close_rejected = book.close({"leg": _quote(float("nan"), 120.0, 22_000)}, timestamp_ns=22_000, exit_fees=7.0)
    assert close_rejected is None
    assert book.snapshot().leg_snapshots["leg"].lifecycle == "OPEN"
    assert book.snapshot().fees == 5.0


def test_same_instrument_different_strategy_instances_are_isolated():
    book_a = StrategyAccountingBook(strategy_instance_id="a")
    book_b = StrategyAccountingBook(strategy_instance_id="b")
    leg_a = _leg("leg", secid=600, side=TradeSide.BUY, quantity=2, strategy="a")
    leg_b = _leg("leg", secid=600, side=TradeSide.BUY, quantity=3, strategy="b")
    book_a.add_leg(leg_a)
    book_b.add_leg(leg_b)

    book_a.initialize({"leg": _quote(100.0, 110.0, 30_000)}, timestamp_ns=30_000, entry_fees=0.0)
    book_b.initialize({"leg": _quote(100.0, 110.0, 30_000)}, timestamp_ns=30_000, entry_fees=0.0)

    assert book_a.snapshot().net_pnl != book_b.snapshot().net_pnl
    assert book_a.snapshot().strategy_instance_id != book_b.snapshot().strategy_instance_id


def test_recent_movement_is_distinct_from_lifetime_pnl():
    book = StrategyAccountingBook(strategy_instance_id="s7")
    leg = _leg("leg", secid=701, side=TradeSide.BUY, quantity=1, strategy="s7")
    book.add_leg(leg)
    book.initialize({"leg": _quote(100.0, 110.0, 40_000)}, timestamp_ns=40_000, entry_fees=5.0)
    book.mark({"leg": _quote(105.0, 111.0, 41_000)}, timestamp_ns=41_000)
    book.mark({"leg": _quote(120.0, 122.0, 42_000)}, timestamp_ns=42_000)

    movement = book.recent_movement(window_ns=1_500)
    assert movement is not None
    assert movement.start_ts_ns == 40_000
    assert movement.end_ts_ns == 42_000
    assert movement.net_pnl_delta == 20.0
    assert movement.gross_realized_delta == 0.0
    assert movement.gross_unrealized_delta == 20.0
    assert movement.liquidation_value_delta == 20.0
    assert book.snapshot().lifetime_net_pnl == 5.0


def test_partial_close_retains_closed_leg_and_exact_mutually_exclusive_totals():
    book = StrategyAccountingBook(strategy_instance_id="partial")
    first = _leg("first", secid=801, side=TradeSide.BUY, quantity=2, strategy="partial")
    second = _leg("second", secid=802, side=TradeSide.BUY, quantity=4, strategy="partial")
    book.add_leg(first)
    book.add_leg(second)
    book.initialize(
        {"first": _quote(95.0, 100.0, 50_000), "second": _quote(90.0, 95.0, 50_000)},
        timestamp_ns=50_000,
        entry_fees=30.0,
    )

    partial = book.close_legs(
        ["first"],
        {"first": _quote(105.0, 106.0, 51_000), "second": _quote(88.0, 90.0, 51_000)},
        timestamp_ns=51_000,
        exit_fees=4.0,
    )

    assert partial is not None
    closed = partial.leg_snapshots["first"]
    open_leg = partial.leg_snapshots["second"]
    assert closed.lifecycle.value == "CLOSED"
    assert closed.gross_realized_pnl == 10.0
    assert closed.gross_unrealized_pnl == 0.0
    assert closed.entry_cash_flow == -200.0
    assert closed.exit_cash_flow == 210.0
    assert closed.fees_incurred == 14.0
    assert closed.executable_liquidation_value == 0.0
    assert open_leg.lifecycle.value == "OPEN"
    assert open_leg.gross_realized_pnl == 0.0
    assert open_leg.gross_unrealized_pnl == -28.0
    assert open_leg.fees_incurred == 20.0
    assert open_leg.executable_liquidation_value == 352.0
    assert partial.gross_realized_pnl == 10.0
    assert partial.gross_unrealized_pnl == -28.0
    assert partial.fees == 34.0
    assert partial.net_pnl == -52.0
    assert partial.liquidation_value == 352.0

    mark = book.mark({"second": _quote(110.0, 111.0, 52_000)}, timestamp_ns=52_000)
    assert mark is not None
    assert mark.strategy_lifecycle.value == "PARTIALLY_CLOSED"
    assert mark.leg_snapshots["first"] == closed
    assert mark.leg_snapshots["second"].gross_unrealized_pnl == 60.0
    assert mark.gross_realized_pnl == 10.0
    assert mark.gross_unrealized_pnl == 60.0
    assert mark.fees == 34.0
    assert mark.net_pnl == 36.0
    assert mark.liquidation_value == 440.0


def test_partial_close_rejects_incomplete_open_leg_batch_without_mutation():
    book = StrategyAccountingBook(strategy_instance_id="close_batch")
    first = _leg("first", secid=851, side=TradeSide.BUY, quantity=1, strategy="close_batch")
    second = _leg("second", secid=852, side=TradeSide.SELL, quantity=1, strategy="close_batch")
    book.add_leg(first)
    book.add_leg(second)
    initial = book.initialize(
        {"first": _quote(99.0, 100.0, 55_000), "second": _quote(99.0, 100.0, 55_000)},
        timestamp_ns=55_000,
        entry_fees=2.0,
    )
    assert initial is not None

    rejected = book.close_legs(
        ["first"],
        {"first": _quote(105.0, 106.0, 56_000)},
        timestamp_ns=56_000,
        exit_fees=8.0,
    )
    assert rejected is None
    assert book.snapshot() == initial
    assert book.lifecycle == "INITIALIZED"
    assert book.snapshot().leg_snapshots["first"].lifecycle.value == "OPEN"
    assert book.snapshot().leg_snapshots["second"].lifecycle.value == "OPEN"
    assert book.snapshot().fees == 2.0
    assert book.last_rejection.valuation_state.value == "MISSING_QUOTE"

    accepted = book.close_legs(
        ["first"],
        {"first": _quote(105.0, 106.0, 57_000), "second": _quote(101.0, 102.0, 57_000)},
        timestamp_ns=57_000,
        exit_fees=8.0,
    )
    assert accepted is not None
    assert accepted.strategy_lifecycle.value == "PARTIALLY_CLOSED"
    assert accepted.timestamp_ns == 57_000
    assert accepted.leg_snapshots["second"].current_mark == 102.0
    assert accepted.leg_snapshots["second"].gross_unrealized_pnl == -3.0


def test_final_close_preserves_extrema_and_exact_strategy_totals():
    book = StrategyAccountingBook(strategy_instance_id="extrema")
    first = _leg("first", secid=901, side=TradeSide.BUY, quantity=2, strategy="extrema")
    second = _leg("second", secid=902, side=TradeSide.BUY, quantity=4, strategy="extrema")
    book.add_leg(first)
    book.add_leg(second)
    initial = book.initialize(
        {"first": _quote(95.0, 100.0, 60_000), "second": _quote(90.0, 95.0, 60_000)},
        timestamp_ns=60_000,
        entry_fees=30.0,
    )
    assert initial is not None
    assert initial.net_pnl == -60.0
    assert initial.mfe == 0.0
    assert initial.mae == -60.0

    peak = book.mark(
        {"first": _quote(110.0, 111.0, 61_000), "second": _quote(92.0, 93.0, 61_000)},
        timestamp_ns=61_000,
    )
    assert peak is not None
    assert peak.net_pnl == -22.0
    assert peak.mfe == 0.0
    assert peak.mae == -60.0
    partial = book.close_legs(
        ["first"],
        {"first": _quote(105.0, 106.0, 62_000), "second": _quote(92.0, 93.0, 62_000)},
        timestamp_ns=62_000,
        exit_fees=4.0,
    )
    assert partial is not None
    assert partial.net_pnl == -36.0
    assert partial.mfe == 0.0
    assert partial.mae == -60.0
    recovered = book.mark({"second": _quote(120.0, 121.0, 63_000)}, timestamp_ns=63_000)
    assert recovered is not None
    assert recovered.net_pnl == 76.0
    assert recovered.mfe == 76.0
    assert recovered.mae == -60.0

    final = book.close({"second": _quote(110.0, 111.0, 64_000)}, timestamp_ns=64_000, exit_fees=6.0)
    assert final is not None
    assert final.gross_realized_pnl == 70.0
    assert final.gross_unrealized_pnl == 0.0
    assert final.fees == 40.0
    assert final.net_pnl == 30.0
    assert final.mfe == 76.0
    assert final.mae == -60.0


def test_accepted_snapshots_and_leg_collections_are_deeply_immutable():
    book = StrategyAccountingBook(strategy_instance_id="immutable")
    leg = _leg("leg", secid=1001, side=TradeSide.BUY, quantity=1, strategy="immutable")
    book.add_leg(leg)
    snapshot = book.initialize({"leg": _quote(99.0, 100.0, 70_000)}, timestamp_ns=70_000)
    assert snapshot is not None
    with pytest.raises(TypeError):
        snapshot.leg_snapshots["other"] = snapshot.leg_snapshots["leg"]
    with pytest.raises(Exception):
        snapshot.leg_snapshots["leg"].net_pnl = 99.0
    assert book.history[0].net_pnl == -1.0


@pytest.mark.parametrize(
    ("quotes", "expected"),
    [
        ({}, "MISSING_QUOTE"),
        ({"leg": _quote(0.0, 100.0, 80_000)}, "INVALID_QUOTE"),
        ({"leg": _quote(99.0, 100.0, 80_001)}, "UNCOHERENT_BATCH"),
    ],
)
def test_invalid_or_incomplete_initialization_is_atomic(quotes, expected):
    book = StrategyAccountingBook(strategy_instance_id="validation")
    book.add_leg(_leg("leg", secid=1101, side=TradeSide.BUY, quantity=1, strategy="validation"))
    assert book.initialize(quotes, timestamp_ns=80_000, entry_fees=7.0) is None
    assert book.snapshot() is None
    assert book.lifecycle == "PLANNED"
    assert book.last_rejection.valuation_state.value == expected


def test_duplicate_out_of_order_stale_and_incomplete_marks_are_no_ops():
    book = StrategyAccountingBook(strategy_instance_id="quality")
    book.add_leg(_leg("one", secid=1201, side=TradeSide.BUY, quantity=1, strategy="quality"))
    book.add_leg(_leg("two", secid=1202, side=TradeSide.SELL, quantity=1, strategy="quality"))
    book.initialize({"one": _quote(99.0, 100.0, 90_000), "two": _quote(99.0, 100.0, 90_000)}, timestamp_ns=90_000)
    before = book.snapshot()
    assert book.mark({"one": _quote(99.0, 100.0, 90_000), "two": _quote(99.0, 100.0, 90_000)}, timestamp_ns=90_000) is None
    assert book.last_rejection.valuation_state.value == "DUPLICATE_TIMESTAMP"
    assert book.mark({"one": _quote(99.0, 100.0, 89_000), "two": _quote(99.0, 100.0, 89_000)}, timestamp_ns=89_000) is None
    assert book.last_rejection.valuation_state.value == "OUT_OF_ORDER_TIMESTAMP"
    assert book.mark({"one": _quote(99.0, 100.0, 91_000), "two": _quote(99.0, 100.0, 91_000)}, timestamp_ns=91_000, stale=True) is None
    assert book.last_rejection.valuation_state.value == "STALE_QUOTE"
    assert book.mark({"one": _quote(99.0, 100.0, 91_000)}, timestamp_ns=91_000) is None
    assert book.last_rejection.valuation_state.value == "MISSING_QUOTE"
    assert book.snapshot() == before


def test_stale_age_is_caller_supplied_and_leg_lifecycle_is_minimal():
    book = StrategyAccountingBook(strategy_instance_id="age")
    leg = _leg("leg", secid=1301, side=TradeSide.BUY, quantity=1, strategy="age")
    book.add_leg(leg)
    assert book.initialize({"leg": _quote(99.0, 100.0, 100_000)}, timestamp_ns=100_000) is not None
    assert book.snapshot().leg_snapshots["leg"].lifecycle.value == "OPEN"
    assert book.mark(
        {"leg": _quote(100.0, 101.0, 101_000)},
        timestamp_ns=101_000,
        ages_ns={"leg": 11},
        max_age_ns=10,
    ) is None
    assert book.last_rejection.valuation_state.value == "STALE_QUOTE"
    closed = book.close({"leg": _quote(100.0, 101.0, 102_000)}, timestamp_ns=102_000)
    assert closed is not None
    assert closed.leg_snapshots["leg"].lifecycle.value == "CLOSED"
