from dhan_engine.application.deeplob.virtual_strategy_books import (
    ExecutableStrategyLedger,
    MarketMark,
)


def mark(timestamp, future, ce_bid, ce_ask, pe_bid, pe_ask):
    return MarketMark(
        received_ts=timestamp,
        future_ltp=future,
        ce_bid=ce_bid,
        ce_ask=ce_ask,
        pe_bid=pe_bid,
        pe_ask=pe_ask,
    )


def test_strategy_books_start_with_executable_spread_cost():
    ledger = ExecutableStrategyLedger()

    snapshot = ledger.update(mark(1000.0, 24350.0, 99.5, 100.0, 109.5, 110.0))
    books = snapshot["books"]

    assert snapshot["ready"] is True
    assert snapshot["updates"] == 1
    assert books["long_ce"]["pnl"] == -0.5
    assert books["long_pe"]["pnl"] == -0.5
    assert books["long_straddle"]["pnl"] == -1.0
    assert books["short_straddle"]["pnl"] == -1.0
    assert snapshot["valuation"]["options"] == "EXECUTABLE_BID_ASK"
    assert snapshot["valuation"]["paper_only_short_books"] is True


def test_bullish_market_marks_directional_books_and_excursions():
    ledger = ExecutableStrategyLedger()
    ledger.update(mark(1000.0, 24350.0, 99.5, 100.0, 109.5, 110.0))

    snapshot = ledger.update(mark(1005.0, 24370.0, 103.5, 104.0, 106.5, 107.0))
    books = snapshot["books"]

    assert books["future_long"]["pnl"] > books["future_short"]["pnl"]
    assert books["long_ce"]["pnl_pct"] > books["long_pe"]["pnl_pct"]
    assert books["synthetic_long"]["pnl_pct"] > books["synthetic_short"]["pnl_pct"]
    assert books["long_ce"]["mfe_pct"] == books["long_ce"]["pnl_pct"]
    assert books["long_pe"]["mae_pct"] == books["long_pe"]["pnl_pct"]
    assert snapshot["updates"] == 2


def test_invalid_quotes_do_not_initialize_or_mutate_books():
    ledger = ExecutableStrategyLedger()

    empty = ledger.update(mark(1000.0, 24350.0, 0.0, 100.0, 109.5, 110.0))
    assert empty["ready"] is False

    ledger.update(mark(1001.0, 24350.0, 99.5, 100.0, 109.5, 110.0))
    before = ledger.snapshot(1001.0)
    after = ledger.update(mark(1002.0, 24360.0, 0.0, 104.0, 106.5, 107.0))

    assert after["updates"] == before["updates"]
    assert {
        name: (book["pnl"], book["pnl_pct"], book["updates"])
        for name, book in after["books"].items()
    } == {
        name: (book["pnl"], book["pnl_pct"], book["updates"])
        for name, book in before["books"].items()
    }
