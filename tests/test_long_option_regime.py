import time
from datetime import date, time as clock_time
from types import SimpleNamespace

from dhan_engine.application.deeplob.long_option_regime import (
    LongOptionRegimeExecutor,
    LongOptionRegimeSettings,
)


class FakePaperTrader:
    LOT_SIZES = {"NIFTY": 65}

    def __init__(self):
        self.positions = {}
        self.last_trade_summary = None

    def has_open_position(self):
        return bool(self.positions)

    def on_entry(self, secid, tag, side, ltp, lots=1, reason="ENTRY", metadata=None):
        self.positions[int(secid)] = {
            "secid": int(secid),
            "tag": tag,
            "side": side,
            "entry": float(ltp),
            "ltp": float(ltp),
            "qty": 65 * int(lots),
            **dict(metadata or {}),
        }
        return True

    def on_tick(self, secid, ltp):
        if int(secid) in self.positions:
            self.positions[int(secid)]["ltp"] = float(ltp)

    def on_exit(self, secid, ltp, reason="EXIT"):
        position = self.positions.pop(int(secid))
        gross = (float(ltp) - position["entry"]) * position["qty"]
        self.last_trade_summary = {
            "secid": int(secid),
            "tag": position["tag"],
            "entry": position["entry"],
            "exit": float(ltp),
            "gross_pnl": gross,
            "fee": 60.0,
            "net_pnl": gross - 60.0,
            "exit_reason": reason,
        }


class FakeSink:
    def __init__(self):
        self.records = []

    def record(self, summary):
        self.records.append(dict(summary))


def settings():
    return LongOptionRegimeSettings(
        enabled=True,
        capital=500000.0,
        max_quote_age_sec=10.0,
        observation_sec=6.0,
        minimum_samples=4,
        state_confirmations=2,
        reversal_confirmations=2,
        minimum_state_score=0.25,
        fee_buffer_multiple=1.0,
        round_trip_fee=60.0,
        catastrophic_loss_pct=10.0,
        catastrophic_confirmations=3,
        market_start=clock_time(0, 0),
        entry_cutoff=clock_time(23, 59),
        market_end=clock_time(23, 59),
    )


def composite(pressure, *, future_ltp=24350.0, received_ts=None):
    return SimpleNamespace(
        features=SimpleNamespace(pressure_score=pressure),
        full_quote={
            "ltp": future_ltp,
            "received_ts": float(received_ts if received_ts is not None else time.time()),
        },
    )


def evidence(*, ce_change=1.0, pe_change=-1.0, pressure=0.2):
    direction = 1.0 if ce_change > pe_change else -1.0
    return {
        "ce": {
            "ltp": 101.0,
            "change_pct": ce_change,
            "velocity_pct_sec": ce_change / 10.0,
            "acceleration": ce_change / 100.0,
            "range_pct": abs(ce_change),
        },
        "pe": {
            "ltp": 99.0,
            "change_pct": pe_change,
            "velocity_pct_sec": pe_change / 10.0,
            "acceleration": pe_change / 100.0,
            "range_pct": abs(pe_change),
        },
        "directional_pct": ce_change - pe_change,
        "velocity_spread": (ce_change - pe_change) / 10.0,
        "acceleration_spread": (ce_change - pe_change) / 100.0,
        "long_vol_pct": ce_change + pe_change,
        "pressure": pressure,
        "future_ltp": 24350.0,
        "future": {
            "ltp": 24350.0,
            "change_pct": 0.1 * direction,
            "short_change_pct": -0.1 * direction,
            "velocity_pct_sec": 0.01 * direction,
            "range_pct": 0.1,
        },
        "v1_books": {
            "future_long_pct": 0.1 * direction,
            "future_short_pct": -0.1 * direction,
            "long_ce_pct": ce_change,
            "long_pe_pct": pe_change,
            "synthetic_long_pct": ce_change - pe_change,
            "synthetic_short_pct": pe_change - ce_change,
            "long_straddle_pct": ce_change + pe_change,
            "direction_score": 0.8 * direction,
            "bull_support": 4 if direction > 0 else 0,
            "bear_support": 4 if direction < 0 else 0,
            "hybrid_ready": False,
            "hybrid_agreement": True,
        },
        "model_action": "BUY_CE" if pressure >= 0 else "BUY_PE",
        "model_confidence": 0.8,
        "state_score": 0.9,
        "pair_structure": "STRADDLE",
        "signal_metadata": {},
        "expiry_cycle": {},
    }


def publish_pair(
    executor,
    timestamp,
    ce_ltp,
    pe_ltp,
    action,
    pressure,
    *,
    future_ltp=24350.0,
):
    executor.on_quote(
        101, "NIFTY_CE", ce_ltp, bid=ce_ltp - 0.05, ask=ce_ltp,
        received_ts=timestamp,
    )
    executor.on_quote(
        102, "NIFTY_PE", pe_ltp, bid=pe_ltp - 0.05, ask=pe_ltp,
        received_ts=timestamp,
    )
    executor.on_prediction(
        paper_action=action,
        confidence=0.8,
        composite=composite(
            pressure,
            future_ltp=future_ltp,
            received_ts=timestamp,
        ),
        probability_down=0.1,
        probability_flat=0.1,
        probability_up=0.8,
        model_version="test",
        horizon_sec=30,
    )


def test_v1_bullish_state_drives_v2_ce_entry_and_reversal_exit():
    trader = FakePaperTrader()
    sink = FakeSink()
    executor = LongOptionRegimeExecutor(settings(), trader, trade_summary_sink=sink)
    subscriptions = executor.register_contracts(
        {
            "CE": {"security_id": 101, "strike": 24350, "expiry": "2026-08-25"},
            "PE": {"security_id": 102, "strike": 24350, "expiry": "2026-08-25"},
        }
    )
    assert len(subscriptions) == 2
    assert executor.health()["v1_state"] == "UNCERTAIN"

    base = 1_800_000_000.0
    for index in range(8):
        publish_pair(
            executor,
            base + index,
            100.0 + index * 1.5,
            100.0 - index * 1.0,
            "BUY_CE",
            0.20,
            future_ltp=24350.0 + index * 2.0,
        )

    assert executor.health()["v1_state"] == "BULLISH_EXPANSION"
    assert list(trader.positions) == [101]

    for index in range(8, 18):
        publish_pair(
            executor,
            base + index,
            111.0 - (index - 7) * 1.5,
            93.0 + (index - 7) * 2.0,
            "BUY_PE",
            -0.20,
            future_ltp=24364.0 - (index - 7) * 3.0,
        )

    assert list(trader.positions) == [102]
    assert trader.positions[102]["tag"] == "NIFTY_PE"
    assert executor.health()["v1_state"] == "BEARISH_EXPANSION"
    assert len(sink.records) == 1
    assert sink.records[0]["profile"] == "regime_v2"
    assert sink.records[0]["strategy"] == "deeplob_long_option_regime_v2"
    assert sink.records[0]["v1_entry_state"] == "BULLISH_EXPANSION"
    assert sink.records[0]["v1_exit_state"] in {
        "BULLISH_EXHAUSTION",
        "BEARISH_EXPANSION",
    }
    assert sink.records[0]["exit_reason"].startswith("DEEPLOB_V2_EXIT:")


def test_pair_structure_reflects_selected_strikes_without_extra_contracts():
    executor = LongOptionRegimeExecutor(settings(), FakePaperTrader())
    subscriptions = executor.register_contracts(
        {
            "CE": {"security_id": 101, "strike": 24400},
            "PE": {"security_id": 102, "strike": 24300},
        }
    )
    assert len(subscriptions) == 2
    assert executor._pair_structure() == "STRANGLE"


def test_v1_books_use_future_options_synthetic_straddle_and_depth():
    executor = LongOptionRegimeExecutor(settings(), FakePaperTrader())
    executor.register_contracts(
        {
            "CE": {"security_id": 101, "strike": 24350},
            "PE": {"security_id": 102, "strike": 24350},
        }
    )
    base = 1_800_000_000.0
    for index in range(6):
        publish_pair(
            executor,
            base + index,
            100.0 + index,
            100.0 - index * 0.6,
            "BUY_CE",
            0.35,
            future_ltp=24350.0 + index * 1.5,
        )

    books = executor.health()["v1_books"]
    assert books["future_long_pct"] > 0
    assert books["future_short_pct"] < 0
    assert books["long_ce_pct"] > books["long_pe_pct"]
    assert books["synthetic_long_pct"] > books["synthetic_short_pct"]
    assert "long_straddle_pct" in books
    assert books["bull_support"] >= 3
    assert books["direction_score"] > 0
    assert books["executable_books"]["valuation"]["options"] == "EXECUTABLE_BID_ASK"


def test_warm_hybrid_books_veto_fast_direction_when_executable_pnl_disagrees():
    executor = LongOptionRegimeExecutor(settings(), FakePaperTrader())
    snapshot = evidence()
    snapshot["v1_books"].update(
        hybrid_ready=True,
        hybrid_agreement=False,
        fast_direction_score=0.8,
        executable_direction_score=-0.7,
    )

    assert executor._classify(snapshot) not in {
        "BULLISH_EXPANSION",
        "REVERSAL_TO_BULLISH",
    }


def test_warm_hybrid_books_allow_direction_when_both_layers_agree():
    executor = LongOptionRegimeExecutor(settings(), FakePaperTrader())
    snapshot = evidence()
    snapshot["v1_books"].update(
        hybrid_ready=True,
        hybrid_agreement=True,
        fast_direction_score=0.8,
        executable_direction_score=0.6,
    )

    assert executor._classify(snapshot) == "BULLISH_EXPANSION"


def test_volatility_disagreement_waits_and_holds_existing_ce():
    trader = FakePaperTrader()
    executor = LongOptionRegimeExecutor(settings(), trader)
    executor.register_contracts(
        {
            "CE": {"security_id": 101, "strike": 24350},
            "PE": {"security_id": 102, "strike": 24350},
        }
    )
    trader.on_entry(101, "NIFTY_CE", "LONG", 100.0)
    executor.on_quote(
        101,
        "NIFTY_CE",
        101.0,
        bid=100.95,
        ask=101.0,
        received_ts=time.time(),
    )

    for state in ("VOLATILITY_EXPANSION", "VOLATILITY_CONTRACTION", "UNCERTAIN"):
        executor._state = state
        executor._instant_state = state
        executor._manage_open_position(evidence(), state)
        assert list(trader.positions) == [101]


def test_bullish_exhaustion_exits_ce_without_early_pe_entry():
    trader = FakePaperTrader()
    sink = FakeSink()
    executor = LongOptionRegimeExecutor(settings(), trader, trade_summary_sink=sink)
    executor.register_contracts(
        {
            "CE": {"security_id": 101, "strike": 24350},
            "PE": {"security_id": 102, "strike": 24350},
        }
    )
    trader.on_entry(101, "NIFTY_CE", "LONG", 100.0)
    executor.on_quote(
        101,
        "NIFTY_CE",
        101.0,
        bid=100.95,
        ask=101.0,
        received_ts=time.time(),
    )
    executor._state = "BULLISH_EXHAUSTION"
    executor._instant_state = "BULLISH_EXHAUSTION"

    executor._manage_open_position(evidence(), "BULLISH_EXHAUSTION")

    assert trader.positions == {}
    assert sink.records[0]["exit_reason"] == "DEEPLOB_V2_EXIT:BULLISH_EXHAUSTION"
    assert executor.health()["entries"] == 0


def test_state_confirmed_entry_does_not_use_fixed_profit_forecast_gate():
    trader = FakePaperTrader()
    executor = LongOptionRegimeExecutor(settings(), trader)
    executor.register_contracts(
        {
            "CE": {"security_id": 101, "strike": 24350},
            "PE": {"security_id": 102, "strike": 24350},
        }
    )
    now = time.time()
    executor.on_quote(101, "NIFTY_CE", 100.0, bid=99.95, ask=100.0, received_ts=now)
    executor.on_quote(102, "NIFTY_PE", 100.0, bid=99.95, ask=100.0, received_ts=now)

    executor._try_entry("CE", evidence(ce_change=0.01, pe_change=-0.01),)

    assert list(trader.positions) == [101]


def test_executable_spread_alone_does_not_exit_position():
    trader = FakePaperTrader()
    executor = LongOptionRegimeExecutor(settings(), trader)
    executor.register_contracts(
        {
            "CE": {"security_id": 101, "strike": 24350},
            "PE": {"security_id": 102, "strike": 24350},
        }
    )
    trader.on_entry(
        101,
        "NIFTY_CE",
        "LONG",
        100.0,
        metadata={"entry_option_spread": 0.05},
    )

    executor.on_quote(
        101,
        "NIFTY_CE",
        99.0,
        bid=98.0,
        ask=100.0,
        received_ts=time.time(),
    )

    assert list(trader.positions) == [101]
    assert trader.last_trade_summary is None


def test_entry_requires_current_instant_state_to_match_stable_state():
    trader = FakePaperTrader()
    executor = LongOptionRegimeExecutor(settings(), trader)
    executor.register_contracts(
        {
            "CE": {"security_id": 101, "strike": 24350},
            "PE": {"security_id": 102, "strike": 24350},
        }
    )
    now = time.time()
    executor.on_quote(101, "NIFTY_CE", 101.0, bid=100.95, ask=101.0, received_ts=now)
    executor.on_quote(102, "NIFTY_PE", 99.0, bid=98.95, ask=99.0, received_ts=now)
    executor._state = "BULLISH_EXPANSION"
    executor._candidate = "BULLISH_EXPANSION"
    executor._candidate_count = settings().state_confirmations
    executor._derive_evidence = lambda *args, **kwargs: evidence()
    executor._classify = lambda current: "UNCERTAIN"

    executor.on_prediction(
        paper_action="BUY_CE",
        confidence=0.8,
        composite=composite(0.2),
        probability_down=0.1,
        probability_flat=0.1,
        probability_up=0.8,
        model_version="test",
        horizon_sec=30,
    )

    assert executor.health()["v1_state"] == "BULLISH_EXPANSION"
    assert executor.health()["v1_instant_state"] == "UNCERTAIN"
    assert trader.positions == {}


def test_confirmed_opposite_v1_state_exits_without_second_confirmation_loop():
    trader = FakePaperTrader()
    sink = FakeSink()
    executor = LongOptionRegimeExecutor(settings(), trader, trade_summary_sink=sink)
    executor.register_contracts(
        {
            "CE": {"security_id": 101, "strike": 24350},
            "PE": {"security_id": 102, "strike": 24350},
        }
    )
    trader.on_entry(101, "NIFTY_CE", "LONG", 100.0)
    executor.on_quote(
        101,
        "NIFTY_CE",
        101.0,
        bid=100.95,
        ask=101.0,
        received_ts=time.time(),
    )
    executor._state = "BEARISH_EXPANSION"
    executor._instant_state = "BEARISH_EXPANSION"

    executor._manage_open_position(
        evidence(ce_change=-1.0, pe_change=1.0, pressure=-0.2),
        "BEARISH_EXPANSION",
    )

    assert trader.positions == {}
    assert len(sink.records) == 1
    assert sink.records[0]["exit_reason"] == "DEEPLOB_V2_EXIT:V1_REVERSAL_TO_PE"


def test_catastrophic_guard_requires_repeated_aligned_depth_evidence():
    trader = FakePaperTrader()
    executor = LongOptionRegimeExecutor(settings(), trader)
    executor.register_contracts(
        {
            "CE": {"security_id": 101, "strike": 24350},
            "PE": {"security_id": 102, "strike": 24350},
        }
    )
    trader.on_entry(
        101,
        "NIFTY_CE",
        "LONG",
        100.0,
        metadata={"entry_option_spread": 0.05},
    )
    executor.on_quote(
        101,
        "NIFTY_CE",
        89.5,
        bid=89.0,
        ask=90.0,
        received_ts=time.time(),
    )
    position = trader.positions[101]
    adverse = evidence(ce_change=-2.0, pe_change=2.0, pressure=-0.5)

    assert not executor._catastrophic_guard_triggered(
        "CE", position, adverse, "BEARISH_EXPANSION"
    )
    assert not executor._catastrophic_guard_triggered(
        "CE", position, adverse, "BEARISH_EXPANSION"
    )
    assert executor._catastrophic_guard_triggered(
        "CE", position, adverse, "BEARISH_EXPANSION"
    )


def test_expiry_cycle_marks_tuesday_expiry_as_day_five():
    executor = LongOptionRegimeExecutor(settings(), FakePaperTrader())
    executor.register_contracts(
        {
            "CE": {"security_id": 101, "strike": 24350, "expiry": "2026-08-25"},
            "PE": {"security_id": 102, "strike": 24350, "expiry": "2026-08-25"},
        }
    )

    executor._refresh_expiry_cycle(date(2026, 8, 25))

    assert executor.health()["expiry_cycle"]["cycle_label"] == "DAY_5"
    assert executor.health()["expiry_cycle"]["premium_regime"] == "EXPIRY_GAMMA_DECAY"
