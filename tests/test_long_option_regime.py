from datetime import time as clock_time
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
        maximum_loss_pct=10.0,
        market_start=clock_time(0, 0),
        entry_cutoff=clock_time(23, 59),
        market_end=clock_time(23, 59),
    )


def composite(pressure):
    return SimpleNamespace(
        features=SimpleNamespace(pressure_score=pressure),
        full_quote={"ltp": 24350.0},
    )


def publish_pair(executor, timestamp, ce_ltp, pe_ltp, action, pressure):
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
        composite=composite(pressure),
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
