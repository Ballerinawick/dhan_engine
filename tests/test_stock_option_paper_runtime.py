import os
import tempfile
import time
from datetime import time as clock_time
from unittest.mock import patch

import pandas as pd
import pytest

from dhan_engine.application.deeplob.long_option_regime import LongOptionRegimeSettings
from dhan_engine.application.stocks.option_paper_runtime import (
    StockOptionPaperSettings,
    StockOptionRegimeExecutor,
    _RootedTradeSummarySink,
)
from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster


class FakeSink:
    def __init__(self):
        self.records = []

    def record(self, summary):
        self.records.append(dict(summary))
        return True


class FakePaperTrader:
    def __init__(self):
        self.LOT_SIZES = {"HDFCBANK": 550, "NIFTY": 550}
        self.positions = {}
        self.last_trade_summary = None

    def has_open_position(self):
        return bool(self.positions)

    def on_tick(self, secid, ltp):
        if int(secid) in self.positions:
            self.positions[int(secid)]["ltp"] = float(ltp)

    def on_entry(self, secid, tag, side, ltp, lots=1, reason="ENTRY", metadata=None):
        self.positions[int(secid)] = {
            "secid": int(secid),
            "tag": tag,
            "side": side,
            "entry": float(ltp),
            "ltp": float(ltp),
            "qty": 550 * int(lots),
            **dict(metadata or {}),
        }
        return True

    def on_exit(self, secid, ltp, reason="EXIT"):
        position = self.positions.pop(int(secid))
        gross = (float(ltp) - position["entry"]) * position["qty"]
        self.last_trade_summary = {
            "secid": int(secid),
            "tag": position["tag"],
            "gross_pnl": gross,
            "fee": 60.0,
            "net_pnl": gross - 60.0,
            "exit_reason": reason,
        }


def regime_settings():
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


def derivative_row(kind, symbol, secid, expiry, lot_size, strike=0, option_type=""):
    return {
        "SEM_EXM_EXCH_ID": "NSE",
        "SEM_SEGMENT": "D",
        "SEM_SMST_SECURITY_ID": str(secid),
        "SEM_INSTRUMENT_NAME": kind,
        "SEM_TRADING_SYMBOL": symbol,
        "SEM_CUSTOM_SYMBOL": symbol,
        "SEM_OPTION_TYPE": option_type,
        "SEM_STRIKE_PRICE": str(strike),
        "SEM_LOT_UNITS": str(lot_size),
        "SEM_EXPIRY_DATE": expiry,
    }


def test_stock_master_resolves_exact_future_and_same_strike_option_pair():
    today = pd.Timestamp.now().normalize()
    near = today + pd.Timedelta(days=10)
    far = today + pd.Timedelta(days=40)
    rows = [
        derivative_row("FUTSTK", "HDFCBANK-NEAR-FUT", 101, near, 550),
        derivative_row("FUTSTK", "HDFCBANK-FAR-FUT", 102, far, 550),
        derivative_row("FUTSTK", "HDFCBANKPP-NEAR-FUT", 999, near, 1),
    ]
    for strike, ce_id, pe_id in ((980, 201, 202), (1000, 203, 204), (1020, 205, 206)):
        rows.extend(
            [
                derivative_row(
                    "OPTSTK", f"HDFCBANK-NEAR-{strike}-CE", ce_id, near, 550,
                    strike, "CE",
                ),
                derivative_row(
                    "OPTSTK", f"HDFCBANK-NEAR-{strike}-PE", pe_id, near, 550,
                    strike, "PE",
                ),
            ]
        )
    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "master.csv")
        pd.DataFrame(rows).to_csv(path, index=False)
        master = InstrumentMaster(path, debug=False)
        future = master.get_nearest_stock_future("HDFCBANK")
        pair = master.get_nearest_stock_option_pair(
            "HDFCBANK", 1007.0, expiry=near
        )

    assert future["security_id"] == 101
    assert future["symbol"] == "HDFCBANK-NEAR-FUT"
    assert pair["CE"]["security_id"] == 203
    assert pair["PE"]["security_id"] == 204
    assert pair["CE"]["strike"] == pair["PE"]["strike"] == 1000.0
    assert pair["CE"]["lot_size"] == 550


def test_stock_executor_retags_contracts_and_summary_without_nifty_partition():
    sink = FakeSink()
    trader = FakePaperTrader()
    executor = StockOptionRegimeExecutor(
        "HDFCBANK", regime_settings(), trader, trade_summary_sink=sink
    )
    subscriptions = executor.register_contracts(
        {
            "CE": {
                "security_id": 201,
                "strike": 1000,
                "expiry": "2026-09-29",
                "lot_size": 550,
            },
            "PE": {
                "security_id": 202,
                "strike": 1000,
                "expiry": "2026-09-29",
                "lot_size": 550,
            },
        }
    )
    assert {item["tag"] for item in subscriptions} == {
        "HDFCBANK_CE",
        "HDFCBANK_PE",
    }
    assert executor.contracts["CE"]["tag"] == "HDFCBANK_CE"
    assert executor.health()["underlying"] == "HDFCBANK"

    rooted = _RootedTradeSummarySink("HDFCBANK", sink)
    assert rooted.record(
        {
            "index": "NIFTY",
            "runtime": "deeplob_live_regime_v2",
            "profile": "regime_v2",
            "tag": "HDFCBANK_CE",
        }
    )
    assert sink.records[-1]["index"] == "HDFCBANK"
    assert sink.records[-1]["runtime"] == "stock_option_paper_regime_v2"
    assert sink.records[-1]["profile"] == "stock_option_regime_v2"


def test_stock_settings_are_opt_in_and_reject_unapproved_symbols():
    environment = {
        "DHAN_CLIENT_ID": "client",
        "DHAN_ACCESS_TOKEN": "token",
        "DEEPLOB_S3_BUCKET": "bucket",
        "STOCK_OPTION_SYMBOLS": "HDFCBANK,TCS",
    }
    with patch.dict(os.environ, environment, clear=True):
        with pytest.raises(ValueError, match="restricted to HDFCBANK and RELIANCE"):
            StockOptionPaperSettings.from_env()


def test_nifty_executor_defaults_remain_unchanged():
    from dhan_engine.application.deeplob.long_option_regime import (
        LongOptionRegimeExecutor,
    )

    executor = LongOptionRegimeExecutor(regime_settings(), FakePaperTrader())
    subscriptions = executor.register_contracts(
        {
            "CE": {"security_id": 301, "strike": 24500},
            "PE": {"security_id": 302, "strike": 24500},
        }
    )
    assert executor.profile == "regime_v2"
    assert executor.strategy == "deeplob_long_option_regime_v2"
    assert {item["tag"] for item in subscriptions} == {"NIFTY_CE", "NIFTY_PE"}


def test_stock_executor_records_executable_best_observed_pnl():
    sink = FakeSink()
    trader = FakePaperTrader()
    executor = StockOptionRegimeExecutor(
        "HDFCBANK", regime_settings(), trader, trade_summary_sink=sink
    )
    executor.register_contracts(
        {
            "CE": {"security_id": 201, "strike": 1000},
            "PE": {"security_id": 202, "strike": 1000},
        }
    )
    trader.on_entry(201, "HDFCBANK_CE", "LONG", 100.0)
    executor.on_quote(
        201,
        "HDFCBANK_CE",
        102.0,
        bid=102.0,
        ask=102.1,
        received_ts=time.time(),
    )

    executor._exit("DEEPLOB_V2_EXIT:MARKET_CLOSE")

    assert sink.records[-1]["index"] == "HDFCBANK"
    assert sink.records[-1]["BestObservedPnl"] == 1100.0
    assert sink.records[-1]["CapturedGrossPnl"] == 1100.0
    assert sink.records[-1]["mfe_capture_ratio"] == 1.0
