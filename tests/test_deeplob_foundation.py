
import json
import io
import tempfile
import time
import unittest
import os
from datetime import time as market_time
from types import SimpleNamespace
from unittest.mock import patch

from dhan_engine.analytics.deeplob_recorder import (
    DepthInstrument,
    DepthRecorderSettings,
    ParquetDepthRecorder,
)
from dhan_engine.application.deeplob.live_runtime import DeepLobLiveSettings, DeepLobLiveRuntime
from dhan_engine.application.deeplob.liquidity_pulse_scalp import (
    LiquidityPulseScalpRuntime,
)
from dhan_engine.application.deeplob.option_paper_executor import (
    DeepLobOptionPaperExecutor,
    DeepLobOptionPaperSettings,
    ParallelDeepLobOptionPaperExecutor,
)
from dhan_engine.application.deeplob.trade_summary_s3 import (
    TradeSummaryS3Settings,
    TradeSummaryS3Sink,
)
from dhan_engine.application.deeplob.recorder_runtime import (
    DeepLobRecorderRuntime,
    DeepLobRecorderRuntimeSettings,
)
from dhan_engine.application.deeplob.post_market_analysis import (
    PostMarketAnalysisRuntime,
    PostMarketAnalysisSettings,
)
from dhan_engine.application.deeplob.premodel_paper_runtime import (
    MarketByPricePaperRuntime,
    MarketByPricePaperSettings,
)
from dhan_engine.domain.market.deeplob_model import encode_book, paper_option_action
from dhan_engine.domain.market.expiry_cycle import expiry_cycle_context
from dhan_engine.domain.market.full_depth_microstructure import BookSnapshot
from dhan_engine.domain.market.market_by_price_execution import (
    derive_market_by_price_features,
    estimate_market_execution,
    validate_composite_snapshot,
)
from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster


def snapshot(name="NIFTY_FUT"):
    return BookSnapshot.build(
        123,
        name,
        [(100.0, 10, 2), (99.5, 20, 3)],
        [(100.5, 12, 2), (101.0, 18, 4)],
        received_ts=1_700_000_000.0,
        received_mono=10.0,
    )


class DeepLobFoundationTest(unittest.TestCase):
    def test_nifty_resolution_excludes_similarly_named_index(self):
        import pandas as pd

        future_expiry = (pd.Timestamp.now() + pd.Timedelta(days=30)).date().isoformat()
        option_expiry = (pd.Timestamp.now() + pd.Timedelta(days=7)).date().isoformat()
        rows = []
        for symbol, secid, expiry, instrument in (
            ("NIFTYFPI-Test-FUT", "35937", future_expiry, "FUTIDX"),
            ("NIFTY-Test-FUT", "61093", future_expiry, "FUTIDX"),
            ("NIFTYFPI-Test-24000-CE", "1", option_expiry, "OPTIDX"),
            ("NIFTY-Test-24000-CE", "2", option_expiry, "OPTIDX"),
        ):
            rows.append(
                {
                    "SEM_EXM_EXCH_ID": "NSE",
                    "SEM_SEGMENT": "D",
                    "SEM_SMST_SECURITY_ID": secid,
                    "SEM_INSTRUMENT_NAME": instrument,
                    "SEM_TRADING_SYMBOL": symbol,
                    "SEM_CUSTOM_SYMBOL": symbol,
                    "SEM_OPTION_TYPE": "CE" if instrument == "OPTIDX" else "NA",
                    "SEM_STRIKE_PRICE": "24000",
                    "SEM_LOT_UNITS": "65",
                    "SEM_EXPIRY_DATE": expiry,
                }
            )
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "master.csv")
            pd.DataFrame(rows).to_csv(path, index=False)
            master = InstrumentMaster(path, debug=False)
            future = master.get_nearest_future("NIFTY")
            options = master._get_optidx_df("NIFTY")
        self.assertEqual(future["symbol"], "NIFTY-Test-FUT")
        self.assertEqual(options["SEM_TRADING_SYMBOL"].tolist(), ["NIFTY-Test-24000-CE"])

    def test_encoder_has_fixed_shape_and_finite_values(self):
        values = encode_book(snapshot(), levels=4)
        self.assertEqual(len(values), 4 * 3 * 2)
        self.assertTrue(all(value == value for value in values))
        self.assertLess(values[12], 0.0)

    def test_recorder_callback_does_not_block_when_queue_is_full(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = ParquetDepthRecorder(
                DepthRecorderSettings(
                    output_dir=directory,
                    sample_interval_ms=0,
                    queue_size=128,
                    rows_per_file=10_000,
                )
            )
            started = time.perf_counter()
            for _ in range(1000):
                recorder.record("NIFTY_FUT", snapshot())
            self.assertLess(time.perf_counter() - started, 1.0)
            self.assertGreater(recorder._dropped, 0)

    def test_direction_maps_only_to_paper_option_action_above_threshold(self):
        self.assertEqual(paper_option_action("UP", 0.72, 0.65), "BUY_CE")
        self.assertEqual(paper_option_action("DOWN", 0.72, 0.65), "BUY_PE")
        self.assertEqual(paper_option_action("UP", 0.60, 0.65), "NO_TRADE")
        self.assertEqual(paper_option_action("FLAT", 0.90, 0.65), "NO_TRADE")

    def test_recorder_is_restricted_to_nifty(self):
        environment = {
            "DHAN_CLIENT_ID": "client",
            "DHAN_ACCESS_TOKEN": "token",
            "DEEPLOB_INDEXES": "NIFTY",
        }
        with patch.dict("os.environ", environment, clear=True):
            self.assertEqual(DeepLobRecorderRuntimeSettings.from_env().indexes, ("NIFTY",))
        environment["DEEPLOB_INDEXES"] = "NIFTY,BANKNIFTY"
        with patch.dict("os.environ", environment, clear=True):
            with self.assertRaisesRegex(ValueError, "restricted"):
                DeepLobRecorderRuntimeSettings.from_env()

    def test_recorder_runtime_persists_only_synchronized_fullquote_and_depth(self):
        class Recorder:
            def __init__(self):
                self.calls = []

            def record(self, *args, **kwargs):
                self.calls.append((args, kwargs))

        recorder = Recorder()
        runtime = DeepLobRecorderRuntime(None, None, None, None, recorder)
        runtime.instruments["NIFTY_FUT"] = {
            "index": "NIFTY",
            "symbol": "NIFTY-Jul2026-FUT",
            "expiry": "2026-07-30",
        }
        book = snapshot()
        runtime.on_book("NIFTY_FUT", book)
        self.assertEqual(runtime._sync_rejections, 1)
        self.assertEqual(recorder.calls, [])

        runtime._latest_fullquote[book.security_id] = {
            "ltp": 100.25,
            "received_ts": book.received_ts,
            "received_ns": int(book.received_ts * 1_000_000_000),
        }
        runtime.on_book("NIFTY_FUT", book)
        self.assertEqual(len(recorder.calls), 1)
        self.assertEqual(recorder.calls[0][1]["full_quote"]["ltp"], 100.25)
        self.assertEqual(recorder.calls[0][1]["full_quote"]["age_ms"], 0.0)

        runtime._latest_fullquote[book.security_id]["received_ts"] = book.received_ts + 2.0
        runtime.on_book("NIFTY_FUT", book)
        self.assertEqual(runtime._sync_rejections, 2)
        self.assertEqual(len(recorder.calls), 1)

    def test_recorder_sampling_retains_one_snapshot_per_interval(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = ParquetDepthRecorder(
                DepthRecorderSettings(output_dir=directory, sample_interval_ms=250)
            )
            recorder.record("NIFTY_FUT", snapshot())
            recorder.record("NIFTY_FUT", snapshot())
            self.assertEqual(recorder._queue.qsize(), 1)
            self.assertEqual(recorder._sampled_out, 1)

    def test_recorder_drops_empty_book_before_worker_encoding(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = ParquetDepthRecorder(
                DepthRecorderSettings(output_dir=directory, sample_interval_ms=0)
            )
            empty = BookSnapshot.build(
                123,
                "NIFTY_FUT",
                [],
                [],
                received_ts=1_700_000_000.0,
                received_mono=10.0,
            )
            recorder.record("NIFTY_FUT", empty)
            self.assertEqual(recorder._invalid_books, 1)
            self.assertEqual(recorder._queue.qsize(), 0)
            self.assertEqual(recorder._failures, 0)

    def test_recorder_row_contains_training_partitions_and_full_depth_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = ParquetDepthRecorder(
                DepthRecorderSettings(output_dir=directory, levels=4)
            )
            row = recorder._to_row(
                snapshot(),
                DepthInstrument("NIFTY", "NIFTY-Jul2026-FUT", "2026-07-30"),
                {
                    "ltp": 100.25,
                    "volume": 1234,
                    "oi": 5678,
                    "received_ns": 1_700_000_000_100_000_000,
                    "age_ms": 25.0,
                },
            )
            self.assertEqual(row["schema_version"], 1)
            self.assertEqual(row["index"], "NIFTY")
            self.assertEqual(row["symbol"], "NIFTY-Jul2026-FUT")
            self.assertEqual(row["expiry"], "2026-07-30")
            self.assertEqual(len(row["bid_price"]), 4)
            self.assertEqual(len(row["ask_price"]), 4)
            self.assertEqual(row["received_ns"], 1_700_000_000_000_000_000)
            self.assertEqual(row["ltp"], 100.25)
            self.assertEqual(row["volume"], 1234)
            self.assertEqual(row["oi"], 5678)
            self.assertEqual(row["fullquote_age_ms"], 25.0)
            self.assertTrue(row["fullquote_synchronized"])
            unsynchronized = recorder._to_row(
                snapshot(),
                DepthInstrument("NIFTY", "NIFTY-Jul2026-FUT", "2026-07-30"),
                None,
            )
            self.assertFalse(unsynchronized["fullquote_synchronized"])

    def test_recorder_can_store_every_book_while_inference_keeps_250ms_sampling(self):
        environment = {
            "DEEPLOB_RECORD_SAMPLE_INTERVAL_MS": "0",
            "DEEPLOB_SAMPLE_INTERVAL_MS": "250",
        }
        with patch.dict("os.environ", environment, clear=True):
            self.assertEqual(DepthRecorderSettings.from_env().sample_interval_ms, 0)

    def test_live_settings_require_s3_bucket(self):
        environment = {
            "DHAN_CLIENT_ID": "client",
            "DHAN_ACCESS_TOKEN": "token",
            "DEEPLOB_INDEXES": "NIFTY",
            "DEEPLOB_MODEL_PATH": "model.pt",
            "DEEPLOB_METADATA_PATH": "model.json",
        }
        with patch.dict("os.environ", environment, clear=True):
            with self.assertRaisesRegex(RuntimeError, "DEEPLOB_S3_BUCKET"):
                DeepLobLiveSettings.from_env()

    def test_live_settings_select_premodel_paper_service(self):
        environment = {
            "DHAN_CLIENT_ID": "client",
            "DHAN_ACCESS_TOKEN": "token",
            "DHAN_SERVICE": "deeplob-paper",
            "DEEPLOB_INDEXES": "NIFTY",
            "DEEPLOB_S3_BUCKET": "market-data",
        }
        with patch.dict("os.environ", environment, clear=True):
            settings = DeepLobLiveSettings.from_env()
        self.assertTrue(settings.premodel_paper)

    def test_premodel_worker_emits_causal_paper_signal(self):
        predictions = []
        settings = MarketByPricePaperSettings(
            sample_interval_ms=50,
            signal_interval_sec=0,
            sequence_length=4,
            signal_threshold=0.05,
            stale_after_sec=1.5,
            queue_size=8,
            horizon_sec=600,
        )
        runtime = MarketByPricePaperRuntime(
            settings,
            prediction_sink=lambda **payload: predictions.append(payload),
        )
        runtime.start_worker()
        now = time.monotonic()
        features = SimpleNamespace(
            pressure_score=0.30,
            weighted_imbalance_20=0.30,
            imbalance_20=0.25,
            microprice_bps=0.50,
        )
        composite = SimpleNamespace(features=features, quote_age_ms=10.0)
        for index in range(4):
            book = BookSnapshot.build(
                123,
                "NIFTY_FUT",
                [(100.0, 30, 5)],
                [(100.5, 5, 1)],
                received_ts=time.time(),
                received_mono=now + index * 0.06,
            )
            runtime.on_book("NIFTY_FUT", book, composite)
        deadline = time.time() + 1.0
        while not predictions and time.time() < deadline:
            time.sleep(0.01)
        runtime.close_worker()
        self.assertEqual(predictions[0]["paper_action"], "BUY_CE")
        self.assertEqual(predictions[0]["model_version"], "MBP_PREMODEL_V1")

    def test_live_callback_fans_out_to_recorder_and_inference(self):
        class Sink:
            def __init__(self):
                self.calls = []

            def record(self, *args, **kwargs):
                self.calls.append((args, kwargs))

            def on_book(self, *args):
                self.calls.append(args)

        recorder = Sink()
        inference = Sink()
        runtime = DeepLobLiveRuntime(None, None, None, None, recorder, inference)
        runtime._max_spread_bps = 100
        runtime.instrument_metadata["NIFTY_FUT"] = {
            "index": "NIFTY",
            "symbol": "NIFTY-Jul2026-FUT",
            "expiry": "2026-07-30",
        }
        book = snapshot()
        runtime._latest_fullquote[book.security_id] = {
            "ltp": 100.25,
            "received_ts": book.received_ts,
            "received_ns": int(book.received_ts * 1_000_000_000),
        }
        runtime.on_book("NIFTY_FUT", book)
        self.assertEqual(runtime._received, 1)
        self.assertEqual(recorder.calls[0][0], ("NIFTY_FUT", book))
        self.assertEqual(inference.calls[0][0:2], ("NIFTY_FUT", book))
        self.assertEqual(inference.calls[0][2].full_quote["ltp"], 100.25)

    def test_live_callback_records_depth_when_fullquote_is_missing(self):
        class Sink:
            def __init__(self):
                self.calls = []

            def record(self, *args, **kwargs):
                self.calls.append((args, kwargs))

            def on_book(self, *args):
                self.calls.append(args)

        recorder = Sink()
        inference = Sink()
        runtime = DeepLobLiveRuntime(None, None, None, None, recorder, inference)
        runtime._max_spread_bps = 100
        runtime.instrument_metadata["NIFTY_FUT"] = {
            "index": "NIFTY",
            "symbol": "NIFTY-Jul2026-FUT",
            "expiry": "2026-07-30",
        }

        runtime.on_book("NIFTY_FUT", snapshot())

        self.assertEqual(len(recorder.calls), 1)
        self.assertIsNone(recorder.calls[0][1]["full_quote"])
        self.assertEqual(inference.calls, [])
        self.assertEqual(runtime._quality_rejections["FULLQUOTE_MISSING"], 1)

    def test_option_selection_failure_retries_without_stopping_recorder(self):
        class Selector:
            def __init__(self):
                self.calls = 0

            def select_best(self, _index):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("temporary option-chain failure")
                return {
                    "CE": {"security_id": 101, "tag": "NIFTY_CE"},
                    "PE": {"security_id": 102, "tag": "NIFTY_PE"},
                }

        class Paper:
            def __init__(self):
                self.contracts = {}

            def register_contracts(self, selection):
                self.contracts = dict(selection)
                return [
                    {
                        "ExchangeSegment": "NSE_FNO",
                        "SecurityId": str(selection["CE"]["security_id"]),
                        "tag": selection["CE"]["tag"],
                    },
                    {
                        "ExchangeSegment": "NSE_FNO",
                        "SecurityId": str(selection["PE"]["security_id"]),
                        "tag": selection["PE"]["tag"],
                    },
                ]

        class Feed:
            def __init__(self):
                self.subscriptions = []

            def subscribe_full(self, items):
                self.subscriptions.append(list(items))

            def replace_subscriptions(self, items, **_kwargs):
                self.subscriptions.append(list(items))
                return True

            def refresh_full_subscriptions(self, **_kwargs):
                return True

        selector = Selector()
        paper = Paper()
        feed = Feed()
        runtime = DeepLobLiveRuntime(
            None,
            None,
            None,
            feed,
            None,
            None,
            option_paper=paper,
            option_selector=selector,
        )

        self.assertFalse(runtime._ensure_option_contracts(force=True))
        self.assertEqual(feed.subscriptions, [])
        self.assertTrue(runtime._ensure_option_contracts(force=True))
        self.assertEqual(
            feed.subscriptions,
            [[
                {"ExchangeSegment": "NSE_FNO", "SecurityId": "101", "tag": "NIFTY_CE"},
                {"ExchangeSegment": "NSE_FNO", "SecurityId": "102", "tag": "NIFTY_PE"},
            ]],
        )
        self.assertEqual(runtime._option_selection_attempts, 2)
        self.assertEqual(runtime._option_selection_failures, 1)

    def test_composite_rejects_missing_or_stale_fullquote(self):
        book = snapshot()
        valid, reason, _ = validate_composite_snapshot(
            book, None, max_quote_age_ms=1500, max_spread_bps=100
        )
        self.assertFalse(valid)
        self.assertEqual(reason, "FULLQUOTE_MISSING")
        valid, reason, age = validate_composite_snapshot(
            book,
            {
                "ltp": 100.25,
                "received_ns": int((book.received_ts - 2.0) * 1_000_000_000),
            },
            max_quote_age_ms=1500,
            max_spread_bps=100,
        )
        self.assertFalse(valid)
        self.assertEqual(reason, "FULLQUOTE_STALE")
        self.assertGreaterEqual(age, 1999)

    def test_execution_estimate_walks_visible_ask_levels(self):
        book = snapshot()
        estimate = estimate_market_execution(
            book.asks, side="BUY", quantity=20, reference_price=100.25
        )
        self.assertEqual(estimate.filled_qty, 20)
        self.assertAlmostEqual(estimate.average_price, 100.7)
        self.assertGreater(estimate.slippage_bps, 0)

    def test_pressure_features_are_causal_and_bounded(self):
        old = snapshot()
        current = BookSnapshot.build(
            123,
            "NIFTY_FUT",
            [(100.0, 25, 4), (99.5, 20, 3)],
            [(100.5, 6, 1), (101.0, 18, 4)],
            received_ts=old.received_ts + 0.25,
            received_mono=old.received_mono + 0.25,
        )
        features = derive_market_by_price_features(current, old, probe_quantity=20)
        self.assertGreater(features.pressure_score, 0)
        self.assertLessEqual(abs(features.pressure_score), 1.0)

    def test_option_paper_entry_uses_ask_and_exit_uses_bid(self):
        class Paper:
            def __init__(self):
                self.positions = {}
                self.entries = []
                self.exits = []

            def has_open_position(self):
                return bool(self.positions)

            def on_entry(self, secid, tag, side, price, **kwargs):
                self.entries.append((secid, tag, price, kwargs))
                self.positions[secid] = {
                    "tag": tag,
                    "entry": price,
                    "entry_ts": time.time(),
                }
                return True

            def on_tick(self, secid, ltp):
                if secid in self.positions:
                    self.positions[secid]["ltp"] = ltp

            def on_exit(self, secid, price, reason):
                self.exits.append((secid, price, reason))
                self.positions.pop(secid)

        settings = DeepLobOptionPaperSettings(
            enabled=True,
            capital=500000,
            confidence_threshold=0.65,
            pressure_threshold=0.05,
            confirmation_count=2,
            entry_cooldown_sec=0,
            max_quote_age_sec=2,
            take_profit_pct=2,
            stop_loss_pct=1.5,
            max_hold_sec=600,
            enforce_market_hours=False,
            market_start=market_time(9, 15),
            entry_cutoff=market_time(15, 25),
            market_end=market_time(15, 30),
        )
        paper = Paper()
        executor = DeepLobOptionPaperExecutor(settings, paper)
        executor.register_contracts(
            {
                "CE": {
                    "security_id": 201,
                    "strike": 24100,
                    "expiry": "2026-07-28",
                },
                "PE": {
                    "security_id": 202,
                    "strike": 24100,
                    "expiry": "2026-07-28",
                },
            }
        )
        executor.on_quote(
            201,
            "NIFTY_CE",
            100.5,
            bid=100.0,
            ask=101.0,
            received_ts=time.time(),
        )
        old = snapshot()
        current = BookSnapshot.build(
            123,
            "NIFTY_FUT",
            [(100.0, 30, 5), (99.5, 20, 3)],
            [(100.5, 5, 1), (101.0, 18, 4)],
            received_ts=old.received_ts + 0.25,
            received_mono=old.received_mono + 0.25,
        )
        features = derive_market_by_price_features(current, old)
        composite = type(
            "Composite",
            (),
            {
                "features": features,
                "full_quote": {"ltp": 100.25},
            },
        )()
        prediction = {
            "paper_action": "BUY_CE",
            "confidence": 0.8,
            "composite": composite,
            "probability_down": 0.1,
            "probability_flat": 0.1,
            "probability_up": 0.8,
            "model_version": "test",
            "horizon_sec": 600,
        }
        executor.on_prediction(**prediction)
        executor.on_prediction(**prediction)
        self.assertEqual(paper.entries[0][2], 101.0)
        executor.on_quote(
            201,
            "NIFTY_CE",
            103.2,
            bid=103.1,
            ask=103.3,
            received_ts=time.time(),
        )
        self.assertEqual(paper.exits[0][1], 103.1)
        self.assertEqual(paper.exits[0][2], "DEEPLOB_MBP_EXIT:TAKE_PROFIT")

    def test_option_paper_blocks_stale_option_quote(self):
        class Paper:
            positions = {}

            @staticmethod
            def has_open_position():
                return False

            @staticmethod
            def on_entry(*args, **kwargs):
                raise AssertionError("stale quote must not enter")

            @staticmethod
            def on_tick(*args, **kwargs):
                return None

        settings = DeepLobOptionPaperSettings(
            enabled=True,
            capital=500000,
            confidence_threshold=0.65,
            pressure_threshold=0,
            confirmation_count=1,
            entry_cooldown_sec=0,
            max_quote_age_sec=1,
            take_profit_pct=2,
            stop_loss_pct=1.5,
            max_hold_sec=600,
            enforce_market_hours=False,
            market_start=market_time(9, 15),
            entry_cutoff=market_time(15, 25),
            market_end=market_time(15, 30),
        )
        executor = DeepLobOptionPaperExecutor(settings, Paper())
        executor.register_contracts({"CE": {"security_id": 201}})
        executor.on_quote(
            201,
            "NIFTY_CE",
            100,
            bid=99.5,
            ask=100.5,
            received_ts=time.time() - 2,
        )
        composite = type(
            "Composite",
            (),
            {
                "features": type("Features", (), {"pressure_score": 0.5, "mid": 100})(),
                "full_quote": {"ltp": 100},
            },
        )()
        executor.on_prediction(
            paper_action="BUY_CE",
            confidence=0.8,
            composite=composite,
            probability_down=0.1,
            probability_flat=0.1,
            probability_up=0.8,
            model_version="test",
            horizon_sec=600,
        )
        self.assertEqual(executor.health()["blocks"], 1)

    def test_parallel_option_paper_profiles_are_isolated(self):
        class Executor:
            def __init__(self, profile):
                self.profile = profile
                self.contracts = {}
                self.predictions = 0
                self.quotes = 0

            def register_contracts(self, selection):
                self.contracts = dict(selection)
                return [
                    {
                        "ExchangeSegment": "NSE_FNO",
                        "SecurityId": "201",
                        "tag": "NIFTY_CE",
                    }
                ]

            def on_prediction(self, **kwargs):
                self.predictions += 1

            def on_quote(self, *args, **kwargs):
                self.quotes += 1

            def heartbeat(self):
                return None

            def health(self):
                return {"predictions": self.predictions, "quotes": self.quotes}

        dynamic = Executor("dynamic")
        scalp = Executor("scalp")
        parallel = ParallelDeepLobOptionPaperExecutor([dynamic, scalp])
        subscriptions = parallel.register_contracts({"CE": {"security_id": 201}})
        parallel.on_prediction(paper_action="BUY_CE")
        parallel.on_quote(201, "NIFTY_CE", 100, bid=99, ask=101, received_ts=time.time())

        self.assertEqual(len(subscriptions), 1)
        self.assertEqual(dynamic.predictions, 1)
        self.assertEqual(scalp.predictions, 1)
        self.assertEqual(dynamic.quotes, 1)
        self.assertEqual(scalp.quotes, 1)
        self.assertIn("dynamic", parallel.health()["profiles"])
        self.assertIn("scalp", parallel.health()["profiles"])

    def test_scalp_settings_use_independent_environment_prefix(self):
        environment = {
            "DEEPLOB_OPTION_PAPER_CONFIDENCE": "0.81",
            "DEEPLOB_SCALP_PAPER_CONFIDENCE": "0.59",
            "DEEPLOB_SCALP_PAPER_MAX_HOLD_SEC": "75",
        }
        with patch.dict("os.environ", environment, clear=True):
            dynamic = DeepLobOptionPaperSettings.from_env()
            scalp = DeepLobOptionPaperSettings.from_env(
                "DEEPLOB_SCALP_PAPER",
                defaults={"CONFIDENCE": "0.60", "MAX_HOLD_SEC": "120"},
            )
        self.assertEqual(dynamic.confidence_threshold, 0.81)
        self.assertEqual(scalp.confidence_threshold, 0.59)
        self.assertEqual(scalp.max_hold_sec, 75)

    def test_expiry_cycle_maps_wednesday_through_tuesday(self):
        self.assertEqual(
            expiry_cycle_context("2026-08-05", "2026-08-11").cycle_label,
            "DAY_1",
        )
        self.assertEqual(
            expiry_cycle_context("2026-08-11", "2026-08-11").premium_regime,
            "EXPIRY_GAMMA_DECAY",
        )

    def test_liquidity_pulse_uses_depth_change_and_velocity_evidence(self):
        features = SimpleNamespace(
            microprice_bps=1.0,
            ask_depletion=0.6,
            bid_depletion=0.1,
            bid_replenishment=0.5,
            ask_replenishment=0.1,
            imbalance_50=0.4,
            weighted_imbalance_20=0.5,
            imbalance_5=0.4,
            ofi_top=0.3,
        )
        score = LiquidityPulseScalpRuntime._pulse(
            SimpleNamespace(features=features)
        )
        self.assertGreater(score, 0.25)

    def test_adaptive_scalp_settings_are_fee_aware_and_isolated(self):
        with patch.dict(
            "os.environ",
            {
                "DEEPLOB_SCALP_ENABLED": "1",
                "DEEPLOB_SCALP_ROUND_TRIP_FEE": "60",
                "DEEPLOB_SCALP_MIN_COST_MULTIPLE": "2",
            },
            clear=True,
        ):
            settings = DeepLobOptionPaperSettings.scalp_from_env()
        self.assertEqual(settings.profile, "scalp")
        self.assertTrue(settings.edge_decay_exit)
        self.assertEqual(settings.round_trip_fee, 60)
        self.assertEqual(settings.minimum_cost_multiple, 2)

    def test_trade_summary_s3_sink_consolidates_one_daily_json(self):
        class S3:
            def __init__(self):
                self.calls = []

            def put_object(self, **kwargs):
                self.calls.append(kwargs)

        client = S3()
        sink = TradeSummaryS3Sink(
            TradeSummaryS3Settings(
                bucket="market-data",
                prefix="paper-trades/deeplob",
                queue_size=16,
            ),
            s3_client=client,
        )
        sink.start()
        self.assertTrue(
            sink.record(
                {
                    "secid": 201,
                    "tag": "NIFTY_CE",
                    "index": "NIFTY",
                    "entry": 100.0,
                    "exit": 103.0,
                    "net_pnl": 135.0,
                    "exit_ts": 1785224400.0,
                    "strategy": "deeplob_mbp_option_paper_v1",
                }
            )
        )
        self.assertTrue(
            sink.record(
                {
                    "secid": 202,
                    "tag": "NIFTY_PE",
                    "index": "NIFTY",
                    "profile": "scalp",
                    "entry": 90.0,
                    "exit": 91.0,
                    "gross_pnl": 65.0,
                    "fee": 60.0,
                    "net_pnl": 5.0,
                    "exit_ts": 1785224460.0,
                    "strategy": "deeplob_mbp_liquidity_pulse_scalp_v1",
                }
            )
        )
        sink.close()
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(client.calls[0]["Key"], client.calls[1]["Key"])
        uploaded = client.calls[-1]
        self.assertEqual(uploaded["Bucket"], "market-data")
        self.assertIn("trade_date=2026-07-28", uploaded["Key"])
        self.assertTrue(uploaded["Key"].endswith("daily-trades.json"))
        ledger = json.loads(uploaded["Body"])
        self.assertEqual(ledger["summary"]["trade_count"], 2)
        self.assertEqual(len(ledger["trades"]), 2)
        self.assertEqual(ledger["summary"]["by_profile"]["scalp"]["trades"], 1)

    def test_trade_summary_prefix_must_be_separate_from_market_data(self):
        environment = {
            "DEEPLOB_S3_PREFIX": "market-data/deeplob",
            "DEEPLOB_TRADE_SUMMARY_S3_PREFIX": "market-data/deeplob/trades",
        }
        with patch.dict("os.environ", environment, clear=True):
            with self.assertRaisesRegex(ValueError, "must be separate"):
                TradeSummaryS3Settings.from_env()

    def test_post_market_report_reads_canonical_s3_parquet(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            self.skipTest("pyarrow is installed in the DeepLOB deployment image")

        buffer = io.BytesIO()
        pq.write_table(
            pa.table(
                {
                    "received_ns": [1_000_000_000, 11_000_000_000],
                    "ltp": [24000.0, 24024.0],
                    "mid_price": [24000.0, 24024.0],
                    "bid_qty": [[10] * 20, [12] * 20],
                    "ask_qty": [[8] * 20, [9] * 20],
                    "symbol": ["NIFTY-Aug2026-FUT"] * 2,
                    "security_id": [61093] * 2,
                }
            ),
            buffer,
        )
        rejected_buffer = io.BytesIO()
        pq.write_table(
            pa.table(
                {
                    "received_ns": [5_000_000_000],
                    "ltp": [1600.0],
                    "mid_price": [1600.0],
                    "bid_qty": [[100] * 20],
                    "ask_qty": [[100] * 20],
                    "symbol": ["NIFTYFPI-Aug2026-FUT"],
                    "security_id": [35937],
                }
            ),
            rejected_buffer,
        )
        parquet_key = (
            "market-data/deeplob/schema=v1/index=NIFTY/expiry=2026-08-25/"
            "trade_date=2026-08-11/instrument=NIFTY_FUT/"
            "symbol=NIFTY-Aug2026-FUT/hour=15/depth.parquet"
        )
        rejected_key = (
            "market-data/deeplob/schema=v1/index=NIFTY/expiry=2026-08-25/"
            "trade_date=2026-08-11/instrument=NIFTY_FUT/"
            "symbol=NIFTYFPI-Aug2026-FUT/hour=15/depth.parquet"
        )

        class Body:
            def __init__(self, value):
                self.value = value

            def read(self):
                return self.value

        class S3:
            def __init__(self):
                self.report = None

            def list_objects_v2(self, **kwargs):
                return {
                    "Contents": [{"Key": parquet_key}, {"Key": rejected_key}],
                    "IsTruncated": False,
                }

            def get_object(self, **kwargs):
                if kwargs["Key"].endswith("daily-trades.json"):
                    ledger = {
                        "summary": {"trade_count": 1, "net_pnl": 5.0},
                        "trades": [
                            {
                                "entry_ts": 1.0,
                                "model_horizon_sec": 10,
                                "tag": "NIFTY_CE",
                                "profile": "scalp",
                                "probability_up": 0.8,
                                "option_expiry": "2026-08-11",
                            }
                        ],
                    }
                    return {"Body": Body(json.dumps(ledger).encode("utf-8"))}
                if "NIFTYFPI" in kwargs["Key"]:
                    return {"Body": Body(rejected_buffer.getvalue())}
                return {"Body": Body(buffer.getvalue())}

            def put_object(self, **kwargs):
                self.report = kwargs

        client = S3()
        runtime = PostMarketAnalysisRuntime(
            PostMarketAnalysisSettings(
                enabled=True,
                bucket="market-data",
                market_prefix="market-data/deeplob",
                report_prefix="analysis/deeplob",
                run_after=market_time(15, 35),
                sideways_bps=5.0,
            ),
            s3_client=client,
        )
        runtime._analyze("2026-08-11")
        report = json.loads(client.report["Body"])
        self.assertEqual(report["realized_trend"], "UP")
        self.assertEqual(report["symbol"], "NIFTY-Aug2026-FUT")
        self.assertEqual(report["security_id"], 61093)
        self.assertEqual(report["files"], 1)
        self.assertEqual(report["rejected_files"], 1)
        self.assertGreater(report["low"], 20_000)
        self.assertEqual(report["expiry_cycle"]["cycle_label"], "DAY_5")
        self.assertEqual(report["expiry_cycle"]["expiry_date"], "2026-08-11")
        evaluation = report["prediction_evaluation"]
        self.assertEqual(evaluation["evaluated"], 1)
        self.assertEqual(evaluation["accuracy_pct"], 100.0)
        self.assertAlmostEqual(evaluation["brier_score"], 0.04)
        self.assertEqual(
            evaluation["by_profile_horizon"]["scalp:10s"]["accuracy_pct"],
            100.0,
        )
        self.assertAlmostEqual(
            evaluation["by_profile_horizon"]["scalp:10s"]["brier_score"],
            0.04,
        )


if __name__ == "__main__":
    unittest.main()


