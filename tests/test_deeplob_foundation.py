
import tempfile
import time
import unittest
from datetime import time as market_time
from types import SimpleNamespace
from unittest.mock import patch

from dhan_engine.analytics.deeplob_recorder import (
    DepthInstrument,
    DepthRecorderSettings,
    ParquetDepthRecorder,
)
from dhan_engine.application.deeplob.live_runtime import DeepLobLiveSettings, DeepLobLiveRuntime
from dhan_engine.application.deeplob.option_paper_executor import (
    DeepLobOptionPaperExecutor,
    DeepLobOptionPaperSettings,
)
from dhan_engine.application.deeplob.trade_summary_s3 import (
    TradeSummaryS3Settings,
    TradeSummaryS3Sink,
)
from dhan_engine.application.deeplob.recorder_runtime import (
    DeepLobRecorderRuntime,
    DeepLobRecorderRuntimeSettings,
)
from dhan_engine.application.deeplob.premodel_paper_runtime import (
    MarketByPricePaperRuntime,
    MarketByPricePaperSettings,
)
from dhan_engine.domain.market.deeplob_model import encode_book, paper_option_action
from dhan_engine.domain.market.full_depth_microstructure import BookSnapshot
from dhan_engine.domain.market.market_by_price_execution import (
    derive_market_by_price_features,
    estimate_market_execution,
    validate_composite_snapshot,
)


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
                    (selection["CE"]["security_id"], selection["CE"]["tag"]),
                    (selection["PE"]["security_id"], selection["PE"]["tag"]),
                ]

        class Feed:
            def __init__(self):
                self.subscriptions = []

            def subscribe_full(self, items):
                self.subscriptions.append(list(items))

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
        self.assertEqual(feed.subscriptions, [[(101, "NIFTY_CE"), (102, "NIFTY_PE")]])
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

    def test_trade_summary_s3_sink_uploads_partitioned_json(self):
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
        sink.close()
        self.assertEqual(len(client.calls), 1)
        uploaded = client.calls[0]
        self.assertEqual(uploaded["Bucket"], "market-data")
        self.assertIn("trade_date=2026-07-28", uploaded["Key"])
        self.assertIn("instrument=NIFTY_CE", uploaded["Key"])
        self.assertIn(b'"net_pnl":135.0', uploaded["Body"])

    def test_trade_summary_prefix_must_be_separate_from_market_data(self):
        environment = {
            "DEEPLOB_S3_PREFIX": "market-data/deeplob",
            "DEEPLOB_TRADE_SUMMARY_S3_PREFIX": "market-data/deeplob/trades",
        }
        with patch.dict("os.environ", environment, clear=True):
            with self.assertRaisesRegex(ValueError, "must be separate"):
                TradeSummaryS3Settings.from_env()


if __name__ == "__main__":
    unittest.main()


