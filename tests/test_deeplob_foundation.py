import tempfile
import time
import unittest
from unittest.mock import patch

from dhan_engine.analytics.deeplob_recorder import (
    DepthInstrument,
    DepthRecorderSettings,
    ParquetDepthRecorder,
)
from dhan_engine.application.deeplob.live_runtime import DeepLobLiveSettings, DeepLobLiveRuntime
from dhan_engine.application.deeplob.recorder_runtime import DeepLobRecorderRuntimeSettings
from dhan_engine.domain.market.deeplob_model import encode_book, paper_option_action
from dhan_engine.domain.market.full_depth_microstructure import BookSnapshot


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

    def test_recorder_sampling_retains_one_snapshot_per_interval(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = ParquetDepthRecorder(
                DepthRecorderSettings(output_dir=directory, sample_interval_ms=250)
            )
            recorder.record("NIFTY_FUT", snapshot())
            recorder.record("NIFTY_FUT", snapshot())
            self.assertEqual(recorder._queue.qsize(), 1)
            self.assertEqual(recorder._sampled_out, 1)

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
        runtime.instrument_metadata["NIFTY_FUT"] = {
            "index": "NIFTY",
            "symbol": "NIFTY-Jul2026-FUT",
            "expiry": "2026-07-30",
        }
        book = snapshot()
        runtime.on_book("NIFTY_FUT", book)
        self.assertEqual(runtime._received, 1)
        self.assertEqual(recorder.calls[0][0], ("NIFTY_FUT", book))
        self.assertEqual(inference.calls[0], ("NIFTY_FUT", book))


if __name__ == "__main__":
    unittest.main()

