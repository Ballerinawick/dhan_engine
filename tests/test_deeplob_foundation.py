import tempfile
import time
import unittest
from unittest.mock import patch

from dhan_engine.analytics.deeplob_recorder import DepthRecorderSettings, ParquetDepthRecorder
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


if __name__ == "__main__":
    unittest.main()
