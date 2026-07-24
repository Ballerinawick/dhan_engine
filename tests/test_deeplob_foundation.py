import tempfile
import time
import unittest

from dhan_engine.analytics.deeplob_recorder import DepthRecorderSettings, ParquetDepthRecorder
from dhan_engine.domain.market.deeplob_model import encode_book
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
                DepthRecorderSettings(output_dir=directory, queue_size=128, rows_per_file=10_000)
            )
            started = time.perf_counter()
            for _ in range(1000):
                recorder.record("NIFTY_FUT", snapshot())
            self.assertLess(time.perf_counter() - started, 1.0)
            self.assertGreater(recorder._dropped, 0)


if __name__ == "__main__":
    unittest.main()
