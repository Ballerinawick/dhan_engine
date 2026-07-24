import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from dhan_engine.infrastructure.dhan.master_csv import refresh_master_csv


class MasterCsvRefreshTests(unittest.TestCase):
    @patch("dhan_engine.infrastructure.dhan.master_csv.requests.get")
    def test_refresh_replaces_existing_file_atomically(self, get: Mock):
        response = Mock()
        response.content = b"x" * 2048
        response.raise_for_status.return_value = None
        get.return_value = response

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "api-scrip-master.csv"
            destination.write_bytes(b"old" * 500)

            refreshed = refresh_master_csv("https://example.test/master.csv", str(destination))

            self.assertTrue(refreshed)
            self.assertEqual(destination.read_bytes(), response.content)
            self.assertEqual(list(destination.parent.glob("*.tmp")), [])

    @patch("dhan_engine.infrastructure.dhan.master_csv.requests.get")
    def test_refresh_failure_preserves_existing_valid_file(self, get: Mock):
        get.side_effect = TimeoutError("network unavailable")

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "api-scrip-master.csv"
            existing = b"valid-existing" * 100
            destination.write_bytes(existing)

            refreshed = refresh_master_csv("https://example.test/master.csv", str(destination))

            self.assertFalse(refreshed)
            self.assertEqual(destination.read_bytes(), existing)

    @patch("dhan_engine.infrastructure.dhan.master_csv.requests.get")
    def test_refresh_failure_without_valid_fallback_raises(self, get: Mock):
        get.side_effect = TimeoutError("network unavailable")

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "api-scrip-master.csv"
            with self.assertRaises(TimeoutError):
                refresh_master_csv("https://example.test/master.csv", str(destination))


if __name__ == "__main__":
    unittest.main()
