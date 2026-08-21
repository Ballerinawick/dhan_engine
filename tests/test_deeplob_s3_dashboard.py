import io
import json
import unittest
from datetime import datetime, timezone

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # Base installs intentionally omit recorder dependencies.
    pa = None
    pq = None

from dhan_engine.interfaces.web.deeplob_s3_dashboard import (
    DeepLobDashboardSettings,
    DeepLobS3Dashboard,
    _book_features,
)


class _Body:
    def __init__(self, value):
        self._value = value

    def read(self):
        return self._value


class _FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def list_objects_v2(self, **kwargs):
        prefix = kwargs["Prefix"]
        contents = [
            {
                "Key": key,
                "Size": len(value),
                "LastModified": datetime(2026, 8, 21, tzinfo=timezone.utc),
            }
            for key, value in self.objects.items()
            if key.startswith(prefix)
        ]
        return {"Contents": contents, "IsTruncated": False}

    def get_object(self, **kwargs):
        key = kwargs["Key"]
        if key not in self.objects:
            error = RuntimeError("missing")
            error.response = {"Error": {"Code": "NoSuchKey"}}
            raise error
        return {"Body": _Body(self.objects[key])}


def _parquet_bytes():
    table = pa.table(
        {
            "received_ns": [1_785_000_000_100_000_000, 1_785_000_004_100_000_000],
            "security_id": [41016, 41016],
            "ltp": [24200.0, 24202.0],
            "mid_price": [24200.0, 24202.0],
            "spread": [0.5, 0.5],
            "volume": [1000, 1010],
            "ltq": [10, 12],
            "oi": [5000, 5010],
            "best_bid": [24199.75, 24201.75],
            "best_ask": [24200.25, 24202.25],
            "bid_qty": [[100.0, 80.0], [120.0, 90.0]],
            "ask_qty": [[50.0, 70.0], [60.0, 80.0]],
            "bid_orders": [[5.0, 4.0], [6.0, 5.0]],
            "ask_orders": [[3.0, 4.0], [4.0, 4.0]],
            "fullquote_age_ms": [2.0, 3.0],
            "fullquote_synchronized": [True, True],
        }
    )
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


@unittest.skipUnless(pa is not None, "pyarrow recorder dependency is not installed")
class DeepLobS3DashboardTest(unittest.TestCase):
    def test_projects_market_data_and_trade_markers(self):
        market_key = (
            "market-data/deeplob/schema=v1/index=NIFTY/expiry=2026-08-25/"
            "trade_date=2026-08-21/instrument=NIFTY_FUT/symbol=NIFTY-Aug2026-FUT/"
            "hour=09/depth-test.parquet"
        )
        trade_key = (
            "paper-trades/deeplob/schema=v2/trade_date=2026-08-21/index=NIFTY/"
            "daily-trades.json"
        )
        trade = {
            "tag": "NIFTY_CE",
            "entry_ts": 1_785_000_000.1,
            "exit_ts": 1_785_000_004.1,
            "entry": 100.0,
            "exit": 102.0,
            "net_pnl": 70.0,
        }
        dashboard = DeepLobS3Dashboard(
            DeepLobDashboardSettings(bucket="test"),
            s3_client=_FakeS3(
                {
                    market_key: _parquet_bytes(),
                    trade_key: json.dumps({"trades": [trade]}).encode(),
                }
            ),
        )

        sessions = dashboard.list_sessions()
        payload = dashboard.session_payload("2026-08-21", bucket_sec=5)

        self.assertEqual(sessions[0]["symbol"], "NIFTY-Aug2026-FUT")
        self.assertEqual(len(payload["ticks"]), 1)
        self.assertEqual(payload["ticks"][0]["open"], 24200.0)
        self.assertEqual(payload["ticks"][0]["close"], 24202.0)
        self.assertGreater(payload["ticks"][0]["features"]["book_imbalance_20"], 0)
        self.assertEqual([row["kind"] for row in payload["signals"]], ["PAPER_ENTRY", "PAPER_EXIT"])


class DeepLobProjectionMathTest(unittest.TestCase):
    def test_book_imbalance_uses_both_sides(self):
        features = _book_features(
            {
                "bid_qty": [100, 50],
                "ask_qty": [25, 25],
                "bid_orders": [4, 2],
                "ask_orders": [1, 1],
            }
        )
        self.assertAlmostEqual(features["book_imbalance_20"], 0.5)
        self.assertAlmostEqual(features["order_imbalance_20"], 0.5)


if __name__ == "__main__":
    unittest.main()
