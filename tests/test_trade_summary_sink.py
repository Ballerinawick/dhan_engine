from __future__ import annotations

import os
import unittest

from dhan_engine.infrastructure.mongo.trade_summary_sink import TradeSummarySink


class FakeCollection:
    def __init__(self) -> None:
        self.documents = []

    def insert_one(self, document):
        self.documents.append(document)


class TradeSummarySinkTests(unittest.TestCase):
    def test_disabled_without_uri_is_noop(self):
        old_uri = os.environ.pop("TRADE_SUMMARY_MONGO_URI", None)
        old_mongodb_uri = os.environ.pop("MONGODB_URI", None)
        try:
            sink = TradeSummarySink()
            self.assertFalse(sink.enabled)
            self.assertFalse(sink.record("stocks", {"symbol": "SBIN", "net_pnl": 10.0}))
        finally:
            if old_uri is not None:
                os.environ["TRADE_SUMMARY_MONGO_URI"] = old_uri
            if old_mongodb_uri is not None:
                os.environ["MONGODB_URI"] = old_mongodb_uri

    def test_record_adds_common_fields(self):
        sink = TradeSummarySink()
        collection = FakeCollection()
        sink.enabled = True
        sink._collection = collection

        stored = sink.record("commodities", {"symbol": "GOLD", "net_pnl": 125.5})

        self.assertTrue(stored)
        self.assertEqual(len(collection.documents), 1)
        document = collection.documents[0]
        self.assertEqual(document["section"], "commodities")
        self.assertEqual(document["symbol"], "GOLD")
        self.assertEqual(document["net_pnl"], 125.5)
        self.assertTrue(document["paper"])
        self.assertIn("created_at_utc", document)
        self.assertIn("created_at_ist", document)
        self.assertIn("trade_date_ist", document)
        self.assertIn("deployment_id", document)


if __name__ == "__main__":
    unittest.main()
