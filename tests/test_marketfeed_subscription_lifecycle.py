import json
import threading
import unittest
from unittest.mock import Mock

from dhan_engine.infrastructure.dhan.marketfeed_ws import (
    DhanLiveMarketFeedWS,
    REQ_FULL,
    REQ_UNSUB_FULL,
)


class MarketFeedSubscriptionLifecycleTests(unittest.TestCase):
    def test_refresh_sends_official_unsubscribe_before_subscribe(self):
        client = DhanLiveMarketFeedWS(token="token", client_id="client")
        client._ws = Mock()
        client._connected.set()
        client._connection_id = "test-1"
        client._subs = [
            {"ExchangeSegment": "NSE_FNO", "SecurityId": "101", "tag": "NIFTY_CE"},
            {"ExchangeSegment": "NSE_FNO", "SecurityId": "102", "tag": "NIFTY_PE"},
        ]
        client._sub_keys = {("NSE_FNO", "101"), ("NSE_FNO", "102")}

        self.assertTrue(client.refresh_full_subscriptions(reason="test"))

        payloads = [json.loads(call.args[0]) for call in client._ws.send.call_args_list]
        self.assertEqual([payload["RequestCode"] for payload in payloads], [REQ_UNSUB_FULL, REQ_FULL])
        self.assertEqual(payloads[0]["InstrumentCount"], 2)
        self.assertEqual(payloads[1]["InstrumentCount"], 2)

    def test_wait_closed_reports_live_thread(self):
        client = DhanLiveMarketFeedWS(token="token", client_id="client")
        blocker = threading.Event()
        client._thread = threading.Thread(target=lambda: blocker.wait(1.0), name="test-live-thread")
        client._thread.start()
        try:
            self.assertFalse(client.wait_closed(timeout=0.01))
        finally:
            blocker.set()
            client._thread.join(timeout=1.0)


if __name__ == "__main__":
    unittest.main()
