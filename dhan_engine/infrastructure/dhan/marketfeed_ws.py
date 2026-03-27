import json
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import websocket

try:
    from dhanhq.marketfeed import DhanFeed
except Exception:  # pragma: no cover - optional dependency path
    DhanFeed = None


REQ_FULL = 21


@dataclass
class QuoteDepth:
    bid_price: List[float]
    bid_qty: List[int]
    ask_price: List[float]
    ask_qty: List[int]
    ts: float


class DhanLiveMarketFeedWS:
    """
    Stable market-feed websocket client for full quote subscriptions.

    This client is used for the dedicated future stream so underlying
    data is live over WS instead of REST polling.
    """

    def __init__(
        self,
        token: str,
        client_id: str,
        auth_type: int = 2,
        on_full: Optional[Callable[[int, str, float, QuoteDepth], None]] = None,
        debug: bool = False,
    ):
        self.token = token
        self.client_id = client_id
        self.auth_type = auth_type
        self.on_full = on_full
        self.debug = debug

        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._subs: List[Dict[str, str]] = []
        self._tags: Dict[int, str] = {}
        self._reconnect_attempt = 0
        self._lock = threading.Lock()
        self._feed_parser = (
            DhanFeed(
                client_id=self.client_id,
                access_token=self.token,
                instruments=[],
                version="v2",
            )
            if DhanFeed is not None
            else None
        )

    def connect(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run_loop, name="DhanMarketFeedWS", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

    def subscribe_full(self, instruments: List[Dict[str, str]]) -> None:
        with self._lock:
            self._subs = instruments[:]
            for item in instruments:
                self._tags[int(item["SecurityId"])] = item.get("tag", item["SecurityId"])

        if self._connected.is_set():
            self._send_subscribe()

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            self._connected.clear()

            url = (
                f"wss://api-feed.dhan.co"
                f"?version=2"
                f"&token={self.token}"
                f"&clientId={self.client_id}"
                f"&authType={self.auth_type}"
            )

            if self.debug:
                print("WS_FULLQUOTE_CONNECT", url)

            self._ws = websocket.WebSocketApp(
                url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )

            try:
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                print(f"FULLQUOTE_WS_EXCEPTION | error={exc}")

            if self._stop.is_set():
                break

            self._reconnect_attempt += 1
            wait = min(30, 2 ** min(self._reconnect_attempt, 4))
            print(f"FULLQUOTE_WS_RECONNECT_WAIT | sec={wait}")
            time.sleep(wait)

    def _send_subscribe(self) -> None:
        if not self._ws or not self._subs:
            return

        payload = {
            "RequestCode": REQ_FULL,  # must stay 21 (FULL quote mode)
            "InstrumentCount": len(self._subs),
            "InstrumentList": [
                {
                    "ExchangeSegment": item["ExchangeSegment"],
                    "SecurityId": item["SecurityId"],
                }
                for item in self._subs
            ],
        }

        if self.debug:
            print("WS_FULLQUOTE_SUB", payload)

        try:
            print("📤 WS SUB PAYLOAD:", payload)
            self._ws.send(json.dumps(payload))
        except Exception as exc:
            print(f"FULLQUOTE_WS_SUBSCRIBE_ERROR | error={exc}")

    def _on_open(self, ws) -> None:
        self._connected.set()
        self._reconnect_attempt = 0
        print("🔥 WS CONNECTED — READY TO SUBSCRIBE")
        self._send_subscribe()

    def _on_error(self, ws, error) -> None:
        print(f"FULLQUOTE_WS_ERROR | error={error}")

    def _on_close(self, ws, code, message) -> None:
        self._connected.clear()
        if self.debug:
            print(f"WS_FULLQUOTE_CLOSED | code={code} | message={message}")

    def process_data(self, data: bytes):
        if self._feed_parser is None:
            raise RuntimeError("dhanhq is not installed. Run: pip install dhanhq")
        return self._feed_parser.process_data(data)

    def _on_message(self, ws, message) -> None:
        try:
            if not isinstance(message, (bytes, bytearray)):
                print("⚠️ NON-BINARY MESSAGE")
                return

            parsed = self.process_data(bytes(message))

            if parsed:
                print("✅ PARSED DATA:", parsed)

                if parsed.get("type") == "Full Data":
                    secid = int(parsed.get("security_id"))
                    ltp = float(parsed.get("LTP"))
                    tag = self._tags.get(secid, str(secid))

                    depth = parsed.get("depth") or []
                    bid_price = [float(item.get("bid_price", 0.0)) for item in depth]
                    bid_qty = [int(item.get("bid_quantity", 0)) for item in depth]
                    ask_price = [float(item.get("ask_price", 0.0)) for item in depth]
                    ask_qty = [int(item.get("ask_quantity", 0)) for item in depth]

                    print("🔥 FUTURE LTP:", secid, ltp)

                    if self.on_full:
                        self.on_full(
                            secid,
                            tag,
                            ltp,
                            QuoteDepth(
                                bid_price=bid_price,
                                bid_qty=bid_qty,
                                ask_price=ask_price,
                                ask_qty=ask_qty,
                                ts=time.time(),
                            ),
                        )
        except Exception as e:
            print("❌ WS ERROR:", e)
            import traceback

            traceback.print_exc()
