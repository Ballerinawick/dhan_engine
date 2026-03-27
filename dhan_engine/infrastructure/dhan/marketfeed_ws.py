import json
import struct
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import websocket


REQ_FULL = 21
RESP_FULL = 8


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
            "RequestCode": REQ_FULL,
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

    def _on_message(self, ws, message) -> None:
        print("📥 WS RAW MESSAGE RECEIVED | type=", type(message))
        if isinstance(message, str):
            return

        data = message
        offset = 0
        total = len(data)

        while offset + 8 <= total:
            feed_code = data[offset]
            msg_len = struct.unpack_from("<h", data, offset + 1)[0]
            secid = struct.unpack_from("<i", data, offset + 4)[0]

            packet_len = 8 + msg_len
            if packet_len <= 8 or offset + packet_len > total:
                break

            packet = data[offset : offset + packet_len]
            offset += packet_len

            print("📦 WS PACKET | code=", feed_code, "| secid=", secid)

            if feed_code == RESP_FULL:
                self._parse_full(secid, packet)

    def _parse_full(self, secid: int, packet: bytes) -> None:
        if len(packet) < 12:
            print("❌ INVALID PACKET LENGTH")
            return

        try:
            ltp = struct.unpack_from("<f", packet, 8)[0]
        except Exception as e:
            print("❌ LTP PARSE FAILED", e)
            return

        depth_start = 8 + 62

        bid_price: List[float] = []
        bid_qty: List[int] = []
        ask_price: List[float] = []
        ask_qty: List[int] = []

        try:
            if len(packet) >= depth_start + (5 * 20):
                for i in range(5):
                    base = depth_start + (i * 20)

                    bid_q = struct.unpack_from("<i", packet, base)[0]
                    ask_q = struct.unpack_from("<i", packet, base + 4)[0]

                    bid_p = struct.unpack_from("<f", packet, base + 12)[0]
                    ask_p = struct.unpack_from("<f", packet, base + 16)[0]

                    bid_price.append(float(bid_p))
                    bid_qty.append(int(bid_q))

                    ask_price.append(float(ask_p))
                    ask_qty.append(int(ask_q))
        except Exception as e:
            print("❌ DEPTH PARSE ERROR", e)

        tag = self._tags.get(secid, str(secid))

        print("✅ FULL_WS_TICK RECEIVED | secid=", secid, "| ltp=", ltp)

        if self.on_full:
            try:
                self.on_full(
                    secid,
                    tag,
                    float(ltp),
                    QuoteDepth(
                        bid_price=bid_price,
                        bid_qty=bid_qty,
                        ask_price=ask_price,
                        ask_qty=ask_qty,
                        ts=time.time(),
                    ),
                )
            except Exception as exc:
                print("❌ CALLBACK ERROR", exc)
