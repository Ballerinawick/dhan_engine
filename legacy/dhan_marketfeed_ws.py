# dhan_marketfeed_ws.py
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
class Depth5:
    bid_price: List[float]
    bid_qty: List[int]
    ask_price: List[float]
    ask_qty: List[int]
    ts: float


class DhanLiveMarketFeedWS:
    """
    Stable MarketFeed WS (v2).
    - ONE connection for entire day
    - Handles reconnect safely
    - Auto-resubscribe futures after reconnect
    """

    def __init__(
        self,
        token: str,
        client_id: str,
        auth_type: int = 2,
        on_full: Optional[Callable[[int, str, float, Depth5], None]] = None,
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

    # ---------------- PUBLIC ----------------
    def connect(self):
        if self._thread and self._thread.is_alive():
            return

        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def close(self):
        self._stop.set()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

    def subscribe_full(self, instruments: List[Dict[str, str]]):
        with self._lock:
            self._subs = instruments[:]  # replace, don’t append
            for it in instruments:
                self._tags[int(it["SecurityId"])] = it.get("tag", it["SecurityId"])

        if self._connected.is_set():
            self._send_subscribe()

    # ---------------- INTERNAL ----------------
    def _run_loop(self):
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
                print("🔗 MarketFeed connect:", url)

            self._ws = websocket.WebSocketApp(
                url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )

            try:
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                print("❌ MarketFeed exception:", e)

            if self._stop.is_set():
                break

            self._reconnect_attempt += 1
            wait = min(30, 2 ** min(self._reconnect_attempt, 4))
            print(f"⚠️ MarketFeed reconnect in {wait}s")
            time.sleep(wait)

    def _send_subscribe(self):
        if not self._ws or not self._subs:
            return

        inst = [
            {"ExchangeSegment": it["ExchangeSegment"], "SecurityId": it["SecurityId"]}
            for it in self._subs
        ]

        payload = {
            "RequestCode": REQ_FULL,
            "InstrumentCount": len(inst),
            "InstrumentList": inst,
        }

        if self.debug:
            print("📤 MarketFeed SUB:", payload)

        try:
            self._ws.send(json.dumps(payload))
        except Exception as e:
            print("❌ MarketFeed send failed:", e)

    # ---------------- WS CALLBACKS ----------------
    def _on_open(self, ws):
        self._connected.set()
        self._reconnect_attempt = 0
        if self.debug:
            print("✅ MarketFeed connected")
        self._send_subscribe()

    def _on_error(self, ws, error):
        print("❌ MarketFeed WS error:", error)

    def _on_close(self, ws, code, msg):
        self._connected.clear()
        if self.debug:
            print("⚠️ MarketFeed closed:", code, msg)

    def _on_message(self, ws, message):
        if isinstance(message, str):
            return

        data = message
        off = 0
        n = len(data)

        while off + 8 <= n:
            feed_code = data[off]
            msg_len = struct.unpack_from("<h", data, off + 1)[0]
            secid = struct.unpack_from("<i", data, off + 4)[0]

            pkt_len = 8 + msg_len
            if pkt_len <= 8 or off + pkt_len > n:
                break

            pkt = data[off: off + pkt_len]
            off += pkt_len

            if feed_code == RESP_FULL:
                self._parse_full(secid, pkt)

    def _parse_full(self, secid: int, pkt: bytes):
        if len(pkt) < 12:
            return

        ltp = struct.unpack_from("<f", pkt, 8)[0]

        depth_start = 8 + 62
        if len(pkt) < depth_start + 100:
            return

        bid_p, bid_q, ask_p, ask_q = [], [], [], []

        for i in range(5):
            base = depth_start + (i * 20)
            price = struct.unpack_from("<f", pkt, base)[0]
            qty = struct.unpack_from("<i", pkt, base + 4)[0]

            if i < 2:
                bid_p.append(price); bid_q.append(qty)
            else:
                ask_p.append(price); ask_q.append(qty)

        tag = self._tags.get(secid, str(secid))
        if self.on_full:
            self.on_full(secid, tag, float(ltp),
                         Depth5(bid_p, bid_q, ask_p, ask_q, time.time()))
