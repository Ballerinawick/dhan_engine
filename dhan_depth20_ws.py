# dhan_depth20_ws.py
import json
import struct
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import websocket  # pip install websocket-client

FEED_BID = 41  # Bid Data (Buy)
FEED_ASK = 51  # Ask Data (Sell)


@dataclass
class DepthSide:
    prices: List[float]
    qty: List[int]
    orders: List[int]
    ts: float


class DhanTwentyDepthWS:
    """
    Stable 20-level depth WebSocket client.
    - Single WS connection handles up to 50 instruments.
    - Safe reconnect with cooldown (prevents 429 block loops).
    - Queues subscriptions until connected.
    - Resubscribes automatically after reconnect.
    """

    def __init__(
        self,
        token: str,
        client_id: str,
        auth_type: int = 2,
        exchange_segment: str = "NSE_FNO",
        on_depth: Optional[Callable[[int, str, DepthSide, DepthSide], None]] = None,
        debug: bool = False,
        ping_interval: int = 25,
        ping_timeout: int = 10,
    ):
        self.token = token
        self.client_id = client_id
        self.auth_type = auth_type
        self.exchange_segment = exchange_segment
        self.on_depth = on_depth
        self.debug = debug

        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout

        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None

        self._stop = threading.Event()
        self._connected = threading.Event()
        self._connecting_lock = threading.Lock()

        self._pending_subs: List[Dict[str, str]] = []
        self._subscribed: Dict[int, str] = {}  # secid -> tag

        self._latest_bid: Dict[int, DepthSide] = {}
        self._latest_ask: Dict[int, DepthSide] = {}

        self._reconnect_attempt = 0

        # If server rate-limits / blocks, we pause reconnects.
        self._cooldown_until_epoch = 0.0

        # Track last error to detect 429 / block message
        self._last_error_text = ""

    # ---------------- public API ----------------
    def connect(self):
        """
        Starts (or ensures) the WS thread is running.
        Does NOT create multiple threads.
        """
        if self._stop.is_set():
            return

        if self._thread and self._thread.is_alive():
            # already running
            return

        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def is_connected(self) -> bool:
        return self._connected.is_set()

    def close(self):
        self._stop.set()
        self._connected.clear()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

    def subscribe(self, instruments: List[Dict[str, str]]):
        """
        instruments: [{"SecurityId":"40471","tag":"NIFTY_CE_26100"}, ...]
        NOTE: This does NOT open new WS per instrument.
        It sends ONE subscribe message with multiple instruments.
        """
        if not instruments:
            return

        # record for resubscribe
        for it in instruments:
            secid = int(it["SecurityId"])
            tag = it.get("tag", str(secid))
            self._subscribed[secid] = tag

        # if not connected, queue
        if not self._connected.is_set():
            self._pending_subs.extend(instruments)
            return

        self._send_subscribe(instruments)

    # ---------------- internal: connection loop ----------------
    def _run_loop(self):
        while not self._stop.is_set():
            # cooldown handling (429 / blocked)
            now = time.time()
            if now < self._cooldown_until_epoch:
                sleep_for = max(1.0, self._cooldown_until_epoch - now)
                if self.debug:
                    print(f"🧊 Cooldown active. Sleeping {sleep_for:.1f}s (to avoid 429/block)")
                time.sleep(min(sleep_for, 5.0))
                continue

            # ensure we don't run multiple connects in parallel
            with self._connecting_lock:
                if self._stop.is_set():
                    break

                self._connected.clear()
                self._ws = self._create_ws()

            try:
                # run_forever is blocking until close/error
                self._ws.run_forever(
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                    reconnect=0,  # IMPORTANT: we handle reconnect ourselves (avoid internal storms)
                )
            except Exception as e:
                self._last_error_text = str(e)
                print("❌ 20Depth WS run_forever exception:", e)

            if self._stop.is_set():
                break

            # decide reconnect wait
            self._connected.clear()

            # if last error suggests rate limit / block, enforce cooldown
            if self._looks_like_rate_limited(self._last_error_text):
                # Hard cooldown to prevent "client id blocked"
                self._cooldown_until_epoch = time.time() + (15 * 60)  # 15 minutes
                print("🛑 Detected rate-limit/block. Cooling down 15 minutes to avoid permanent block.")
                continue

            self._reconnect_attempt += 1
            wait = self._calc_backoff(self._reconnect_attempt)
            print(f"⚠️ 20Depth WS reconnecting in {wait}s ...")
            time.sleep(wait)

    def _create_ws(self) -> websocket.WebSocketApp:
        url = (
            f"wss://depth-api-feed.dhan.co/twentydepth"
            f"?token={self.token}&clientId={self.client_id}&authType={self.auth_type}"
        )

        if self.debug:
            print("🔗 Connecting to 20Depth WS URL:", url)

        return websocket.WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

    # ---------------- internal: subscribe ----------------
    def _send_subscribe(self, instruments: List[Dict[str, str]]):
        if not self._ws:
            return

        inst_list = []
        for it in instruments:
            secid = int(it["SecurityId"])
            inst_list.append(
                {
                    "ExchangeSegment": self.exchange_segment,
                    "SecurityId": str(secid),
                }
            )

        payload = {
            "RequestCode": 23,
            "InstrumentCount": len(inst_list),
            "InstrumentList": inst_list,
        }

        if self.debug:
            print("📤 SUBSCRIBE:", payload)

        try:
            self._ws.send(json.dumps(payload))
        except Exception as e:
            self._last_error_text = str(e)
            print("❌ SUBSCRIBE send failed:", e)

    # ---------------- handlers ----------------
    def _on_open(self, ws):
        self._connected.set()
        self._reconnect_attempt = 0
        self._last_error_text = ""

        if self.debug:
            print("✅ 20Depth WS connected")

        # Resubscribe known instruments (only once on open)
        if self._subscribed:
            resub = [{"SecurityId": str(k), "tag": v} for k, v in self._subscribed.items()]
            self._send_subscribe(resub)

        # Flush queued subs
        if self._pending_subs:
            subs = self._pending_subs[:]
            self._pending_subs.clear()
            self._send_subscribe(subs)

    def _on_error(self, ws, error):
        # websocket-client sometimes passes exceptions or strings
        txt = str(error)
        self._last_error_text = txt

        # Detect 429 "Too Many Requests" / blocked
        if self._looks_like_rate_limited(txt):
            # Start cooldown now (don’t keep hammering)
            self._cooldown_until_epoch = time.time() + (15 * 60)  # 15 minutes
            print("🛑 20Depth WS rate-limited/blocked detected. Cooling down 15 minutes.")
            try:
                ws.close()
            except Exception:
                pass
            return

        print("❌ 20Depth WS error:", error)

    def _on_close(self, ws, code, msg):
        self._connected.clear()
        if msg:
            self._last_error_text = str(msg)

        if self.debug:
            print("⚠️ 20Depth WS closed:", code, msg)

    def _on_message(self, ws, message):
        # We expect binary. If it's str, ignore.
        if isinstance(message, str):
            # Some servers send text errors - capture it
            self._last_error_text = message
            if self._looks_like_rate_limited(message):
                self._cooldown_until_epoch = time.time() + (15 * 60)
                print("🛑 20Depth WS text rate-limit/block. Cooling down 15 minutes.")
                try:
                    ws.close()
                except Exception:
                    pass
            return

        data = message
        off = 0
        n = len(data)

        # Each packet begins with length (int16), then packet bytes
        while off + 2 <= n:
            pkt_len = struct.unpack_from("<h", data, off)[0]
            if pkt_len <= 0 or off + pkt_len > n:
                break

            pkt = data[off : off + pkt_len]
            off += pkt_len
            self._parse_one_packet(pkt)

    def _parse_one_packet(self, pkt: bytes):
        if len(pkt) < 12:
            return

        feed_code = pkt[2]
        secid = struct.unpack_from("<i", pkt, 4)[0]

        # body must have 20 levels = 20 * 16 bytes = 320 bytes
        if len(pkt) < 12 + 320:
            return

        levels = []
        off = 12
        for _ in range(20):
            price, qty, orders = struct.unpack_from("<dII", pkt, off)
            levels.append((price, qty, orders))
            off += 16

        prices = [x[0] for x in levels]
        qtys = [int(x[1]) for x in levels]
        orders = [int(x[2]) for x in levels]
        ts = time.time()
        side = DepthSide(prices=prices, qty=qtys, orders=orders, ts=ts)

        if feed_code == FEED_BID:
            self._latest_bid[secid] = side
        elif feed_code == FEED_ASK:
            self._latest_ask[secid] = side
        else:
            return

        bid = self._latest_bid.get(secid)
        ask = self._latest_ask.get(secid)
        if not bid or not ask:
            return

        tag = self._subscribed.get(secid, str(secid))
        if self.on_depth:
            try:
                self.on_depth(secid, tag, bid, ask)
            except Exception as e:
                # never crash WS thread due to callback bug
                print("❌ on_depth callback error:", e)

    # ---------------- helpers ----------------
    @staticmethod
    def _calc_backoff(attempt: int) -> int:
        """
        Slower backoff to prevent hammering.
        1st: 10s, 2nd: 20s, 3rd: 40s, 4th: 60s, then max 300s.
        """
        if attempt <= 1:
            return 10
        if attempt == 2:
            return 20
        if attempt == 3:
            return 40
        if attempt == 4:
            return 60
        return min(300, 60 + (attempt - 4) * 30)

    @staticmethod
    def _looks_like_rate_limited(text: str) -> bool:
        t = (text or "").lower()
        return (
            "429" in t
            or "too many requests" in t
            or "blocked" in t
            or "client id is blocked" in t
            or "rate" in t and "limit" in t
        )
