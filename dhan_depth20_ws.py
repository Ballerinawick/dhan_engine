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
    Adds diagnostics for cloud deployments:
    - message counters
    - last message timestamps
    - "NO DATA" warnings if WS is connected but no packets arrive
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
        # NEW: diagnostics
        no_data_warn_sec: int = 20,      # warn if no packets after connect
        diag_print_every: int = 200,     # print 1 line every N binary messages
    ):
        self.token = token
        self.client_id = client_id
        self.auth_type = auth_type
        self.exchange_segment = exchange_segment
        self.on_depth = on_depth
        self.debug = debug

        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout

        self.no_data_warn_sec = int(no_data_warn_sec)
        self.diag_print_every = int(diag_print_every)

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
        self._cooldown_until_epoch = 0.0
        self._last_error_text = ""

        # --- diagnostics ---
        self._connected_at = 0.0
        self._last_msg_at = 0.0
        self._bin_msg_count = 0
        self._text_msg_count = 0
        self._parse_ok_packets = 0
        self._parse_bad_packets = 0

    # ---------------- public API ----------------
    def connect(self):
        if self._stop.is_set():
            return
        if self._thread and self._thread.is_alive():
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
        if not instruments:
            return

        for it in instruments:
            secid = int(it["SecurityId"])
            tag = it.get("tag", str(secid))
            self._subscribed[secid] = tag

        if not self._connected.is_set():
            self._pending_subs.extend(instruments)
            return

        self._send_subscribe(instruments)

    # ---------------- internal: connection loop ----------------
    def _run_loop(self):
        while not self._stop.is_set():
            now = time.time()
            if now < self._cooldown_until_epoch:
                sleep_for = max(1.0, self._cooldown_until_epoch - now)
                if self.debug:
                    print(f"🧊 Cooldown active. Sleeping {sleep_for:.1f}s")
                time.sleep(min(sleep_for, 5.0))
                continue

            with self._connecting_lock:
                if self._stop.is_set():
                    break

                self._connected.clear()
                self._ws = self._create_ws()

            try:
                self._ws.run_forever(
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                    reconnect=0,
                )
            except Exception as e:
                self._last_error_text = str(e)
                print("❌ 20Depth WS run_forever exception:", e)

            if self._stop.is_set():
                break

            self._connected.clear()

            if self._looks_like_rate_limited(self._last_error_text):
                self._cooldown_until_epoch = time.time() + (15 * 60)
                print("🛑 Detected rate-limit/block. Cooling down 15 minutes.")
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

        # diagnostics reset for this session
        self._connected_at = time.time()
        self._last_msg_at = 0.0
        self._bin_msg_count = 0
        self._text_msg_count = 0
        self._parse_ok_packets = 0
        self._parse_bad_packets = 0

        print("✅ 20Depth WS connected")

        if self._subscribed:
            resub = [{"SecurityId": str(k), "tag": v} for k, v in self._subscribed.items()]
            self._send_subscribe(resub)

        if self._pending_subs:
            subs = self._pending_subs[:]
            self._pending_subs.clear()
            self._send_subscribe(subs)

    def _on_error(self, ws, error):
        txt = str(error)
        self._last_error_text = txt

        if self._looks_like_rate_limited(txt):
            self._cooldown_until_epoch = time.time() + (15 * 60)
            print("🛑 20Depth WS rate-limited/blocked detected. Cooling down 15 minutes.")
            try:
                ws.close()
            except Exception:
                pass
            return

        print("❌ 20Depth WS error:", txt)

    def _on_close(self, ws, code, msg):
        self._connected.clear()
        if msg:
            self._last_error_text = str(msg)
        print("⚠️ 20Depth WS closed:", code, msg)

    def _on_message(self, ws, message):
        # text message (often error/notice)
        if isinstance(message, str):
            self._text_msg_count += 1
            self._last_error_text = message
            print("📩 20Depth WS TEXT:", message[:300])

            if self._looks_like_rate_limited(message):
                self._cooldown_until_epoch = time.time() + (15 * 60)
                print("🛑 20Depth WS text rate-limit/block. Cooling down 15 minutes.")
                try:
                    ws.close()
                except Exception:
                    pass
            return

        # binary message
        self._bin_msg_count += 1
        self._last_msg_at = time.time()

        # periodic diag line
        if self.diag_print_every > 0 and (self._bin_msg_count % self.diag_print_every == 0):
            print(
                f"📦 20D BIN msgs:{self._bin_msg_count} | "
                f"ok_pkts:{self._parse_ok_packets} bad_pkts:{self._parse_bad_packets}"
            )

        data = message
        off = 0
        n = len(data)

        # NEW: warn if connected but no data for long time (runs on first message too late)
        # We'll do a separate warning in main loop by time checks (see below in parser also)
        while off + 2 <= n:
            # IMPORTANT: use unsigned short (signed <h can become negative and break parsing)
            pkt_len = struct.unpack_from("<H", data, off)[0]
            if pkt_len <= 0 or off + pkt_len > n:
                self._parse_bad_packets += 1
                break

            pkt = data[off: off + pkt_len]
            off += pkt_len
            self._parse_one_packet(pkt)

    def _parse_one_packet(self, pkt: bytes):
        try:
            if len(pkt) < 12:
                self._parse_bad_packets += 1
                return

            feed_code = pkt[2]
            packet_type = feed_code
            secid = struct.unpack_from("<i", pkt, 4)[0]

            # body must have 20 levels = 20 * 16 bytes = 320 bytes
            if len(pkt) < 12 + 320:
                self._parse_bad_packets += 1
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
                # unknown feed code
                return

            bid = self._latest_bid.get(secid)
            ask = self._latest_ask.get(secid)
            if not bid or not ask:
                return

            bids = [
                (bid.prices[i], bid.qty[i], bid.orders[i])
                for i in range(min(len(bid.prices), len(bid.qty), len(bid.orders)))
            ]
            asks = [
                (ask.prices[i], ask.qty[i], ask.orders[i])
                for i in range(min(len(ask.prices), len(ask.qty), len(ask.orders)))
            ]

            self._parse_ok_packets += 1

            tag = self._subscribed.get(secid, str(secid))
            if self.on_depth:
                try:
                    self.on_depth(secid, tag, bid, ask)
                except Exception as e:
                    print("❌ on_depth callback error:", e)

        except Exception:
            self._parse_bad_packets += 1
            # keep silent unless debug
            if self.debug:
                print("❌ parse_one_packet exception")

    # ---------------- helpers ----------------
    @staticmethod
    def _calc_backoff(attempt: int) -> int:
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
            or ("rate" in t and "limit" in t)
        )

    # NEW: you can call this from your main loop if needed
    def diag_should_warn_no_data(self) -> bool:
        if not self._connected.is_set():
            return False
        if self._connected_at <= 0:
            return False
        if self._last_msg_at > 0:
            return False
        return (time.time() - self._connected_at) >= float(self.no_data_warn_sec)
