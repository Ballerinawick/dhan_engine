import json
import struct
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import websocket  # pip install websocket-client

FEED_BID = 41
FEED_ASK = 51


@dataclass
class DepthSide:
    prices: List[float]
    qty: List[int]
    orders: List[int]
    ts: float


class DhanTwentyDepthWS:
    """
    Production-safe 20-level depth WebSocket client
    with strong diagnostics.
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
        no_data_warn_sec: int = 20,
        diag_print_every: int = 200,
    ):
        self.token = token
        self.client_id = client_id
        self.auth_type = auth_type
        self.exchange_segment = str(exchange_segment).strip()
        allowed = {"NSE_EQ", "NSE_FNO"}
        if self.exchange_segment not in allowed:
            print(f"🚨 20D_BAD_SEGMENT | got={self.exchange_segment} | expected one of {sorted(allowed)}")
        else:
            print(f"✅ 20D_SEGMENT_OK | seg={self.exchange_segment}")
        self.on_depth = on_depth
        self.debug = debug

        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout

        self.no_data_warn_sec = int(no_data_warn_sec)
        self.diag_print_every = int(diag_print_every)

        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._diag_thread: Optional[threading.Thread] = None

        self._stop = threading.Event()
        self._connected = threading.Event()
        self._connecting_lock = threading.Lock()

        self._pending_subs: List[Dict[str, str]] = []
        self._subscribed: Dict[int, str] = {}

        self._latest_bid: Dict[int, DepthSide] = {}
        self._latest_ask: Dict[int, DepthSide] = {}

        self._reconnect_attempt = 0
        self._cooldown_until_epoch = 0.0
        self._last_error_text = ""

        # Diagnostics
        self._connected_at = 0.0
        self._last_msg_at = 0.0
        self._bin_msg_count = 0
        self._text_msg_count = 0
        self._parse_ok_packets = 0
        self._parse_bad_packets = 0
        self._first_bin_printed = False
        self._last_sub_payload = None

    # ==================================================
    # PUBLIC API
    # ==================================================
    def connect(self):
        if self._stop.is_set():
            return
        if self._thread and self._thread.is_alive():
            return

        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

        self._diag_thread = threading.Thread(target=self._diag_loop, daemon=True)
        self._diag_thread.start()

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

        secids = []
        for it in instruments:
            secid = int(it["SecurityId"])
            tag = it.get("tag", str(secid))
            self._subscribed[secid] = tag
            secids.append(secid)

        print(f"🛰️ DEPTH_SUBSCRIBE_CALLED | connected={self._connected.is_set()} | secids={secids}")

        if not self._connected.is_set():
            print("🕓 DEPTH_SUB_QUEUED (waiting for connection)")
            self._pending_subs.extend(instruments)
            return

        self._send_subscribe(instruments)

    # ==================================================
    # INTERNAL LOOP
    # ==================================================
    def _run_loop(self):
        while not self._stop.is_set():

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
                print("❌ WS run_forever exception:", e)

            if self._stop.is_set():
                break

            self._connected.clear()
            self._reconnect_attempt += 1
            wait = min(60, 10 + self._reconnect_attempt * 5)
            print(f"⚠️ Reconnecting in {wait}s...")
            time.sleep(wait)

    def _create_ws(self):
        url = (
            f"wss://depth-api-feed.dhan.co/twentydepth"
            f"?token={self.token}&clientId={self.client_id}&authType={self.auth_type}"
        )
        print("🔗 20Depth URL:", url)

        return websocket.WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

    # ==================================================
    # DIAGNOSTIC LOOP
    # ==================================================
    def _diag_loop(self):
        while not self._stop.is_set():
            time.sleep(10)

            if not self._connected.is_set():
                continue

            last_age = (
                round(time.time() - self._last_msg_at, 1)
                if self._last_msg_at > 0
                else "NO_MSG_YET"
            )

            print(
                f"🛰️ 20D_DIAG | "
                f"bin={self._bin_msg_count} | "
                f"text={self._text_msg_count} | "
                f"ok={self._parse_ok_packets} | "
                f"bad={self._parse_bad_packets} | "
                f"last_msg_age={last_age}"
            )

            self.diag_should_warn_no_data()

    def diag_snapshot(self):
        last_age = "NO_MSG_YET" if self._last_msg_at <= 0 else round(time.time() - self._last_msg_at, 2)
        return {
            "connected": self._connected.is_set(),
            "bin": self._bin_msg_count,
            "text": self._text_msg_count,
            "ok": self._parse_ok_packets,
            "bad": self._parse_bad_packets,
            "last_msg_age": last_age,
            "seg": self.exchange_segment,
        }

    def diag_should_warn_no_data(self):
        if self._last_msg_at == 0 and (time.time() - self._connected_at) > self.no_data_warn_sec:
            print(
                "🚨 20D_NO_DATA_WARNING – CONNECTED BUT NO BINARY PACKETS"
                f" | pending_subs={len(self._pending_subs)} | subscribed={len(self._subscribed)}"
            )
            return True
        return False

    # ==================================================
    # SUBSCRIBE
    # ==================================================
    def _send_subscribe(self, instruments):
        if not self._ws:
            return

        inst_list = [
            {
                "ExchangeSegment": self.exchange_segment,  # numeric FIX
                "SecurityId": str(int(it["SecurityId"])),
            }
            for it in instruments
        ]

        payload = {
            "RequestCode": 23,
            "InstrumentCount": len(inst_list),
            "InstrumentList": inst_list,
        }

        self._last_sub_payload = payload

        print(
            f"📤 20D_SUBSCRIBE_SENT | count={len(inst_list)} | seg={self.exchange_segment} "
            f"| secids={[x['SecurityId'] for x in inst_list]}"
        )
        print("📤 20D_PAYLOAD:", payload)

        try:
            self._ws.send(json.dumps(payload))
        except Exception as e:
            print("❌ SUBSCRIBE send failed:", e)

    # ==================================================
    # HANDLERS
    # ==================================================
    def _on_open(self, ws):
        self._connected.set()
        self._connected_at = time.time()
        self._last_msg_at = 0.0
        self._bin_msg_count = 0
        self._parse_ok_packets = 0
        self._parse_bad_packets = 0
        self._first_bin_printed = False

        print("✅ 20Depth WS connected")
        print(
            f"🧷 20D_CONNECTED | seg={self.exchange_segment} | "
            f"subscribed_count={len(self._subscribed)} | pending_count={len(self._pending_subs)}"
        )

        if self._subscribed:
            subs = [{"SecurityId": str(k)} for k in self._subscribed.keys()]
            self._send_subscribe(subs)

        if self._pending_subs:
            subs = self._pending_subs[:]
            self._pending_subs.clear()
            self._send_subscribe(subs)

    def _on_error(self, ws, error):
        self._last_error_text = str(error)
        print("❌ 20Depth WS ERROR:", error)

    def _on_close(self, ws, code, msg):
        self._connected.clear()
        print("⚠️ 20Depth WS CLOSED:", code, msg)

    def _on_message(self, ws, message):
        if isinstance(message, str):
            self._text_msg_count += 1
            print("📩 20Depth TEXT:", message[:200])
            return

        self._bin_msg_count += 1
        self._last_msg_at = time.time()

        if self._bin_msg_count == 1:
            print("📥 20D_FIRST_BINARY_MSG_RECEIVED")

        if not self._first_bin_printed:
            print("📥 FIRST_BINARY_FRAME len=", len(message))
            print("HEX_HEAD:", message[:48].hex())
            self._first_bin_printed = True

        data = message
        off = 0
        n = len(data)

        while off + 2 <= n:
            pkt_len = struct.unpack_from("<H", data, off)[0]
            if pkt_len <= 0 or off + pkt_len > n:
                self._parse_bad_packets += 1
                print("⚠️ PARSE_BREAK | pkt_len=", pkt_len)
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
            secid = struct.unpack_from("<i", pkt, 4)[0]

            if len(pkt) < 332:
                self._parse_bad_packets += 1
                return

            levels = []
            off = 12
            for _ in range(20):
                price, qty, orders = struct.unpack_from("<dII", pkt, off)
                levels.append((price, qty, orders))
                off += 16

            side = DepthSide(
                prices=[x[0] for x in levels],
                qty=[x[1] for x in levels],
                orders=[x[2] for x in levels],
                ts=time.time(),
            )

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

            self._parse_ok_packets += 1

            tag = self._subscribed.get(secid, str(secid))
            if self.on_depth:
                self.on_depth(secid, tag, bid, ask)

        except Exception as e:
            self._parse_bad_packets += 1
            print("❌ PARSE_EXCEPTION:", e)
