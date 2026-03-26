import asyncio
import json
import struct
import threading
import time
from typing import Dict, List, Optional, Tuple

import websocket


depth_feed_wss = "wss://depth-api-feed.dhan.co/twentydepth"


class FullDepth:
    def __init__(self, client_id, access_token):
        self.client_id = str(client_id)
        self.access_token = str(access_token)

        self._subscribed: List[Tuple[int, str]] = []
        self._send_lock = threading.Lock()

        self._ws: Optional[websocket.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._connected_evt = threading.Event()
        self._stop_evt = threading.Event()

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._queue: Optional[asyncio.Queue] = None

        self._first_binary_logged = False
        self._last_message_ts = 0.0
        self._connected_ts = 0.0

    @staticmethod
    def _normalize_exchange_segment(segment) -> str:
        seg_text = str(segment).strip().upper()

        segment_map = {
            "1": "NSE_EQ",
            "2": "NSE_FNO",
            "NSE_EQ": "NSE_EQ",
            "NSE_FNO": "NSE_FNO",
        }

        return segment_map.get(seg_text, seg_text)

    def _url(self) -> str:
        return (
            f"{depth_feed_wss}?token={self.access_token}"
            f"&clientId={self.client_id}&authType=2"
        )

    async def connect(self):
        loop = asyncio.get_running_loop()
        if self._loop is None or self._loop.is_closed():
            self._loop = loop
        if self._queue is None:
            self._queue = asyncio.Queue()

        if self._ws_thread and self._ws_thread.is_alive():
            if not self._connected_evt.is_set():
                connected = await asyncio.to_thread(self._connected_evt.wait, 8.0)
                if not connected:
                    print("WARNING DEPTH WS connect wait timed out")
                return connected
            return True

        self._stop_evt.clear()
        self._connected_evt.clear()
        self._ws_thread = threading.Thread(
            target=self._run_socket_loop,
            name="FullDepthWS",
            daemon=True,
        )
        self._ws_thread.start()

        connected = await asyncio.to_thread(self._connected_evt.wait, 8.0)
        if not connected:
            print("WARNING DEPTH WS connect wait timed out")
        return connected

    async def subscribe_async(self, instruments):
        self._subscribed = list(instruments or [])
        if not self._subscribed:
            return

        await self.connect()

        sent = await asyncio.to_thread(self._send_subscription_now, self._subscribed)
        if not sent:
            print("WARNING DEPTH subscription send skipped because socket is not connected yet")

    def subscribe(self, instruments):
        self._subscribed = list(instruments or [])
        if not self._subscribed:
            return

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.subscribe_async(self._subscribed))
        except RuntimeError:
            print("WARNING subscribe() called without asyncio loop")

    async def disconnect(self):
        self._stop_evt.set()
        ws = self._ws
        if ws is not None:
            await asyncio.to_thread(ws.close)
        self._connected_evt.clear()

    async def get_instrument_data(self):
        await self.connect()

        while True:
            if self._queue is None:
                await asyncio.sleep(0.1)
                continue

            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=65.0)
            except asyncio.TimeoutError:
                idle_anchor = self._last_message_ts or self._connected_ts
                idle_for = time.time() - idle_anchor if idle_anchor else 0.0
                if self._connected_evt.is_set():
                    print(
                        "WARNING DEPTH feed idle"
                        f" | connected=True | idle_for={idle_for:.1f}s"
                    )
                    continue

                print("WARNING DEPTH WS disconnected while waiting for data, reconnecting")
                await self.connect()
                continue

            yield item

    def _run_socket_loop(self):
        while not self._stop_evt.is_set():
            print("CONNECTING DEPTH WS")

            ws = websocket.WebSocketApp(
                self._url(),
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._ws = ws

            try:
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                print(f"ERROR DEPTH WS exception: {exc}")
            finally:
                self._connected_evt.clear()
                self._ws = None

            if self._stop_evt.is_set():
                break

            time.sleep(2.0)

    def _on_open(self, ws):
        self._connected_evt.set()
        self._connected_ts = time.time()
        print("DEPTH WS CONNECTED")

        if self._subscribed:
            self._send_subscription_now(self._subscribed)

    def _on_message(self, ws, message):
        self._last_message_ts = time.time()

        if isinstance(message, str):
            text = message.strip()
            if text:
                print(f"DEPTH WS TEXT {text}")
                try:
                    payload = json.loads(text)
                except Exception:
                    return
                self._push_async(payload)
            return

        if not self._first_binary_logged:
            self._first_binary_logged = True
            print(f"DEPTH WS FIRST_BINARY size={len(message)}")

        packets = self._parse_binary_message(message)
        if not packets:
            print(f"WARNING DEPTH binary frame parsed empty | size={len(message)}")
            return

        for packet in packets:
            self._push_async(packet)

    def _on_error(self, ws, error):
        print(f"ERROR DEPTH WS error: {error}")

    def _on_close(self, ws, code, reason):
        self._connected_evt.clear()
        self._connected_ts = 0.0
        print(f"WARNING DEPTH WS closed | code={code} | reason={reason}")

    def _push_async(self, payload):
        if self._loop is None or self._queue is None or self._loop.is_closed():
            return
        self._loop.call_soon_threadsafe(self._queue.put_nowait, payload)

    def _send_subscription_now(self, instruments) -> bool:
        if not instruments:
            return False

        payload = {
            "RequestCode": 23,
            "InstrumentCount": len(instruments),
            "InstrumentList": [
                {
                    "ExchangeSegment": self._normalize_exchange_segment(seg),
                    "SecurityId": str(secid),
                }
                for seg, secid in instruments
            ],
        }

        print("DEPTH_SUB_PAYLOAD", json.dumps(payload, separators=(",", ":")))

        with self._send_lock:
            if self._ws is None or not self._connected_evt.is_set():
                return False

            try:
                self._ws.send(json.dumps(payload))
                print("SUB_SENT_OK")
                return True
            except Exception as exc:
                print(f"ERROR DEPTH subscription send failed: {exc}")
                return False

    @staticmethod
    def _parse_binary_message(data: bytes) -> List[Dict]:
        packets: List[Dict] = []
        offset = 0
        total = len(data)

        while offset + 12 <= total:
            (packet_len,) = struct.unpack_from("<H", data, offset)
            if packet_len < 12 + 16:
                break
            if offset + packet_len > total:
                break

            packet = data[offset: offset + packet_len]
            decoded = FullDepth._parse_packet(packet)
            if decoded:
                packets.append(decoded)

            offset += packet_len

        return packets

    @staticmethod
    def _parse_packet(packet: bytes) -> Optional[Dict]:
        if len(packet) < 12 + 16:
            return None

        _, msg_code, exchange_segment, security_id, _ = struct.unpack_from(
            "<HBBiI", packet, 0
        )

        if msg_code not in (41, 51):
            return None

        levels = []
        offset = 12

        while offset + 16 <= len(packet):
            price, qty, orders = struct.unpack_from("<dII", packet, offset)
            levels.append(
                {
                    "price": float(price),
                    "qty": int(qty),
                    "orders": int(orders),
                }
            )
            offset += 16

        return {
            "msg_code": int(msg_code),
            "exchange_segment": int(exchange_segment),
            "security_id": int(security_id),
            "levels": levels,
        }
