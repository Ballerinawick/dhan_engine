import asyncio
import json
import os
import struct
import threading
import time
from typing import Dict, List, Optional, Tuple

import websocket


TWENTY_DEPTH_WSS = "wss://depth-api-feed.dhan.co/twentydepth"
TWO_HUNDRED_DEPTH_WSS = "wss://full-depth-api.dhan.co/twohundreddepth"


class FullDepth:
    def __init__(self, client_id, access_token, levels: int = 20):
        self.client_id = str(client_id)
        self.access_token = str(access_token)
        if int(levels) not in (20, 200):
            raise ValueError("levels must be 20 or 200")
        self.levels = int(levels)

        self._subscribed: List[Tuple[str, str]] = []
        self._sub_keys = set()
        self._subscription_lock = threading.Lock()
        self._send_lock = threading.Lock()

        self._ws: Optional[websocket.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._connected_evt = threading.Event()
        self._stop_evt = threading.Event()

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._queue: Optional[asyncio.Queue] = None
        self._latest_payload_by_key: Dict[object, dict] = {}
        self._queued_keys = set()
        self._queue_max_size = max(4, int(os.getenv("DEPTH_QUEUE_MAX_SIZE", "64") or 64))

        self._first_binary_logged = False
        self._last_message_ts = 0.0
        self._connected_ts = 0.0
        self._connection_seq = 0
        self._connection_id = ""
        self._last_queue_health_ts = 0.0
        self._queue_health_interval_sec = max(
            1.0,
            float(os.getenv("DEPTH_QUEUE_HEALTH_INTERVAL_SEC", "10") or 10),
        )
        self._queue_high_watermark = 0
        self._dropped_payload_count = 0

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
        endpoint = TWO_HUNDRED_DEPTH_WSS if self.levels == 200 else TWENTY_DEPTH_WSS
        return (
            f"{endpoint}?token={self.access_token}"
            f"&clientId={self.client_id}&authType=2"
        )

    async def connect(self):
        loop = asyncio.get_running_loop()
        if self._loop is None or self._loop.is_closed():
            self._loop = loop
        if self._queue is None:
            self._queue = asyncio.Queue(maxsize=self._queue_max_size)

        if self._ws_thread and self._ws_thread.is_alive():
            print(
                "DUPLICATE_THREAD_WARNING | feed=DEPTH | "
                f"thread={self._ws_thread.name} | active_thread_count={threading.active_count()}"
            )
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
        new_items = self._merge_subscriptions(instruments)
        if not self._subscribed:
            return
        if not new_items:
            return

        was_connected = self._connected_evt.is_set()
        connected = await self.connect()
        if not connected:
            print("WARNING DEPTH subscription send skipped because socket is not connected yet")
            return
        if not was_connected:
            return

        sent = await asyncio.to_thread(self._send_subscription_now, new_items)
        if not sent:
            print("WARNING DEPTH subscription send skipped because socket is not connected yet")

    def subscribe(self, instruments):
        incoming = list(instruments or [])
        if not incoming:
            return

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.subscribe_async(incoming))
        except RuntimeError:
            print("WARNING subscribe() called without asyncio loop")

    def _merge_subscriptions(self, instruments) -> List[Tuple[str, str]]:
        new_items: List[Tuple[str, str]] = []
        duplicate_count = 0
        with self._subscription_lock:
            for seg, secid in instruments or []:
                normalized = self._normalize_exchange_segment(seg)
                key = (normalized, str(secid))
                if key in self._sub_keys:
                    duplicate_count += 1
                    continue
                self._sub_keys.add(key)
                self._subscribed.append(key)
                new_items.append(key)
                if self.levels == 200 and len(self._subscribed) > 1:
                    self._subscribed.pop()
                    self._sub_keys.remove(key)
                    raise ValueError("Dhan 200-depth permits exactly one instrument per websocket")
            total_subscribed = len(self._subscribed)
            total_unique = len(self._sub_keys)
        if duplicate_count:
            print(
                "DUPLICATE_SUBSCRIPTION_WARNING | feed=DEPTH | "
                f"duplicates={duplicate_count} | subscribed_instrument_count={total_subscribed} | "
                f"unique_subscribed_instrument_keys={total_unique}"
            )
        return new_items

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
                key = await asyncio.wait_for(self._queue.get(), timeout=65.0)
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

            self._queued_keys.discard(key)
            item = self._latest_payload_by_key.pop(key, None)
            if item is not None:
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

            print(
                "WS_RECONNECT | feed=DEPTH | "
                f"connection_id={self._connection_id or 'unknown'} | reconnect_count={self._connection_seq}"
            )
            time.sleep(2.0)

    def _on_open(self, ws):
        self._connected_evt.set()
        self._connected_ts = time.time()
        self._connection_seq += 1
        self._connection_id = f"DEPTH-{id(self)}-{self._connection_seq}"
        print(
            "WS_CONNECTION_OPENED | feed=DEPTH | "
            f"connection_id={self._connection_id} | reconnect_count={self._connection_seq - 1} | "
            f"active_thread_count={threading.active_count()}"
        )
        print("DEPTH WS CONNECTED")

        if self._subscribed:
            self._send_subscription_now(self._subscribed)

    def _on_message(self, ws, message):
        received_ts = time.time()
        received_mono = time.monotonic()
        self._last_message_ts = received_ts

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

        packets = self._parse_binary_message(message, expected_levels=self.levels)
        if not packets:
            print(f"WARNING DEPTH binary frame parsed empty | size={len(message)}")
            return

        for packet in packets:
            packet["received_ts"] = received_ts
            packet["received_mono"] = received_mono
            self._push_async(packet)

    def _on_error(self, ws, error):
        print(f"ERROR DEPTH WS error: {error}")

    def _on_close(self, ws, code, reason):
        self._connected_evt.clear()
        self._connected_ts = 0.0
        print(f"WARNING DEPTH WS closed | code={code} | reason={reason}")

    def _push_async(self, payload):
        if self._loop is None or self._queue is None or self._loop.is_closed():
            self._dropped_payload_count += 1
            return
        self._loop.call_soon_threadsafe(self._enqueue_latest, payload)

    def _enqueue_latest(self, payload):
        """Keep only the latest pending depth update per instrument."""
        if self._queue is None:
            self._dropped_payload_count += 1
            return

        security_id = payload.get("security_id") if isinstance(payload, dict) else None
        msg_code = payload.get("msg_code") if isinstance(payload, dict) else None
        if security_id is not None and msg_code in (41, 51):
            # Dhan publishes bid and ask books as separate packets. Preserve one
            # latest queue slot per side so a fresh ask cannot replace a pending
            # bid (or vice versa) before the adapter pairs them.
            key = ("security_side", int(security_id), int(msg_code))
        elif security_id is not None:
            key = ("security", int(security_id))
        else:
            key = ("control", 0)
        if key in self._queued_keys:
            self._latest_payload_by_key[key] = payload
            self._dropped_payload_count += 1
        else:
            if self._queue.full():
                try:
                    oldest_key = self._queue.get_nowait()
                    self._queued_keys.discard(oldest_key)
                    self._latest_payload_by_key.pop(oldest_key, None)
                    self._dropped_payload_count += 1
                except asyncio.QueueEmpty:
                    pass
            self._latest_payload_by_key[key] = payload
            self._queued_keys.add(key)
            self._queue.put_nowait(key)

        try:
            queue_size = int(self._queue.qsize())
        except Exception:
            queue_size = -1
        if queue_size > self._queue_high_watermark:
            self._queue_high_watermark = queue_size
        now = time.time()
        if now - self._last_queue_health_ts >= self._queue_health_interval_sec:
            self._last_queue_health_ts = now
            print(
                "QUEUE_HEALTH | feed=DEPTH | "
                f"connection_id={self._connection_id or 'pending'} | queue_size={queue_size} | "
                f"queue_max_size={self._queue_max_size} | queue_high_watermark={self._queue_high_watermark} | "
                f"dropped_tick_count={self._dropped_payload_count}"
            )

    def _send_subscription_now(self, instruments) -> bool:
        if not instruments:
            return False
        unique_keys = {
            (self._normalize_exchange_segment(seg), str(secid))
            for seg, secid in instruments
        }
        with self._subscription_lock:
            total_subscribed = len(self._subscribed)
            total_unique = len(self._sub_keys)
        print(
            "WS_SUBSCRIPTION_SNAPSHOT | feed=DEPTH | "
            f"connection_id={self._connection_id or 'pending'} | "
            f"subscribed_instrument_count={total_subscribed} | "
            f"message_instrument_count={len(instruments)} | unique_message_keys={len(unique_keys)} | "
            f"unique_subscribed_instrument_keys={total_unique}"
        )

        if self.levels == 200:
            if len(instruments) != 1:
                raise ValueError("Dhan 200-depth subscription requires one instrument")
            seg, secid = instruments[0]
            payload = {
                "RequestCode": 23,
                "ExchangeSegment": self._normalize_exchange_segment(seg),
                "SecurityId": str(secid),
            }
        else:
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
    def _parse_binary_message(data: bytes, expected_levels: int = 20) -> List[Dict]:
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
            decoded = FullDepth._parse_packet(packet, expected_levels=expected_levels)
            if decoded:
                packets.append(decoded)

            offset += packet_len

        return packets

    @staticmethod
    def _parse_packet(packet: bytes, expected_levels: int = 20) -> Optional[Dict]:
        if len(packet) < 12 + 16:
            return None

        _, msg_code, exchange_segment, security_id, header_value = struct.unpack_from(
            "<HBBiI", packet, 0
        )

        if msg_code not in (41, 51):
            return None

        available_rows = (len(packet) - 12) // 16
        row_count = int(header_value) if expected_levels == 200 else available_rows
        if row_count <= 0 or row_count > available_rows:
            return None
        levels = []
        offset = 12

        while len(levels) < row_count and offset + 16 <= len(packet):
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
            "level_count": len(levels),
        }

