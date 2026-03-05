import asyncio
import json
import struct
from typing import Dict, List, Optional, Tuple

import websockets


depth_feed_wss = "wss://depth-api-feed.dhan.co/twentydepth"


class FullDepth:

    def __init__(self, client_id, access_token):
        self.client_id = client_id
        self.access_token = access_token
        self.ws = None
        self._subscribed: List[Tuple[int, str]] = []
        self._lock = asyncio.Lock()

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

    async def connect(self):

        if self.ws is None or getattr(self.ws, "close_code", None) is not None:

            url = (
                f"{depth_feed_wss}?token={self.access_token}"
                f"&clientId={self.client_id}&authType=2"
            )

            print("🌐 CONNECTING DEPTH WS")

            self.ws = await websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20
            )

            print("✅ DEPTH WS CONNECTED")

            if self._subscribed:
                await self._send_subscription(self._subscribed)

        else:
            try:
                await self.ws.ping()
            except websockets.ConnectionClosed:
                print("⚠️ WS ping failed, reconnecting")
                self.ws = None
                await self.connect()

    def subscribe(self, instruments):

        self._subscribed = list(instruments or [])

        if not self._subscribed:
            return

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._send_subscription(self._subscribed))
        except RuntimeError:
            print("⚠️ subscribe() called without asyncio loop")

    async def subscribe_async(self, instruments):

        self._subscribed = list(instruments or [])

        if not self._subscribed:
            return

        await self._send_subscription(self._subscribed)

    async def _send_subscription(self, instruments):

        if not instruments:
            return

        async with self._lock:

            if self.ws is None or getattr(self.ws, "close_code", None) is not None:
                return

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

            print(
                "📨 DEPTH_SUB_PAYLOAD",
                json.dumps(payload, separators=(",", ":"))
            )

            await self.ws.send(json.dumps(payload))

            print("✅ SUB_SENT_OK")

    async def disconnect(self):

        if self.ws:
            await self.ws.close()
            self.ws = None

    async def get_instrument_data(self):

        while True:

            if self.ws is None:
                await self.connect()

            try:
                data = await asyncio.wait_for(self.ws.recv(), timeout=30)

            except asyncio.TimeoutError:
                print("⚠️ WS TIMEOUT — reconnecting")
                self.ws = None
                continue

            except websockets.ConnectionClosed:
                print("⚠️ WS CLOSED — reconnecting")
                self.ws = None
                continue

            if isinstance(data, str):

                try:
                    yield json.loads(data)
                except Exception:
                    continue

            else:

                print(f"📥 BIN_FRAME_RX size={len(data)}")

                packets = self._parse_binary_message(data)

                for packet in packets:
                    yield packet

    @staticmethod
    def _parse_binary_message(data: bytes) -> List[Dict]:

        packets: List[Dict] = []

        i = 0
        n = len(data)

        while i + 2 <= n:

            (packet_len,) = struct.unpack_from("<h", data, i)

            if packet_len <= 0 or i + packet_len > n:
                break

            packet = data[i: i + packet_len]

            decoded = FullDepth._parse_packet(packet)

            if decoded:
                packets.append(decoded)

            i += packet_len

        return packets

    @staticmethod
    def _parse_packet(packet: bytes) -> Optional[Dict]:

        if len(packet) < 12 + 16:
            return None

        _, msg_code, exchange_segment, security_id, _ = struct.unpack_from(
            "<hBBiI", packet, 0
        )

        if msg_code not in (41, 51):
            return None

        levels = []

        off = 12

        while off + 16 <= len(packet):

            price = struct.unpack_from("<d", packet, off)[0]
            qty = struct.unpack_from("<I", packet, off + 8)[0]
            orders = struct.unpack_from("<I", packet, off + 12)[0]

            levels.append(
                {
                    "price": float(price),
                    "qty": int(qty),
                    "orders": int(orders),
                }
            )

            off += 16

        return {
            "msg_code": int(msg_code),
            "exchange_segment": int(exchange_segment),
            "security_id": int(security_id),
            "levels": levels,
        }