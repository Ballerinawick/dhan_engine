import asyncio
import time
import threading
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

from dhan_engine.infrastructure.dhan.full_depth import FullDepth


@dataclass
class DepthSide:
    prices: List[float]
    qty: List[int]
    orders: List[int]
    ts: float


class DhanAsyncDepthAdapter:
    def __init__(
        self,
        client_id: str,
        token: str,
        exchange_segment: str,
        on_depth: Optional[Callable[[int, str, DepthSide, DepthSide], None]] = None,
    ):
        self.client_id = str(client_id)
        self.token = str(token)
        self.exchange_segment = str(exchange_segment)
        self.on_depth = on_depth

        print("🚀 DEPTH_ADAPTER_INITIALIZING")
        self.full_depth = FullDepth(client_id=self.client_id, access_token=self.token)

        self._latest_bid: Dict[int, Tuple[List[float], List[int], List[int]]] = {}
        self._latest_ask: Dict[int, Tuple[List[float], List[int], List[int]]] = {}
        self._secid_tag_map: Dict[int, str] = {}
        self._loop = None
        self._connected_evt = threading.Event()

        self._first_packet_logged = False
        self._first_pair_logged = False
        self._first_parsed_logged = False

    def start(self):
        threading.Thread(
            target=lambda: asyncio.run(self._run()),
            daemon=True,
        ).start()

    def subscribe(self, instruments: List[Tuple[str, str, str]]):
        if not instruments:
            return

        secids = []
        broker_instruments = []
        for exchange_segment, secid, tag in instruments:
            secid_int = int(secid)
            self._secid_tag_map[secid_int] = tag
            secids.append(secid_int)
            broker_instruments.append((exchange_segment, secid))

        print("📤 ASYNC_20DEPTH_SUBSCRIBED | secids=", secids)

        if not self._connected_evt.wait(timeout=5):
            print("⚠️ ASYNC_SUBSCRIBE_BEFORE_CONNECTED")
            return

        if not self._loop:
            print("❌ ASYNC_NO_LOOP_FOR_SUBSCRIBE")
            return

        asyncio.run_coroutine_threadsafe(
            self.full_depth.subscribe_async(broker_instruments),
            self._loop,
        )

    async def _run(self):
        self._loop = asyncio.get_running_loop()
        await self.full_depth.connect()
        print("✅ ASYNC_20DEPTH_CONNECTED")
        self._connected_evt.set()

        while True:
            async for update in self.full_depth.get_instrument_data():
                if not self._first_packet_logged:
                    self._first_packet_logged = True
                    print("📥 ASYNC_FIRST_PACKET_RECEIVED")
                self._process_update(update)

    @staticmethod
    def _pick_msg_code(update: dict) -> Optional[int]:
        for key in ("msg_code", "message_code", "feed_response_code", "code", "MessageCode"):
            if key in update:
                try:
                    return int(update[key])
                except Exception:
                    return None
        return None

    @staticmethod
    def _pick_secid(update: dict) -> Optional[int]:
        for key in ("security_id", "SecurityId", "secid", "securityId"):
            if key in update:
                try:
                    return int(update[key])
                except Exception:
                    return None
        return None

    @staticmethod
    def _pick_levels(update: dict) -> Optional[Tuple[List[float], List[int], List[int]]]:
        levels = None
        for key in ("levels", "depth", "book", "data", "Depth"):
            if key in update and isinstance(update[key], list):
                levels = update[key]
                break
        if levels is None:
            return None

        prices: List[float] = []
        qty: List[int] = []
        orders: List[int] = []

        for level in levels:
            if not isinstance(level, dict):
                continue
            px = level.get("price", level.get("Price", 0.0))
            q = level.get("qty", level.get("quantity", level.get("Qty", 0)))
            o = level.get("orders", level.get("Orders", 0))
            prices.append(float(px or 0.0))
            qty.append(int(q or 0))
            orders.append(int(o or 0))

        if not prices:
            return None

        return prices, qty, orders

    def _process_update(self, update):
        if isinstance(update, list):
            for item in update:
                self._process_update(item)
            return

        if not isinstance(update, dict):
            return

        msg_code = self._pick_msg_code(update)
        secid = self._pick_secid(update)
        levels = self._pick_levels(update)

        if not self._first_parsed_logged and msg_code is not None and secid is not None:
            self._first_parsed_logged = True
            print(f"🧭 ASYNC_FIRST_PARSED msg_code={msg_code} secid={secid}")

        if msg_code is None or secid is None or levels is None:
            return

        if msg_code == 41:
            self._latest_bid[secid] = levels
        elif msg_code == 51:
            self._latest_ask[secid] = levels
        else:
            return

        bid = self._latest_bid.get(secid)
        ask = self._latest_ask.get(secid)
        if bid is None or ask is None:
            return

        bid_side = DepthSide(prices=bid[0], qty=bid[1], orders=bid[2], ts=time.time())
        ask_side = DepthSide(prices=ask[0], qty=ask[1], orders=ask[2], ts=time.time())

        if not self._first_pair_logged:
            self._first_pair_logged = True
            print("🔥 ASYNC_DEPTH_STREAM_ACTIVE")

        if self.on_depth:
            tag = self._secid_tag_map.get(secid, str(secid))
            try:
                self.on_depth(secid, tag, bid_side, ask_side)
            except Exception as exc:
                print(f"âŒ ASYNC_DEPTH_CALLBACK_ERROR | secid={secid} | tag={tag} | error={exc}")
