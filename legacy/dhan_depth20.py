# dhan_depth20.py
import json
import struct
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import websocket  # pip install websocket-client


@dataclass
class DepthLevel:
    price: float
    qty: int
    orders: int


@dataclass
class DepthSnapshot:
    security_id: int
    exchange_segment: int
    side: str  # "BID" or "ASK"
    levels: List[DepthLevel]
    ts: float


class Depth20Book:
    """
    Maintains latest 20-level book per security_id and derives features.
    """
    def __init__(self):
        self.bids: Dict[int, List[DepthLevel]] = {}
        self.asks: Dict[int, List[DepthLevel]] = {}

        # for depth-flow approx
        self.prev_sum_bid5: Dict[int, int] = {}
        self.prev_sum_ask5: Dict[int, int] = {}

    def update(self, snap: DepthSnapshot) -> Optional[dict]:
        if snap.side == "BID":
            self.bids[snap.security_id] = snap.levels
        else:
            self.asks[snap.security_id] = snap.levels

        # only emit features when we have BOTH sides
        if snap.security_id not in self.bids or snap.security_id not in self.asks:
            return None

        bids = self.bids[snap.security_id]
        asks = self.asks[snap.security_id]
        if not bids or not asks:
            return None

        def sums(levels: List[DepthLevel], n: int) -> int:
            return int(sum(x.qty for x in levels[:n]))

        best_bid = bids[0].price
        best_ask = asks[0].price
        best_bid_qty = bids[0].qty
        best_ask_qty = asks[0].qty

        sum_bid5 = sums(bids, 5)
        sum_ask5 = sums(asks, 5)
        sum_bid20 = sums(bids, 20)
        sum_ask20 = sums(asks, 20)

        def imbalance(a: int, b: int) -> float:
            den = (a + b)
            return round((a - b) / den, 4) if den else 0.0

        imb5 = imbalance(sum_bid5, sum_ask5)
        imb20 = imbalance(sum_bid20, sum_ask20)

        # microprice (weighted mid based on queue)
        denom = (best_bid_qty + best_ask_qty)
        microprice = round(
            ((best_ask * best_bid_qty) + (best_bid * best_ask_qty)) / denom, 4
        ) if denom else round((best_bid + best_ask) / 2.0, 4)

        # “vacuum” using top-3 queue (much more stable than single level)
        top3_bid = sums(bids, 3)
        top3_ask = sums(asks, 3)
        vacuum_bid = top3_bid < 150  # tune later
        vacuum_ask = top3_ask < 150  # tune later
        vacuum_flag = vacuum_bid or vacuum_ask

        # depth-flow approx: change in queue over last update
        prev_b = self.prev_sum_bid5.get(snap.security_id, sum_bid5)
        prev_a = self.prev_sum_ask5.get(snap.security_id, sum_ask5)
        self.prev_sum_bid5[snap.security_id] = sum_bid5
        self.prev_sum_ask5[snap.security_id] = sum_ask5

        depth_flow = (sum_bid5 - prev_b) - (sum_ask5 - prev_a)

        return {
            "security_id": snap.security_id,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "best_bid_qty": best_bid_qty,
            "best_ask_qty": best_ask_qty,
            "sum_bid5": sum_bid5,
            "sum_ask5": sum_ask5,
            "sum_bid20": sum_bid20,
            "sum_ask20": sum_ask20,
            "imbalance_5": imb5,
            "imbalance_20": imb20,
            "microprice": microprice,
            "vacuum_flag": vacuum_flag,
            "depth_flow": float(depth_flow),
            "ts": snap.ts,
        }


def _parse_one_packet(packet: bytes) -> Optional[DepthSnapshot]:
    """
    Header is 12 bytes:
      int16 length,
      byte feed_response_code,
      byte exchange_segment,
      int32 security_id,
      uint32 seq/noRows

    For 20 depth, payload is 320 bytes = 20 * 16.
    Each row: float64 price, uint32 qty, uint32 orders
    """
    if len(packet) < 12 + 320:
        return None

    # NOTE: Endianness is not explicitly mentioned in doc.
    # Dhan feeds are typically little-endian; we also sanity-check prices.
    length, code, seg, sec_id, seq = struct.unpack_from("<hBBiI", packet, 0)

    # 41=Bid, 51=Ask per doc
    if code == 41:
        side = "BID"
    elif code == 51:
        side = "ASK"
    else:
        return None

    levels: List[DepthLevel] = []
    off = 12
    for i in range(20):
        price = struct.unpack_from("<d", packet, off + (i * 16))[0]
        qty = struct.unpack_from("<I", packet, off + (i * 16) + 8)[0]
        orders = struct.unpack_from("<I", packet, off + (i * 16) + 12)[0]

        # basic sanity: ignore crazy values
        if price <= 0 or price > 10_00_000:
            continue

        levels.append(DepthLevel(price=float(price), qty=int(qty), orders=int(orders)))

    if not levels:
        return None

    return DepthSnapshot(
        security_id=int(sec_id),
        exchange_segment=int(seg),
        side=side,
        levels=levels,
        ts=time.time(),
    )


def parse_stacked_message(data: bytes) -> List[DepthSnapshot]:
    """
    Doc says: multiple packets can be stacked back-to-back, so break by length.
    Length is first int16 in each packet header.
    """
    snaps: List[DepthSnapshot] = []
    i = 0
    n = len(data)

    while i + 2 <= n:
        (pkt_len,) = struct.unpack_from("<h", data, i)
        if pkt_len <= 0:
            break
        if i + pkt_len > n:
            # partial packet (wait for next frame)
            break

        pkt = data[i : i + pkt_len]
        snap = _parse_one_packet(pkt)
        if snap:
            snaps.append(snap)

        i += pkt_len

    return snaps


class DhanDepth20Client:
    def __init__(self, token: str, client_id: str):
        self.token = token
        self.client_id = client_id
        self.ws: Optional[websocket.WebSocketApp] = None
        self.book = Depth20Book()

        # latest features by security_id
        self.latest: Dict[int, dict] = {}

    def connect(self):
        url = (
            f"wss://depth-api-feed.dhan.co/twentydepth"
            f"?token={self.token}&clientId={self.client_id}&authType=2"
        )
        self.ws = websocket.WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self.ws.run_forever(ping_interval=20, ping_timeout=10)

    def subscribe_many(self, instruments: List[dict]):
        """
        instruments item example:
        {"ExchangeSegment": "NSE_FNO", "SecurityId": "12345"}
        """
        if not self.ws:
            return
        payload = {
            "RequestCode": 23,
            "InstrumentCount": len(instruments),
            "InstrumentList": instruments,
        }
        self.ws.send(json.dumps(payload))

    def _on_open(self, ws):
        print("✅ Depth20 WS connected")

    def _on_message(self, ws, message):
        if isinstance(message, str):
            # depth feed responses are binary, but keep safe
            return

        snaps = parse_stacked_message(message)
        for s in snaps:
            feat = self.book.update(s)
            if feat:
                self.latest[feat["security_id"]] = feat

    def _on_error(self, ws, error):
        print("❌ Depth20 WS error:", error)

    def _on_close(self, ws, code, reason):
        print(f"⚠️ Depth20 WS closed | code={code} reason={reason}")
