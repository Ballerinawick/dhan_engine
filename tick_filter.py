import time
from datetime import datetime


class TickFilter:
    """
    Depth → Synthetic Tick converter

    Designed for:
    - Dhan 20Depth WS (bid / ask only)
    - No dependency on trade prints
    - Momentum-safe (price reacts first in book)
    """

    def __init__(self):
        self.prev_mid = None
        self.prev_ts = None
        self.prev_bid_qty = None
        self.prev_ask_qty = None

    def extract(self, raw_tick: dict):
        """
        raw_tick expected keys:
          - bid: DepthSide
          - ask: DepthSide
          - tag
        """

        ts = time.time()

        bid = raw_tick.get("bid")
        ask = raw_tick.get("ask")

        if not bid or not ask:
            return None

        # ---- TOP OF BOOK ----
        bid_price = float(bid.prices[0])
        ask_price = float(ask.prices[0])
        bid_qty = int(bid.qty[0])
        ask_qty = int(ask.qty[0])

        if bid_price <= 0 or ask_price <= 0:
            return None

        # ---- SYNTHETIC PRICE ----
        mid = (bid_price + ask_price) / 2.0

        # ---- PRICE SPEED ----
        price_speed = 0.0
        if self.prev_mid is not None and self.prev_ts is not None:
            dt = max(0.001, ts - self.prev_ts)
            price_speed = (mid - self.prev_mid) / dt

        # ---- SYNTHETIC VOLUME (QUEUE CHANGE) ----
        vol = 0
        if self.prev_bid_qty is not None and self.prev_ask_qty is not None:
            vol = abs(bid_qty - self.prev_bid_qty) + abs(ask_qty - self.prev_ask_qty)

        self.prev_mid = mid
        self.prev_ts = ts
        self.prev_bid_qty = bid_qty
        self.prev_ask_qty = ask_qty

        return {
            "ts": datetime.utcnow(),

            # core price
            "ltp": mid,
            "bid_price": bid_price,
            "ask_price": ask_price,
            "spread": ask_price - bid_price,

            # order flow
            "bid_qty": bid_qty,
            "ask_qty": ask_qty,
            "volume": vol,

            # momentum primitives
            "price_speed": price_speed,

            # tags
            "tag": raw_tick.get("tag"),
        }