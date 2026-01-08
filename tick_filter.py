# tick_filter.py
from datetime import datetime

class TickFilter:
    """
    Works for:
    - Dhan WS ticks (depth: {buy/sell})
    - Dhan OptionChain legs (top_bid_price/top_ask_price etc.)
    Ensures Greeks + IV + OI + volume pass through to your pipeline.
    """

    def __init__(self):
        self.prev_volume = None
        self.prev_oi = None

    def extract(self, raw_tick: dict):
        # ---- timestamp (keep utc for stable math) ----
        ts = datetime.utcnow()

        # -----------------------------
        # (A) LTP
        # -----------------------------
        ltp = float(raw_tick.get("last_price", 0.0) or 0.0)

        # -----------------------------
        # (B) BID/ASK (support both WS & OptionChain)
        # -----------------------------
        bid_price = bid_qty = ask_price = ask_qty = 0.0

        depth = raw_tick.get("depth") or {}
        buy = depth.get("buy") or []
        sell = depth.get("sell") or []

        if buy or sell:
            # WS-style depth
            bid_price = float(buy[0]["price"]) if buy else 0.0
            bid_qty   = float(buy[0]["quantity"]) if buy else 0.0

            ask_price = float(sell[0]["price"]) if sell else 0.0
            ask_qty   = float(sell[0]["quantity"]) if sell else 0.0
        else:
            # OptionChain-style top of book
            bid_price = float(raw_tick.get("top_bid_price", 0.0) or 0.0)
            ask_price = float(raw_tick.get("top_ask_price", 0.0) or 0.0)
            bid_qty   = float(raw_tick.get("top_bid_quantity", 0.0) or 0.0)
            ask_qty   = float(raw_tick.get("top_ask_quantity", 0.0) or 0.0)

        spread = (ask_price - bid_price) if (bid_price > 0 and ask_price > 0) else 0.0

        # -----------------------------
        # (C) Volume / OI + deltas
        # -----------------------------
        volume = int(raw_tick.get("volume", 0) or 0)
        oi     = int(raw_tick.get("oi", 0) or 0)

        last_traded_qty = 0
        if self.prev_volume is not None and volume >= self.prev_volume:
            last_traded_qty = volume - self.prev_volume
        self.prev_volume = volume

        oi_change = 0
        if self.prev_oi is not None:
            oi_change = oi - self.prev_oi
        self.prev_oi = oi

        # -----------------------------
        # (D) Greeks / IV (pass-through)
        # -----------------------------
        delta = float(raw_tick.get("delta", 0.0) or 0.0)
        gamma = float(raw_tick.get("gamma", 0.0) or 0.0)
        theta = float(raw_tick.get("theta", 0.0) or 0.0)
        vega  = float(raw_tick.get("vega", 0.0) or 0.0)
        iv    = float(raw_tick.get("implied_volatility", 0.0) or 0.0)

        # -----------------------------
        # (E) Optional metadata (expiry / underlying)
        # -----------------------------
        expiry = raw_tick.get("expiry")              # "YYYY-MM-DD"
        underlying_ltp = float(raw_tick.get("underlying_ltp", 0.0) or 0.0)

        # For WS ticks these exist:
        buy_qty = int(raw_tick.get("buy_quantity", 0) or 0)
        sell_qty = int(raw_tick.get("sell_quantity", 0) or 0)

        return {
            "ts": ts,
            "ltp": ltp,

            "bid_price": bid_price,
            "bid_qty": bid_qty,
            "ask_price": ask_price,
            "ask_qty": ask_qty,
            "spread": spread,

            "volume": volume,
            "oi": oi,
            "oi_change": oi_change,
            "last_traded_qty": last_traded_qty,

            "buy_qty": buy_qty,
            "sell_qty": sell_qty,

            "delta": delta,
            "gamma": gamma,
            "theta": theta,
            "vega": vega,
            "iv": iv,

            "expiry": expiry,
            "underlying_ltp": underlying_ltp,
        }
