# quant_processor.py
from collections import deque

class QuantProcessor:
    """
    Supports both:
    - True flow (WS buy_qty/sell_qty)
    - Synthetic flow (OptionChain) using bid/ask qty change (top of book)
    """

    def __init__(self):
        self.prev_buy_qty = None
        self.prev_sell_qty = None
        self.prev_ltp = None

        self.prev_bid_qty = None
        self.prev_ask_qty = None

        self.prev_iv = None
        self.prev_underlying = None

        self.flow_window  = deque(maxlen=3)
        self.eff_window   = deque(maxlen=3)
        self.price_window = deque(maxlen=10)

        # --- tuning for synthetic flow ---
        self.FLOW_SCALE = 50.0          # converts qty-diff to "flow units"
        self.MIN_FLOW = 800.0           # lowered vs your old 1200 (because synthetic flow)

        self.MIN_EFF  = 0.00006
        self.MIN_PERSIST = 1
        self.RANGE_BREAK_TICKS = 0.10

    def compute(self, tick: dict):
        ltp = float(tick.get("ltp", 0.0) or 0.0)

        buy_qty = int(tick.get("buy_qty", 0) or 0)
        sell_qty = int(tick.get("sell_qty", 0) or 0)

        bid_qty = float(tick.get("bid_qty", 0.0) or 0.0)
        ask_qty = float(tick.get("ask_qty", 0.0) or 0.0)

        iv = float(tick.get("iv", 0.0) or 0.0)
        underlying = float(tick.get("underlying_ltp", 0.0) or 0.0)

        # -----------------------------
        # FLOW: WS real flow OR OptionChain synthetic flow
        # -----------------------------
        if buy_qty > 0 or sell_qty > 0:
            # WS-real flow
            delta_buy  = max(0, buy_qty - self.prev_buy_qty) if self.prev_buy_qty is not None else 0
            delta_sell = max(0, sell_qty - self.prev_sell_qty) if self.prev_sell_qty is not None else 0
            flow = float(delta_buy - delta_sell)
        else:
            # OptionChain-synthetic flow: bid/ask qty change
            d_bid = (bid_qty - self.prev_bid_qty) if self.prev_bid_qty is not None else 0.0
            d_ask = (ask_qty - self.prev_ask_qty) if self.prev_ask_qty is not None else 0.0
            flow = (d_bid - d_ask) * self.FLOW_SCALE

        ltp_change = ltp - self.prev_ltp if self.prev_ltp is not None else 0.0
        efficiency = abs(ltp_change) / abs(flow) if abs(flow) >= self.MIN_FLOW else 0.0

        # -----------------------------
        # IV / Underlying change (for option math awareness)
        # -----------------------------
        iv_change = (iv - self.prev_iv) if self.prev_iv is not None else 0.0
        ul_change = (underlying - self.prev_underlying) if self.prev_underlying is not None else 0.0

        self.flow_window.append(flow)
        self.eff_window.append(efficiency)
        self.price_window.append(ltp)

        valid_long_flow = sum(
            1 for f, e in zip(self.flow_window, self.eff_window)
            if f >= self.MIN_FLOW and e >= self.MIN_EFF
        ) >= self.MIN_PERSIST

        valid_short_flow = sum(
            1 for f, e in zip(self.flow_window, self.eff_window)
            if f <= -self.MIN_FLOW and e >= self.MIN_EFF
        ) >= self.MIN_PERSIST

        range_high = max(self.price_window) if self.price_window else ltp
        range_low  = min(self.price_window) if self.price_window else ltp

        range_break_up   = ltp >= range_high and (range_high - range_low) >= self.RANGE_BREAK_TICKS
        range_break_down = ltp <= range_low  and (range_high - range_low) >= self.RANGE_BREAK_TICKS

        vwap = (sum(self.price_window) / len(self.price_window)) if self.price_window else ltp

        # update prevs
        self.prev_buy_qty = buy_qty
        self.prev_sell_qty = sell_qty
        self.prev_ltp = ltp
        self.prev_bid_qty = bid_qty
        self.prev_ask_qty = ask_qty
        self.prev_iv = iv
        self.prev_underlying = underlying

        out = dict(tick)
        out.update({
            "flow": flow,
            "ltp_change": ltp_change,
            "efficiency": efficiency,

            "valid_long_flow": valid_long_flow,
            "valid_short_flow": valid_short_flow,

            "range_break_up": range_break_up,
            "range_break_down": range_break_down,

            "vwap": round(vwap, 2),
            "above_vwap": ltp > vwap,
            "below_vwap": ltp < vwap,

            "iv_change": iv_change,
            "underlying_change": ul_change,
        })
        return out
