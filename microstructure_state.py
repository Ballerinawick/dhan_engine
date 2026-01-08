# microstructure_state.py
from collections import defaultdict, deque
from datetime import datetime

class MicrostructureState:
    """
    Works for BOTH:
    1) Real tick feed (with last_traded_qty / depth)
    2) Dhan OptionChain snapshots (with top_bid/ask qty, volume, oi, greeks)

    Key idea:
    - For OptionChain, approximate "traded qty" using volume delta.
    - Keep the same flags your SignalEngine expects:
        absorption_flag, absorption_strength, vacuum_flag
    """

    def __init__(self):
        # price -> {"start_time": float_seconds, "volume": int}
        self.price_state = defaultdict(lambda: {"start_time": None, "volume": 0})

        self.last_price = None

        # --- thresholds (keep same meaning as your original) ---
        self.MIN_PAUSE_SECS = 2.0
        self.MIN_ABS_VOLUME = 1500
        self.VACUUM_QTY = 50

        # --- safety / memory ---
        self.recent_prices = deque(maxlen=20)
        self.vacuum_window = deque(maxlen=3)

        # --- NEW: snapshot delta tracking (per instrument) ---
        # We don’t assume instrument_key exists. We'll use a best-effort stable id.
        self.prev_volume = {}
        self.prev_oi = {}

    def _ts_to_seconds(self, ts):
        # Accept datetime or float/int.
        if isinstance(ts, datetime):
            return ts.timestamp()
        try:
            return float(ts)
        except Exception:
            return datetime.utcnow().timestamp()

    def _instrument_id(self, q):
        # Best effort stable id without assumptions.
        # If you later add "security_id" or "symbol", it will auto-use it.
        return str(
            q.get("security_id")
            or q.get("securityId")
            or q.get("symbol")
            or q.get("trading_symbol")
            or "UNKNOWN"
        )

    def update(self, q):
        # ---------- normalize inputs ----------
        ts_sec = self._ts_to_seconds(q.get("ts"))
        ltp = float(q.get("ltp", 0.0) or 0.0)

        inst_id = self._instrument_id(q)

        # ---------- pull snapshot fields if present ----------
        vol = int(q.get("volume", 0) or 0)
        oi = int(q.get("oi", 0) or 0)

        # OptionChain (adapter) uses: top_bid_quantity / top_ask_quantity
        bid_qty = float(q.get("bid_qty", 0) or 0)
        ask_qty = float(q.get("ask_qty", 0) or 0)

        # ---------- compute volume delta as traded-qty proxy ----------
        prev_vol = self.prev_volume.get(inst_id, vol)
        vol_delta = max(0, vol - prev_vol)
        self.prev_volume[inst_id] = vol

        # If you have real tick qty, prefer it; else use vol_delta.
        real_qty = q.get("last_traded_qty", None)
        if real_qty is None:
            qty = vol_delta
        else:
            try:
                qty = int(real_qty) if int(real_qty) > 0 else vol_delta
            except Exception:
                qty = vol_delta

        # ---------- OI delta (can be useful later) ----------
        prev_oi = self.prev_oi.get(inst_id, oi)
        oi_delta = oi - prev_oi
        self.prev_oi[inst_id] = oi

        # ---------- price change reset ----------
        if self.last_price != ltp:
            self.price_state[ltp]["start_time"] = ts_sec
            self.price_state[ltp]["volume"] = 0
            self.recent_prices.append(ltp)

        # ---------- accumulate "volume at price" ----------
        self.price_state[ltp]["volume"] += int(qty)

        start_time = self.price_state[ltp]["start_time"]
        time_at_price = (ts_sec - start_time) if start_time else 0.0
        volume_at_price = self.price_state[ltp]["volume"]

        # ---------- absorption logic (same meaning) ----------
        absorption_flag = (
            time_at_price >= self.MIN_PAUSE_SECS and
            volume_at_price >= self.MIN_ABS_VOLUME
        )

        absorption_strength = round(
            min(1.0, volume_at_price / (self.MIN_ABS_VOLUME * 2)),
            2
        )

        # ---------- vacuum logic (same meaning, but stable) ----------
        vacuum_tick = (bid_qty < self.VACUUM_QTY) or (ask_qty < self.VACUUM_QTY)
        self.vacuum_window.append(vacuum_tick)
        vacuum_flag = sum(self.vacuum_window) >= 2

        # ---------- cleanup old prices ----------
        # Remove old price states if our ring buffer is full.
        if len(self.recent_prices) == self.recent_prices.maxlen:
            old_price = self.recent_prices[0]
            # If old price not equal current, safe to delete.
            if old_price != ltp and old_price in self.price_state:
                del self.price_state[old_price]

        self.last_price = ltp

        # ---------- enrich q (backward compatible + option extras) ----------
        q["time_at_price"] = round(time_at_price, 2)
        q["volume_at_price"] = int(volume_at_price)

        q["absorption_flag"] = bool(absorption_flag)
        q["absorption_strength"] = float(absorption_strength)
        q["vacuum_flag"] = bool(vacuum_flag)

        # Snapshot deltas (useful for option logic)
        q["vol_delta"] = int(vol_delta)
        q["oi_delta"] = int(oi_delta)

        # Greeks passthrough (if present)
        q["iv"] = float(q.get("implied_volatility", 0.0) or 0.0)
        q["delta"] = float(q.get("delta", 0.0) or 0.0)
        q["gamma"] = float(q.get("gamma", 0.0) or 0.0)
        q["theta"] = float(q.get("theta", 0.0) or 0.0)
        q["vega"]  = float(q.get("vega", 0.0) or 0.0)

        return q
