# signal_engine.py
import time
from collections import deque
from datetime import datetime, date


def _parse_expiry(exp):
    # exp is like "2026-01-06"
    try:
        y, m, d = exp.split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None


def _trading_days_left(today: date, expiry: date):
    # rough: counts Mon-Fri only (no holidays)
    if not expiry or expiry < today:
        return 0
    days = 0
    cur = today
    while cur <= expiry:
        if cur.weekday() < 5:  # Mon-Fri
            days += 1
        cur = date.fromordinal(cur.toordinal() + 1)
    return max(0, days - 1)  # exclude today


class _SignalEngineCore:
    """Per-instrument state machine (one instance per tag)."""

    def __init__(self):
        self.position = None
        self.entry_price = None
        self.entry_time = None
        self.last_exit_time = None

        # --- Tunables ---
        self.MIN_PRICE_CONFIRM = 0.25       # for options, 0.04 is too tiny/noisy
        self.STOP_LOSS_MOVE = 1.00          # options move fast; 0.50 may be too tight

        self.MIN_HOLD_SECS = 3
        self.MAX_HOLD_SECS = 25
        self.COOLDOWN_SECS = 5

        self.flow_buffer = deque(maxlen=3)
        self.price_buffer = deque(maxlen=3)

        self.last_block_reason = None

    def generate(self, q):
        now = time.time()
        ltp = float(q.get("ltp", 0.0))
        flow = float(q.get("flow", 0.0))

        # ---------------- TIME + DAYS-TO-EXPIRY (DTE) ----------------
        now_dt = datetime.now()
        today = now_dt.date()
        expiry_dt = _parse_expiry(q.get("expiry", ""))
        tdl = _trading_days_left(today, expiry_dt)  # 0..N

        # Day buckets (matches your 1,2,3,4,5 idea)
        # 5 = fresh week, 1 = last day before expiry, 0 = expiry/today
        if tdl >= 4:
            day_bucket = 5
        elif tdl == 3:
            day_bucket = 4
        elif tdl == 2:
            day_bucket = 3
        elif tdl == 1:
            day_bucket = 2
        else:
            day_bucket = 1

        # Session bucket
        hour, minute = now_dt.hour, now_dt.minute
        is_morning = hour < 10 or (hour == 10 and minute <= 30)
        is_midday = (hour > 10 and hour < 14) or (hour == 14 and minute < 30)
        is_late = (hour == 14 and minute >= 30) or hour >= 15

        # ---------------- Greeks + OI ----------------
        delta = float(q.get("delta", 0.0) or 0.0)
        gamma = float(q.get("gamma", 0.0) or 0.0)
        theta = float(q.get("theta", 0.0) or 0.0)
        vega = float(q.get("vega", 0.0) or 0.0)
        iv = float(q.get("iv", 0.0) or 0.0)

        oi = int(q.get("oi", 0) or 0)
        prev_oi = int(q.get("previous_oi", 0) or 0)
        oi_delta = oi - prev_oi

        # If greeks totally missing -> NO trade
        greeks_missing = (delta == 0.0 and gamma == 0.0 and theta == 0.0 and vega == 0.0)
        if greeks_missing:
            return "HOLD"

        # Validity (relaxed a bit)
        if abs(delta) < 0.25:
            return "HOLD"

        # ---------------- cooldown ----------------
        if self.last_exit_time and now - self.last_exit_time < self.COOLDOWN_SECS:
            return "HOLD"

        # Buffers
        self.flow_buffer.append(flow)
        self.price_buffer.append(ltp)

        # ============================================================
        # ENTRY LOGIC (Two modes)
        # A) "Flow + microstructure" mode (when depth signals exist)
        # B) "OptionChain-only" fallback (still safe, but less edge)
        # ============================================================

        # Detect whether depth-based features exist
        has_depth = (
            q.get("absorption_flag") is not None
            or q.get("vacuum_flag") is not None
            or q.get("bid_qty") is not None
            or q.get("ask_qty") is not None
        )

        # ================= ENTRY =================
        if self.position is None and len(self.flow_buffer) == 3:
            flow_strength = sum(self.flow_buffer)
            flow_momentum = self.flow_buffer[-1] - self.flow_buffer[0]
            price_delta = self.price_buffer[-1] - self.price_buffer[0]

            price_confirm_up = price_delta > self.MIN_PRICE_CONFIRM
            price_confirm_dn = price_delta < -self.MIN_PRICE_CONFIRM

            # DTE behaviour: near expiry -> reduce breakout aggression
            allow_breakout = (day_bucket >= 3) and (not is_late)
            allow_decay_scalp = (day_bucket <= 2) or is_late

            # ------------- DEPTH MODE (preferred) -------------
            strong_absorption = (
                bool(q.get("absorption_flag"))
                and float(q.get("absorption_strength", 0.0) or 0.0) >= 0.6
            )

            if has_depth:
                # LONG
                if (
                    allow_breakout
                    and flow_strength > 2500
                    and flow_momentum > 0
                    and strong_absorption
                    and bool(q.get("range_break_up"))
                    and bool(q.get("above_vwap"))
                    and bool(q.get("vacuum_flag"))
                    and float(q.get("bid_qty", 0) or 0) >= float(q.get("ask_qty", 0) or 0)
                    and price_confirm_up
                    and delta > 0.35
                    and oi_delta >= 0  # avoid fading when OI collapses
                ):
                    self.position = "LONG"
                    self.entry_price = ltp
                    self.entry_time = now
                    self.last_block_reason = None
                    return "ENTER_LONG"

                # SHORT
                if (
                    allow_breakout
                    and flow_strength < -2500
                    and flow_momentum < 0
                    and strong_absorption
                    and bool(q.get("range_break_down"))
                    and bool(q.get("below_vwap"))
                    and bool(q.get("vacuum_flag"))
                    and float(q.get("ask_qty", 0) or 0) >= float(q.get("bid_qty", 0) or 0)
                    and price_confirm_dn
                    and delta < -0.35
                    and oi_delta >= 0
                ):
                    self.position = "SHORT"
                    self.entry_price = ltp
                    self.entry_time = now
                    self.last_block_reason = None
                    return "ENTER_SHORT"

            # ------------- OPTIONCHAIN-ONLY FALLBACK -------------
            # This will give *some* signals using:
            # - price momentum
            # - delta direction
            # - OI increasing confirmation
            # - time-of-day filtering
            #
            # NOTE: This is NOT as strong as depth. Use for PAPER / study.
            if not has_depth:
                if allow_breakout:
                    if price_confirm_up and delta > 0.35 and oi_delta > 0 and flow_strength > 0:
                        self.position = "LONG"
                        self.entry_price = ltp
                        self.entry_time = now
                        return "ENTER_LONG"

                    if price_confirm_dn and delta < -0.35 and oi_delta > 0 and flow_strength < 0:
                        self.position = "SHORT"
                        self.entry_price = ltp
                        self.entry_time = now
                        return "slENTER_SHORT"

                # expiry/late: quick mean-reversion / decay scalp (very tight)
                if allow_decay_scalp and is_late and abs(theta) > 5:
                    if abs(price_delta) < 0.30 and oi_delta > 0:
                        self.position = "LONG" if delta > 0 else "SHORT"
                        self.entry_price = ltp
                        self.entry_time = now
                        return f"ENTER_{self.position}"

            # Diagnostics
            if abs(flow_strength) > 2000:
                self.last_block_reason = {
                    "flow_strength": flow_strength,
                    "flow_momentum": flow_momentum,
                    "price_delta": round(price_delta, 3),
                    "delta": round(delta, 3),
                    "theta": round(theta, 2),
                    "oi_delta": int(oi_delta),
                    "day_bucket": day_bucket,
                    "is_late": is_late,
                }

            return "HOLD"

        # ================= EXIT =================
        if self.position:
            hold_time = now - self.entry_time

            # tighten near expiry / late
            max_hold = 10 if (day_bucket <= 2 or is_late) else self.MAX_HOLD_SECS
            min_hold = 2 if (day_bucket <= 2) else self.MIN_HOLD_SECS

            if hold_time < min_hold:
                return "HOLD"

            if hold_time > max_hold:
                side = self.position
                self._reset(now)
                return f"EXIT_{side}"

            # stall exit
            if hold_time > 6 and abs(ltp - self.entry_price) < 0.20:
                side = self.position
                self._reset(now)
                return f"EXIT_{side}"

            # stop / early failure
            if self.position == "LONG":
                if ltp < self.entry_price - self.STOP_LOSS_MOVE:
                    self._reset(now)
                    return "EXIT_LONG"
                if flow < -1500 and not bool(q.get("absorption_flag")):
                    self._reset(now)
                    return "EXIT_LONG"

            if self.position == "SHORT":
                if ltp > self.entry_price + self.STOP_LOSS_MOVE:
                    self._reset(now)
                    return "EXIT_SHORT"
                if flow > 1500 and not bool(q.get("absorption_flag")):
                    self._reset(now)
                    return "EXIT_SHORT"

        return "HOLD"

    def _reset(self, now):
        self.position = None
        self.entry_price = None
        self.entry_time = None
        self.last_exit_time = now
        self.flow_buffer.clear()
        self.price_buffer.clear()
        self.last_block_reason = None


class SignalEngine:
    """
    Router that maintains one _SignalEngineCore per instrument tag.
    This fixes your "mixed buffers" problem permanently.
    """

    def __init__(self):
        self._engines = {}

    def generate(self, q):
        tag = q.get("tag") or "DEFAULT"
        eng = self._engines.get(tag)
        if eng is None:
            eng = _SignalEngineCore()
            self._engines[tag] = eng
        return eng.generate(q)
