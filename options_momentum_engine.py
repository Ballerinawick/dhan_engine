from collections import defaultdict, deque
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import time


class OptionsMomentumEngine:
    """
    OPTIONS MOMENTUM ENGINE (REAL-TIME)

    Fixes:
      ✅ NEVER return tuple ("NO_TRADE",)
      ✅ Sideways exit uses short vs long compression correctly
    """

    IST = ZoneInfo("Asia/Kolkata")

    MARKET_START = dtime(9, 10)
    MARKET_END = dtime(15, 30)

    BASE_A_SPEED = 1.8
    BASE_A_VOL = 2.5

    BASE_B_WICK = 0.65
    BASE_B_SPEED = 0.7

    MICRO_MAX_SPREAD_PCT = 0.020
    MICRO_MIN_ABS_IMB = 0.07
    MICRO_MIN_ABSORB = 0.10
    MICRO_MIN_FLOW = 600

    EXIT_PULLBACK = 0.40
    EXIT_VOL_DROP = 0.40

    # SIDEWAYS EXIT
    SIDEWAYS_MIN_HOLD_SEC = 120
    SIDEWAYS_RANGE_RATIO = 0.35
    SIDEWAYS_VOL_RATIO = 0.60
    SIDEWAYS_LOW_SPEED_RATIO = 0.40
    SIDEWAYS_MAX_PNL_PCT = 0.15

    def __init__(self):
        self.tick_buffer = defaultdict(deque)
        self.candles = defaultdict(deque)

        self.active_trade = {}
        self.last_action_sec = {}

        self.last_trade_pnl = defaultdict(float)
        self.cum_pnl = defaultdict(float)

        self._last_ctx_print_sec = None

        self.last_exit_reason = {}
        self.last_micro_reject_sec = {}

    # --------------------------------------------------
    def market_open(self) -> bool:
        now = datetime.now(self.IST)
        if now.weekday() >= 5:
            return False
        return self.MARKET_START <= now.time() <= self.MARKET_END

    # Wed day1 → Tue day5 expiry
    def option_day_index(self) -> int:
        wd = datetime.now(self.IST).weekday()
        mapping = {2: 1, 3: 2, 4: 3, 0: 4, 1: 5}
        return mapping.get(wd, 0)

    def option_day_label(self) -> str:
        now = datetime.now(self.IST)
        wd_name = now.strftime("%a")
        d = self.option_day_index()
        if d == 0:
            return f"{wd_name} | Day0(Neutral)"
        if d == 5:
            return f"{wd_name} | Day5(Expiry)"
        return f"{wd_name} | Day{d}"

    def time_regime(self) -> str:
        t = datetime.now(self.IST).time()
        if t < dtime(9, 45):
            return "OPEN"
        if t < dtime(13, 30):
            return "MID"
        if t < dtime(15, 0):
            return "TREND"
        return "CLOSE"

    def _print_day_context(self, ts: float):
        sec = int(ts)
        minute_key = sec // 60
        if self._last_ctx_print_sec == minute_key:
            return
        self._last_ctx_print_sec = minute_key

        now = datetime.now(self.IST)
        print(f"🗓️ {now.strftime('%H:%M:%S')} IST | {self.option_day_label()} | Regime:{self.time_regime()}")

    # --------------------------------------------------
    def on_tick(self, secid: int, tick: dict) -> str:
        ts = tick.get("ts")
        ltp = float(tick.get("ltp", 0) or 0)
        if not ts or ltp <= 0:
            return "NO_TRADE"

        self._print_day_context(float(ts))

        if not self.market_open():
            return "NO_TRADE"

        sec = int(ts)
        self.tick_buffer[secid].append(tick)

        while self.tick_buffer[secid] and int(self.tick_buffer[secid][0]["ts"]) < sec:
            self.tick_buffer[secid].popleft()

        candle = self._build_1s_candle(secid)
        if not candle:
            return "NO_TRADE"

        self.candles[secid].append(candle)
        if len(self.candles[secid]) > 40:
            self.candles[secid].popleft()

        return self._evaluate(secid)

    def _build_1s_candle(self, secid):
        ticks = self.tick_buffer[secid]
        if len(ticks) < 2:
            return None

        prices = [float(t.get("ltp", 0) or 0) for t in ticks if float(t.get("ltp", 0) or 0) > 0]
        if not prices:
            return None

        o = prices[0]
        h = max(prices)
        l = min(prices)
        c = prices[-1]

        activity = 0.0
        for i in range(1, len(ticks)):
            prev = ticks[i - 1]
            cur = ticks[i]
            prev_depth = float(prev.get("bid_qty", 0) or 0) + float(prev.get("ask_qty", 0) or 0)
            cur_depth = float(cur.get("bid_qty", 0) or 0) + float(cur.get("ask_qty", 0) or 0)
            activity += abs(cur_depth - prev_depth)

        activity += sum(abs(float(t.get("flow", 0) or 0)) for t in ticks)

        return {
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "range": h - l,
            "volume": activity,
            "ts": float(ticks[-1]["ts"]),
            "sec": int(ticks[-1]["ts"]),
        }

    def _micro_ok(self, secid: int, last_tick: dict, cur_sec: int) -> bool:
        ltp = float(last_tick.get("ltp", 0) or 0)
        bid = float(last_tick.get("bid", 0) or 0)
        ask = float(last_tick.get("ask", 0) or 0)
        spread = float(last_tick.get("spread", 0) or 0)
        if spread <= 0 and bid > 0 and ask > 0:
            spread = max(ask - bid, 0.0)

        spread_pct = spread / max(ltp, 1e-9)
        imb = float(last_tick.get("imbalance_5", 0) or 0)
        flow = float(last_tick.get("flow", 0) or 0)
        vac = bool(last_tick.get("vacuum_flag", False))
        absorb_strength = float(last_tick.get("absorption_strength", 0) or 0)
        absorb_flag = bool(last_tick.get("absorption_flag", False))

        spread_ok = spread_pct <= self.MICRO_MAX_SPREAD_PCT
        direction_ok = (imb >= self.MICRO_MIN_ABS_IMB) or (absorb_flag and absorb_strength >= self.MICRO_MIN_ABSORB)
        confirm_ok = (absorb_strength >= self.MICRO_MIN_ABSORB) or (flow >= self.MICRO_MIN_FLOW)
        micro_ok = spread_ok and (not vac) and direction_ok and confirm_ok

        if (not micro_ok) and self.last_micro_reject_sec.get(secid) != cur_sec:
            self.last_micro_reject_sec[secid] = cur_sec
            print(
                f"🚫 MICRO_REJECT | secid={secid} | spr%={spread_pct:.2%} "
                f"imb={imb:.3f} flow={flow:.0f} vac={vac} absorb={absorb_strength:.2f}"
            )

        return micro_ok

    def _trade_pnl(self, side, entry, exit_price):
        return (exit_price - entry) if side == "LONG" else (entry - exit_price)

    # --------------------------------------------------
    def _avg_range_vol(self, candles_list):
        if not candles_list:
            return 0.0, 0.0
        avg_range = sum(x["range"] for x in candles_list) / len(candles_list)
        avg_vol = sum(x["volume"] for x in candles_list) / len(candles_list)
        return avg_range, avg_vol

    def _evaluate(self, secid: int) -> str:
        c = self.candles[secid]
        if len(c) < 8:
            return "NO_TRADE"

        last, prev = c[-1], c[-2]
        cur_sec = int(last["sec"])

        if self.last_action_sec.get(secid) == cur_sec:
            return "NO_TRADE"

        price_speed = abs(last["close"] - prev["close"])

        last5 = list(c)[-5:]
        last20 = list(c)[-20:] if len(c) >= 20 else list(c)

        avg_range_5, avg_vol_5 = self._avg_range_vol(last5)
        avg_range_20, avg_vol_20 = self._avg_range_vol(last20)

        day = self.option_day_index()
        regime = self.time_regime()

        decay_penalty = 1.0 + (day * 0.15)

        if regime == "OPEN":
            time_boost = 1.10
        elif regime == "TREND":
            time_boost = 0.90
        elif regime == "CLOSE":
            time_boost = 1.15
        else:
            time_boost = 1.00

        A_SPEED = self.BASE_A_SPEED * decay_penalty * time_boost
        A_VOL = self.BASE_A_VOL * decay_penalty

        B_SPEED = self.BASE_B_SPEED * decay_penalty * time_boost
        B_WICK = self.BASE_B_WICK

        # ==================================================
        # EXIT (dynamic)
        # ==================================================
        if secid in self.active_trade:
            t = self.active_trade[secid]
            pnl = self._trade_pnl(t["side"], t["entry"], last["close"])
            pull = abs(last["close"] - t["entry"]) / max(t["entry"], 1e-9)

            vol_exit = (avg_vol_5 > 0) and (last["volume"] < avg_vol_5 * self.EXIT_VOL_DROP)

            if pull > self.EXIT_PULLBACK or vol_exit:
                self.last_trade_pnl[secid] = round(pnl, 2)
                self.cum_pnl[secid] += pnl
                self.active_trade.pop(secid, None)
                self.last_action_sec[secid] = cur_sec
                self.last_exit_reason[secid] = "DYNAMIC"
                return "EXIT"

        # ==================================================
        # SIDEWAYS EXIT (correct compression logic)
        # ==================================================
        if secid in self.active_trade:
            t = self.active_trade[secid]
            age = float(last["ts"]) - float(t["ts"])

            pnl = self._trade_pnl(t["side"], t["entry"], last["close"])
            pnl_pct = abs(pnl) / max(t["entry"], 1e-9)

            compressed = (avg_range_20 > 0) and (avg_range_5 < avg_range_20 * self.SIDEWAYS_RANGE_RATIO)
            weak_volume = (avg_vol_20 > 0) and (avg_vol_5 < avg_vol_20 * self.SIDEWAYS_VOL_RATIO)
            low_speed = (avg_range_20 > 0) and (price_speed < avg_range_20 * self.SIDEWAYS_LOW_SPEED_RATIO)

            if (
                age >= self.SIDEWAYS_MIN_HOLD_SEC
                and compressed
                and weak_volume
                and low_speed
                and pnl_pct <= self.SIDEWAYS_MAX_PNL_PCT
            ):
                self.active_trade.pop(secid, None)
                self.last_action_sec[secid] = cur_sec
                self.last_exit_reason[secid] = "SIDEWAYS"
                return "EXIT"

        if secid in self.active_trade:
            return "NO_TRADE"

        last_tick = self.tick_buffer[secid][-1] if self.tick_buffer[secid] else {}
        if not self._micro_ok(secid, last_tick, cur_sec):
            return "NO_TRADE"

        # ==================================================
        # STRATEGY A
        # ==================================================
        if (
            avg_range_5 > 0
            and price_speed > avg_range_5 * A_SPEED
            and avg_vol_5 > 0
            and last["volume"] > avg_vol_5 * A_VOL
        ):
            self.active_trade[secid] = {
                "type": "A",
                "side": "LONG",
                "entry": float(last["close"]),
                "ts": float(last["ts"]),
            }
            self.last_action_sec[secid] = cur_sec
            return "A_ENTRY"

        # ==================================================
        # STRATEGY B
        # ==================================================
        rng = float(last["range"])
        if rng > 0 and avg_range_5 > 0:
            uw = float(last["high"]) - max(float(last["open"]), float(last["close"]))
            lw = min(float(last["open"]), float(last["close"])) - float(last["low"])

            trap_down = lw > rng * B_WICK

            if price_speed > avg_range_5 * B_SPEED and trap_down:
                self.active_trade[secid] = {
                    "type": "B",
                    "side": "LONG",
                    "entry": float(last["close"]),
                    "ts": float(last["ts"]),
                }
                self.last_action_sec[secid] = cur_sec
                return "B_ENTRY"

        return "NO_TRADE"
