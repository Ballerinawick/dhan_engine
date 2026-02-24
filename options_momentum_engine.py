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

    BASE_A_SPEED = 2.4
    BASE_A_VOL = 2.5

    BASE_B_WICK = 0.65
    BASE_B_SPEED = 0.7

    MICRO_MAX_SPREAD_PCT = 0.015
    MICRO_MIN_ABS_IMB = 0.08
    MICRO_MIN_ABSORB = 0.12
    MICRO_MIN_FLOW = 800

    EXIT_PULLBACK = 0.40
    EXIT_VOL_DROP = 0.40
    MIN_HOLD_SEC = 180
    EARLY_EXIT_PULLBACK = 0.70
    VOL_EXIT_STREAK = 5

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
        self.last_micro_debug_sec = {}
        self.micro_reject_window = defaultdict(deque)

        self.last_candle_sec = {}
        self.vol_exit_counter = defaultdict(int)

        self.micro_stats_window = defaultdict(int)
        self.last_micro_stats_sec = None

        self.entries_taken = 0
        self.exits_taken = 0
        self.total_hold_sec = 0.0
        self.fees_paid = 0.0
        self.fee_per_trade = 0.0
        self.last_engine_state_sec = None

        # --- DEBUG: print tick format once per secid ---
        self._printed_sample_tick = set()
        self._printed_sample_tick_ts = {}

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

        # --- DEBUG: print tick dict format once per instrument ---
        if secid not in self._printed_sample_tick:
            print("🔍 SAMPLE_TICK_FORMAT | secid=", secid, "| keys=", sorted(list(tick.keys())))
            print("🔍 SAMPLE_TICK_DICT  |", tick)
            self._printed_sample_tick.add(secid)
            self._printed_sample_tick_ts[secid] = time.time()

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

        candle_sec = int(candle["sec"])
        if self.last_candle_sec.get(secid) == candle_sec:
            return "NO_TRADE"

        self.last_candle_sec[secid] = candle_sec

        self.candles[secid].append(candle)
        if len(self.candles[secid]) > 40:
            self.candles[secid].popleft()

        return self._evaluate(secid)

    def _build_1s_candle(self, secid):
        ticks = self.tick_buffer[secid]
        prices = [float(t.get("ltp", 0) or 0) for t in ticks if float(t.get("ltp", 0) or 0) > 0]
        if len(prices) < 2:
            return None

        candle = {
            "open": prices[0],
            "high": prices[0],
            "low": prices[0],
            "close": prices[0],
        }
        for price in prices[1:]:
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price

        activity = 0.0
        for i in range(1, len(ticks)):
            prev = ticks[i - 1]
            cur = ticks[i]
            prev_depth = float(prev.get("bid_qty", 0) or 0) + float(prev.get("ask_qty", 0) or 0)
            cur_depth = float(cur.get("bid_qty", 0) or 0) + float(cur.get("ask_qty", 0) or 0)
            activity += abs(cur_depth - prev_depth)

        activity += sum(abs(float(t.get("flow", 0) or 0)) for t in ticks)

        return {
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "range": candle["high"] - candle["low"],
            "volume": activity,
            "ts": float(ticks[-1]["ts"]),
            "sec": int(ticks[-1]["ts"]),
        }

    def _micro_ok(self, secid: int, last_tick: dict, cur_sec: int) -> bool:
        return True

    def _trade_pnl(self, side, entry, exit_price):
        return (exit_price - entry) if side == "LONG" else (entry - exit_price)

    # --------------------------------------------------
    def _avg_range_vol(self, candles_list):
        if not candles_list:
            return 0.0, 0.0
        avg_range = sum(x["range"] for x in candles_list) / len(candles_list)
        avg_vol = sum(x["volume"] for x in candles_list) / len(candles_list)
        return avg_range, avg_vol

    def _log_engine_state(self, cur_sec: int):
        bucket_60 = cur_sec // 60
        if self.last_engine_state_sec == bucket_60:
            return
        self.last_engine_state_sec = bucket_60

        open_trades = len(self.active_trade)
        avg_hold = self.total_hold_sec / max(self.exits_taken, 1)
        churn_ratio = self.exits_taken / max(self.entries_taken, 1)
        print(
            "📈 ENGINE_STATE | "
            f"OpenTrades={open_trades} | "
            f"EntriesTaken={self.entries_taken} | "
            f"AvgHoldTime={avg_hold:.1f} | "
            f"CumPnL={sum(self.cum_pnl.values()):.2f} | "
            f"FeesPaid={self.fees_paid:.2f} | "
            f"ChurnRatio={churn_ratio:.3f}"
        )

    def _evaluate(self, secid: int) -> str:
        c = self.candles[secid]
        if len(c) < 8:
            return "NO_TRADE"

        last, prev = c[-1], c[-2]
        cur_sec = int(last["sec"])

        if self.last_action_sec.get(secid) == cur_sec:
            return "NO_TRADE"

        price_speed = abs(last["close"] - prev["close"])

        debug_last5 = list(c)[-5:]
        avg_range_5, avg_vol_5 = self._avg_range_vol(debug_last5)

        # ---- DEBUG ENTRY DIAGNOSTIC ----
        if len(c) >= 5:
            print(
                f"🧪 ENTRY_CHECK | secid={secid} | "
                f"close={last['close']:.2f} | "
                f"speed={price_speed:.4f} | "
                f"avg_range_5={avg_range_5:.4f} | "
                f"speed_ratio={(price_speed / max(avg_range_5,1e-9)):.2f} | "
                f"vol={last['volume']:.2f} | "
                f"vol_ratio={(last['volume'] / max(avg_vol_5,1e-9)):.2f}"
            )

        last5 = list(c)[-5:]
        last20 = list(c)[-20:] if len(c) >= 20 else list(c)

        avg_range_5, avg_vol_5 = self._avg_range_vol(last5)
        avg_range_20, avg_vol_20 = self._avg_range_vol(last20)

        day = self.option_day_index()
        regime = self.time_regime()
        self._log_engine_state(cur_sec)

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
            age = float(last["ts"]) - float(t["ts"])
            last_tick = self.tick_buffer[secid][-1] if self.tick_buffer[secid] else {}
            vacuum_flag = bool(last_tick.get("vacuum_flag", False))

            if avg_vol_5 > 0 and last["volume"] < avg_vol_5 * self.EXIT_VOL_DROP:
                self.vol_exit_counter[secid] += 1
            else:
                self.vol_exit_counter[secid] = 0

            vol_exit = self.vol_exit_counter[secid] >= self.VOL_EXIT_STREAK

            if age < self.MIN_HOLD_SEC:
                if vol_exit:
                    print(
                        f"🛡 HOLD_LOCK | secid={secid} | age={age:.1f} | pull={pull:.3f} | "
                        "reason=vol_exit_blocked"
                    )
                if pull <= self.EARLY_EXIT_PULLBACK and (not vacuum_flag):
                    return "NO_TRADE"
                vol_exit = False

            if pull > self.EXIT_PULLBACK or vol_exit:
                self.last_trade_pnl[secid] = round(pnl, 2)
                self.cum_pnl[secid] += pnl
                self.active_trade.pop(secid, None)
                self.exits_taken += 1
                self.total_hold_sec += max(age, 0.0)
                self.fees_paid += self.fee_per_trade
                self.last_action_sec[secid] = cur_sec
                self.last_exit_reason[secid] = "DYNAMIC"
                print(
                    f"🔻 EXIT_SIGNAL | secid={secid} | reason=DYNAMIC | pnl={pnl:.2f} | "
                    f"age={age:.1f} | vol_counter={self.vol_exit_counter[secid]}"
                )
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
                age >= max(self.SIDEWAYS_MIN_HOLD_SEC, self.MIN_HOLD_SEC)
                and compressed
                and weak_volume
                and low_speed
                and pnl_pct <= self.SIDEWAYS_MAX_PNL_PCT
            ):
                self.active_trade.pop(secid, None)
                self.exits_taken += 1
                self.total_hold_sec += max(age, 0.0)
                self.fees_paid += self.fee_per_trade
                self.last_action_sec[secid] = cur_sec
                self.last_exit_reason[secid] = "SIDEWAYS"
                print(
                    f"🔻 EXIT_SIGNAL | secid={secid} | reason=SIDEWAYS | pnl={pnl:.2f} | "
                    f"age={age:.1f} | vol_counter={self.vol_exit_counter[secid]}"
                )
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
            self.entries_taken += 1
            self.last_action_sec[secid] = cur_sec
            print(
                f"🟢 ENTRY | secid={secid} | type=A | price={last['close']:.2f} | "
                f"speed={price_speed:.4f} | avg_range_5={avg_range_5:.4f} | "
                f"volume={last['volume']:.2f} | avg_vol_5={avg_vol_5:.2f} | regime={regime} | day={day}"
            )
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
                self.entries_taken += 1
                self.last_action_sec[secid] = cur_sec
                print(
                    f"🟢 ENTRY | secid={secid} | type=B | price={last['close']:.2f} | "
                    f"speed={price_speed:.4f} | avg_range_5={avg_range_5:.4f} | "
                    f"volume={last['volume']:.2f} | avg_vol_5={avg_vol_5:.2f} | regime={regime} | day={day}"
                )
                return "B_ENTRY"

        return "NO_TRADE"
