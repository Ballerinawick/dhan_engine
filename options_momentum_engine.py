from collections import defaultdict, deque
from datetime import datetime, time as dtime
from statistics import median
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

    TURN_SPEED_RATIO_THRESHOLD = 3.0

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

        # 30-minute warmup baseline stats (collection-only)
        self.warmup_start_sec = {}
        self.warmup_reported = set()
        self.warmup_active = False
        self.baseline_printed = set()
        self.warmup_duration_sec = 120  # TEMP TEST WINDOW (2 minutes)
        self._last_warmup_status_minute = {}
        self.warmup_stats = defaultdict(lambda: {
            "avg_range_5": [],
            "speed": [],
            "abs_flow": [],
            "abs_imbalance_5": [],
            "absorption_true": 0,
            "absorption_total": 0,
            "spread": [],
        })

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

        missing_fields = (
            abs(imb) <= 1e-9
            and abs(flow) <= 1e-9
            and abs(absorb_strength) <= 1e-9
            and (not absorb_flag)
        )

        spread_ok = spread_pct <= self.MICRO_MAX_SPREAD_PCT
        direction_ok = missing_fields or (abs(imb) >= self.MICRO_MIN_ABS_IMB) or (absorb_flag and absorb_strength >= self.MICRO_MIN_ABSORB)
        confirm_ok = missing_fields or (absorb_strength >= self.MICRO_MIN_ABSORB) or (abs(flow) >= self.MICRO_MIN_FLOW)
        micro_ok = spread_ok and (not vac) and direction_ok and confirm_ok

        failed_reasons = []
        if not spread_ok:
            failed_reasons.append("spread")
        if not direction_ok:
            failed_reasons.append("direction")
        if not confirm_ok:
            failed_reasons.append("confirm")
        if vac:
            failed_reasons.append("vacuum")
        if missing_fields:
            failed_reasons.append("missing_fields")

        self.micro_stats_window["pass" if micro_ok else "fail"] += 1
        if not spread_ok:
            self.micro_stats_window["spread_fail"] += 1
        if not direction_ok:
            self.micro_stats_window["direction_fail"] += 1
        if not confirm_ok:
            self.micro_stats_window["confirm_fail"] += 1
        if vac:
            self.micro_stats_window["vacuum_fail"] += 1

        bucket_30 = cur_sec // 30
        if self.last_micro_stats_sec != bucket_30:
            self.last_micro_stats_sec = bucket_30
            print(
                "🧠 MICRO_STATS | "
                f"pass={self.micro_stats_window['pass']} | "
                f"spread_fail={self.micro_stats_window['spread_fail']} | "
                f"direction_fail={self.micro_stats_window['direction_fail']} | "
                f"confirm_fail={self.micro_stats_window['confirm_fail']} | "
                f"vacuum_fail={self.micro_stats_window['vacuum_fail']}"
            )
            self.micro_stats_window = defaultdict(int)

        return micro_ok

    def _trade_pnl(self, side, entry, exit_price):
        return (exit_price - entry) if side == "LONG" else (entry - exit_price)

    # --------------------------------------------------
    def _avg_range_vol(self, candles_list):
        if not candles_list:
            return 0.0, 0.0
        avg_range = sum(x["range"] for x in candles_list) / len(candles_list)
        avg_range = max(avg_range, 0.01)
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

    def _dist_summary(self, values):
        if not values:
            return "n=0"
        sorted_vals = sorted(float(v) for v in values)
        n = len(sorted_vals)

        def pct(p):
            idx = int((n - 1) * p)
            return sorted_vals[idx]

        return (
            f"n={n} p50={pct(0.50):.4f} p90={pct(0.90):.4f} "
            f"p99={pct(0.99):.4f} max={sorted_vals[-1]:.4f}"
        )

    def _collect_warmup_stats(self, secid: int, cur_sec: int, speed: float, avg_range_5: float, last_tick: dict):
        if secid in self.warmup_reported:
            return

        if secid not in self.warmup_start_sec:
            self.warmup_start_sec[secid] = cur_sec

        elapsed_sec = max(cur_sec - self.warmup_start_sec[secid], 0)
        elapsed_minutes = elapsed_sec / 60.0
        self.warmup_active = elapsed_sec < self.warmup_duration_sec
        minute_bucket = cur_sec // 60
        if self._last_warmup_status_minute.get(secid) != minute_bucket:
            self._last_warmup_status_minute[secid] = minute_bucket
            print(f"🧪 WARMUP_STATUS | secid={secid} | elapsed={elapsed_minutes:.2f} min | collecting={self.warmup_active}")

        stats = self.warmup_stats[secid]
        stats["avg_range_5"].append(float(avg_range_5))
        stats["speed"].append(abs(float(speed)))
        stats["abs_flow"].append(abs(float(last_tick.get("flow", 0) or 0)))
        stats["abs_imbalance_5"].append(abs(float(last_tick.get("imbalance_5", 0) or 0)))
        stats["absorption_total"] += 1
        if bool(last_tick.get("absorption_flag", False)):
            stats["absorption_true"] += 1

        spread = float(last_tick.get("spread", 0) or 0)
        bid = float(last_tick.get("bid", 0) or 0)
        ask = float(last_tick.get("ask", 0) or 0)
        if spread <= 0 and bid > 0 and ask > 0:
            spread = max(ask - bid, 0.0)
        stats["spread"].append(float(spread))

        if elapsed_sec < self.warmup_duration_sec:
            return

        avg_range_values = stats["avg_range_5"]
        speed_values = stats["speed"]
        spread_values = stats["spread"]

        avg_range_mean = (sum(avg_range_values) / len(avg_range_values)) if avg_range_values else 0.0
        avg_speed = (sum(speed_values) / len(speed_values)) if speed_values else 0.0
        absorption_freq = stats["absorption_true"] / max(stats["absorption_total"], 1)
        spread_med = median(spread_values) if spread_values else 0.0

        if secid not in self.baseline_printed:
            stats_dict = {
                "secid": secid,
                "avg_range_5_mean": round(avg_range_mean, 4),
                "avg_range_5_median": round(median(avg_range_values) if avg_range_values else 0.0, 4),
                "avg_speed": round(avg_speed, 4),
                "abs_flow_dist": self._dist_summary(stats["abs_flow"]),
                "abs_imbalance_5_dist": self._dist_summary(stats["abs_imbalance_5"]),
                "absorption_freq": round(absorption_freq, 3),
                "spread_median": round(spread_med, 4),
            }
            print("SESSION_BASELINE_REPORT", stats_dict)
            self.baseline_printed.add(secid)
        print(
            f"📊 BASELINE | secid={secid} | "
            f"avg_range_5_mean={avg_range_mean:.4f} | "
            f"avg_range_5_median={median(avg_range_values) if avg_range_values else 0.0:.4f} | "
            f"avg_speed={avg_speed:.4f} | "
            f"abs(flow)_dist=({self._dist_summary(stats['abs_flow'])}) | "
            f"abs(imbalance_5)_dist=({self._dist_summary(stats['abs_imbalance_5'])}) | "
            f"absorption_freq={absorption_freq:.3f} | "
            f"spread_median={spread_med:.4f}"
        )
        self.warmup_reported.add(secid)

    def _evaluate(self, secid: int) -> str:
        c = self.candles[secid]
        if len(c) < 8:
            return "NO_TRADE"

        last, prev = c[-1], c[-2]
        cur_sec = int(last["sec"])

        if self.last_action_sec.get(secid) == cur_sec:
            return "NO_TRADE"

        if len(c) < 3:
            return "NO_TRADE"

        prev2 = c[-3]
        speed = float(last["close"] - prev["close"])
        prev_speed = float(prev["close"] - prev2["close"])

        debug_last5 = list(c)[-5:]
        avg_range_5, avg_vol_5 = self._avg_range_vol(debug_last5)

        speed_ratio = min(abs(speed) / max(avg_range_5, 1e-6), 50.0)
        vol_ratio = (last["volume"] / avg_vol_5) if avg_vol_5 > 0 and last["volume"] > 0 else 0.0

        # ---- DEBUG ENTRY DIAGNOSTIC ----
        if len(c) >= 5:
            print(
                f"🧪 ENTRY_CHECK | secid={secid} | "
                f"close={last['close']:.2f} | "
                f"speed={abs(speed):.4f} | "
                f"avg_range_5={avg_range_5:.4f} | "
                f"speed_ratio={speed_ratio:.2f} | "
                f"vol={last['volume']:.2f} | "
                f"vol_ratio={vol_ratio:.2f}"
            )

        last5 = list(c)[-5:]
        last20 = list(c)[-20:] if len(c) >= 20 else list(c)

        avg_range_5, avg_vol_5 = self._avg_range_vol(last5)
        avg_range_20, avg_vol_20 = self._avg_range_vol(last20)

        last_tick = self.tick_buffer[secid][-1] if self.tick_buffer[secid] else {}
        self._collect_warmup_stats(secid, cur_sec, speed, avg_range_5, last_tick)

        self._log_engine_state(cur_sec)

        shrinking_range = float(last["range"]) < float(prev["range"])
        speed_collapse = abs(speed) < abs(prev_speed) * 0.6
        midpoint_prev = (float(prev["high"]) + float(prev["low"])) / 2.0

        # ==================================================
        # EXIT ON OPPOSITE TURN (LONG-ONLY)
        # ==================================================
        if secid in self.active_trade:
            opposite_turn_exit = (
                prev_speed > 0
                and speed_ratio > self.TURN_SPEED_RATIO_THRESHOLD
                and shrinking_range
                and speed_collapse
                and float(last["close"]) < midpoint_prev
            )

            if opposite_turn_exit:
                t = self.active_trade[secid]
                pnl = self._trade_pnl(t["side"], t["entry"], last["close"])
                self.active_trade.pop(secid, None)
                self.exits_taken += 1
                self.total_hold_sec += max(float(last["ts"]) - float(t["ts"]), 0.0)
                self.fees_paid += self.fee_per_trade
                self.last_action_sec[secid] = cur_sec
                self.last_exit_reason[secid] = "OPPOSITE_TURN"
                self.last_trade_pnl[secid] = round(pnl, 2)
                self.cum_pnl[secid] += pnl
                print(
                    f"🔻 TURN_EXIT LONG | secid={secid} | close={last['close']:.2f} | "
                    "reason=OPPOSITE_TURN"
                )
                return "EXIT"

        if secid in self.active_trade:
            return "NO_TRADE"

        if not self._micro_ok(secid, last_tick, cur_sec):
            return "NO_TRADE"

        prior_move_down = prev_speed < 0
        down_exhaustion = (
            speed_ratio > self.TURN_SPEED_RATIO_THRESHOLD
            and shrinking_range
            and speed_collapse
        )
        reversal_confirm = float(last["close"]) > midpoint_prev

        if prior_move_down and down_exhaustion and reversal_confirm:
            self.active_trade[secid] = {
                "type": "TURN",
                "side": "LONG",
                "entry": float(last["close"]),
                "ts": float(last["ts"]),
            }
            self.entries_taken += 1
            self.last_action_sec[secid] = cur_sec
            print(
                f"🟢 TURN_ENTRY LONG | secid={secid} | close={last['close']:.2f} | "
                f"speed_ratio={speed_ratio:.2f} | shrink={shrinking_range} | collapse={speed_collapse}"
            )
            return "TURN_ENTRY"

        return "NO_TRADE"
