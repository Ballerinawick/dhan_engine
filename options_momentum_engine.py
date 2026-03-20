from collections import defaultdict, deque
from datetime import datetime, time as dtime, timezone, timedelta
from statistics import median
from zoneinfo import ZoneInfo
import time

from trade_lifecycle.trade_state import TradeState
from trade_lifecycle.entry_acceptance_analyzer import EntryAcceptanceAnalyzer
from trade_lifecycle.failure_analyzer import FailureAnalyzer
from trade_lifecycle.multi_tf_bias_filter import MultiTimeframeBiasFilter
from trade_lifecycle.momentum_phase_manager import MomentumPhaseManager


class OptionsMomentumEngine:
    """
    OPTIONS MOMENTUM ENGINE (REAL-TIME)

    Critical lifecycle fixes:
      ✅ TradeState last_price tuple bug removed
      ✅ All exits now close active_trade + trade_state consistently
      ✅ PnL / fees / hold time updated consistently for every exit path
      ✅ FailureAnalyzer only handles true failure exits
      ✅ Trailing giveback stays inside engine
      ✅ Entry rejection exit respects phase gate
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
    MICRO_MIN_FLOW = 250

    TURN_SPEED_RATIO_THRESHOLD = 1.2

    def __init__(self):
        self.IST = timezone(timedelta(hours=5, minutes=30))

        self.tick_buffer = defaultdict(deque)
        self.candles = defaultdict(deque)
        self.candles_3s = defaultdict(deque)
        self.last_3s_bucket = {}

        self.active_trade = {}
        self.trade_state = {}
        self.acceptance_analyzer = EntryAcceptanceAnalyzer()
        self.failure_analyzer = FailureAnalyzer()
        self.bias_filter = MultiTimeframeBiasFilter()
        self.phase_manager = MomentumPhaseManager()
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

        self._printed_sample_tick = set()
        self._printed_sample_tick_ts = {}

        self.warmup_start_sec = {}
        self.warmup_reported = set()
        self.warmup_active = False
        self.baseline_printed = set()
        self.warmup_duration_sec = 120
        self.warmup_stats = defaultdict(lambda: {
            "avg_range_5": [],
            "speed": [],
            "abs_flow": [],
            "abs_imbalance_5": [],
            "absorption_true": 0,
            "absorption_total": 0,
            "spread": [],
        })

    def market_open(self) -> bool:
        now = datetime.now(self.IST)
        if now.weekday() >= 5:
            return False
        return self.MARKET_START <= now.time() <= self.MARKET_END

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

    def on_tick(self, secid: int, tick: dict) -> str:
        ts = tick.get("ts")
        ltp = float(tick.get("ltp", 0) or 0)
        if not ts or ltp <= 0:
            return "NO_TRADE"

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

        bucket_3 = candle_sec // 3
        if self.last_3s_bucket.get(secid) != bucket_3:
            self.last_3s_bucket[secid] = bucket_3
            last3 = list(self.candles[secid])[-3:]
            if len(last3) == 3:
                o = last3[0]["open"]
                h = max(x["high"] for x in last3)
                l = min(x["low"] for x in last3)
                c3 = last3[-1]["close"]

                self.candles_3s[secid].append({
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c3,
                    "sec": candle_sec
                })

                if len(self.candles_3s[secid]) > 20:
                    self.candles_3s[secid].popleft()

                print(f"🕒 3S_CANDLE | secid={secid} | close={c3:.2f}")

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
        bid = float(last_tick.get("bid_price") or last_tick.get("bid") or 0)
        ask = float(last_tick.get("ask_price") or last_tick.get("ask") or 0)
        spread = float(last_tick.get("spread", 0) or 0)
        if spread <= 0 and bid > 0 and ask > 0:
            spread = max(ask - bid, 0.0)
        if spread <= 0:
            spread = max(ask - bid, 0.01)

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

        return f"n={n} p50={pct(0.50):.4f} p90={pct(0.90):.4f} p99={pct(0.99):.4f} max={sorted_vals[-1]:.4f}"

    def _collect_warmup_stats(self, secid: int, cur_sec: int, speed: float, avg_range_5: float, last_tick: dict):
        if secid in self.warmup_reported:
            return

        if secid not in self.warmup_start_sec:
            self.warmup_start_sec[secid] = cur_sec

        elapsed_sec = max(cur_sec - self.warmup_start_sec[secid], 0)
        self.warmup_active = elapsed_sec < self.warmup_duration_sec
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

    def _close_trade(self, secid: int, exit_price: float, cur_sec: int, reason: str) -> str:
        trade = self.active_trade.pop(secid, None)
        self.trade_state.pop(secid, None)

        if trade is None:
            print(f"⚠️ CLOSE_TRADE_SKIPPED | secid={secid} | reason=no_active_trade | exit_reason={reason}")
            return "NO_TRADE"

        pnl = self._trade_pnl(trade["side"], float(trade["entry"]), float(exit_price))
        hold_sec = max(float(trade.get("last_ts", trade["ts"])) - float(trade["ts"]), 0.0)
        exit_time = datetime.fromtimestamp(float(trade.get("last_ts", trade["ts"])), self.IST).strftime("%H:%M:%S")
        entry_time = datetime.fromtimestamp(float(trade["ts"]), self.IST).strftime("%H:%M:%S")

        self.exits_taken += 1
        self.total_hold_sec += hold_sec
        self.fees_paid += self.fee_per_trade
        self.last_action_sec[secid] = cur_sec
        self.last_exit_reason[secid] = reason
        self.last_trade_pnl[secid] = round(pnl, 2)
        self.cum_pnl[secid] += pnl

        print(
            f"📘 TRADE_SUMMARY | "
            f"entry_time={entry_time} | "
            f"exit_time={exit_time} | "
            f"hold={hold_sec:.2f}s | "
            f"entry={trade['entry']:.2f} | "
            f"exit={exit_price:.2f} | "
            f"pnl={pnl:.2f} | "
            f"reason={reason}"
        )
        print(f"🧹 TRADE_STATE_CLEAN | secid={secid} | price={exit_price:.2f}")
        print(f"🚪 EXIT_REASON | secid={secid} | price={exit_price:.2f} | reason={reason}")
        print(
            f"🔻 TURN_EXIT_LONG | secid={secid} | exit={exit_price:.2f} | "
            f"pnl={pnl:.2f} | hold_sec={hold_sec:.2f} | reason={reason}"
        )
        return "EXIT"

    def _evaluate(self, secid: int) -> str:
        c = self.candles[secid]
        if len(c) < 8:
            return "NO_TRADE"

        last, prev = c[-1], c[-2]
        cur_sec = int(last["sec"])

        if self.last_action_sec.get(secid) == cur_sec:
            return "NO_TRADE"

        prev2 = c[-3]
        speed = float(last["close"] - prev["close"])
        prev_speed = float(prev["close"] - prev2["close"])

        debug_last5 = list(c)[-5:]
        avg_range_5, avg_vol_5 = self._avg_range_vol(debug_last5)

        speed_ratio = min(abs(speed) / max(avg_range_5, 1e-6), 50.0)
        vol_ratio = (last["volume"] / avg_vol_5) if avg_vol_5 > 0 and last["volume"] > 0 else 0.0

        last5 = list(c)[-5:]
        last20 = list(c)[-20:] if len(c) >= 20 else list(c)

        avg_range_5, avg_vol_5 = self._avg_range_vol(last5)
        avg_range_20, avg_vol_20 = self._avg_range_vol(last20)

        print(
            f"🔎 TURN_CHECK | secid={secid} | "
            f"speed={speed:.4f} | prev_speed={prev_speed:.4f} | "
            f"speed_ratio={speed_ratio:.2f} | "
            f"avg_range_5={avg_range_5:.4f}"
        )

        last_tick = self.tick_buffer[secid][-1] if self.tick_buffer[secid] else {}
        tick_prices = [
            float(t.get("ltp", 0) or 0)
            for t in self.tick_buffer[secid]
            if float(t.get("ltp", 0) or 0) > 0
        ]
        last_prices = tick_prices[-6:-1] if len(tick_prices) >= 6 else tick_prices[-5:]
        if len(last_prices) < 5:
            candle_closes = [float(x["close"]) for x in list(c)]
            last_prices = candle_closes[-6:-1] if len(candle_closes) >= 6 else candle_closes[-5:]

        recent_high = max(last_prices[-5:])
        recent_low = min(last_prices[-5:])

        imbalance = float(last_tick.get("imbalance_5", 0) or 0)
        flow = float(last_tick.get("flow", 0) or 0)
        ofi = float(last_tick.get("ofi", 0) or 0)
        absorb_strength = float(last_tick.get("absorption_strength", 0) or 0)

        pressure_score = (
            0.40 * imbalance +
            0.30 * (ofi / 1000.0) +
            0.20 * (flow / 1000.0) +
            0.10 * absorb_strength
        )
        pressure_score = max(min(pressure_score, 1.0), -1.0)

        self._collect_warmup_stats(secid, cur_sec, speed, avg_range_5, last_tick)
        self._log_engine_state(cur_sec)

        shrinking_range = float(last["range"]) < float(prev["range"])
        speed_collapse = abs(speed) < abs(prev_speed) * 0.6
        midpoint_prev = (float(prev["high"]) + float(prev["low"])) / 2.0

        if secid in self.active_trade:
            t = self.active_trade[secid]
            entry = float(t["entry"])
            price = float(last["close"])
            t["last_ts"] = float(last["ts"])

            state = self.trade_state.get(secid)
            if state:
                state.update(price, last["ts"])
                phase = self.phase_manager.get_phase(state)

                print(
                    f"🚀 MOMENTUM_PHASE | secid={secid} | phase={phase} | "
                    f"seconds_in_trade={state.seconds_in_trade:.2f}"
                )

                print(
                    f"📊 TRADE_STATE_UPDATE | secid={secid} | price={price:.2f} | "
                    f"mfe={state.mfe:.3f} | mae={state.mae:.3f} | "
                    f"below_entry={state.seconds_below_entry:.2f} | "
                    f"retests={state.retests}"
                )

            t["best_price"] = max(float(t["best_price"]), price)
            t["worst_price"] = min(float(t["worst_price"]), price)
            t["mfe"] = max(float(t["best_price"]) - entry, 0.0)
            t["mae"] = max(entry - float(t["worst_price"]), 0.0)

            spread = max(float(t["entry_spread"]), 0.05)

            # --- SMART HOLD SYSTEM ---
            if state:

                # 🟡 PROBE PHASE (0–15 sec)
                if state.seconds_in_trade < 15:

                    # Only exit if BOTH strong adverse move + no recovery attempt
                    strong_adverse = state.mae > max(spread * 10, 2.0)

                    no_bounce = (
                        price < entry - (spread * 2)
                        and state.mfe < spread * 0.3
                    )

                    if strong_adverse and no_bounce:
                        print(f"🚪 EARLY_STRUCTURE_FAIL | secid={secid}")
                        return self._close_trade(secid, price, cur_sec, "EARLY_STRUCTURE_FAIL")

                    return "HOLD"

                # 🛡️ Minimum hold guarantee before any later exit
                if state.seconds_in_trade < 20:
                    return "HOLD"

                # 🟠 BUILD PHASE (20–60 sec)
                if state.seconds_in_trade < 60:

                    # Allow recovery zone
                    if state.mae > max(spread * 12, 2.5):

                        # Only exit if structure is clearly broken
                        if price < entry - (spread * 2):
                            print(f"🚪 STRUCTURE_FAIL_EXIT | secid={secid}")
                            return self._close_trade(secid, price, cur_sec, "STRUCTURE_FAIL")

                    return "HOLD"

            if state and state.seconds_in_trade > 60 and state.mfe < spread * 1.5:
                print(f"🚫 NO_EXPANSION_EXIT | secid={secid} | mfe={state.mfe:.2f}")
                return self._close_trade(secid, price, cur_sec, "NO_EXPANSION")

            if state:
                fail = self.failure_analyzer.check(state, spread)
                if fail and self.phase_manager.allow_failure_exit(state):
                    print(f"🚪 FAILURE_EXIT | secid={secid} | price={price:.2f} | reason={fail['reason']}")
                    return self._close_trade(secid, price, cur_sec, fail["reason"])

                acc = self.acceptance_analyzer.evaluate(state)
                print(f"✅ ACCEPTANCE_STATUS | secid={secid} | price={price:.2f} | status={acc}")

                if acc == "REJECTED" and self.phase_manager.allow_acceptance_reject_exit(state):
                    print(f"🚪 ENTRY_REJECTION_EXIT | secid={secid} | price={price:.2f}")
                    return self._close_trade(secid, price, cur_sec, "ENTRY_REJECTED")

            if not t["breakeven_armed"] and t["mfe"] >= spread:
                t["breakeven_armed"] = True
                print(f"🛡 BREAKEVEN_ARMED | secid={secid} | price={price:.2f}")

            if not t["profit_lock_armed"] and t["mfe"] >= spread * 3:
                t["profit_lock_armed"] = True
                print(
                    f"🔒 PROFIT_LOCK_ARMED | secid={secid} | "
                    f"mfe={t['mfe']:.3f} | spread={spread:.3f}"
                )

            if (
                state
                and state.seconds_in_trade > 60
                and state.mfe > spread * 6.0
                and state.mae > state.mfe * 0.5
            ):
                print(f"🚨 PROFIT_PROTECTION_EXIT | secid={secid}")
                return self._close_trade(secid, price, cur_sec, "PROFIT_PROTECTION")

            if state and t["profit_lock_armed"] and self.phase_manager.allow_trailing_exit(state):
                if t["mfe"] < max(spread * 2, 1.2):
                    pass

                trail_distance = max(spread * 2, 1.5)
                locked_price = float(t["best_price"]) - trail_distance

                if t.get("locked_price") is None:
                    t["locked_price"] = locked_price
                else:
                    t["locked_price"] = max(t["locked_price"], locked_price)

                t["locked_price"] = max(t["locked_price"], entry + spread)

                if t.get("locked_price") is not None and price <= t["locked_price"]:
                    print(
                        f"🔐 LOCKED_PROFIT_EXIT | secid={secid} | "
                        f"price={price:.2f} | locked_price={t['locked_price']:.2f}"
                    )
                    return self._close_trade(secid, price, cur_sec, "PROFIT_TRAIL")

                giveback = float(t["best_price"]) - price
                mfe = float(t["mfe"])

                if mfe < spread * 4:
                    threshold = mfe * 0.60
                elif mfe < spread * 8:
                    threshold = mfe * 0.45
                else:
                    threshold = mfe * 0.35

                if giveback >= threshold:
                    print(
                        f"📉 TRAILING_EXIT | secid={secid} | "
                        f"price={price:.2f} | best={t['best_price']:.2f} | "
                        f"mfe={t['mfe']:.3f} | giveback={giveback:.3f} | threshold={threshold:.3f}"
                    )
                    return self._close_trade(secid, price, cur_sec, "PROFIT_TRAIL")

            opposite_turn_exit = (
                prev_speed > 0
                and speed < 0
                and speed_ratio > self.TURN_SPEED_RATIO_THRESHOLD
                and shrinking_range
                and speed_collapse
                and float(last["close"]) < midpoint_prev
            )

            momentum_confirmed = (
                t["mfe"] >= spread
                or (state is not None and state.accepted)
            )

            if opposite_turn_exit and not momentum_confirmed:
                print(
                    f"🧱 TURN_BLOCKED_NO_MOMENTUM | "
                    f"secid={secid} | price={price:.2f} | "
                    f"mfe={t['mfe']:.3f} | spread={spread:.3f}"
                )

            if state and opposite_turn_exit and momentum_confirmed and self.phase_manager.allow_turn_exit(state):
                print(
                    f"⚠️ OPPOSITE_TURN_CHECK | secid={secid} | "
                    f"speed={speed:.4f} | prev_speed={prev_speed:.4f} | "
                    f"ratio={speed_ratio:.2f} | close={last['close']:.2f} | "
                    f"midpoint_prev={midpoint_prev:.2f}"
                )
                return self._close_trade(secid, price, cur_sec, "OPPOSITE_TURN")

            return "NO_TRADE"

        if not self._micro_ok(secid, last_tick, cur_sec):
            return "NO_TRADE"

        current_time = datetime.fromtimestamp(float(last["ts"]), self.IST)
        weekday = current_time.weekday()  # 0=Mon ... 6=Sun

        # Avoid slow start day (low edge)
        if weekday == 2:  # Wednesday
            return "NO_TRADE"

        prior_move_down = prev_speed < 0
        exhaust_score = 0

        if speed_ratio > self.TURN_SPEED_RATIO_THRESHOLD:
            exhaust_score += 1
        if shrinking_range:
            exhaust_score += 1
        if speed_collapse:
            exhaust_score += 1

        down_exhaustion = exhaust_score >= 2
        recent_high_5 = max(x["high"] for x in list(c)[-5:])
        recent_low_5 = min(x["low"] for x in list(c)[-5:])

        breakout_confirm = float(last["close"]) > recent_high_5

        failed_breakdown = (
            float(prev["low"]) < recent_low_5 and
            float(last["close"]) > float(prev["close"])
        )

        structure_reversal = breakout_confirm or failed_breakdown

        snap_reversal = (
            prev_speed < 0 and
            speed > 0 and
            abs(speed) > avg_range_5 * 0.8 and
            float(last["close"]) > float(prev["close"]) and
            float(last_tick.get("flow", 0) or 0) > 300
        )

        allow_snap = snap_reversal

        weak_snap = (
            prev_speed < 0 and
            speed > 0 and
            abs(speed) < avg_range_5 * 0.4
        )

        if weak_snap and not breakout_confirm:
            return "NO_TRADE"

        reversal_confirm = structure_reversal or allow_snap

        strong_downtrend = (
            prev_speed < 0 and
            speed < 0 and
            abs(speed) > avg_range_5 * 0.5
        )

        if strong_downtrend and not breakout_confirm and not allow_snap:
            return "NO_TRADE"

        weak_bounce = (
            prev_speed < 0 and
            speed > 0 and
            abs(speed) < avg_range_5 * 0.3
        )

        if weak_bounce and not breakout_confirm:
            return "NO_TRADE"

        base_formed = (
            abs(float(last["close"]) - recent_low_5) < avg_range_5 * 1.5
        )

        if not base_formed and not breakout_confirm and not allow_snap:
            return "NO_TRADE"

        tf3_ok = self.bias_filter.check(self.candles[secid], self.candles_3s[secid])

        print(
            f"🧭 TF_CONFIRM | secid={secid} | price={float(last['close']):.2f} | "
            f"1s_turn={prior_move_down and down_exhaustion and reversal_confirm} | "
            f"3s_ok={tf3_ok}"
        )

        pressure_ok = pressure_score > 0.12 or ofi > 150
        print(f"🟢 ENTRY | secid={secid} | price={float(last['close']):.2f} | pressure_ok={pressure_ok}")

        score = 0
        score += 1 if prior_move_down else 0
        score += 1 if down_exhaustion else 0
        score += 1 if reversal_confirm else 0
        score += 1 if tf3_ok else 0
        score += 1 if pressure_ok else 0

        flow = float(last_tick.get("flow", 0) or 0)
        imbalance = abs(float(last_tick.get("imbalance_5", 0) or 0))
        liquidity_impulse = (
            flow > 300 or imbalance > 0.18
        )
        if not liquidity_impulse:
            score -= 1

        print(
            f"🧠 ENTRY_SCORE | secid={secid} | score={score} | "
            f"prior={prior_move_down} exhaust={down_exhaustion} "
            f"reversal={reversal_confirm} tf3={tf3_ok} pressure={pressure_ok}"
        )

        if score < 5 and not allow_snap:
            print(
                f"🚫 ENTRY_REJECT | secid={secid} | "
                f"prior_down={prior_move_down} | "
                f"exhaust={down_exhaustion} | "
                f"reversal={reversal_confirm} | "
                f"tf3={tf3_ok} | "
                f"pressure_ok={pressure_ok}"
            )
            return "NO_TRADE"

        print(
            f"🧠 PRESSURE_CHECK | secid={secid} | "
            f"pressure={pressure_score:.3f} | "
            f"imb={imbalance:.3f} | flow={flow:.1f} | ofi={ofi:.1f}"
        )

        spread_value = float(last_tick.get("spread", 0) or 0)
        bid = float(last_tick.get("bid_price") or last_tick.get("bid") or 0)
        ask = float(last_tick.get("ask_price") or last_tick.get("ask") or 0)
        if spread_value <= 0 and bid > 0 and ask > 0:
            spread_value = max(ask - bid, 0.0)
        spread_value = max(spread_value, 0.01)
        expected_move = avg_range_5 * 3
        micro_range = recent_high - recent_low

        # Slight restriction on expiry volatility
        if weekday == 1:  # Tuesday
            if expected_move < spread_value * 3:
                return "NO_TRADE"

        print(
            f"ENTRY_FILTER | micro_range={micro_range:.3f} | "
            f"recent_high={recent_high:.3f} | "
            f"recent_low={recent_low:.3f}"
        )

        # --- PRE-ENTRY MOMENTUM CONFIRMATION ---
        if avg_range_5 < spread_value * 1.2:
            return "NO_TRADE"

        if micro_range < spread_value * 1.1:
            return "NO_TRADE"

        price = float(last["close"])
        # Allow early breakout participation
        if price < recent_high * 0.998:
            return "NO_TRADE"

        print(
            f"📊 EXPECTED_MOVE_CHECK | secid={secid} | "
            f"expected_move={expected_move:.4f} | spread={spread_value:.4f}"
        )

        if expected_move <= spread_value * 1.2:
            print(
                f"🚫 ENTRY_BLOCK_FEE_RISK | secid={secid} | "
                f"expected_move={expected_move:.4f} | spread={spread_value:.4f}"
            )
            return "NO_TRADE"

        if secid in self.last_action_sec:
            if cur_sec - self.last_action_sec[secid] < 8:
                return "NO_TRADE"

        # --- LIQUIDITY + IMPULSE FILTER ---
        if not (
            abs(speed) > 0.12
            and abs(imbalance) > 0.08
            and flow > 400
        ):
            return "NO_TRADE"

        # --- ATM FILTER ---
        atm_strike = float(last_tick.get("atm_strike", price) or price)
        if abs(price - atm_strike) > spread_value * 10:
            return "NO_TRADE"

        self.active_trade[secid] = {
            "type": "TURN",
            "side": "LONG",
            "entry": float(last["close"]),
            "ts": float(last["ts"]),
            "last_ts": float(last["ts"]),
            "best_price": float(last["close"]),
            "worst_price": float(last["close"]),
            "mfe": 0.0,
            "locked_price": None,
            "mae": 0.0,
            "breakeven_armed": False,
            "profit_lock_armed": False,
            "entry_spread": float(last_tick.get("spread", 0) or 0),
            "entry_pressure": pressure_score,
        }

        self.trade_state[secid] = TradeState(
            entry_price=float(last["close"]),
            ts=float(last["ts"])
        )

        print(
            f"📍 TRADE_STATE_INIT | secid={secid} | price={float(last['close']):.2f} | entry={last['close']:.2f}"
        )
        print(
            f"🟢 ENTRY_LOG | "
            f"time={datetime.fromtimestamp(last['ts'], self.IST).strftime('%H:%M:%S')} | "
            f"secid={secid} | "
            f"type={'CE' if 'CE' in str(secid) else 'PE'} | "
            f"price={last['close']:.2f}"
        )
        self.entries_taken += 1
        self.last_action_sec[secid] = cur_sec

        print(
            f"🟢 TURN_ENTRY | secid={secid} | price={float(last['close']):.2f} | side=LONG | "
            f"speed_ratio={speed_ratio:.2f} | shrink={shrinking_range} | collapse={speed_collapse}"
        )
        print(f"✅ ENTRY_ALLOWED | secid={secid} | price={float(last['close']):.2f} | speed_ratio={speed_ratio:.2f}")
        return "TURN_ENTRY"
