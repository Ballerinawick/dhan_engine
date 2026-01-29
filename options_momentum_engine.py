from collections import defaultdict, deque
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo  # ✅ built-in (no install needed)

from trading_models import ScoringInputs, expected_move, score_momentum


class OptionsMomentumEngine:
    """
    OPTIONS MOMENTUM ENGINE (REAL-TIME)

    Keeps:
      ✅ Day-of-cycle awareness
      ✅ Time-of-day regime awareness
      ✅ Adaptive aggression (NO Greeks needed)
      ✅ Strategy A + Strategy B (original working)
      ✅ Existing dynamic exit untouched

    Adds:
      ✅ Sideways exit (behavior-based) WITHOUT breaking entry logic
    """

    IST = ZoneInfo("Asia/Kolkata")

    MARKET_START = dtime(9, 10)
    MARKET_END = dtime(15, 30)  # keep as you had

    # -----------------------------
    # BASE STRATEGY THRESHOLDS
    # -----------------------------
    BASE_A_SPEED = 1.8
    BASE_A_VOL = 2.5

    BASE_B_WICK = 0.65
    BASE_B_SPEED = 0.7

    EXIT_PULLBACK = 0.40
    EXIT_VOL_DROP = 0.40

    # -----------------------------
    # SIDEWAYS EXIT (NEW)
    # -----------------------------
    SIDEWAYS_MIN_HOLD_SEC = 120         # must hold at least 2 mins
    SIDEWAYS_RANGE_RATIO = 0.35         # compressed volatility
    SIDEWAYS_VOL_RATIO = 0.60           # volume dries
    SIDEWAYS_LOW_SPEED_RATIO = 0.40     # speed is low vs avg_range
    SIDEWAYS_MAX_PNL_PCT = 0.15         # only exit if still small/flat (protect winners)

    def __init__(self):
        self.tick_buffer = defaultdict(deque)
        self.candles = defaultdict(deque)

        self.active_trade = {}
        self.last_action_sec = {}

        self.last_trade_pnl = defaultdict(float)
        self.cum_pnl = defaultdict(float)

        self._last_ctx_print_sec = None

        # optional: keep last exit reason per secid (for logging later)
        self.last_exit_reason = {}

    # --------------------------------------------------
    def market_open(self) -> bool:
        now = datetime.now(self.IST)
        if now.weekday() >= 5:
            return False
        return self.MARKET_START <= now.time() <= self.MARKET_END

    # --------------------------------------------------
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

    # --------------------------------------------------
    def time_regime(self) -> str:
        t = datetime.now(self.IST).time()
        if t < dtime(9, 45):
            return "OPEN"
        if t < dtime(13, 30):
            return "MID"
        if t < dtime(15, 0):
            return "TREND"
        return "CLOSE"

    # --------------------------------------------------
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
        if len(self.candles[secid]) > 30:
            self.candles[secid].popleft()

        return self._evaluate(secid)

    # --------------------------------------------------
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

        return {
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "range": h - l,
            "volume": sum(int(t.get("last_traded_qty", 0) or 0) for t in ticks),
            "ts": float(ticks[-1]["ts"]),
            "sec": int(ticks[-1]["ts"]),
        }

    # --------------------------------------------------
    def _trade_pnl(self, side, entry, exit_price):
        return (exit_price - entry) if side == "LONG" else (entry - exit_price)

    # --------------------------------------------------
    def _evaluate(self, secid: int) -> str:
        c = self.candles[secid]
        if len(c) < 5:
            return "NO_TRADE"

        last, prev = c[-1], c[-2]
        cur_sec = int(last["sec"])

        if self.last_action_sec.get(secid) == cur_sec:
            return "NO_TRADE"

        price_speed = abs(last["close"] - prev["close"])

        last5 = list(c)[-5:]
        avg_range = sum(x["range"] for x in last5) / max(len(last5), 1)
        avg_vol = sum(x["volume"] for x in last5) / max(len(last5), 1)

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
        # 🔴 EXISTING DYNAMIC EXIT (UNCHANGED)
        # ==================================================
        if secid in self.active_trade:
            t = self.active_trade[secid]
            pnl = self._trade_pnl(t["side"], t["entry"], last["close"])
            pull = abs(last["close"] - t["entry"]) / max(t["entry"], 1e-9)

            vol_exit = (avg_vol > 0) and (last["volume"] < avg_vol * self.EXIT_VOL_DROP)

            if pull > self.EXIT_PULLBACK or vol_exit:
                self.last_trade_pnl[secid] = round(pnl, 2)
                self.cum_pnl[secid] += pnl
                self.active_trade.pop(secid, None)
                self.last_action_sec[secid] = cur_sec
                self.last_exit_reason[secid] = "DYNAMIC"
                return "EXIT"

        # ==================================================
        # 🟡 SIDEWAYS EXIT (NEW) — ONLY if trade is stuck/flat
        # IMPORTANT: must be BEFORE "return NO_TRADE"
        # ==================================================
        if secid in self.active_trade:
            t = self.active_trade[secid]
            age = float(last["ts"]) - float(t["ts"])

            pnl = self._trade_pnl(t["side"], t["entry"], last["close"])
            pnl_pct = abs(pnl) / max(t["entry"], 1e-9)

            compressed_range = avg_range > 0 and (avg_range < (avg_range * self.SIDEWAYS_RANGE_RATIO + 1e-9))
            weak_volume = (avg_vol > 0) and (last["volume"] < avg_vol * self.SIDEWAYS_VOL_RATIO)
            low_speed = (avg_range > 0) and (price_speed < avg_range * self.SIDEWAYS_LOW_SPEED_RATIO)

            if (
                age >= self.SIDEWAYS_MIN_HOLD_SEC
                and weak_volume
                and low_speed
                and pnl_pct <= self.SIDEWAYS_MAX_PNL_PCT
            ):
                self.active_trade.pop(secid, None)
                self.last_action_sec[secid] = cur_sec
                self.last_exit_reason[secid] = "SIDEWAYS"
                return "EXIT"

        # if trade still active after exits → do nothing
        if secid in self.active_trade:
            return "NO_TRADE"

        # ==================================================
        # 🟢 STRATEGY A (Breakout LONG) — original working
        # ==================================================
        if (
            avg_range > 0
            and price_speed > avg_range * A_SPEED
            and avg_vol > 0
            and last["volume"] > avg_vol * A_VOL
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
        # 🟣 STRATEGY B (Trap Reversal) — original working
        # ==================================================
        rng = float(last["range"])
        if rng > 0 and avg_range > 0:
            uw = float(last["high"]) - max(float(last["open"]), float(last["close"]))
            lw = min(float(last["open"]), float(last["close"])) - float(last["low"])

            trap_up = uw > rng * B_WICK
            trap_down = lw > rng * B_WICK

            if price_speed > avg_range * B_SPEED:
                if trap_up:
                    # LONG-ONLY enforcement: ignore SHORT trap entries
                    pass

                if trap_down:
                    self.active_trade[secid] = {
                        "type": "B",
                        "side": "LONG",
                        "entry": float(last["close"]),
                        "ts": float(last["ts"]),
                    }
                    self.last_action_sec[secid] = cur_sec
                    return "B_ENTRY"

        return "NO_TRADE"
