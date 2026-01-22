from collections import defaultdict, deque
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from trading_models import ScoringInputs, expected_move, score_momentum


class OptionsMomentumEngine:

    IST = ZoneInfo("Asia/Kolkata")

    MARKET_START = dtime(9, 10)
    MARKET_END = dtime(15, 30)

    # ---------------- BASE STRATEGY ----------------
    BASE_A_SPEED = 1.8
    BASE_A_VOL = 2.5

    BASE_B_WICK = 0.65
    BASE_B_SPEED = 0.7

    EXIT_PULLBACK = 0.40
    EXIT_VOL_DROP = 0.40

    # ---------------- SIDEWAYS EXIT (NEW) ----------------
    SIDEWAYS_MIN_HOLD_SEC = 120      # must survive at least 2 mins
    SIDEWAYS_RANGE_RATIO = 0.35      # candle compression
    SIDEWAYS_VOL_RATIO = 0.60        # volume drying
    SIDEWAYS_MAX_PNL_PCT = 0.15      # only small PnL / flat trades

    def __init__(self):
        self.tick_buffer = defaultdict(deque)
        self.candles = defaultdict(deque)

        self.active_trade = {}
        self.last_action_sec = {}

        self.last_trade_pnl = defaultdict(float)
        self.cum_pnl = defaultdict(float)

        self._last_ctx_print_sec = None

    # --------------------------------------------------
    def market_open(self):
        now = datetime.now(self.IST)
        if now.weekday() >= 5:
            return False
        return self.MARKET_START <= now.time() <= self.MARKET_END

    # --------------------------------------------------
    def option_day_index(self):
        wd = datetime.now(self.IST).weekday()
        return {2: 1, 3: 2, 4: 3, 0: 4, 1: 5}.get(wd, 0)

    def option_day_label(self):
        d = self.option_day_index()
        wd = datetime.now(self.IST).strftime("%a")
        return f"{wd} | Day{d}" if d else f"{wd} | Day0"

    # --------------------------------------------------
    def time_regime(self):
        t = datetime.now(self.IST).time()
        if t < dtime(9, 45):
            return "OPEN"
        if t < dtime(13, 30):
            return "MID"
        if t < dtime(15, 0):
            return "TREND"
        return "CLOSE"

    # --------------------------------------------------
    def _print_day_context(self, ts):
        sec = int(ts)
        minute_key = sec // 60
        if self._last_ctx_print_sec == minute_key:
            return
        self._last_ctx_print_sec = minute_key

        now = datetime.now(self.IST)
        print(
            f"🗓️ {now.strftime('%H:%M:%S')} IST | "
            f"{self.option_day_label()} | Regime:{self.time_regime()}"
        )

    # --------------------------------------------------
    def on_tick(self, secid, tick):
        ts = tick.get("ts")
        ltp = float(tick.get("ltp", 0) or 0)
        if not ts or ltp <= 0:
            return "NO_TRADE"

        self._print_day_context(ts)

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

        return {
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "range": max(prices) - min(prices),
            "volume": sum(int(t.get("last_traded_qty", 0) or 0) for t in ticks),
            "ts": float(ticks[-1]["ts"]),
            "sec": int(ticks[-1]["ts"]),
        }

    # --------------------------------------------------
    def _trade_pnl(self, side, entry, exit_price):
        return (exit_price - entry) if side == "LONG" else (entry - exit_price)

    # --------------------------------------------------
    def _evaluate(self, secid):
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
        time_boost = 1.10 if regime == "OPEN" else 0.90 if regime == "TREND" else 1.15 if regime == "CLOSE" else 1.00

        A_SPEED = self.BASE_A_SPEED * decay_penalty * time_boost
        A_VOL = self.BASE_A_VOL * decay_penalty

        B_SPEED = self.BASE_B_SPEED * decay_penalty * time_boost

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
                self.active_trade.pop(secid)
                self.last_action_sec[secid] = cur_sec
                return "EXIT"

        # ==================================================
        # 🟡 SIDEWAYS EXIT (NEW, SAFE)
        # ==================================================
        if secid in self.active_trade:
            t = self.active_trade[secid]
            age = last["ts"] - t["ts"]
            pnl = self._trade_pnl(t["side"], t["entry"], last["close"])
            pnl_pct = abs(pnl) / max(t["entry"], 1e-9)

            compressed_range = avg_range < (prev["range"] * self.SIDEWAYS_RANGE_RATIO)
            weak_volume = avg_vol > 0 and last["volume"] < avg_vol * self.SIDEWAYS_VOL_RATIO
            low_speed = price_speed < avg_range * 0.4

            if (
                age >= self.SIDEWAYS_MIN_HOLD_SEC
                and compressed_range
                and weak_volume
                and low_speed
                and pnl_pct < self.SIDEWAYS_MAX_PNL_PCT
            ):
                self.active_trade.pop(secid)
                self.last_action_sec[secid] = cur_sec
                return "EXIT"

        if secid in self.active_trade:
            return "NO_TRADE"

        # ==================================================
        # 🟢 STRATEGY A (Breakout LONG) — UNCHANGED
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

        return "NO_TRADE"