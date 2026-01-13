# options_momentum_engine.py
import time
from collections import defaultdict, deque
from datetime import datetime, time as dtime
import pytz


class OptionsMomentumEngine:
    """
    OPTIONS MOMENTUM ENGINE (REAL-TIME)

    ADDED:
      ✅ Day-of-cycle awareness (Tue → Expiry)
      ✅ Time-of-day regime awareness
      ✅ Adaptive aggression (NO Greeks needed)

    FIXED:
      ✅ Per-trade PnL
      ✅ No EXIT→ENTRY same second
      ✅ Correct LONG / SHORT PnL
    """

    IST = pytz.timezone("Asia/Kolkata")

    MARKET_START = dtime(9, 10)
    MARKET_END = dtime(15, 35)

    # -----------------------------
    # BASE STRATEGY THRESHOLDS
    # -----------------------------
    BASE_A_SPEED = 1.8
    BASE_A_VOL = 2.5

    BASE_B_WICK = 0.65
    BASE_B_SPEED = 0.7

    EXIT_PULLBACK = 0.40
    EXIT_VOL_DROP = 0.40

    def __init__(self):
        self.tick_buffer = defaultdict(deque)
        self.candles = defaultdict(deque)

        self.active_trade = {}
        self.last_action_sec = {}

        self.last_trade_pnl = defaultdict(float)
        self.cum_pnl = defaultdict(float)

    # --------------------------------------------------
    # MARKET HOURS
    # --------------------------------------------------
    def market_open(self) -> bool:
        now = datetime.now(self.IST)
        if now.weekday() >= 5:
            return False
        return self.MARKET_START <= now.time() <= self.MARKET_END

    # --------------------------------------------------
    # DAY CYCLE (Tue start → Tue expiry)
    # --------------------------------------------------
    def option_day_index(self) -> int:
        wd = datetime.now(self.IST).weekday()
        # Tue=1, Wed=2, Fri=4, Mon=0
        if wd == 1:
            return 1  # Tue
        if wd == 2:
            return 2  # Wed
        if wd == 4:
            return 3  # Fri
        if wd == 0:
            return 4  # Mon
        if wd == 1:
            return 5  # Expiry Tue
        return 0

    # --------------------------------------------------
    # TIME REGIME
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
    # MAIN ENTRY
    # --------------------------------------------------
    def on_tick(self, secid: int, tick: dict) -> str:
        if not self.market_open():
            return "NO_TRADE"

        ts = tick.get("ts")
        ltp = float(tick.get("ltp", 0) or 0)
        if not ts or ltp <= 0:
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
    # 1-SECOND OHLC
    # --------------------------------------------------
    def _build_1s_candle(self, secid):
        ticks = self.tick_buffer[secid]
        if len(ticks) < 2:
            return None

        prices = [t["ltp"] for t in ticks if t.get("ltp", 0) > 0]
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
    # PnL
    # --------------------------------------------------
    def _trade_pnl(self, side, entry, exit_price):
        return (exit_price - entry) if side == "LONG" else (entry - exit_price)

    # --------------------------------------------------
    # CORE LOGIC
    # --------------------------------------------------
    def _evaluate(self, secid: int) -> str:
        c = self.candles[secid]
        if len(c) < 5:
            return "NO_TRADE"

        last, prev = c[-1], c[-2]
        cur_sec = last["sec"]

        if self.last_action_sec.get(secid) == cur_sec:
            return "NO_TRADE"

        price_speed = abs(last["close"] - prev["close"])
        avg_range = sum(x["range"] for x in c[-5:]) / 5
        avg_vol = sum(x["volume"] for x in c[-5:]) / 5

        day = self.option_day_index()
        regime = self.time_regime()

        # -------- adaptive multipliers --------
        decay_penalty = 1.0 + (day * 0.15)      # expiry → stricter
        time_boost = 0.8 if regime == "OPEN" else 1.2 if regime == "TREND" else 1.0

        A_SPEED = self.BASE_A_SPEED * decay_penalty * time_boost
        A_VOL = self.BASE_A_VOL * decay_penalty

        # -------- EXIT --------
        if secid in self.active_trade:
            t = self.active_trade[secid]
            pnl = self._trade_pnl(t["side"], t["entry"], last["close"])
            pull = abs(last["close"] - t["entry"]) / max(t["entry"], 1e-9)

            if pull > self.EXIT_PULLBACK or last["volume"] < avg_vol * self.EXIT_VOL_DROP:
                self.last_trade_pnl[secid] = round(pnl, 2)
                self.cum_pnl[secid] += pnl
                self.active_trade.pop(secid)
                self.last_action_sec[secid] = cur_sec
                return "EXIT"

        if secid in self.active_trade:
            return "NO_TRADE"

        # -------- STRATEGY A --------
        if avg_range > 0 and price_speed > avg_range * A_SPEED and last["volume"] > avg_vol * A_VOL:
            self.active_trade[secid] = {
                "type": "A",
                "side": "LONG",
                "entry": last["close"],
                "ts": last["ts"],
            }
            self.last_action_sec[secid] = cur_sec
            return "A_ENTRY"

        # -------- STRATEGY B --------
        rng = last["range"]
        if rng > 0:
            uw = last["high"] - max(last["open"], last["close"])
            lw = min(last["open"], last["close"]) - last["low"]

            if uw > rng * self.BASE_B_WICK:
                self.active_trade[secid] = {
                    "type": "B",
                    "side": "SHORT",
                    "entry": last["close"],
                    "ts": last["ts"],
                }
                self.last_action_sec[secid] = cur_sec
                return "B_ENTRY"

            if lw > rng * self.BASE_B_WICK:
                self.active_trade[secid] = {
                    "type": "B",
                    "side": "LONG",
                    "entry": last["close"],
                    "ts": last["ts"],
                }
                self.last_action_sec[secid] = cur_sec
                return "B_ENTRY"

        return "NO_TRADE"

    # --------------------------------------------------
    # READ-ONLY HELPERS
    # --------------------------------------------------
    def get_trade_pnl(self, secid: int) -> float:
        return round(self.last_trade_pnl.get(secid, 0.0), 2)

    def get_cum_pnl(self, secid: int) -> float:
        return round(self.cum_pnl.get(secid, 0.0), 2)