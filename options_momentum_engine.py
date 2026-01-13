# options_momentum_engine.py
import time
from collections import defaultdict, deque
from datetime import datetime, time as dtime
import pytz


class OptionsMomentumEngine:
    """
    OPTIONS MOMENTUM ENGINE (REAL-TIME)

    STEP 1: 1-second OHLC candles
    STEP 2: Price Speed (ΔLTP / sec)
    STEP 3: Strategy-A (Scalp Breakout)
    STEP 4: Strategy-B (Trap Reversal)
    STEP 5: Paper PnL Simulator

    Output:
      A_ENTRY, B_ENTRY, EXIT, NO_TRADE
    """

    IST = pytz.timezone("Asia/Kolkata")

    MARKET_START = dtime(9, 10)
    MARKET_END = dtime(15, 35)

    # -----------------------------
    # STRATEGY THRESHOLDS (TUNABLE)
    # -----------------------------
    A_SPEED_MULT = 1.8     # breakout aggression
    A_VOL_MULT = 2.5

    B_TRAP_WICK = 0.65     # wick dominance
    B_REV_SPEED = 0.7

    EXIT_PULLBACK = 0.40
    EXIT_VOL_DROP = 0.40

    def __init__(self):
        self.tick_buffer = defaultdict(deque)     # secid -> ticks (current second)
        self.candles = defaultdict(deque)         # secid -> last N candles
        self.active_trade = {}                    # secid -> trade state
        self.pnl = defaultdict(float)             # secid -> cumulative pnl

    # --------------------------------------------------
    # MARKET HOURS GATE
    # --------------------------------------------------
    def market_open(self) -> bool:
        now = datetime.now(self.IST)
        if now.weekday() >= 5:
            return False
        return self.MARKET_START <= now.time() <= self.MARKET_END

    # --------------------------------------------------
    # MAIN ENTRY (CALLED FROM WS CALLBACK)
    # --------------------------------------------------
    def on_tick(self, secid: int, tick: dict) -> str:
        if not self.market_open():
            return "NO_TRADE"

        ts = tick.get("ts")
        ltp = tick.get("ltp", 0)
        if not ts or ltp <= 0:
            return "NO_TRADE"

        sec = int(ts)
        self.tick_buffer[secid].append(tick)

        # remove old second ticks
        while self.tick_buffer[secid] and int(self.tick_buffer[secid][0]["ts"]) < sec:
            self.tick_buffer[secid].popleft()

        candle = self._build_1s_candle(secid)
        if candle is None:
            return "NO_TRADE"

        self.candles[secid].append(candle)
        if len(self.candles[secid]) > 30:
            self.candles[secid].popleft()

        return self._evaluate(secid)

    # --------------------------------------------------
    # STEP 1: BUILD 1-SECOND OHLC
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
            "volume": sum(t.get("last_traded_qty", 0) for t in ticks),
            "ts": ticks[-1]["ts"],
        }

    # --------------------------------------------------
    # STEP 2–5: STRATEGIES + EXIT + PnL
    # --------------------------------------------------
    def _evaluate(self, secid: int) -> str:
        c = self.candles[secid]
        if len(c) < 5:
            return "NO_TRADE"

        last = c[-1]
        prev = c[-2]

        # STEP 2: PRICE SPEED
        price_speed = abs(last["close"] - prev["close"])
        avg_range = sum(x["range"] for x in list(c)[-5:]) / 5
        avg_vol = sum(x["volume"] for x in list(c)[-5:]) / 5

        # --------------------------------------------------
        # EXIT LOGIC (PRIORITY)
        # --------------------------------------------------
        if secid in self.active_trade:
            trade = self.active_trade[secid]
            entry = trade["entry"]
            side = trade["side"]

            pnl = (last["close"] - entry) if side == "LONG" else (entry - last["close"])
            pullback = abs(pnl) / max(entry, 1)

            if pullback > self.EXIT_PULLBACK or last["volume"] < avg_vol * self.EXIT_VOL_DROP:
                self.pnl[secid] += pnl
                self.active_trade.pop(secid, None)
                return "EXIT"

        # --------------------------------------------------
        # STEP 3: STRATEGY A — SCALP BREAKOUT
        # --------------------------------------------------
        if (
            price_speed > avg_range * self.A_SPEED_MULT
            and last["volume"] > avg_vol * self.A_VOL_MULT
            and secid not in self.active_trade
        ):
            self.active_trade[secid] = {
                "type": "A",
                "side": "LONG",
                "entry": last["close"],
                "ts": last["ts"],
            }
            return "A_ENTRY"

        # --------------------------------------------------
        # STEP 4: STRATEGY B — TRAP REVERSAL
        # --------------------------------------------------
        upper_wick = last["high"] - max(last["open"], last["close"])
        lower_wick = min(last["open"], last["close"]) - last["low"]

        trap_up = upper_wick > last["range"] * self.B_TRAP_WICK
        trap_down = lower_wick > last["range"] * self.B_TRAP_WICK

        if (
            trap_up
            and price_speed > avg_range * self.B_REV_SPEED
            and secid not in self.active_trade
        ):
            self.active_trade[secid] = {
                "type": "B",
                "side": "SHORT",
                "entry": last["close"],
                "ts": last["ts"],
            }
            return "B_ENTRY"

        if (
            trap_down
            and price_speed > avg_range * self.B_REV_SPEED
            and secid not in self.active_trade
        ):
            self.active_trade[secid] = {
                "type": "B",
                "side": "LONG",
                "entry": last["close"],
                "ts": last["ts"],
            }
            return "B_ENTRY"

        return "NO_TRADE"

    # --------------------------------------------------
    # STEP 5: READ-ONLY PnL (PAPER MODE)
    # --------------------------------------------------
    def get_pnl(self, secid: int) -> float:
        return round(self.pnl.get(secid, 0.0), 2)