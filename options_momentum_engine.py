# options_momentum_engine.py
import time
from collections import defaultdict, deque
from datetime import datetime, time as dtime
import pytz


class OptionsMomentumEngine:
    """
    OPTIONS MOMENTUM ENGINE (REAL-TIME)

    FIXED:
      ✅ Bug 1: PnL now per-trade (and optional cumulative)
      ✅ Bug 2: Prevent EXIT → ENTRY in same second (cooldown per sec)
      ✅ Bug 3: Correct PnL sign logic for LONG/SHORT + correct pullback calc

    STEP 1: 1-second OHLC candles
    STEP 2: Price Speed (ΔLTP / sec)
    STEP 3: Strategy-A (Scalp Breakout)
    STEP 4: Strategy-B (Trap Reversal)
    STEP 5: Paper PnL Simulator

    Output:
      "A_ENTRY"
      "B_ENTRY"
      "EXIT"
      "NO_TRADE"
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

        # active trade per secid
        # {
        #   "type": "A"/"B",
        #   "side": "LONG"/"SHORT",
        #   "entry": float,
        #   "entry_ts": float,
        # }
        self.active_trade = {}

        # Optional cumulative pnl (keep for dashboard; but EXIT returns per-trade pnl separately)
        self.cum_pnl = defaultdict(float)          # secid -> cumulative pnl

        # Prevent EXIT → ENTRY in same second
        self.last_action_sec = {}                  # secid -> int(second)

        # Last per-trade pnl snapshot (readable)
        self.last_trade_pnl = defaultdict(float)   # secid -> last closed trade pnl

    # --------------------------------------------------
    # MARKET HOURS GATE
    # --------------------------------------------------
    def market_open(self) -> bool:
        now = datetime.now(self.IST)
        if now.weekday() >= 5:  # Sat / Sun
            return False
        return self.MARKET_START <= now.time() <= self.MARKET_END

    # --------------------------------------------------
    # MAIN ENTRY (CALLED FROM WS CALLBACK)
    # tick must contain: {"ltp": float, "ts": float, optional: "last_traded_qty": int}
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

        prices = [float(t.get("ltp", 0) or 0) for t in ticks if float(t.get("ltp", 0) or 0) > 0]
        if not prices:
            return None

        o = prices[0]
        h = max(prices)
        l = min(prices)
        c = prices[-1]
        rng = h - l

        return {
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "range": rng,
            "volume": sum(int(t.get("last_traded_qty", 0) or 0) for t in ticks),
            "ts": float(ticks[-1]["ts"]),
            "sec": int(ticks[-1]["ts"]),
        }

    # --------------------------------------------------
    # INTERNAL HELPERS
    # --------------------------------------------------
    @staticmethod
    def _safe_div(a: float, b: float) -> float:
        return a / b if b else 0.0

    def _compute_trade_pnl(self, side: str, entry: float, exit_price: float) -> float:
        # ✅ Bug 3 fixed: correct sign
        if side == "LONG":
            return exit_price - entry
        return entry - exit_price  # SHORT

    # --------------------------------------------------
    # STEP 2–5: STRATEGIES + EXIT + PnL
    # --------------------------------------------------
    def _evaluate(self, secid: int) -> str:
        c = self.candles[secid]
        if len(c) < 5:
            return "NO_TRADE"

        last = c[-1]
        prev = c[-2]

        # prevent multiple actions in same second
        cur_sec = int(last["sec"])
        if self.last_action_sec.get(secid) == cur_sec:
            return "NO_TRADE"

        # STEP 2: PRICE SPEED
        price_speed = abs(last["close"] - prev["close"])
        avg_range = sum(x["range"] for x in list(c)[-5:]) / 5.0
        avg_vol = sum(x["volume"] for x in list(c)[-5:]) / 5.0

        # --------------------------------------------------
        # EXIT LOGIC (PRIORITY)
        # --------------------------------------------------
        if secid in self.active_trade:
            trade = self.active_trade[secid]
            entry = float(trade["entry"])
            side = trade["side"]
            exit_price = float(last["close"])

            trade_pnl = self._compute_trade_pnl(side, entry, exit_price)

            # ✅ Bug fix: pullback based on PRICE move, not PnL magnitude
            price_pullback = abs(exit_price - entry) / max(entry, 1e-9)

            vol_exit = (avg_vol > 0) and (last["volume"] < avg_vol * self.EXIT_VOL_DROP)

            if price_pullback > self.EXIT_PULLBACK or vol_exit:
                # per-trade pnl snapshot
                self.last_trade_pnl[secid] = round(trade_pnl, 2)

                # optional cumulative pnl
                self.cum_pnl[secid] += trade_pnl

                # close trade
                self.active_trade.pop(secid, None)

                # ✅ Bug 2 fixed: block re-entry same second
                self.last_action_sec[secid] = cur_sec

                return "EXIT"

        # --------------------------------------------------
        # ENTRY LOGIC (only if no trade open)
        # --------------------------------------------------
        if secid in self.active_trade:
            return "NO_TRADE"

        # --------------------------------------------------
        # STEP 3: STRATEGY A — SCALP BREAKOUT (LONG)
        # --------------------------------------------------
        if (
            avg_range > 0
            and price_speed > avg_range * self.A_SPEED_MULT
            and avg_vol > 0
            and last["volume"] > avg_vol * self.A_VOL_MULT
        ):
            self.active_trade[secid] = {
                "type": "A",
                "side": "LONG",
                "entry": float(last["close"]),
                "entry_ts": float(last["ts"]),
            }
            self.last_action_sec[secid] = cur_sec
            return "A_ENTRY"

        # --------------------------------------------------
        # STEP 4: STRATEGY B — TRAP REVERSAL
        # --------------------------------------------------
        rng = float(last["range"])
        if rng > 0:
            upper_wick = last["high"] - max(last["open"], last["close"])
            lower_wick = min(last["open"], last["close"]) - last["low"]

            trap_up = upper_wick > rng * self.B_TRAP_WICK
            trap_down = lower_wick > rng * self.B_TRAP_WICK

            if (avg_range > 0) and (price_speed > avg_range * self.B_REV_SPEED):
                # trap up => SHORT
                if trap_up:
                    self.active_trade[secid] = {
                        "type": "B",
                        "side": "SHORT",
                        "entry": float(last["close"]),
                        "entry_ts": float(last["ts"]),
                    }
                    self.last_action_sec[secid] = cur_sec
                    return "B_ENTRY"

                # trap down => LONG
                if trap_down:
                    self.active_trade[secid] = {
                        "type": "B",
                        "side": "LONG",
                        "entry": float(last["close"]),
                        "entry_ts": float(last["ts"]),
                    }
                    self.last_action_sec[secid] = cur_sec
                    return "B_ENTRY"

        return "NO_TRADE"

    # --------------------------------------------------
    # READ-ONLY PnL HELPERS (PAPER MODE)
    # --------------------------------------------------
    def get_trade_pnl(self, secid: int) -> float:
        """Last closed trade pnl (per-trade)."""
        return round(self.last_trade_pnl.get(secid, 0.0), 2)

    def get_cum_pnl(self, secid: int) -> float:
        """Cumulative pnl (optional)."""
        return round(self.cum_pnl.get(secid, 0.0), 2)