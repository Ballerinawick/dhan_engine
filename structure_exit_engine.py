import time
from collections import deque


class StructureExitEngine:
    """
    STRUCTURE EXIT ENGINE v2 — MATURITY + ENTRY-AWARE

    DESIGN GOALS (FIXED):
    ✅ Do NOT exit on first HL break
    ✅ Require structure maturity before exits
    ✅ Entry-price aware (profit vs loss)
    ✅ Separate SCALP vs TREND logic
    ✅ Allow healthy pullbacks
    ✅ Exit only on PROVEN structure failure
    """

    # ================= CONFIG =================

    # Candle building
    CANDLE_SEC = 1
    MAX_CANDLES = 120

    # Structure maturity
    MIN_PIVOTS_SCALP = 4          # HH → HL → HH → HL
    MIN_PIVOTS_TREND = 6          # stronger confirmation

    # Time guards
    MIN_SECONDS_AFTER_ENTRY = 6
    MIN_TREND_SECONDS = 20

    # Pullback tolerance (dynamic, % of entry)
    MAX_ADVERSE_FROM_ENTRY_PCT = 0.9    # % loss allowed before structure exits activate

    # Profit-based exit allowance
    MIN_PROFIT_TO_EXIT_EARLY_PCT = 0.4  # if in profit, allow smart exits

    # HL / LL buffers
    HL_BREAK_BUFFER_PCT = 0.15
    LL_BREAK_BUFFER_PCT = 0.20

    # Visuals
    TIMELINE_MAX = 20

    # ==================================================

    def __init__(self, debug=True):
        self.debug = debug
        self.ctx = {}

    # ---------------- logging ----------------
    def _log(self, msg):
        if self.debug:
            print(msg)

    # ---------------- context ----------------
    def _new_ctx(self, entry):
        return {
            "entry": float(entry),
            "cur_sec": None,
            "o": None,
            "h": None,
            "l": None,
            "c": None,
            "candles": deque(maxlen=self.MAX_CANDLES),

            # structure
            "pivots": [],        # ("HH","HL","LH","LL")
            "pivot_prices": [],
            "last_high": None,
            "last_low": None,

            # guards
            "has_maturity": False,
            "trend_start_ts": None,
            "last_mode": None,
            "last_exit_ts": 0.0,

            # visuals
            "timeline": []
        }

    # ---------------- candle ----------------
    def _update_candle(self, c, ltp, ts):
        sec = int(ts)
        if c["cur_sec"] is None:
            c["cur_sec"] = sec
            c["o"] = c["h"] = c["l"] = c["c"] = ltp
            return

        if sec == c["cur_sec"]:
            c["h"] = max(c["h"], ltp)
            c["l"] = min(c["l"], ltp)
            c["c"] = ltp
            return

        c["candles"].append({
            "ts": float(c["cur_sec"]),
            "h": c["h"],
            "l": c["l"],
        })
        c["cur_sec"] = sec
        c["o"] = c["h"] = c["l"] = c["c"] = ltp

    # ---------------- pivot detection ----------------
    def _detect_pivot(self, c):
        if len(c["candles"]) < 5:
            return None

        arr = list(c["candles"])[-5:]
        mid = arr[2]

        is_ph = all(mid["h"] > x["h"] for i, x in enumerate(arr) if i != 2)
        is_pl = all(mid["l"] < x["l"] for i, x in enumerate(arr) if i != 2)

        if is_ph:
            return ("PH", mid["h"])
        if is_pl:
            return ("PL", mid["l"])
        return None

    # ---------------- mode ----------------
    def _read_mode(self, secid, decision_engine):
        try:
            return decision_engine.trade_ctx.get(secid, {}).get("mode", "SCALP")
        except Exception:
            return "SCALP"

    # ================= MAIN =================
    def on_tick(self, *, secid, tag, ltp, paper_trader, decision_engine):

        pos = paper_trader.positions.get(secid)
        if not pos:
            self.ctx.pop(secid, None)
            return None

        now = time.time()
        entry = pos["entry"]
        entry_ts = pos["entry_ts"]
        ltp = float(ltp)

        c = self.ctx.setdefault(secid, self._new_ctx(entry))

        # anti-spam
        if now - c["last_exit_ts"] < 1.0:
            return None

        # build candles
        self._update_candle(c, ltp, now)

        # hard guard: observe market first
        if now - entry_ts < self.MIN_SECONDS_AFTER_ENTRY:
            return None

        # mode
        mode = self._read_mode(secid, decision_engine)

        if c["last_mode"] != mode:
            c["last_mode"] = mode
            if mode == "TREND":
                c["trend_start_ts"] = now

        # detect pivots
        pivot = self._detect_pivot(c)
        if pivot:
            kind, px = pivot

            if kind == "PH":
                if c["last_high"] is None or px > c["last_high"]:
                    c["last_high"] = px
                    c["pivots"].append("HH")
                    c["pivot_prices"].append(px)
                    c["timeline"].append(f"HH@{px:.2f}")
                else:
                    c["pivots"].append("LH")
                    c["pivot_prices"].append(px)
                    c["timeline"].append(f"LH@{px:.2f}")

            if kind == "PL":
                if c["last_low"] is None or px > c["last_low"]:
                    c["last_low"] = px
                    c["pivots"].append("HL")
                    c["pivot_prices"].append(px)
                    c["timeline"].append(f"HL@{px:.2f}")
                else:
                    c["pivots"].append("LL")
                    c["pivot_prices"].append(px)
                    c["timeline"].append(f"LL@{px:.2f}")

            c["timeline"] = c["timeline"][-self.TIMELINE_MAX:]
            self._log(f"🧱 STRUCT | {tag} | {' → '.join(c['timeline'])}")

        # ---------------- maturity ----------------
        need = self.MIN_PIVOTS_TREND if mode == "TREND" else self.MIN_PIVOTS_SCALP
        if len(c["pivots"]) >= need:
            c["has_maturity"] = True

        # ---------------- profit / loss context ----------------
        pnl_pct = (ltp - entry) / entry * 100.0

        # LOSS SIDE: do nothing unless severe
        if pnl_pct < -self.MAX_ADVERSE_FROM_ENTRY_PCT:
            return None

        # TREND early safety
        if mode == "TREND":
            if now - (c["trend_start_ts"] or now) < self.MIN_TREND_SECONDS:
                return None

        # ---------------- EXIT LOGIC ----------------

        # A) STRUCTURE FAILURE (real)
        if c["has_maturity"]:
            last = c["pivots"][-1]

            # TREND: only LL after maturity
            if mode == "TREND" and last == "LL":
                c["last_exit_ts"] = now
                self._log(f"📉 STRUCT_EXIT | {tag} | TREND_LL_FAIL")
                return {"exit": True, "reason": "STRUCT_TREND_FAILURE"}

            # SCALP: LH after HH with profit
            if mode == "SCALP" and last == "LH" and pnl_pct > self.MIN_PROFIT_TO_EXIT_EARLY_PCT:
                c["last_exit_ts"] = now
                self._log(f"📉 STRUCT_EXIT | {tag} | SCALP_LH_PROFIT")
                return {"exit": True, "reason": "STRUCT_SCALP_PROFIT"}

        return None