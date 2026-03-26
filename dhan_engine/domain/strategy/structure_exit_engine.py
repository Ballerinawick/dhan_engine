import time
from collections import deque


class StructureExitEngine:
    """
    STRUCTURE EXIT ENGINE v3.1 — PROFIT-AWARE TREND EXIT

    UPGRADE FROM v2.5:
    ✅ TREND exit only if:
        - LL breaks BELOW last confirmed HL (real failure), OR
        - profit already >= LOCKED_PROFIT_PCT
    ✅ Prevents flat / premature trend exits
    ✅ Does NOT affect SCALP logic
    ✅ Fully backward-compatible with v2.5
    """

    # Candle building
    CANDLE_SEC = 1
    MAX_CANDLES = 600

    # Observe windows
    OBSERVE_SCALP_CANDLES = 120
    OBSERVE_TREND_CANDLES = 300

    # Structure maturity
    MIN_PIVOTS_SCALP = 4
    MIN_PIVOTS_TREND = 6

    # Time guards
    MIN_SECONDS_AFTER_ENTRY = 6
    MIN_TREND_SECONDS = 20

    # Loss guard
    MAX_ADVERSE_FROM_ENTRY_PCT = 0.9

    # Profit logic
    MIN_PROFIT_TO_EXIT_EARLY_PCT = 0.4     # SCALP
    LOCKED_PROFIT_TREND_PCT = 1.0           # 🔥 NEW (trend profit lock)

    # Visuals
    TIMELINE_MAX = 20

    def __init__(self, debug=True):
        self.debug = debug
        self.ctx = {}

    def _log(self, msg):
        if self.debug:
            print(msg)

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
            "pivots": [],
            "last_high": None,
            "last_low": None,
            "last_confirmed_hl": None,   # 🔥 NEW
            "timeline": [],

            # state
            "state": "OBSERVE",
            "observe_need": None,
            "has_maturity": False,
            "trend_start_ts": None,
            "last_mode": None,
            "last_exit_ts": 0.0,

            # clean logs
            "logged_observe_start": False,
            "logged_observe_done": False,
            "last_struct_print_len": 0,
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

    # ---------------- pivot ----------------
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
        entry = float(pos["entry"])
        entry_ts = float(pos["entry_ts"])
        ltp = float(ltp)

        c = self.ctx.setdefault(secid, self._new_ctx(entry))

        if now - c["last_exit_ts"] < 1.0:
            return None

        self._update_candle(c, ltp, now)

        if now - entry_ts < self.MIN_SECONDS_AFTER_ENTRY:
            return None

        mode = self._read_mode(secid, decision_engine)
        if c["last_mode"] != mode:
            c["last_mode"] = mode
            if mode == "TREND":
                c["trend_start_ts"] = now
            self._log(f"🧭 MODE | {tag} | mode={mode}")

        observe_need = self.OBSERVE_TREND_CANDLES if mode == "TREND" else self.OBSERVE_SCALP_CANDLES
        c["observe_need"] = observe_need

        if not c["logged_observe_start"]:
            c["logged_observe_start"] = True
            self._log(f"👀 OBSERVE_START | {tag} | need={observe_need}")

        if c["state"] == "OBSERVE":
            if len(c["candles"]) >= observe_need:
                c["state"] = "ACTIVE"
                self._log(f"✅ OBSERVE_DONE | {tag}")
            else:
                return None

        pivot = self._detect_pivot(c)
        if pivot:
            kind, px = pivot

            if kind == "PH":
                if c["last_high"] is None or px > c["last_high"]:
                    c["last_high"] = px
                    c["pivots"].append("HH")
                    c["timeline"].append(f"HH@{px:.2f}")
                else:
                    c["pivots"].append("LH")
                    c["timeline"].append(f"LH@{px:.2f}")

            if kind == "PL":
                if c["last_low"] is None or px > c["last_low"]:
                    c["last_low"] = px
                    c["last_confirmed_hl"] = px     # 🔥 capture HL
                    c["pivots"].append("HL")
                    c["timeline"].append(f"HL@{px:.2f}")
                else:
                    c["pivots"].append("LL")
                    c["timeline"].append(f"LL@{px:.2f}")

            c["timeline"] = c["timeline"][-self.TIMELINE_MAX:]

            if len(c["timeline"]) != c["last_struct_print_len"]:
                c["last_struct_print_len"] = len(c["timeline"])
                self._log(f"🧱 STRUCT | {tag} | {' → '.join(c['timeline'])}")

        need_pivots = self.MIN_PIVOTS_TREND if mode == "TREND" else self.MIN_PIVOTS_SCALP
        if not c["has_maturity"] and len(c["pivots"]) >= need_pivots:
            c["has_maturity"] = True
            self._log(f"🧠 STRUCT_READY | {tag} | mode={mode}")

        pnl_pct = (ltp - entry) / entry * 100.0

        if pnl_pct < -self.MAX_ADVERSE_FROM_ENTRY_PCT:
            return None

        if mode == "TREND" and now - (c["trend_start_ts"] or now) < self.MIN_TREND_SECONDS:
            return None

        if c["has_maturity"] and c["pivots"]:
            last = c["pivots"][-1]

            # 🔥 TREND EXIT (SMART)
            if mode == "TREND" and last == "LL":
                if (
                    (c["last_confirmed_hl"] and ltp < c["last_confirmed_hl"])
                    or pnl_pct >= self.LOCKED_PROFIT_TREND_PCT
                ):
                    c["last_exit_ts"] = now
                    self._log(f"📉 EXIT_STRUCT | {tag} | TREND_LL_FAIL | pnl={pnl_pct:+.2f}%")
                    return {"exit": True, "reason": "STRUCT_TREND_FAILURE"}

            # SCALP unchanged
            if mode == "SCALP" and last == "LH" and pnl_pct > self.MIN_PROFIT_TO_EXIT_EARLY_PCT:
                c["last_exit_ts"] = now
                self._log(f"📉 EXIT_STRUCT | {tag} | SCALP_LH_PROFIT | pnl={pnl_pct:+.2f}%")
                return {"exit": True, "reason": "STRUCT_SCALP_PROFIT"}

        return None