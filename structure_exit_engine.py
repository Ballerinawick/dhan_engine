import time
from collections import deque


class StructureExitEngine:
    """
    STRUCTURE EXIT ENGINE — STABLE (v1.1)

    FIXES APPLIED:
    ✅ Pivot de-duplication (no repeated LL/LH spam)
    ✅ SCALP structure stabilized
    ✅ TREND exits gated until structure matures
    ✅ Clean HH / HL / LH / LL timeline logs
    """

    # ---------------- CONFIG ----------------
    PIVOT_WINDOW = 5                   # must be odd
    MIN_SECONDS_AFTER_ENTRY = 2         # ignore exits immediately after entry

    # ---- TREND SAFETY ----
    MIN_TREND_SECONDS = 12              # TREND exits blocked initially
    REQUIRE_HH_FOR_TREND = True

    # ---- SCALP ----
    SCALP_EXIT_ON_LH = True
    SCALP_LH_MIN_DROP_PCT = 0.15         # ignore micro LH noise

    # ---- HL BREAK ----
    HL_BREAK_BUFFER_PCT = 0.05           # buffer to avoid micro stop-outs
    SCALP_EXIT_ON_HL_BREAK = True
    TREND_EXIT_ON_HL_BREAK = True

    # ---- TREND ----
    TREND_EXIT_ON_LL = True

    TIMELINE_MAX = 18

    def __init__(self, debug=True):
        self.debug = debug
        self.ctx = {}

    # --------------------------------------------------
    def _log(self, msg):
        if self.debug:
            print(msg)

    # --------------------------------------------------
    def _new_ctx(self, entry):
        return {
            "entry": float(entry),

            # candle building
            "cur_sec": None,
            "o": None,
            "h": None,
            "l": None,
            "c": None,
            "candles": deque(maxlen=80),

            # structure
            "last_high": None,
            "last_low": None,
            "last_hh": None,
            "last_hl": None,
            "has_hh": False,

            # pivot guard
            "last_pivot_ts": None,

            # mode tracking
            "last_mode": None,
            "trend_start_ts": None,

            # exit guard
            "last_exit_ts": 0.0,

            # visual
            "timeline": []
        }

    # --------------------------------------------------
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

    # --------------------------------------------------
    def _detect_pivot(self, c):
        w = self.PIVOT_WINDOW
        if len(c["candles"]) < w:
            return None

        arr = list(c["candles"])[-w:]
        mid = w // 2
        m = arr[mid]

        is_ph = all(m["h"] > x["h"] for i, x in enumerate(arr) if i != mid)
        is_pl = all(m["l"] < x["l"] for i, x in enumerate(arr) if i != mid)

        if is_ph:
            return ("PH", m["ts"], m["h"])
        if is_pl:
            return ("PL", m["ts"], m["l"])
        return None

    # --------------------------------------------------
    def _add_timeline(self, c, label, px):
        c["timeline"].append(f"{label}@{px:.2f}")
        c["timeline"] = c["timeline"][-self.TIMELINE_MAX:]

    # --------------------------------------------------
    def _read_mode(self, secid, decision_engine):
        try:
            return decision_engine.trade_ctx.get(secid, {}).get("mode", "SCALP").upper()
        except Exception:
            return "SCALP"

    # --------------------------------------------------
    def on_tick(self, *, secid, tag, ltp, paper_trader, decision_engine):

        pos = paper_trader.positions.get(secid)
        if not pos:
            self.ctx.pop(secid, None)
            return None

        now = time.time()
        entry_ts = pos["entry_ts"]
        ltp = float(ltp)

        c = self.ctx.setdefault(secid, self._new_ctx(pos["entry"]))

        # anti spam
        if now - c["last_exit_ts"] < 1.0:
            return None

        self._update_candle(c, ltp, now)

        if now - entry_ts < self.MIN_SECONDS_AFTER_ENTRY:
            return None

        mode = self._read_mode(secid, decision_engine)

        # track TREND start
        if c["last_mode"] != mode:
            c["last_mode"] = mode
            if mode == "TREND":
                c["trend_start_ts"] = now

        # detect pivot
        pivot = self._detect_pivot(c)
        if pivot:
            kind, ts, px = pivot

            # 🔒 CRITICAL FIX: pivot de-duplication
            if c["last_pivot_ts"] == ts:
                return None
            c["last_pivot_ts"] = ts

            if kind == "PH":
                if c["last_high"] is None or px > c["last_high"]:
                    c["last_high"] = px
                    c["last_hh"] = px
                    c["has_hh"] = True
                    self._log(f"🧱 STRUCT_HH | {tag} | {px:.2f}")
                    self._add_timeline(c, "HH", px)
                else:
                    self._log(f"🧱 STRUCT_LH | {tag} | {px:.2f}")
                    self._add_timeline(c, "LH", px)

            if kind == "PL":
                if c["last_low"] is None or px > c["last_low"]:
                    c["last_low"] = px
                    c["last_hl"] = px
                    self._log(f"🧱 STRUCT_HL | {tag} | {px:.2f}")
                    self._add_timeline(c, "HL", px)
                else:
                    self._log(f"🧱 STRUCT_LL | {tag} | {px:.2f}")
                    self._add_timeline(c, "LL", px)

            self._log(f"🧩 STRUCT | {tag} | {' → '.join(c['timeline'])}")

        # ---------------- EXIT RULES ----------------

        # TREND safety
        if mode == "TREND":
            if self.REQUIRE_HH_FOR_TREND and not c["has_hh"]:
                mode = "SCALP"
            elif now - (c["trend_start_ts"] or now) < self.MIN_TREND_SECONDS:
                return None

        # HL break
        if c["has_hh"] and c["last_hl"]:
            buf = self.HL_BREAK_BUFFER_PCT / 100
            if ltp < c["last_hl"] * (1 - buf):
                c["last_exit_ts"] = now
                self._log(f"📉 STRUCT_EXIT | {tag} | HL_BREAK | mode={mode}")
                return {"exit": True, "reason": f"STRUCT_HL_BREAK_{mode}"}

        # SCALP LH
        if mode == "SCALP" and self.SCALP_EXIT_ON_LH and pivot and pivot[0] == "PH":
            if c["last_hh"]:
                drop = (c["last_hh"] - pivot[2]) / c["last_hh"] * 100
                if drop >= self.SCALP_LH_MIN_DROP_PCT:
                    c["last_exit_ts"] = now
                    self._log(f"📉 STRUCT_EXIT | {tag} | LH_SCALP")
                    return {"exit": True, "reason": "STRUCT_LH_SCALP"}

        # TREND LL
        if mode == "TREND" and self.TREND_EXIT_ON_LL and pivot and pivot[0] == "PL":
            c["last_exit_ts"] = now
            self._log(f"📉 STRUCT_EXIT | {tag} | LL_TREND")
            return {"exit": True, "reason": "STRUCT_LL_TREND"}

        return None