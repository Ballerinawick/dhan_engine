import time
from collections import deque


class StructureExitEngine:
    """
    STRUCTURE EXIT ENGINE (1s candles + pivots) — FINAL STABLE VERSION

    Responsibilities:
    - Build 1-second candles from ticks
    - Detect pivots (PH / PL)
    - Classify HH / HL / LH / LL
    - Exit using structure rules (SCALP vs TREND)
    - Strong guards to prevent instant / noisy exits

    This version is SAFE to deploy.
    """

    # ---------------- CONFIG ----------------
    PIVOT_WINDOW = 5                     # must be odd
    MIN_SECONDS_AFTER_ENTRY = 2

    # ---- TREND guards ----
    MIN_TREND_SECONDS = 12
    REQUIRE_HH_TO_ENABLE_TREND_EXITS = True

    # ---- SCALP exits ----
    SCALP_EXIT_ON_LH = True
    SCALP_REQUIRE_HH_BEFORE_LH_EXIT = True
    SCALP_LH_MIN_DROP_PCT = 0.15         # LH must be meaningfully lower than HH

    # ---- HL break exits ----
    HL_BREAK_BUFFER_PCT = 0.05           # buffer to avoid micro stopouts
    SCALP_EXIT_ON_HL_BREAK = True
    TREND_EXIT_ON_HL_BREAK = True

    # ---- TREND exits ----
    TREND_EXIT_ON_LL = True

    # ---- Logging ----
    TIMELINE_MAX_EVENTS = 18

    def __init__(self, debug=True):
        self.debug = debug
        self.ctx = {}  # secid -> state

    # ---------------- logging ----------------
    def _log(self, msg: str):
        if self.debug:
            print(msg)

    def _timeline_add(self, c, label, ts, px):
        c["timeline"].append((int(ts), label, float(px)))
        if len(c["timeline"]) > self.TIMELINE_MAX_EVENTS:
            c["timeline"] = c["timeline"][-self.TIMELINE_MAX_EVENTS:]

    def _timeline_str(self, c):
        return " → ".join(f"{lab}@{px:.2f}" for _, lab, px in c["timeline"])

    # ---------------- context ----------------
    def _get_or_create_ctx(self, secid, entry):
        if secid in self.ctx:
            return self.ctx[secid]

        c = {
            "entry": float(entry),

            # candle building
            "cur_sec": None,
            "cur_o": None,
            "cur_h": None,
            "cur_l": None,
            "cur_c": None,
            "candles": deque(maxlen=80),

            # structure
            "last_swing_high": None,
            "last_swing_low": None,
            "last_hh": None,
            "last_hl": None,
            "has_hh_since_entry": False,

            # trend timing
            "last_seen_mode": None,
            "trend_start_ts": None,

            # anti spam
            "last_exit_ts": 0.0,

            # visualization
            "timeline": [],
        }

        self.ctx[secid] = c
        return c

    # ---------------- candle logic ----------------
    def _flush_candle(self, c):
        if c["cur_sec"] is None:
            return
        c["candles"].append({
            "ts": c["cur_sec"],
            "h": c["cur_h"],
            "l": c["cur_l"],
        })

    def _update_1s_candle(self, c, ltp, now):
        sec = int(now)

        if c["cur_sec"] is None:
            c["cur_sec"] = sec
            c["cur_o"] = c["cur_h"] = c["cur_l"] = c["cur_c"] = ltp
            return

        if sec == c["cur_sec"]:
            c["cur_h"] = max(c["cur_h"], ltp)
            c["cur_l"] = min(c["cur_l"], ltp)
            c["cur_c"] = ltp
            return

        self._flush_candle(c)
        c["cur_sec"] = sec
        c["cur_o"] = c["cur_h"] = c["cur_l"] = c["cur_c"] = ltp

    # ---------------- pivot detection ----------------
    def _detect_pivot(self, c):
        w = self.PIVOT_WINDOW
        if len(c["candles"]) < w:
            return None

        arr = list(c["candles"])[-w:]
        mid = w // 2
        m = arr[mid]

        if all(m["h"] > x["h"] for i, x in enumerate(arr) if i != mid):
            return ("PH", m["ts"], m["h"])
        if all(m["l"] < x["l"] for i, x in enumerate(arr) if i != mid):
            return ("PL", m["ts"], m["l"])
        return None

    def _classify_pivot(self, c, pivot):
        kind, ts, px = pivot

        if kind == "PH":
            prev = c["last_swing_high"]
            c["last_swing_high"] = px

            if prev is None:
                label = "PH"
            elif px > prev:
                label = "HH"
                c["has_hh_since_entry"] = True
                c["last_hh"] = px
            else:
                label = "LH"

            self._log(f"🧱 STRUCT_{label} | ts={ts} | px={px:.2f}")
            self._timeline_add(c, label, ts, px)
            return label, px

        if kind == "PL":
            prev = c["last_swing_low"]
            c["last_swing_low"] = px

            if prev is None:
                label = "PL"
            elif px > prev:
                label = "HL"
                c["last_hl"] = px
            else:
                label = "LL"

            self._log(f"🧱 STRUCT_{label} | ts={ts} | px={px:.2f}")
            self._timeline_add(c, label, ts, px)
            return label, px

        return None

    # ---------------- mode ----------------
    def _read_mode(self, secid, decision_engine):
        try:
            return decision_engine.trade_ctx.get(secid, {}).get("mode", "SCALP")
        except Exception:
            return "SCALP"

    def _update_trend_start(self, c, mode, now):
        if c["last_seen_mode"] != mode:
            c["last_seen_mode"] = mode
            if mode == "TREND":
                c["trend_start_ts"] = now

    # ---------------- MAIN ----------------
    def on_tick(self, *, secid, tag, ltp, paper_trader, decision_engine):
        pos = paper_trader.positions.get(secid)
        if not pos:
            self.ctx.pop(secid, None)
            return None

        now = time.time()
        entry_ts = pos.get("entry_ts", 0)
        c = self._get_or_create_ctx(secid, pos["entry"])

        if now - c["last_exit_ts"] < 1.0:
            return None

        self._update_1s_candle(c, float(ltp), now)

        if now - entry_ts < self.MIN_SECONDS_AFTER_ENTRY:
            return None

        mode = self._read_mode(secid, decision_engine)
        self._update_trend_start(c, mode, now)

        pivot = self._detect_pivot(c)
        label = None
        if pivot:
            label = self._classify_pivot(c, pivot)
            self._log(f"🧩 STRUCT_TIMELINE | {tag} | {self._timeline_str(c)}")

        # ---- TREND maturity guards ----
        if mode == "TREND" and self.REQUIRE_HH_TO_ENABLE_TREND_EXITS and not c["has_hh_since_entry"]:
            mode = "SCALP"

        if mode == "TREND" and c["trend_start_ts"]:
            if now - c["trend_start_ts"] < self.MIN_TREND_SECONDS:
                return None

        # ---- HL break exit ----
        if c["has_hh_since_entry"] and c["last_hl"]:
            buf = self.HL_BREAK_BUFFER_PCT / 100.0
            if ltp < c["last_hl"] * (1 - buf):
                if (mode == "SCALP" and self.SCALP_EXIT_ON_HL_BREAK) or (
                    mode == "TREND" and self.TREND_EXIT_ON_HL_BREAK
                ):
                    c["last_exit_ts"] = now
                    self._log(f"📉 STRUCTURE_EXIT | {tag} | mode={mode} | reason=HL_BREAK")
                    self._log(f"🧩 EXIT_SNAPSHOT | {tag} | {self._timeline_str(c)}")
                    return {"exit": True, "reason": f"STRUCT_HL_BREAK_{mode}"}

        # ---- SCALP LH exit ----
        if mode == "SCALP" and label and label[0] == "LH" and self.SCALP_EXIT_ON_LH:
            if (not self.SCALP_REQUIRE_HH_BEFORE_LH_EXIT) or c["has_hh_since_entry"]:
                hh = c.get("last_hh")
                if hh:
                    drop = (hh - label[1]) / hh * 100
                    if drop >= self.SCALP_LH_MIN_DROP_PCT:
                        c["last_exit_ts"] = now
                        self._log(f"📉 STRUCTURE_EXIT | {tag} | mode=SCALP | reason=LH_FORMED")
                        self._log(f"🧩 EXIT_SNAPSHOT | {tag} | {self._timeline_str(c)}")
                        return {"exit": True, "reason": "STRUCT_LH_SCALP"}

        # ---- TREND LL exit ----
        if mode == "TREND" and label and label[0] == "LL" and self.TREND_EXIT_ON_LL:
            c["last_exit_ts"] = now
            self._log(f"📉 STRUCTURE_EXIT | {tag} | mode=TREND | reason=LL_FORMED")
            self._log(f"🧩 EXIT_SNAPSHOT | {tag} | {self._timeline_str(c)}")
            return {"exit": True, "reason": "STRUCT_LL_TREND"}

        return None