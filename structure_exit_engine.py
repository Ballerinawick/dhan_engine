import time
from collections import deque


class StructureExitEngine:
    """
    STRUCTURE EXIT ENGINE (1s candles + pivots) — FINAL (stability-fixed)

    What this does:
    - Builds 1-second OHLC candles from tick LTP
    - Detects pivots (PH/PL) using a 5-candle window
    - Labels structure: HH / HL / LH / LL
    - Triggers exits using structure rules, with SAFE guards to prevent "exit ASAP"

    ✅ Key FIXES added (your 3 pain points):
    1) TREND is not allowed to act until structure is mature (must have HH after entry)
       + TREND exits blocked for MIN_TREND_SECONDS after mode upgrade.
    2) Clean visualization logs: prints a compact HH/HL/LH/LL timeline per symbol.
    3) SCALP LH exit is distance-aware (ignores tiny pullback LHs).
       + HL break uses a small buffer so micro-ticks don't stop you out.
    """

    # ---------------- CONFIG ----------------
    # Pivot detection using odd window; 5 is stable + fast
    PIVOT_WINDOW = 5
    MIN_SECONDS_AFTER_ENTRY = 2  # ignore exits immediately after entry (still builds candles)

    # ---- TREND guards (critical fix) ----
    MIN_TREND_SECONDS = 12  # even if decision engine says TREND, don't allow TREND exits immediately
    REQUIRE_HH_TO_ENABLE_TREND_EXITS = True  # TREND exits only after an HH exists post-entry

    # ---- SCALP exits ----
    SCALP_EXIT_ON_LH = True
    SCALP_REQUIRE_HH_BEFORE_LH_EXIT = True

    # SCALP LH distance filter: LH must be meaningfully lower than last HH to exit
    # Example: 0.15% means LH must be at least 0.15% below HH (filters micro-noise).
    SCALP_LH_MIN_DROP_PCT = 0.15

    # ---- HL break exits (both modes) ----
    # Add buffer to avoid micro HL-break stopouts.
    # Example: 0.05% buffer => exit only if ltp < HL * (1 - 0.0005)
    HL_BREAK_BUFFER_PCT = 0.05
    SCALP_EXIT_ON_HL_BREAK = True
    TREND_EXIT_ON_HL_BREAK = True

    # ---- TREND exit ----
    TREND_EXIT_ON_LL = True  # LL pivot = structure failure

    # ---- timeline logs ----
    TIMELINE_MAX_EVENTS = 18  # keep compact

    def __init__(self, debug=True):
        self.debug = debug
        self.ctx = {}  # secid -> state

    # ---------------- logging ----------------
    def _log(self, msg: str):
        if self.debug:
            print(msg)

    def _timeline_add(self, c: dict, label: str, ts: float, px: float):
        # store compact event
        c["timeline"].append((int(ts), str(label), float(px)))
        if len(c["timeline"]) > self.TIMELINE_MAX_EVENTS:
            c["timeline"] = c["timeline"][-self.TIMELINE_MAX_EVENTS :]

    def _timeline_str(self, c: dict) -> str:
        # example: HH@1216.50 → HL@1212.55 → LH@1214.10
        parts = []
        for _, lab, px in c["timeline"]:
            parts.append(f"{lab}@{px:.2f}")
        return " → ".join(parts)

    # ---------------- candle ctx ----------------
    def _get_or_create_ctx(self, secid: int, entry: float):
        c = self.ctx.get(secid)
        if c:
            return c

        c = {
            "entry": float(entry),

            # candle building
            "cur_sec": None,
            "cur_o": None,
            "cur_h": None,
            "cur_l": None,
            "cur_c": None,

            "candles": deque(maxlen=80),

            # pivots (raw)
            "pivot_highs": deque(maxlen=12),  # (ts, px)
            "pivot_lows": deque(maxlen=12),

            # structure tracking
            "last_swing_high": None,  # last pivot high price (for HH/LH compare)
            "last_swing_low": None,   # last pivot low price (for HL/LL compare)

            "last_hh": None,          # last HH price
            "last_hh_ts": None,

            "last_hl": None,          # last HL support price
            "last_hl_ts": None,

            "has_hh_since_entry": False,

            # mode tracking
            "last_seen_mode": None,
            "trend_start_ts": None,   # when we observed mode become TREND

            # anti-spam
            "last_exit_ts": 0.0,

            # visualization timeline
            "timeline": [],
        }

        self.ctx[secid] = c
        return c

    def _flush_candle(self, c: dict):
        if c["cur_sec"] is None:
            return
        candle = {
            "ts": float(c["cur_sec"]),
            "o": float(c["cur_o"]),
            "h": float(c["cur_h"]),
            "l": float(c["cur_l"]),
            "c": float(c["cur_c"]),
        }
        c["candles"].append(candle)

    def _update_1s_candle(self, c: dict, ltp: float, now_ts: float):
        sec = int(now_ts)

        if c["cur_sec"] is None:
            c["cur_sec"] = sec
            c["cur_o"] = ltp
            c["cur_h"] = ltp
            c["cur_l"] = ltp
            c["cur_c"] = ltp
            return

        if sec == c["cur_sec"]:
            c["cur_h"] = max(c["cur_h"], ltp)
            c["cur_l"] = min(c["cur_l"], ltp)
            c["cur_c"] = ltp
            return

        # new second
        self._flush_candle(c)
        c["cur_sec"] = sec
        c["cur_o"] = ltp
        c["cur_h"] = ltp
        c["cur_l"] = ltp
        c["cur_c"] = ltp

    # ---------------- pivot detection ----------------
    def _detect_new_pivot(self, c: dict):
        w = int(self.PIVOT_WINDOW)
        if w < 3 or (w % 2 == 0):
            w = 5
        if len(c["candles"]) < w:
            return None

        arr = list(c["candles"])[-w:]
        mid = w // 2
        mid_c = arr[mid]
        left = arr[:mid]
        right = arr[mid + 1:]

        mh = mid_c["h"]
        ml = mid_c["l"]

        is_ph = all(mh > x["h"] for x in left) and all(mh > x["h"] for x in right)
        is_pl = all(ml < x["l"] for x in left) and all(ml < x["l"] for x in right)

        if is_ph:
            return ("PH", mid_c["ts"], float(mh))
        if is_pl:
            return ("PL", mid_c["ts"], float(ml))
        return None

    def _classify_and_store_pivot(self, c: dict, pivot):
        kind, ts, px = pivot

        if kind == "PH":
            prev = c["last_swing_high"]
            c["pivot_highs"].append((ts, px))

            if prev is None:
                c["last_swing_high"] = px
                self._log(f"🧱 STRUCT_PH | ts={int(ts)} | px={px:.2f}")
                self._timeline_add(c, "PH", ts, px)
                return ("PH", px)

            if px > prev:
                c["last_swing_high"] = px
                c["has_hh_since_entry"] = True
                c["last_hh"] = px
                c["last_hh_ts"] = ts
                self._log(f"🧱 STRUCT_HH | ts={int(ts)} | px={px:.2f}")
                self._timeline_add(c, "HH", ts, px)
                return ("HH", px)

            # lower high
            self._log(f"🧱 STRUCT_LH | ts={int(ts)} | px={px:.2f}")
            self._timeline_add(c, "LH", ts, px)
            return ("LH", px)

        if kind == "PL":
            prev = c["last_swing_low"]
            c["pivot_lows"].append((ts, px))

            if prev is None:
                c["last_swing_low"] = px
                self._log(f"🧱 STRUCT_PL | ts={int(ts)} | px={px:.2f}")
                self._timeline_add(c, "PL", ts, px)
                return ("PL", px)

            if px > prev:
                # Higher Low
                c["last_swing_low"] = px
                c["last_hl"] = px
                c["last_hl_ts"] = ts
                self._log(f"🧱 STRUCT_HL | ts={int(ts)} | px={px:.2f}")
                self._timeline_add(c, "HL", ts, px)
                return ("HL", px)

            # Lower Low
            c["last_swing_low"] = px
            self._log(f"🧱 STRUCT_LL | ts={int(ts)} | px={px:.2f}")
            self._timeline_add(c, "LL", ts, px)
            return ("LL", px)

        return None

    # ---------------- mode read ----------------
    def _read_mode(self, secid, decision_engine) -> str:
        mode = "SCALP"
        try:
            ctx_map = getattr(decision_engine, "trade_ctx", None)
            if ctx_map and secid in ctx_map and ctx_map[secid].get("mode"):
                mode = str(ctx_map[secid]["mode"]).upper()
        except Exception:
            pass
        return mode

    def _update_trend_start(self, c: dict, mode: str, now_ts: float):
        # Track transition moment when mode becomes TREND.
        if c["last_seen_mode"] != mode:
            c["last_seen_mode"] = mode
            if mode == "TREND":
                c["trend_start_ts"] = float(now_ts)

    # ---------------- MAIN API ----------------
    def on_tick(self, *, secid, tag, ltp, paper_trader, decision_engine):
        """
        Returns:
            None
            OR {"exit": True, "reason": "..."}
        """

        pos = paper_trader.positions.get(secid)
        if not pos:
            self.ctx.pop(secid, None)
            return None

        entry = float(pos["entry"])
        entry_ts = float(pos.get("entry_ts") or 0.0)
        now_ts = time.time()
        ltp = float(ltp)

        c = self._get_or_create_ctx(secid, entry)

        # anti-spam: avoid multiple exits in 1 sec
        if now_ts - c["last_exit_ts"] < 1.0:
            return None

        # always build candles
        self._update_1s_candle(c, ltp, now_ts)

        # ignore exits right after entry (but candle continues building)
        if now_ts - entry_ts < self.MIN_SECONDS_AFTER_ENTRY:
            return None

        # read mode and track TREND start
        mode = self._read_mode(secid, decision_engine)
        self._update_trend_start(c, mode, now_ts)

        # pivot detection + classification
        pivot = self._detect_new_pivot(c)
        label = None
        if pivot:
            label = self._classify_and_store_pivot(c, pivot)
            # timeline snapshot (only print on events)
            self._log(f"🧩 STRUCT_TIMELINE | {tag} | {self._timeline_str(c)}")

        # ---------------- SAFETY: TREND maturity gates ----------------
        # If TREND is requested too early, we force SCALP behavior until HH exists.
        if mode == "TREND" and self.REQUIRE_HH_TO_ENABLE_TREND_EXITS and not c["has_hh_since_entry"]:
            mode = "SCALP"

        # If TREND recently started, block TREND exits until MIN_TREND_SECONDS.
        if mode == "TREND":
            ts0 = c.get("trend_start_ts") or 0.0
            if ts0 and (now_ts - ts0) < float(self.MIN_TREND_SECONDS):
                # allow candle/structure to form; don't exit yet
                return None

        # ---------------- EXIT RULES ----------------
        # A) HL break (support break) after HH (both modes, with buffer)
        if c["has_hh_since_entry"] and c["last_hl"] is not None:
            buf = float(self.HL_BREAK_BUFFER_PCT) / 100.0
            thresh = float(c["last_hl"]) * (1.0 - buf)
            if ltp < thresh:
                if (mode == "SCALP" and self.SCALP_EXIT_ON_HL_BREAK) or (mode == "TREND" and self.TREND_EXIT_ON_HL_BREAK):
                    c["last_exit_ts"] = now_ts
                    self._log(
                        f"📉 STRUCTURE_EXIT | {tag} | mode={mode} | reason=HL_BREAK | "
                        f"ltp={ltp:.2f} < hl_buf={thresh:.2f} (hl={c['last_hl']:.2f}, buf={self.HL_BREAK_BUFFER_PCT:.2f}%)"
                    )
                    self._log(f"🧩 EXIT_SNAPSHOT | {tag} | {self._timeline_str(c)}")
                    return {"exit": True, "reason": f"STRUCT_HL_BREAK_{mode}"}

        # B) SCALP exit on LH (only after HH, and only if LH is meaningfully lower)
        if mode == "SCALP" and self.SCALP_EXIT_ON_LH and label and label[0] == "LH":
            if (not self.SCALP_REQUIRE_HH_BEFORE_LH_EXIT) or c["has_hh_since_entry"]:
                hh = c.get("last_hh")
                if hh and hh > 0:
                    drop_pct = (float(hh) - float(label[1])) / float(hh) * 100.0
                else:
                    drop_pct = 999.0  # if no hh known, treat as big (but hh gate normally prevents this)

                if drop_pct >= float(self.SCALP_LH_MIN_DROP_PCT):
                    c["last_exit_ts"] = now_ts
                    self._log(
                        f"📉 STRUCTURE_EXIT | {tag} | mode=SCALP | reason=LH_FORMED | "
                        f"lh_drop={drop_pct:.3f}% (min={self.SCALP_LH_MIN_DROP_PCT:.3f}%)"
                    )
                    self._log(f"🧩 EXIT_SNAPSHOT | {tag} | {self._timeline_str(c)}")
                    return {"exit": True, "reason": "STRUCT_LH_SCALP"}
                else:
                    # too small, ignore (important fix)
                    return None

        # C) TREND exit on LL pivot (trend failure)
        if mode == "TREND" and self.TREND_EXIT_ON_LL and label and label[0] == "LL":
            c["last_exit_ts"] = now_ts
            self._log(f"📉 STRUCTURE_EXIT | {tag} | mode=TREND | reason=LL_FORMED")
            self._log(f"🧩 EXIT_SNAPSHOT | {tag} | {self._timeline_str(c)}")
            return {"exit": True, "reason": "STRUCT_LL_TREND"}

        return None