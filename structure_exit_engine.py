import time
from collections import deque


class StructureExitEngine:
    """
    STRUCTURE EXIT ENGINE (1s candles + pivots)

    Goal:
    - Convert tick LTP -> 1-second candles
    - Detect pivot structure: HH / HL / LH / LL
    - Exit rules differ by regime (SCALP vs TREND)

    Dependencies (already in your system):
    - paper_trader.positions (entry, entry_ts, etc.)
    - decision_engine.trade_ctx (mode: SCALP/TREND)
    """

    # ---------------- CONFIG ----------------
    # Pivot detection using 5-candle window (center candle is pivot candidate)
    PIVOT_WINDOW = 5  # must be odd (5 recommended)
    MIN_SECONDS_AFTER_ENTRY = 2  # ignore structure exits immediately after entry

    # SCALP behavior: protect quickly
    SCALP_REQUIRE_HH_BEFORE_LH_EXIT = True  # avoid exiting on random LH before any HH is formed
    SCALP_EXIT_ON_LH = True                 # once HH exists, first LH triggers exit
    SCALP_EXIT_ON_HL_BREAK = True           # break below last HL after HH => exit

    # TREND behavior: allow more noise
    TREND_EXIT_ON_LL = True                 # confirmed LL pivot => exit (trend weakening)
    TREND_EXIT_ON_HL_BREAK = True           # break below last HL => exit

    def __init__(self, debug=True):
        self.debug = debug

        # per secid state
        self.ctx = {}  # secid -> dict

    def _log(self, msg: str):
        if self.debug:
            print(msg)

    # ---------------- CANDLE BUILDER ----------------
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

            "candles": deque(maxlen=60),  # store recent 1s candles

            # pivots
            "pivot_highs": deque(maxlen=10),  # list of (ts, price)
            "pivot_lows": deque(maxlen=10),

            # last known structure labels
            "last_swing_high": None,
            "last_swing_low": None,
            "has_hh_since_entry": False,

            # last HL support (important for exit)
            "last_hl": None,  # price
            "last_hl_ts": None,

            # for reducing duplicate exits
            "last_exit_ts": 0.0
        }

        self.ctx[secid] = c
        return c

    def _flush_candle(self, c):
        if c["cur_sec"] is None:
            return

        candle = {
            "ts": float(c["cur_sec"]),
            "o": float(c["cur_o"]),
            "h": float(c["cur_h"]),
            "l": float(c["cur_l"]),
            "c": float(c["cur_c"])
        }
        c["candles"].append(candle)

    def _update_1s_candle(self, c, ltp: float, now_ts: float):
        sec = int(now_ts)

        # first tick
        if c["cur_sec"] is None:
            c["cur_sec"] = sec
            c["cur_o"] = ltp
            c["cur_h"] = ltp
            c["cur_l"] = ltp
            c["cur_c"] = ltp
            return

        # same second -> update
        if sec == c["cur_sec"]:
            c["cur_h"] = max(c["cur_h"], ltp)
            c["cur_l"] = min(c["cur_l"], ltp)
            c["cur_c"] = ltp
            return

        # new second -> flush old and start new
        self._flush_candle(c)

        c["cur_sec"] = sec
        c["cur_o"] = ltp
        c["cur_h"] = ltp
        c["cur_l"] = ltp
        c["cur_c"] = ltp

    # ---------------- PIVOT DETECTION ----------------
    def _detect_new_pivot(self, c):
        """
        Uses last 5 candles:
          pivot candidate = middle candle (index -3)
          pivot high if its high > highs of 2 left and 2 right
          pivot low  if its low  < lows  of 2 left and 2 right
        """
        w = self.PIVOT_WINDOW
        if len(c["candles"]) < w:
            return None

        arr = list(c["candles"])[-w:]  # last 5
        mid = w // 2
        mid_c = arr[mid]

        left = arr[:mid]
        right = arr[mid + 1:]

        mid_h = mid_c["h"]
        mid_l = mid_c["l"]

        is_pivot_high = all(mid_h > x["h"] for x in left) and all(mid_h > x["h"] for x in right)
        is_pivot_low = all(mid_l < x["l"] for x in left) and all(mid_l < x["l"] for x in right)

        if is_pivot_high:
            return ("PH", mid_c["ts"], mid_h)
        if is_pivot_low:
            return ("PL", mid_c["ts"], mid_l)

        return None

    def _classify_and_store_pivot(self, c, pivot):
        """
        Maintain:
        - HH/LH for pivot highs
        - HL/LL for pivot lows
        Also track last HL (support) after HH for exits.
        """
        kind, ts, px = pivot

        if kind == "PH":
            prev = c["last_swing_high"]
            c["pivot_highs"].append((ts, px))

            if prev is None:
                c["last_swing_high"] = px
                # first high, no label
                self._log(f"🧱 STRUCT_PH | ts={int(ts)} | px={px:.2f}")
                return ("PH", px)

            if px > prev:
                c["last_swing_high"] = px
                c["has_hh_since_entry"] = True
                self._log(f"🧱 STRUCT_HH | ts={int(ts)} | px={px:.2f}")
                return ("HH", px)
            else:
                # lower high
                self._log(f"🧱 STRUCT_LH | ts={int(ts)} | px={px:.2f}")
                return ("LH", px)

        if kind == "PL":
            prev = c["last_swing_low"]
            c["pivot_lows"].append((ts, px))

            if prev is None:
                c["last_swing_low"] = px
                self._log(f"🧱 STRUCT_PL | ts={int(ts)} | px={px:.2f}")
                return ("PL", px)

            if px > prev:
                # Higher Low (bullish support)
                c["last_swing_low"] = px
                c["last_hl"] = px
                c["last_hl_ts"] = ts
                self._log(f"🧱 STRUCT_HL | ts={int(ts)} | px={px:.2f}")
                return ("HL", px)
            else:
                # Lower Low
                c["last_swing_low"] = px
                self._log(f"🧱 STRUCT_LL | ts={int(ts)} | px={px:.2f}")
                return ("LL", px)

        return None

    # ---------------- MAIN API ----------------
    def on_tick(self, *, secid, tag, ltp, paper_trader, decision_engine):
        """
        Returns:
            None
            OR
            {"exit": True, "reason": "..."}
        """

        pos = paper_trader.positions.get(secid)
        if not pos:
            # clear state if no position
            self.ctx.pop(secid, None)
            return None

        entry = float(pos["entry"])
        entry_ts = float(pos.get("entry_ts") or 0.0)
        now_ts = time.time()

        # quick safety: avoid spamming multiple exits same second
        c = self._get_or_create_ctx(secid, entry)
        if now_ts - c["last_exit_ts"] < 1.0:
            return None

        # ignore immediately after entry (micro-noise)
        if now_ts - entry_ts < self.MIN_SECONDS_AFTER_ENTRY:
            # still build candles though
            self._update_1s_candle(c, float(ltp), now_ts)
            return None

        # get mode from decision engine (default SCALP)
        mode = "SCALP"
        try:
            ctx = getattr(decision_engine, "trade_ctx", None)
            if ctx and secid in ctx and ctx[secid].get("mode"):
                mode = str(ctx[secid]["mode"]).upper()
        except Exception:
            pass

        # update candle
        self._update_1s_candle(c, float(ltp), now_ts)

        # detect pivot only when enough candles exist
        pivot = self._detect_new_pivot(c)
        label = None
        if pivot:
            label = self._classify_and_store_pivot(c, pivot)

        # ---------------- EXIT RULES ----------------
        # Rule A: HL break (support break) after HH (for both SCALP and TREND)
        if c["has_hh_since_entry"] and c["last_hl"] is not None:
            if float(ltp) < float(c["last_hl"]):
                if (mode == "SCALP" and self.SCALP_EXIT_ON_HL_BREAK) or (mode == "TREND" and self.TREND_EXIT_ON_HL_BREAK):
                    c["last_exit_ts"] = now_ts
                    self._log(
                        f"📉 STRUCTURE_EXIT | {tag} | mode={mode} | "
                        f"reason=HL_BREAK | ltp={ltp:.2f} < hl={c['last_hl']:.2f}"
                    )
                    return {"exit": True, "reason": "STRUCT_HL_BREAK"}

        # Rule B: SCALP exit on first LH (only after HH exists to avoid random noise)
        if mode == "SCALP" and self.SCALP_EXIT_ON_LH and label:
            if label[0] == "LH":
                if (not self.SCALP_REQUIRE_HH_BEFORE_LH_EXIT) or c["has_hh_since_entry"]:
                    c["last_exit_ts"] = now_ts
                    self._log(
                        f"📉 STRUCTURE_EXIT | {tag} | mode=SCALP | reason=LH_FORMED"
                    )
                    return {"exit": True, "reason": "STRUCT_LH_SCALP"}

        # Rule C: TREND exit on LL pivot (trend failure)
        if mode == "TREND" and self.TREND_EXIT_ON_LL and label:
            if label[0] == "LL":
                c["last_exit_ts"] = now_ts
                self._log(
                    f"📉 STRUCTURE_EXIT | {tag} | mode=TREND | reason=LL_FORMED"
                )
                return {"exit": True, "reason": "STRUCT_LL_TREND"}

        return None