import time
from collections import defaultdict, deque


class InstitutionalDecisionEngine:
    """
    INSTITUTIONAL DECISION ENGINE — STABLE (v2.2)

    Fixes:
    ✅ Fix deque slicing crash (on_opt_depth error)
    ✅ Clears phantom momentum trades on reject
    ✅ Allows same-side re-entry after cooldown
    ✅ Flip requires shadow confirmation
    """

    STRUCTURE_LOOKBACK_SEC = 45
    STRUCTURE_MIN_POINTS = 6
    STRUCTURE_COMPRESSION_PCT = 0.12
    BALANCE_LOOKBACK_SEC = 30

    DISPLACEMENT_THRESHOLD_PCT = 0.15
    HOLD_CONFIRM_SEC = 3
    POST_ENTRY_VALIDATE_MAX_SEC = 6

    MODE_DEFAULT = "SCALP"
    MODE_UPGRADE_CONFIRM_TICKS = 3

    FLIP_COOLDOWN_SEC = 12
    SHADOW_CONFIRM_TICKS = 3
    SHADOW_WINDOW_SEC = 30

    REENTRY_ALLOW_WITHOUT_STRUCT = True

    def __init__(self, debug=True):
        self.debug = debug

        self.trade_ctx = {}
        self.price_track = defaultdict(deque)

        self.index_active_side = {}
        self.index_active_secid = {}
        self.index_last_exit_ts = {}
        self.index_last_exit_side = {}

        self.shadow = defaultdict(lambda: {
            "CE": {"ticks": 0, "last_ts": 0.0, "structure_ok": False},
            "PE": {"ticks": 0, "last_ts": 0.0, "structure_ok": False},
        })

    # --------------------------------------------------
    def _log(self, msg):
        if self.debug:
            print(msg)

    def _index_from_tag(self, tag):
        return tag.split("_")[0].upper()

    def _side_from_tag(self, tag):
        return "CE" if "CE" in tag else "PE"

    # --------------------------------------------------
    def _update_price_history(self, secid, now, ltp):
        q = self.price_track[secid]
        q.append((now, float(ltp)))
        while q and now - q[0][0] > self.STRUCTURE_LOOKBACK_SEC:
            q.popleft()

    def _structure_ok(self, secid):
        pts = list(self.price_track[secid])
        if len(pts) < self.STRUCTURE_MIN_POINTS:
            return False

        prices = [p for _, p in pts]
        rng = max(prices) - min(prices)
        last = prices[-1]
        return rng <= max(last, 1e-6) * self.STRUCTURE_COMPRESSION_PCT

    # --------------------------------------------------
    def _shadow_update(self, index, side, now, struct_ok):
        s = self.shadow[index][side]
        if s["last_ts"] and now - s["last_ts"] > self.SHADOW_WINDOW_SEC:
            s["ticks"] = 0
        s["ticks"] += 1
        s["last_ts"] = now
        s["structure_ok"] = struct_ok

    def _shadow_confirmed(self, index, side, now):
        s = self.shadow[index][side]
        if now - s["last_ts"] > self.SHADOW_WINDOW_SEC:
            return False
        return s["ticks"] >= self.SHADOW_CONFIRM_TICKS and s["structure_ok"]

    def _cooldown_ok(self, index, now):
        ts = self.index_last_exit_ts.get(index)
        return ts is None or (now - ts) >= self.FLIP_COOLDOWN_SEC

    def _reject_entry(self, index, tag, side, reason, secid, momentum_engine):
        momentum_engine.active_trade.pop(secid, None)
        self._log(f"🧯 ENTRY_REJECT | {index} | {tag} | side={side} | reason={reason}")
        return {"entry_allowed": False}

    # --------------------------------------------------
    def on_signal(self, *, secid, tag, ltp, signal, momentum_engine, paper_trader):
        now = time.time()
        index = self._index_from_tag(tag)
        side = self._side_from_tag(tag)

        self._update_price_history(secid, now, ltp)
        struct_ok = self._structure_ok(secid)

        self._shadow_update(index, side, now, struct_ok)

        if signal in ("A_ENTRY", "B_ENTRY", "EXIT"):
            self._log(f"🧠 SIGNAL | {index} | {tag} | {signal} | ltp={ltp:.2f}")

        # ================= ENTRY =================
        if signal in ("A_ENTRY", "B_ENTRY"):
            trade = momentum_engine.active_trade.get(secid)
            if not trade:
                return {"entry_allowed": False}

            if index in self.index_active_secid and self.index_active_secid[index] in paper_trader.positions:
                return self._reject_entry(index, tag, side, "INDEX_LOCKED", secid, momentum_engine)

            if not self._cooldown_ok(index, now):
                return self._reject_entry(index, tag, side, "COOLDOWN", secid, momentum_engine)

            last_exit_side = self.index_last_exit_side.get(index)
            is_flip = last_exit_side and last_exit_side != side

            if is_flip and not self._shadow_confirmed(index, side, now):
                return self._reject_entry(index, tag, side, "FLIP_NO_SHADOW", secid, momentum_engine)

            if not is_flip and self.REENTRY_ALLOW_WITHOUT_STRUCT:
                struct_ok = True

            if not struct_ok:
                return self._reject_entry(index, tag, side, "STRUCT_NOT_OK", secid, momentum_engine)

            paper_trader.on_entry(
                secid=secid,
                tag=tag,
                side="LONG",
                ltp=trade["entry"],
                lots=1,
                reason=signal
            )

            self.index_active_side[index] = side
            self.index_active_secid[index] = secid

            self.trade_ctx[secid] = {
                "mode": self.MODE_DEFAULT,
                "accept": 0,
                "ts": now,
                "post_validate_until": now + self.POST_ENTRY_VALIDATE_MAX_SEC,
                "disp_start": None,
            }

            self._log(f"✅ ENTRY_COMMITTED | {index} | {tag}")
            return {"entry_allowed": True}

        # ================= EXIT =================
        if signal == "EXIT":
            self.trade_ctx.pop(secid, None)

            if self.index_active_secid.get(index) == secid:
                self.index_last_exit_side[index] = self.index_active_side.get(index)
                self.index_active_side.pop(index, None)
                self.index_active_secid.pop(index, None)
                self.index_last_exit_ts[index] = now

            return {"exit_allowed": True}

        # ================= POST ENTRY =================
        if secid not in paper_trader.positions:
            return

        ctx = self.trade_ctx.get(secid)
        if not ctx:
            return

        ctx["accept"] += 1
        if ctx["mode"] == "SCALP" and ctx["accept"] >= self.MODE_UPGRADE_CONFIRM_TICKS:
            ctx["mode"] = "TREND"
            self._log(f"🧭 MODE_UPGRADE | {index} | {tag}")

        if ctx["mode"] == "TREND":
            return

        # ✅ FIXED deque slicing
        recent = list(self.price_track[secid])[-5:]
        bal = sum(p for _, p in recent) / len(recent) if recent else ltp
        disp = abs(ltp - bal) / max(bal, 1e-6)

        if disp >= self.DISPLACEMENT_THRESHOLD_PCT:
            ctx["disp_start"] = ctx["disp_start"] or now
        else:
            ctx["disp_start"] = None

        if ctx["disp_start"] and now - ctx["disp_start"] >= self.HOLD_CONFIRM_SEC:
            return

        if now > ctx["post_validate_until"]:
            paper_trader.on_exit(secid, ltp, reason="ENTRY_INVALIDATED")
            momentum_engine.active_trade.pop(secid, None)
            self.trade_ctx.pop(secid, None)

            if self.index_active_secid.get(index) == secid:
                self.index_last_exit_side[index] = self.index_active_side.get(index)
                self.index_active_side.pop(index, None)
                self.index_active_secid.pop(index, None)
                self.index_last_exit_ts[index] = now

            self._log(f"❌ POST_ENTRY_KILL | {index} | {tag}")
            return