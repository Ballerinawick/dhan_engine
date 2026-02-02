import time
from collections import defaultdict, deque


class InstitutionalDecisionEngine:
    """
    INSTITUTIONAL DECISION ENGINE (OPTION A)

    CORE IDEAS:
    ✅ Structural entry (already present)
    ✅ SCALP → TREND upgrade
    ✅ Dynamic exits only
    ✅ ONE SIDE ONLY (CE or PE)
    ✅ Flip allowed ONLY after dynamic exit
    """

    # ---------------- CONFIG ----------------
    STRUCTURE_LOOKBACK_SEC = 45
    STRUCTURE_MIN_POINTS = 6
    STRUCTURE_NEAR_EXTREME_PCT = 0.05
    STRUCTURE_SWING_MARGIN_PCT = 0.01
    STRUCTURE_COMPRESSION_PCT = 0.12
    BALANCE_LOOKBACK_SEC = 30

    DISPLACEMENT_THRESHOLD_PCT = 0.15
    HOLD_CONFIRM_SEC = 3
    POST_ENTRY_VALIDATE_MAX_SEC = 6

    MODE_DEFAULT = "SCALP"
    MODE_UPGRADE_CONFIRM_TICKS = 3

    def __init__(self, debug=True):
        self.debug = debug

        self.trade_ctx = {}              # secid -> ctx
        self.price_track = defaultdict(deque)

        self.active_side = {}            # secid -> "CE" or "PE"

    # --------------------------------------------------
    def _log(self, msg):
        if self.debug:
            print(msg)

    def _update_price_history(self, secid, now, ltp):
        q = self.price_track[secid]
        q.append((now, float(ltp)))
        while q and now - q[0][0] > self.STRUCTURE_LOOKBACK_SEC:
            q.popleft()

    def _balance_price(self, secid, now, fallback):
        pts = [p for ts, p in self.price_track[secid]
               if now - ts <= self.BALANCE_LOOKBACK_SEC]
        return sum(pts) / len(pts) if pts else float(fallback)

    # --------------------------------------------------
    def _structure_ok(self, ltp, secid):
        prices = list(self.price_track[secid])
        if len(prices) < self.STRUCTURE_MIN_POINTS:
            return False

        vals = [p for _, p in prices]
        hi, lo = max(vals), min(vals)
        rng = hi - lo

        return rng <= max(vals[-1], 1e-6) * self.STRUCTURE_COMPRESSION_PCT

    # --------------------------------------------------
    def _ctx(self, secid, tag, entry, ts):
        ctx = self.trade_ctx.get(secid)
        if not ctx:
            ctx = {
                "tag": tag,
                "entry": float(entry),
                "ts": ts,
                "mode": self.MODE_DEFAULT,
                "accept_count": 0,
                "post_validate_until": ts + self.POST_ENTRY_VALIDATE_MAX_SEC,
                "disp_start": None,
                "disp_confirmed": False
            }
            self.trade_ctx[secid] = ctx
        return ctx

    # --------------------------------------------------
    def on_signal(self, *, secid, tag, ltp, signal, momentum_engine, paper_trader):
        now = time.time()
        self._update_price_history(secid, now, ltp)

        side = "CE" if "CE" in tag else "PE"

        # =============================
        # ENTRY (SOLE AUTHORITY)
        # =============================
        if signal in ("A_ENTRY", "B_ENTRY"):
            trade = momentum_engine.active_trade.get(secid)
            if not trade:
                return {"entry_allowed": False}

            # ONE SIDE LOCK
            locked = self.active_side.get(secid)
            if locked and locked != side:
                self._log(f"⛔ SIDE_BLOCK | {tag} | active={locked}")
                return {"entry_allowed": False}

            if not self._structure_ok(ltp, secid):
                return {"entry_allowed": False}

            if secid not in paper_trader.positions:
                paper_trader.on_entry(
                    secid=secid,
                    tag=tag,
                    side="LONG",
                    ltp=trade.get("entry", ltp),
                    lots=1,
                    reason=f"{signal}|STRUCT_OK"
                )

                self.active_side[secid] = side
                self._ctx(secid, tag, trade.get("entry", ltp), now)

                self._log(f"✅ ENTRY_COMMITTED | {tag} | side={side}")

            return {"entry_allowed": True}

        # =============================
        # EXIT (DYNAMIC ONLY)
        # =============================
        if signal == "EXIT":
            ctx = self.trade_ctx.get(secid)
            if not ctx:
                return {"exit_allowed": True}

            reason = momentum_engine.last_exit_reason.get(secid, "EXIT")

            # TREND PROTECTION
            if ctx["mode"] == "TREND" and reason in (
                "ENTRY_REJECTED_CONFIRM",
                "ENTRY_INVALIDATED",
                "ENTRY_INVALIDATED_NO_DISPLACEMENT"
            ):
                self._log(f"🛑 EXIT_VETO | {tag} | TREND")
                return {"exit_allowed": False}

            # TRUE EXIT → RESET SIDE LOCK
            self.active_side.pop(secid, None)
            self.trade_ctx.pop(secid, None)

            return {
                "exit_allowed": True,
                "exit_reason": "DYNAMIC"
            }

        # =============================
        # POST-ENTRY VALIDATION
        # =============================
        if secid not in paper_trader.positions:
            return

        ctx = self.trade_ctx.get(secid)
        if not ctx:
            return

        ctx["accept_count"] += 1
        if ctx["mode"] == "SCALP" and ctx["accept_count"] >= self.MODE_UPGRADE_CONFIRM_TICKS:
            ctx["mode"] = "TREND"
            self._log(f"🧭 MODE_UPGRADE | {tag} | SCALP→TREND")

        if ctx["mode"] == "TREND":
            return

        bal = self._balance_price(secid, now, ltp)
        disp = abs(ltp - bal) / max(bal, 1e-6)

        if disp >= self.DISPLACEMENT_THRESHOLD_PCT:
            if ctx["disp_start"] is None:
                ctx["disp_start"] = now
        else:
            ctx["disp_start"] = None

        if ctx["disp_start"] and now - ctx["disp_start"] >= self.HOLD_CONFIRM_SEC:
            ctx["disp_confirmed"] = True
            return

        if now > ctx["post_validate_until"]:
            paper_trader.on_exit(secid, ltp, reason="ENTRY_INVALIDATED")
            momentum_engine.active_trade.pop(secid, None)
            self.trade_ctx.pop(secid, None)
            self.active_side.pop(secid, None)
            self._log(f"❌ POST_ENTRY_KILL | {tag}")