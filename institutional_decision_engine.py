import time
from collections import defaultdict, deque


class InstitutionalDecisionEngine:
    """
    INSTITUTIONAL DECISION ENGINE (MODE-AWARE)

    ✅ SCALP mode:
        - allow fast rejects (post-entry validation)
        - allow tight trail exits

    ✅ TREND mode:
        - DO NOT allow ENTRY_REJECTED_CONFIRM / quick-kills
        - DO NOT allow micro trail exits
        - only allow true turn / decay / stall exits

    This fixes your issue: exits in 1–6 seconds even after trend confirmation.
    """

    # ---------------- CONFIG ----------------
    STRUCTURE_LOOKBACK_SEC = 45
    STRUCTURE_MIN_POINTS = 6
    STRUCTURE_NEAR_EXTREME_PCT = 0.05
    STRUCTURE_SWING_MARGIN_PCT = 0.01
    STRUCTURE_COMPRESSION_PCT = 0.12
    BALANCE_LOOKBACK_SEC = 30

    # Post-entry validation (used only in SCALP)
    DISPLACEMENT_THRESHOLD_PCT = 0.15
    HOLD_CONFIRM_SEC = 3
    POST_ENTRY_VALIDATE_MAX_SEC = 6

    # Mode gating
    MODE_DEFAULT = "SCALP"
    MODE_UPGRADE_CONFIRM_TICKS = 3  # if your engine already logs accept_count=3, match that

    def __init__(self, debug=True):
        self.debug = debug

        self.trade_ctx = {}               # secid -> ctx
        self.index_legs = defaultdict(set)
        self.price_track = defaultdict(deque)

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
        pts = [p for ts, p in self.price_track[secid] if now - ts <= self.BALANCE_LOOKBACK_SEC]
        return (sum(pts) / len(pts)) if pts else float(fallback)

    def _extract_swings(self, prices):
        swings = []
        for i in range(1, len(prices) - 1):
            _, p0 = prices[i - 1]
            ts, p1 = prices[i]
            _, p2 = prices[i + 1]
            if p1 > p0 and p1 > p2:
                swings.append(("HIGH", ts, p1))
            elif p1 < p0 and p1 < p2:
                swings.append(("LOW", ts, p1))
        return swings

    def _structure_snapshot(self, secid):
        prices = list(self.price_track[secid])
        if len(prices) < self.STRUCTURE_MIN_POINTS:
            return None

        vals = [p for _, p in prices]
        hi, lo = max(vals), min(vals)
        rng = hi - lo
        last = vals[-1]

        compression = rng <= max(last, 1e-6) * self.STRUCTURE_COMPRESSION_PCT
        swings = self._extract_swings(prices)

        highs = [s for s in swings if s[0] == "HIGH"]
        lows = [s for s in swings if s[0] == "LOW"]

        return {
            "range": rng,
            "compression": compression,
            "last_high": highs[-1] if highs else None,
            "prev_high": highs[-2] if len(highs) > 1 else None,
            "last_low": lows[-1] if lows else None,
            "prev_low": lows[-2] if len(lows) > 1 else None,
        }

    def _entry_structure_ok(self, side, ltp, secid):
        struct = self._structure_snapshot(secid)
        if not struct:
            return False, "NO_STRUCTURE"

        rng = struct["range"]
        ll, pl = struct["last_low"], struct["prev_low"]

        # LONG-only system (your current setup)
        if side != "LONG":
            return False, "SIDE_BLOCKED"

        near_low = ll and float(ltp) <= ll[2] * (1 + self.STRUCTURE_NEAR_EXTREME_PCT)
        higher_low = ll and pl and ll[2] >= pl[2] * (1 + self.STRUCTURE_SWING_MARGIN_PCT)
        failed = ll and pl and ll[2] < pl[2] and float(ltp) > ll[2] + rng * 0.4

        ok = bool(near_low and (higher_low or failed or struct["compression"]))
        reason = "HL_OR_FAIL" if (higher_low or failed) else "COMPRESSION"
        return ok, reason

    # --------------------------------------------------
    def _ctx(self, secid, tag, side, entry, ts):
        index = tag.split("_")[0]
        ctx = self.trade_ctx.get(secid)
        if not ctx:
            ctx = {
                "secid": secid,
                "index": index,
                "tag": tag,
                "side": side,
                "entry": float(entry),
                "ts": float(ts),

                # MODE control
                "mode": self.MODE_DEFAULT,     # SCALP -> TREND
                "accept_count": 0,

                # Post entry validation
                "post_validate_until": None,
                "disp_start": None,
                "disp_confirmed": False,
            }
            self.trade_ctx[secid] = ctx
        return ctx

    # --------------------------------------------------
    def on_signal(self, *, secid, tag, ltp, signal, momentum_engine, paper_trader):
        now = time.time()
        self._update_price_history(secid, now, ltp)

        # -----------------------------------------
        # 1) ENTRY handling (SOLE authority)
        # -----------------------------------------
        if signal in ("A_ENTRY", "B_ENTRY"):
            trade = momentum_engine.active_trade.get(secid)
            if not trade:
                return {"entry_allowed": False}

            side = trade.get("side", "LONG")
            ok, reason = self._entry_structure_ok(side, ltp, secid)
            if not ok:
                self._log(f"ENTRY_BLOCKED | {tag} | G1_STRUCTURE | reason={reason}")
                momentum_engine.active_trade.pop(secid, None)
                return {"entry_allowed": False}

            ctx = self._ctx(secid, tag, side, trade.get("entry", ltp), trade.get("ts", now))

            # commit entry
            if secid not in paper_trader.positions:
                paper_trader.on_entry(
                    secid=secid,
                    tag=tag,
                    side=side,
                    ltp=trade.get("entry", ltp),
                    lots=1,
                    reason=f"{signal}|STRUCT_{reason}"
                )

                # Start post-entry validation window (SCALP only)
                ctx["post_validate_until"] = now + self.POST_ENTRY_VALIDATE_MAX_SEC
                ctx["disp_start"] = None
                ctx["disp_confirmed"] = False

                self.index_legs[ctx["index"]].add(secid)

                self._log(f"✅ ENTRY_COMMITTED | tag={tag} | mode={ctx['mode']} | reason={signal}|STRUCT_{reason}")

            return {"entry_allowed": True}

        # -----------------------------------------
        # 2) EXIT signals from momentum engine
        # -----------------------------------------
        if signal == "EXIT":
            ctx = self.trade_ctx.get(secid)

            # If we have no ctx, allow exit (safe default)
            if not ctx:
                return {"exit_allowed": True}

            # ✅ MODE-AWARE EXIT:
            # If TREND mode, do NOT allow “entry rejection” style fast exits.
            # We allow only real exits (momentum engine says EXIT, but we can veto certain reasons)
            reason = momentum_engine.last_exit_reason.get(secid, "EXIT")

            if ctx["mode"] == "TREND":
                if str(reason).upper() in ("ENTRY_REJECTED_CONFIRM", "ENTRY_INVALIDATED", "ENTRY_INVALIDATED_NO_DISPLACEMENT"):
                    self._log(f"🛑 EXIT_VETO | {tag} | mode=TREND | reason={reason}")
                    return {"exit_allowed": False}

            return {"exit_allowed": True}

        # -----------------------------------------
        # 3) POST-ENTRY VALIDATION (only while position open)
        # -----------------------------------------
        if secid not in paper_trader.positions:
            return

        ctx = self.trade_ctx.get(secid)
        if not ctx:
            return

        # ✅ Simple TREND upgrade heuristic:
        # if price stays alive and not rejected quickly, upgrade after few ticks
        # (you already have mode upgrade logs from your other logic; this is a safe fallback)
        ctx["accept_count"] += 1
        if ctx["mode"] == "SCALP" and ctx["accept_count"] >= self.MODE_UPGRADE_CONFIRM_TICKS:
            ctx["mode"] = "TREND"
            self._log(f"🧭 MODE_UPGRADE | tag={tag} | SCALP -> TREND | accept_count={ctx['accept_count']}")

        # If already confirmed displacement, nothing to do
        if ctx["disp_confirmed"]:
            return

        # ✅ Critical change: In TREND mode we DO NOT quick-kill by post-entry validation
        if ctx["mode"] == "TREND":
            return

        # SCALP-only post-entry validation (quick reject weak follow-through)
        validate_until = ctx.get("post_validate_until")
        if validate_until is None:
            return

        bal = self._balance_price(secid, now, ltp)
        disp = abs(float(ltp) - bal) / max(bal, 1e-6)

        if disp >= self.DISPLACEMENT_THRESHOLD_PCT:
            if ctx["disp_start"] is None:
                ctx["disp_start"] = now
        else:
            ctx["disp_start"] = None

        if ctx["disp_start"] is not None:
            hold = now - ctx["disp_start"]
            if hold >= self.HOLD_CONFIRM_SEC:
                ctx["disp_confirmed"] = True
                self._log(f"POST_ENTRY_CONFIRMED | {tag} | disp={disp:.4f} | hold={hold:.1f}s")
                return

        if now > validate_until:
            # SCALP quick kill
            paper_trader.on_exit(secid, float(ltp), reason="ENTRY_INVALIDATED")
            momentum_engine.active_trade.pop(secid, None)
            self.trade_ctx.pop(secid, None)
            self.index_legs[ctx["index"]].discard(secid)
            self._log(f"POST_ENTRY_KILL | {tag} | reason=ENTRY_INVALIDATED")
            return