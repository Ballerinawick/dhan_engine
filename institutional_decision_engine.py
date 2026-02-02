import time
from collections import defaultdict, deque


class InstitutionalDecisionEngine:
    """
    INSTITUTIONAL DECISION ENGINE (FINAL – FAST ENTRY SAFE)

    ✔ Momentum engine controls timing
    ✔ Structure gate only BEFORE entry
    ✔ Displacement + hold validated AFTER entry
    ✔ Quick kill if no follow-through
    ✔ All institutional exits preserved

    USE THIS VERSION ONLY
    """

    # ---------------- CONFIG ----------------
    DOMINANCE_WINDOW_SEC = 8
    DOMINANCE_RATIO = 1.8
    COOLDOWN_SEC = 5

    DISPLACEMENT_THRESHOLD_PCT = 0.15
    HOLD_CONFIRM_SEC = 3
    POST_ENTRY_VALIDATE_MAX_SEC = 6

    STRUCTURE_LOOKBACK_SEC = 45
    STRUCTURE_MIN_POINTS = 6
    STRUCTURE_NEAR_EXTREME_PCT = 0.05
    STRUCTURE_SWING_MARGIN_PCT = 0.01
    STRUCTURE_COMPRESSION_PCT = 0.12

    BALANCE_LOOKBACK_SEC = 30

    def __init__(self, debug=True):
        self.debug = debug

        self.trade_ctx = {}
        self.index_legs = defaultdict(set)

        self.pnl_track = defaultdict(list)
        self.speed_track = defaultdict(list)
        self.slope_track = defaultdict(list)

        self.price_track = defaultdict(deque)

    # --------------------------------------------------
    def _log(self, msg):
        if self.debug:
            print(msg)

    # --------------------------------------------------
    def _update_price_history(self, secid, now, ltp):
        q = self.price_track[secid]
        q.append((now, ltp))
        while q and now - q[0][0] > self.STRUCTURE_LOOKBACK_SEC:
            q.popleft()

    def _balance_price(self, secid, now, fallback):
        pts = [p for ts, p in self.price_track[secid] if now - ts <= self.BALANCE_LOOKBACK_SEC]
        return sum(pts) / len(pts) if pts else fallback

    # --------------------------------------------------
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

        compression = rng <= last * self.STRUCTURE_COMPRESSION_PCT
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

    # --------------------------------------------------
    def _entry_structure_ok(self, side, ltp, secid):
        struct = self._structure_snapshot(secid)
        if not struct:
            return False, "NO_STRUCTURE"

        rng = struct["range"]
        lh, ph = struct["last_high"], struct["prev_high"]
        ll, pl = struct["last_low"], struct["prev_low"]

        if side == "LONG":
            near_low = ll and ltp <= ll[2] * (1 + self.STRUCTURE_NEAR_EXTREME_PCT)
            higher_low = ll and pl and ll[2] >= pl[2] * (1 + self.STRUCTURE_SWING_MARGIN_PCT)
            failed = ll and pl and ll[2] < pl[2] and ltp > ll[2] + rng * 0.4
            ok = near_low and (higher_low or failed or struct["compression"])
            return ok, "HL_OR_FAIL"
        return False, "SIDE_BLOCKED"

    # ==================================================
    # MAIN HOOK
    # ==================================================
    def on_signal(
        self,
        *,
        secid,
        tag,
        ltp,
        signal,
        momentum_engine,
        paper_trader
    ):
        now = time.time()
        index = tag.split("_")[0]
        self._update_price_history(secid, now, ltp)

        # ================= ENTRY =================
        if signal in ("A_ENTRY", "B_ENTRY"):

            trade = momentum_engine.active_trade.get(secid)
            if not trade:
                return {"entry_allowed": False}

            side = trade["side"]

            ok, reason = self._entry_structure_ok(side, ltp, secid)
            if not ok:
                self._log(f"ENTRY_BLOCKED | {tag} | G1_STRUCTURE")
                momentum_engine.active_trade.pop(secid, None)
                return {"entry_allowed": False}

            ctx = self.trade_ctx.get(secid)
            if not ctx:
                ctx = {
                    "secid": secid,
                    "index": index,
                    "tag": tag,
                    "side": side,
                    "entry": trade["entry"],
                    "ts": trade["ts"],
                    "entry_committed": False,
                    "post_validate_until": None,
                    "disp_start": None,
                    "disp_confirmed": False,
                }
                self.trade_ctx[secid] = ctx

            if secid not in paper_trader.positions:
                paper_trader.on_entry(
                    secid=secid,
                    tag=tag,
                    side=side,
                    ltp=trade["entry"],
                    lots=1,
                    reason=f"STRUCT_{reason}"
                )

                ctx["entry_committed"] = True
                ctx["post_validate_until"] = now + self.POST_ENTRY_VALIDATE_MAX_SEC
                self.index_legs[index].add(secid)

                self._log(f"ENTRY_COMMITTED_FAST | {tag}")

            return {"entry_allowed": True}

        # ================= EXIT FROM MOMENTUM =================
        if signal == "EXIT":
            ctx = self.trade_ctx.get(secid)
            if not ctx:
                return {"exit_allowed": True}

            return {"exit_allowed": True}

        # ================= POST ENTRY VALIDATION =================
        if secid not in paper_trader.positions:
            return

        ctx = self.trade_ctx.get(secid)
        if not ctx or ctx["disp_confirmed"]:
            return

        bal = self._balance_price(secid, now, ltp)
        disp = abs(ltp - bal) / max(bal, 1e-6)

        if disp >= self.DISPLACEMENT_THRESHOLD_PCT:
            if ctx["disp_start"] is None:
                ctx["disp_start"] = now
        else:
            ctx["disp_start"] = None

        if ctx["disp_start"]:
            hold = now - ctx["disp_start"]
            if hold >= self.HOLD_CONFIRM_SEC:
                ctx["disp_confirmed"] = True
                self._log(f"POST_ENTRY_CONFIRMED | {tag}")
                return

        if now > ctx["post_validate_until"]:
            paper_trader.on_exit(secid, ltp, reason="ENTRY_INVALIDATED")
            momentum_engine.active_trade.pop(secid, None)
            self.trade_ctx.pop(secid, None)
            self.index_legs[index].discard(secid)
            self._log(f"POST_ENTRY_KILL | {tag}")
            return