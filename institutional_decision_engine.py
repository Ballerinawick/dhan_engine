import time
from collections import defaultdict, deque


class InstitutionalDecisionEngine:
    """
    INSTITUTIONAL DECISION ENGINE (OPTIONS-SAFE)

    Gate-1: Structure filter BEFORE entry (lightweight)
    Gate-2: Enter immediately (do NOT delay momentum)
    Gate-3: Post-entry follow-through = move from entry (options-safe)
    Gate-4: Hold confirm for N seconds

    ✅ Fix: removes impossible 15% displacement rule that was killing every entry
    """

    # ---------------- CONFIG ----------------
    STRUCTURE_LOOKBACK_SEC = 45
    STRUCTURE_MIN_POINTS = 6
    STRUCTURE_NEAR_EXTREME_PCT = 0.05
    STRUCTURE_SWING_MARGIN_PCT = 0.01
    STRUCTURE_COMPRESSION_PCT = 0.12

    # ✅ OPTIONS-SAFE post-entry validation
    FOLLOW_THROUGH_PCT = 0.03          # 3% move from entry
    HOLD_CONFIRM_SEC = 1.5             # hold at least 1.5 sec
    POST_ENTRY_VALIDATE_MAX_SEC = 8.0  # total window

    def __init__(self, debug=True):
        self.debug = debug
        self.trade_ctx = {}
        self.index_legs = defaultdict(set)
        self.price_track = defaultdict(deque)

    def _log(self, msg):
        if self.debug:
            print(msg)

    def _update_price_history(self, secid, now, ltp):
        q = self.price_track[secid]
        q.append((now, ltp))
        while q and now - q[0][0] > self.STRUCTURE_LOOKBACK_SEC:
            q.popleft()

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

        compression = rng <= max(last, 1e-9) * self.STRUCTURE_COMPRESSION_PCT
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

        # LONG-only system
        if side != "LONG":
            return False, "SIDE_BLOCKED"

        near_low = ll and ltp <= ll[2] * (1 + self.STRUCTURE_NEAR_EXTREME_PCT)
        higher_low = ll and pl and ll[2] >= pl[2] * (1 + self.STRUCTURE_SWING_MARGIN_PCT)
        failed = ll and pl and ll[2] < pl[2] and ltp > ll[2] + rng * 0.4

        ok = near_low and (higher_low or failed or struct["compression"])
        return ok, "HL_OR_FAIL"

    # ==================================================
    # MAIN HOOK
    # ==================================================
    def on_signal(self, *, secid, tag, ltp, signal, momentum_engine, paper_trader):
        now = time.time()
        index = tag.split("_")[0]
        self._update_price_history(secid, now, ltp)

        # ================= ENTRY =================
        if signal in ("A_ENTRY", "B_ENTRY"):
            trade = momentum_engine.active_trade.get(secid)
            if not trade:
                return {"entry_allowed": False}

            side = trade.get("side", "LONG")
            ok, reason = self._entry_structure_ok(side, ltp, secid)

            if not ok:
                self._log(f"ENTRY_BLOCKED | {tag} | gate=G1_STRUCTURE | reason={reason}")
                momentum_engine.active_trade.pop(secid, None)
                return {"entry_allowed": False}

            ctx = self.trade_ctx.get(secid)
            if not ctx:
                ctx = {
                    "secid": secid,
                    "index": index,
                    "tag": tag,
                    "side": side,
                    "entry": float(trade.get("entry", ltp)),
                    "ts": float(trade.get("ts", now)),
                    "entry_committed": False,
                    "validate_until": None,
                    "ft_start": None,
                    "ft_confirmed": False,
                }
                self.trade_ctx[secid] = ctx

            # ✅ Commit entry here (SOLE AUTHORITY)
            if secid not in paper_trader.positions and not ctx["entry_committed"]:
                paper_trader.on_entry(
                    secid=secid,
                    tag=tag,
                    side=side,
                    ltp=ctx["entry"],
                    lots=1,
                    reason=f"STRUCT_{reason}"
                )

                ctx["entry_committed"] = True
                ctx["validate_until"] = now + self.POST_ENTRY_VALIDATE_MAX_SEC
                ctx["ft_start"] = None
                ctx["ft_confirmed"] = False

                self.index_legs[index].add(secid)

                self._log(
                    f"ENTRY_COMMITTED_FAST | {tag} | "
                    f"post_validate={self.POST_ENTRY_VALIDATE_MAX_SEC}s | "
                    f"ft_pct={self.FOLLOW_THROUGH_PCT:.2f}"
                )

            return {"entry_allowed": True}

        # ================= EXIT =================
        if signal == "EXIT":
            return {"exit_allowed": True}

        # ================= POST-ENTRY VALIDATION =================
        if secid not in paper_trader.positions:
            return

        ctx = self.trade_ctx.get(secid)
        if not ctx or ctx.get("ft_confirmed"):
            return

        entry = ctx["entry"]
        move_pct = abs(ltp - entry) / max(entry, 1e-9)

        # start/stop follow-through hold timer
        if move_pct >= self.FOLLOW_THROUGH_PCT:
            if ctx["ft_start"] is None:
                ctx["ft_start"] = now
        else:
            ctx["ft_start"] = None

        if ctx["ft_start"] is not None:
            hold = now - ctx["ft_start"]
            if hold >= self.HOLD_CONFIRM_SEC:
                ctx["ft_confirmed"] = True
                self._log(f"POST_ENTRY_CONFIRMED | {tag} | move_pct={move_pct:.4f} | hold={hold:.2f}s")
                return

        if now > ctx["validate_until"]:
            # quick kill
            paper_trader.on_exit(secid, ltp, reason=f"ENTRY_INVALIDATED_NO_FT(move={move_pct:.4f})")
            momentum_engine.active_trade.pop(secid, None)
            self.trade_ctx.pop(secid, None)
            self.index_legs[index].discard(secid)
            self._log(f"POST_ENTRY_KILL | {tag} | move_pct={move_pct:.4f}")
            return