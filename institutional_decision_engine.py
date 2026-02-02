import time
from collections import defaultdict, deque


class InstitutionalDecisionEngine:
    """
    INSTITUTIONAL DECISION ENGINE (INTENT-BASED, DYNAMIC)

    Goals:
      ✅ Fast entry (MomentumEngine decides entry timing)
      ✅ No dumb 6-sec kills
      ✅ Dynamic exit based on intent + acceptance/rejection
      ✅ Can veto MomentumEngine EXIT if move is still accepting

    Design:
      - Entry: only light STRUCTURE gate (pre-entry)
      - After entry: evaluate INTENT state from price behavior
        INTENT:
          ACCEPTING  -> hold / allow trend
          BALANCED   -> hold (scalp management)
          REJECTING  -> prepare exit (only if rejection persists)
      - Exit rules use "consecutive rejection confirmations"
        instead of fixed timers.

    IMPORTANT:
      - This engine executes entry itself via paper_trader.on_entry().
      - Your main loop must NOT call paper_trader.on_entry() on A_ENTRY/B_ENTRY.
    """

    # -------------------- STRUCTURE GATE (PRE-ENTRY) --------------------
    STRUCTURE_LOOKBACK_SEC = 45
    STRUCTURE_MIN_POINTS = 6
    STRUCTURE_NEAR_EXTREME_PCT = 0.05
    STRUCTURE_SWING_MARGIN_PCT = 0.01
    STRUCTURE_COMPRESSION_PCT = 0.12

    # -------------------- INTENT / DYNAMIC MANAGEMENT --------------------
    BALANCE_LOOKBACK_SEC = 25  # fair price
    MICRO_LOOKBACK_SEC = 3     # speed/impulse window

    # "Dynamic, not time": we use confirmations (counts)
    REJECTION_CONFIRMATIONS_TO_EXIT = 3      # 3 consecutive REJECTING evaluations
    ACCEPT_CONFIRMATIONS_TO_TREND = 3        # 3 consecutive ACCEPTING -> allow trend hold
    STALL_CONFIRMATIONS_TO_EXIT = 6          # repeated no-progress confirmations

    # thresholds are % of entry (still dynamic across premiums)
    ENTRY_BREAK_PCT = 0.10 / 100             # 0.10% break under entry = weak
    BALANCE_REJECT_PCT = 0.08 / 100          # if price snaps back into balance zone repeatedly
    MIN_PROGRESS_PCT = 0.12 / 100            # if can't exceed this progress after acceptance -> stall risk

    # dynamic trailing protection (scalp vs trend)
    SCALP_TRAIL_PCT = 0.18 / 100             # tighter trail when scalp-like
    TREND_TRAIL_PCT = 0.35 / 100             # looser trail when trend-like

    def __init__(self, debug=True):
        self.debug = debug

        # secid -> ctx
        self.trade_ctx = {}

        # secid -> price history deque[(ts, ltp)]
        self.price_track = defaultdict(deque)

    # --------------------------------------------------
    def _log(self, msg: str):
        if self.debug:
            print(msg)

    # --------------------------------------------------
    def _update_price_history(self, secid: int, now: float, ltp: float):
        q = self.price_track[secid]
        q.append((now, float(ltp)))
        # keep the longest window only
        while q and (now - q[0][0]) > self.STRUCTURE_LOOKBACK_SEC:
            q.popleft()

    def _balance_price(self, secid: int, now: float, fallback: float) -> float:
        q = self.price_track[secid]
        vals = [p for ts, p in q if (now - ts) <= self.BALANCE_LOOKBACK_SEC]
        return (sum(vals) / len(vals)) if vals else float(fallback)

    def _micro_speed(self, secid: int, now: float) -> float:
        """
        Approx speed using last MICRO_LOOKBACK_SEC window:
        (last - first) / seconds
        """
        q = self.price_track[secid]
        pts = [(ts, p) for ts, p in q if (now - ts) <= self.MICRO_LOOKBACK_SEC]
        if len(pts) < 2:
            return 0.0
        dt = pts[-1][0] - pts[0][0]
        if dt <= 0:
            return 0.0
        return (pts[-1][1] - pts[0][1]) / dt

    # ---------------- STRUCTURE HELPERS ----------------
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

    def _structure_snapshot(self, secid: int):
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
            "last_price": last,
        }

    def _entry_structure_ok(self, side: str, ltp: float, secid: int):
        """
        Light pre-entry filter.
        LONG-only safe filter:
          - near recent swing low AND (higher-low OR compression OR failed-continuation)
        """
        struct = self._structure_snapshot(secid)
        if not struct:
            return False, "NO_STRUCTURE"

        rng = struct["range"]
        ll, pl = struct["last_low"], struct["prev_low"]

        if side != "LONG":
            return False, "SIDE_BLOCKED"

        near_low = bool(ll) and ltp <= ll[2] * (1 + self.STRUCTURE_NEAR_EXTREME_PCT)
        higher_low = bool(ll and pl) and ll[2] >= pl[2] * (1 + self.STRUCTURE_SWING_MARGIN_PCT)
        failed = bool(ll and pl) and (ll[2] < pl[2]) and (ltp > ll[2] + rng * 0.4)

        ok = near_low and (higher_low or failed or struct["compression"])
        return (ok, "HL_OR_FAIL" if (higher_low or failed) else "COMPRESSION")

    # ---------------- INTENT ENGINE ----------------
    def _intent_state(self, ctx: dict, ltp: float, bal: float, speed: float) -> str:
        """
        Decide ACCEPTING / BALANCED / REJECTING.

        For LONG:
          - ACCEPTING: price staying above balance and making progress
          - BALANCED: hovering around balance / low speed
          - REJECTING: snapping back below balance or breaking entry repeatedly
        """
        entry = ctx["entry"]
        pnl = ltp - entry
        pnl_pct = pnl / max(entry, 1e-9)

        # balance zone measure
        bal_gap_pct = abs(ltp - bal) / max(bal, 1e-9)

        # "acceptance" = above balance + positive speed or new highs
        above_balance = ltp >= bal
        new_high = ltp > ctx.get("best_price", entry)

        # "rejection" signals
        broke_entry = ltp < entry * (1 - self.ENTRY_BREAK_PCT)
        snapped_to_balance = (bal_gap_pct <= self.BALANCE_REJECT_PCT) and (abs(speed) < 1e-6)

        if above_balance and (speed > 0 or new_high) and pnl_pct >= 0:
            return "ACCEPTING"

        if broke_entry:
            return "REJECTING"

        # if move is flat around balance, it's balanced (not an exit)
        if snapped_to_balance or abs(speed) < 1e-6:
            return "BALANCED"

        # if price below balance with negative speed -> rejecting bias
        if (ltp < bal) and (speed < 0):
            return "REJECTING"

        return "BALANCED"

    def _dynamic_trail_pct(self, ctx: dict) -> float:
        """
        Decide trail based on mode.
        If ACCEPTING confirmed multiple times -> TREND trail,
        else SCALP trail.
        """
        mode = ctx.get("mode", "SCALP")
        return self.TREND_TRAIL_PCT if mode == "TREND" else self.SCALP_TRAIL_PCT

    # ==================================================
    # MAIN HOOK
    # ==================================================
    def on_signal(
        self,
        *,
        secid: int,
        tag: str,
        ltp: float,
        signal: str,
        momentum_engine,
        paper_trader
    ):
        now = time.time()
        self._update_price_history(secid, now, ltp)

        # Normalize signal: treat EXIT as EXIT_SIGNAL
        exit_signal = (signal == "EXIT")

        # ---------------- ENTRY ----------------
        if signal in ("A_ENTRY", "B_ENTRY"):
            trade = momentum_engine.active_trade.get(secid)
            if not trade:
                return {"entry_allowed": False, "entry_handled": False}

            side = trade.get("side", "LONG")

            ok, reason = self._entry_structure_ok(side, ltp, secid)
            if not ok:
                self._log(f"ENTRY_BLOCKED | tag={tag} | gate=G1_STRUCTURE | reason={reason}")
                momentum_engine.active_trade.pop(secid, None)
                return {"entry_allowed": False, "entry_handled": False}

            # Create ctx
            if secid not in self.trade_ctx:
                self.trade_ctx[secid] = {
                    "secid": secid,
                    "tag": tag,
                    "side": side,
                    "entry": float(trade.get("entry", ltp)),
                    "ts": float(trade.get("ts", now)),

                    # intent counters
                    "accept_count": 0,
                    "reject_count": 0,
                    "stall_count": 0,

                    # tracking
                    "best_price": float(trade.get("entry", ltp)),
                    "worst_price": float(trade.get("entry", ltp)),
                    "last_progress_price": float(trade.get("entry", ltp)),

                    # dynamic mode
                    "mode": "SCALP",  # upgrades to TREND if acceptance persists
                }

            ctx = self.trade_ctx[secid]

            # Execute entry here (SOLE authority)
            if secid not in paper_trader.positions:
                paper_trader.on_entry(
                    secid=secid,
                    tag=tag,
                    side=side,
                    ltp=ctx["entry"],
                    lots=1,
                    reason=f"{signal}|STRUCT_{reason}"
                )
                self._log(f"✅ ENTRY_COMMITTED | tag={tag} | mode={ctx['mode']} | reason={signal}|STRUCT_{reason}")

            return {"entry_allowed": True, "entry_handled": True}

        # ---------------- If not in position, nothing to manage ----------------
        if secid not in paper_trader.positions:
            return {"exit_allowed": True}

        ctx = self.trade_ctx.get(secid)
        if not ctx:
            # safety: reconstruct minimal ctx from paper trader if needed
            pos = paper_trader.positions.get(secid, {})
            self.trade_ctx[secid] = {
                "secid": secid,
                "tag": pos.get("tag", tag),
                "side": pos.get("side", "LONG"),
                "entry": float(pos.get("entry", ltp)),
                "ts": float(pos.get("entry_ts", now)),
                "accept_count": 0,
                "reject_count": 0,
                "stall_count": 0,
                "best_price": float(pos.get("entry", ltp)),
                "worst_price": float(pos.get("entry", ltp)),
                "last_progress_price": float(pos.get("entry", ltp)),
                "mode": "SCALP",
            }
            ctx = self.trade_ctx[secid]

        # ---------------- ACTIVE MANAGEMENT ----------------
        entry = ctx["entry"]
        pnl = ltp - entry
        pnl_pct = pnl / max(entry, 1e-9)

        # Update extremes
        if ltp > ctx["best_price"]:
            ctx["best_price"] = float(ltp)
        if ltp < ctx["worst_price"]:
            ctx["worst_price"] = float(ltp)

        bal = self._balance_price(secid, now, fallback=ltp)
        speed = self._micro_speed(secid, now)

        intent = self._intent_state(ctx, ltp, bal, speed)

        # stall detection: no meaningful progress from last_progress_price
        if ltp <= (ctx["last_progress_price"] * (1 + self.MIN_PROGRESS_PCT)):
            ctx["stall_count"] += 1
        else:
            ctx["stall_count"] = 0
            ctx["last_progress_price"] = float(ltp)

        # Update acceptance/rejection counters (dynamic confirmation)
        if intent == "ACCEPTING":
            ctx["accept_count"] += 1
            ctx["reject_count"] = 0
        elif intent == "REJECTING":
            ctx["reject_count"] += 1
            ctx["accept_count"] = 0
        else:
            # BALANCED: decay both slowly (don’t overreact)
            ctx["accept_count"] = max(ctx["accept_count"] - 1, 0)
            ctx["reject_count"] = max(ctx["reject_count"] - 1, 0)

        # Upgrade to TREND only after sustained acceptance
        if ctx["accept_count"] >= self.ACCEPT_CONFIRMATIONS_TO_TREND:
            if ctx["mode"] != "TREND":
                ctx["mode"] = "TREND"
                if hasattr(paper_trader, "note_regime_change"):
                    paper_trader.note_regime_change(secid=secid, tag=tag, mode="TREND", reason="ACCEPTANCE_CONFIRMED")
                self._log(f"🧭 MODE_UPGRADE | tag={tag} | SCALP -> TREND | accept_count={ctx['accept_count']}")

        # ---------------- EXIT LOGIC (DYNAMIC) ----------------

        # 1) Hard invalidation: repeated rejection early (not time-based, confirmation-based)
        if ctx["reject_count"] >= self.REJECTION_CONFIRMATIONS_TO_EXIT:
            # Exit only if not strongly winning (don’t kill real trend winners)
            if pnl_pct <= (self.MIN_PROGRESS_PCT * 2):
                reason = "ENTRY_REJECTED_CONFIRM"
                paper_trader.on_exit(secid, ltp, reason=reason)
                momentum_engine.active_trade.pop(secid, None)
                self.trade_ctx.pop(secid, None)
                self._log(f"🚪 EXIT | tag={tag} | reason={reason} | reject_count={ctx['reject_count']}")
                return {"exit_allowed": True, "exit_reason": reason}

        # 2) Trailing protection: if trend/scalp winner starts rejecting
        peak = ctx["best_price"]
        trail_pct = self._dynamic_trail_pct(ctx)
        trail_stop = peak * (1 - trail_pct)

        if ltp <= trail_stop and pnl_pct > 0:
            # Require some rejection/stall confirmation to avoid noise
            if ctx["reject_count"] >= 2 or ctx["stall_count"] >= self.STALL_CONFIRMATIONS_TO_EXIT:
                reason = "TRAIL_PROTECT"
                paper_trader.on_exit(secid, ltp, reason=reason)
                momentum_engine.active_trade.pop(secid, None)
                self.trade_ctx.pop(secid, None)
                self._log(f"🚪 EXIT | tag={tag} | reason={reason} | peak={peak:.2f} | trail={trail_stop:.2f}")
                return {"exit_allowed": True, "exit_reason": reason}

        # 3) MomentumEngine exit signal: allow or veto
        if exit_signal:
            # If we are in TREND + still accepting -> veto exit
            if ctx.get("mode") == "TREND" and intent == "ACCEPTING" and pnl_pct > 0:
                self._log(f"⛔ EXIT_VETO | tag={tag} | reason=TREND_ACCEPTING | pnl_pct={pnl_pct:.4f}")
                return {"exit_allowed": False}

            # If rejecting/stalling -> allow exit
            if intent == "REJECTING" or ctx["stall_count"] >= self.STALL_CONFIRMATIONS_TO_EXIT:
                return {"exit_allowed": True, "exit_reason": "MOMENTUM_EXIT_ALLOWED"}

            # Balanced but not winning: allow exit (reduce churn)
            if pnl_pct <= self.MIN_PROGRESS_PCT:
                return {"exit_allowed": True, "exit_reason": "MOMENTUM_EXIT_BALANCED"}

            # Otherwise veto
            self._log(f"⛔ EXIT_VETO | tag={tag} | reason=BALANCED_NOT_WEAK | pnl_pct={pnl_pct:.4f}")
            return {"exit_allowed": False}

        # Default: hold
        return {"exit_allowed": False}