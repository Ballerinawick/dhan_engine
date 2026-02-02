import time
from collections import defaultdict, deque


class InstitutionalDecisionEngine:
    """
    INSTITUTIONAL DECISION ENGINE
    - ONE SIDE ONLY per index
    - DUAL LEG (TREND + SCALP on same side)
    - MODE-AWARE exits preserved
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

    MAX_SCALPS_PER_TREND = 6
    SCALP_COOLDOWN_SEC = 10

    def __init__(self, debug=True):
        self.debug = debug

        self.trade_ctx = {}               # secid -> ctx
        self.index_legs = defaultdict(set)
        self.price_track = defaultdict(deque)

        # 🔒 NEW STATE
        self.index_side_lock = {}         # index -> "CE" / "PE"
        self.scalp_count = defaultdict(int)
        self.last_scalp_time = defaultdict(float)

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
                "mode": self.MODE_DEFAULT,
                "accept_count": 0,
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

        index = tag.split("_")[0]
        side = "CE" if "CE" in tag else "PE"

        # ==================================================
        # 1️⃣ ENTRY CONTROL — ONE SIDE ONLY
        # ==================================================
        if signal in ("A_ENTRY", "B_ENTRY"):
            # Side lock check
            locked = self.index_side_lock.get(index)
            if locked and locked != side:
                self._log(f"🚫 ENTRY_BLOCKED | {tag} | reason=SIDE_LOCK({locked})")
                return {"entry_allowed": False}

            trade = momentum_engine.active_trade.get(secid)
            if not trade:
                return {"entry_allowed": False}

            ctx = self._ctx(secid, tag, trade.get("side", "LONG"), trade.get("entry", ltp), now)

            # 🔒 Lock side on first entry
            self.index_side_lock[index] = side

            # TREND entry
            if secid not in paper_trader.positions:
                paper_trader.on_entry(
                    secid=secid,
                    tag=tag,
                    side="LONG",
                    ltp=trade.get("entry", ltp),
                    lots=1,
                    reason=f"{signal}|STRUCT"
                )

                ctx["post_validate_until"] = now + self.POST_ENTRY_VALIDATE_MAX_SEC
                self.index_legs[index].add(secid)

                self._log(f"✅ ENTRY_COMMITTED | {tag} | mode={ctx['mode']} | side={side}")

            return {"entry_allowed": True}

        # ==================================================
        # 2️⃣ EXIT CONTROL — TREND PROTECTION
        # ==================================================
        if signal == "EXIT":
            ctx = self.trade_ctx.get(secid)
            if not ctx:
                return {"exit_allowed": True}

            reason = momentum_engine.last_exit_reason.get(secid, "EXIT")

            # TREND exit protection
            if ctx["mode"] == "TREND":
                if reason in ("ENTRY_REJECTED_CONFIRM", "ENTRY_INVALIDATED"):
                    self._log(f"🛑 EXIT_VETO | {tag} | TREND | reason={reason}")
                    return {"exit_allowed": False}

            # TRUE EXIT → unlock side
            self.index_side_lock.pop(ctx["index"], None)
            self.scalp_count.pop(ctx["index"], None)
            self.last_scalp_time.pop(ctx["index"], None)

            return {"exit_allowed": True}

        # ==================================================
        # 3️⃣ POST ENTRY MODE UPGRADE
        # ==================================================
        if secid not in paper_trader.positions:
            return

        ctx = self.trade_ctx.get(secid)
        if not ctx:
            return

        ctx["accept_count"] += 1
        if ctx["mode"] == "SCALP" and ctx["accept_count"] >= self.MODE_UPGRADE_CONFIRM_TICKS:
            ctx["mode"] = "TREND"
            self._log(f"🧭 MODE_UPGRADE | {tag} | SCALP → TREND")

        # ==================================================
        # 4️⃣ MULTI-SCALP (ONLY IF TREND IS GREEN)
        # ==================================================
        pnl = paper_trader.get_unrealized_pnl(secid, ltp)
        if ctx["mode"] == "TREND" and pnl > 0:
            if self.scalp_count[index] < self.MAX_SCALPS_PER_TREND:
                if now - self.last_scalp_time[index] >= self.SCALP_COOLDOWN_SEC:
                    if momentum_engine.is_fast_momentum(secid):
                        self.scalp_count[index] += 1
                        self.last_scalp_time[index] = now
                        self._log(f"⚡ SCALP_ALLOWED | {tag} | count={self.scalp_count[index]}")
                        return {"entry_allowed": True, "entry_mode": "SCALP"}

        return