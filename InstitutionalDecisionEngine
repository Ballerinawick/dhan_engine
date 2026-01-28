import time
from collections import defaultdict


class InstitutionalDecisionEngine:
    """
    INSTITUTIONAL DECISION ENGINE

    Governs:
    1️⃣ Leg dominance exit (fastest PnL improvement)
    2️⃣ Straddle → directional conversion
    3️⃣ Time-based risk governor
    4️⃣ Market-state awareness (compression / expansion / exhaustion)

    ⚠️ Does NOT create signals
    ⚠️ Does NOT touch broker APIs
    """

    # ---------------- CONFIG ----------------
    DOMINANCE_WINDOW_SEC = 8          # evaluate PnL slope
    DOMINANCE_RATIO = 1.8             # stronger leg must outperform
    MAX_HOLD_SEC = 180                # hard time stop
    COOLDOWN_SEC = 5                  # anti-flip

    COMPRESSION_PNL_RANGE = 0.15      # % of entry price
    EXHAUSTION_PROFIT_LOCK = 0.60     # lock profit if move done

    def __init__(self, debug=True):
        self.debug = debug

        # secid → trade meta
        self.trade_ctx = {}

        # index → active legs
        self.index_legs = defaultdict(set)

        # secid → pnl history
        self.pnl_track = defaultdict(list)

        self.last_action_ts = {}

    # --------------------------------------------------
    def _log(self, msg):
        if self.debug:
            print(msg)

    # --------------------------------------------------
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
        """
        Called on EVERY tick AFTER momentum_engine.on_tick()
        """

        now = time.time()
        index = tag.split("_")[0]

        # ----------------------------------
        # ENTRY GOVERNANCE
        # ----------------------------------
        if signal in ("A_ENTRY", "B_ENTRY"):
            if secid not in momentum_engine.active_trade:
                return

            trade = momentum_engine.active_trade[secid]

            self.trade_ctx[secid] = {
                "index": index,
                "side": trade["side"],
                "entry": trade["entry"],
                "ts": trade["ts"],
            }

            self.index_legs[index].add(secid)
            self.pnl_track[secid].clear()

            self._log(
                f"🏛️ ENTRY_ACCEPTED | {tag} | side={trade['side']} | entry={trade['entry']:.2f}"
            )
            return

        # ----------------------------------
        # TRACK ACTIVE TRADES
        # ----------------------------------
        if secid not in momentum_engine.active_trade:
            return

        trade = momentum_engine.active_trade[secid]
        ctx = self.trade_ctx.get(secid)
        if not ctx:
            return

        pnl = (ltp - ctx["entry"]) if trade["side"] == "LONG" else (ctx["entry"] - ltp)
        self.pnl_track[secid].append((now, pnl))

        # keep last N seconds
        self.pnl_track[secid] = [
            x for x in self.pnl_track[secid]
            if now - x[0] <= self.DOMINANCE_WINDOW_SEC
        ]

        # ----------------------------------
        # 1️⃣ LEG DOMINANCE EXIT
        # ----------------------------------
        legs = list(self.index_legs[index])
        if len(legs) == 2:
            slopes = {}
            for s in legs:
                pts = self.pnl_track.get(s, [])
                if len(pts) >= 2:
                    dt = pts[-1][0] - pts[0][0]
                    dp = pts[-1][1] - pts[0][1]
                    slopes[s] = dp / max(dt, 1e-6)

            if len(slopes) == 2:
                a, b = slopes.items()
                (s1, v1), (s2, v2) = a, b

                if abs(v1) > abs(v2) * self.DOMINANCE_RATIO:
                    loser = s2
                elif abs(v2) > abs(v1) * self.DOMINANCE_RATIO:
                    loser = s1
                else:
                    loser = None

                if loser:
                    if now - self.last_action_ts.get(loser, 0) > self.COOLDOWN_SEC:
                        self._log(
                            f"🏛️ LEG_DOMINANCE | {index} | EXIT weaker leg {loser}"
                        )
                        paper_trader.on_exit(loser, ltp, reason="LEG_DOMINANCE")
                        momentum_engine.active_trade.pop(loser, None)
                        self.index_legs[index].discard(loser)
                        self.last_action_ts[loser] = now
                        return

        # ----------------------------------
        # 2️⃣ STRADDLE → DIRECTIONAL
        # ----------------------------------
        if len(legs) == 2:
            pnls = [abs(pnl) for _, pnl in self.pnl_track[secid][-1:]]
            if pnls and pnls[0] < ctx["entry"] * self.COMPRESSION_PNL_RANGE:
                self._log(
                    f"🏛️ COMPRESSION | {index} | CE+PE probing"
                )
            else:
                self._log(
                    f"🏛️ EXPANSION | {index} | directional bias forming"
                )

        # ----------------------------------
        # 3️⃣ TIME-BASED RISK GOVERNOR
        # ----------------------------------
        age = now - ctx["ts"]
        if age >= self.MAX_HOLD_SEC:
            self._log(
                f"⏱️ TIME_EXIT | {tag} | age={int(age)}s"
            )
            paper_trader.on_exit(secid, ltp, reason="TIME_EXIT")
            momentum_engine.active_trade.pop(secid, None)
            self.index_legs[index].discard(secid)
            return

        # ----------------------------------
        # 4️⃣ EXHAUSTION / PROFIT PROTECT
        # ----------------------------------
        if pnl > ctx["entry"] * self.EXHAUSTION_PROFIT_LOCK:
            self._log(
                f"🏁 EXHAUSTION_EXIT | {tag} | pnl={pnl:.2f}"
            )
            paper_trader.on_exit(secid, ltp, reason="EXHAUSTION")
            momentum_engine.active_trade.pop(secid, None)
            self.index_legs[index].discard(secid)
            return