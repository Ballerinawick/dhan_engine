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
    PROBE_COOLDOWN_SEC = 12          # per-index probe cooldown to prevent rapid re-entries

    def __init__(self, debug=True):
        self.debug = debug

        # secid → trade meta
        self.trade_ctx = {}

        # index → active legs
        self.index_legs = defaultdict(set)

        # secid → pnl history
        self.pnl_track = defaultdict(list)

        self.last_action_ts = {}
        self.last_market_state = {}
        # per-index probe governance state (cooldown + displacement guard)
        self.probe_state = defaultdict(lambda: {
            "active": False,
            "last_exit_ts": 0.0,
            "last_entry_price": 0.0,
            "dominance_resolved": False,
        })

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

            # Guard against repeated probe re-entry during compression for the same index.
            # WHY: prevents rapid CE/PE probe churn when price is still compressed.
            probe = self.probe_state[index]
            if not self.index_legs.get(index):
                in_cooldown = now - probe["last_exit_ts"] < self.PROBE_COOLDOWN_SEC
                displaced = (
                    probe["last_entry_price"] > 0
                    and abs(ltp - probe["last_entry_price"]) >= probe["last_entry_price"] * self.COMPRESSION_PNL_RANGE
                )
                if in_cooldown or (not probe["dominance_resolved"] and not displaced):
                    momentum_engine.active_trade.pop(secid, None)
                    return

            self.trade_ctx[secid] = {
                "index": index,
                "side": trade["side"],
                "entry": trade["entry"],
                "ts": trade["ts"],
                "last_ltp": ltp,
            }

            self.index_legs[index].add(secid)
            self.pnl_track[secid].clear()

            # Start/refresh probe tracking for the index once a new probe is accepted.
            # WHY: ensures single active probe cycle per index with cooldown.
            if not probe["active"]:
                probe["active"] = True
                probe["last_entry_price"] = trade["entry"]
                probe["dominance_resolved"] = False

            self._log(
                f"🏛️ ENTRY_ACCEPTED | {tag} | side={trade['side']} | entry={trade['entry']:.2f}"
            )
            return

        if signal == "EXIT":
            ctx = self.trade_ctx.pop(secid, None)
            if ctx:
                index = ctx["index"]
                self.index_legs[index].discard(secid)
                if not self.index_legs[index]:
                    self.index_legs.pop(index, None)
                    self.last_market_state.pop(index, None)
                    probe = self.probe_state[index]
                    probe["active"] = False
                    probe["last_exit_ts"] = now
                self.pnl_track.pop(secid, None)
                self.last_action_ts.pop(secid, None)
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
        # Track last known LTP per leg for precise dominance exits.
        # WHY: ensure LEG_DOMINANCE uses the leg's own last price, not shared tick LTP.
        ctx["last_ltp"] = ltp
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
        if len(legs) != 2:
            self.last_market_state.pop(index, None)
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
                        loser_ctx = self.trade_ctx.get(loser, {})
                        loser_ltp = loser_ctx.get("last_ltp", ltp)
                        paper_trader.on_exit(loser, loser_ltp, reason="LEG_DOMINANCE")
                        self._log(f"🧾 EXIT_ATTRIBUTION | PROBE_LOSS | {index} | secid={loser}")
                        momentum_engine.active_trade.pop(loser, None)
                        self.index_legs[index].discard(loser)
                        self.last_action_ts[loser] = now
                        self.probe_state[index]["dominance_resolved"] = True
                        return

        # ----------------------------------
        # 2️⃣ STRADDLE → DIRECTIONAL
        # ----------------------------------
        if len(legs) == 2:
            pnls = [abs(pnl) for _, pnl in self.pnl_track[secid][-1:]]
            if pnls and pnls[0] < ctx["entry"] * self.COMPRESSION_PNL_RANGE:
                state = "COMPRESSION"
                msg = f"🏛️ COMPRESSION | {index} | CE+PE probing"
            else:
                state = "EXPANSION"
                msg = f"🏛️ EXPANSION | {index} | directional bias forming"
            if self.last_market_state.get(index) != state:
                self._log(msg)
                self.last_market_state[index] = state

        # ----------------------------------
        # 3️⃣ TIME-BASED RISK GOVERNOR
        # ----------------------------------
        age = now - ctx["ts"]
        if age >= self.MAX_HOLD_SEC:
            self._log(
                f"⏱️ TIME_EXIT | {tag} | age={int(age)}s"
            )
            self._log(f"🧾 EXIT_ATTRIBUTION | TIME_EXIT | {tag}")
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
            self._log(f"🧾 EXIT_ATTRIBUTION | TREND_HOLD | {tag}")
            paper_trader.on_exit(secid, ltp, reason="EXHAUSTION")
            momentum_engine.active_trade.pop(secid, None)
            self.index_legs[index].discard(secid)
            return
