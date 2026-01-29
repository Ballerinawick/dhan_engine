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
    ACCELERATION_ENTRY_THRESHOLD = 0.01   # minimum accel for impulse candidate
    IMPULSE_CONFIRM_SEC = 3               # continuous accel duration to confirm impulse

    def __init__(self, debug=True):
        self.debug = debug

        # secid → trade meta
        self.trade_ctx = {}

        # index → active legs
        self.index_legs = defaultdict(set)

        # secid → pnl history
        self.pnl_track = defaultdict(list)
        # secid → slope history (rolling)
        self.slope_track = defaultdict(list)
        # secid → acceleration state
        self.accel_state = defaultdict(lambda: {
            "last_slope": 0.0,
            "current_slope": 0.0,
            "acceleration_score": 0.0,
            "last_slope_ts": 0.0,
            "peak_acceleration": 0.0,
            "last_acceleration": 0.0,
        })

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

    def _update_acceleration(self, secid: int, now: float) -> float:
        pts = self.pnl_track.get(secid, [])
        if len(pts) >= 2:
            dt = pts[-1][0] - pts[0][0]
            dp = pts[-1][1] - pts[0][1]
            current_slope = dp / max(dt, 1e-6)
        else:
            current_slope = 0.0

        state = self.accel_state[secid]
        last_slope = state.get("current_slope", 0.0)
        last_slope_ts = state.get("last_slope_ts", 0.0) or now
        dt_slope = max(now - last_slope_ts, 0.0)
        if dt_slope > 0:
            acceleration_score = (current_slope - last_slope) / max(dt_slope, 1e-6)
        else:
            acceleration_score = 0.0

        state["last_slope"] = last_slope
        state["current_slope"] = current_slope
        state["acceleration_score"] = acceleration_score
        state["last_slope_ts"] = now
        state["last_acceleration"] = acceleration_score
        if acceleration_score > state.get("peak_acceleration", 0.0):
            state["peak_acceleration"] = acceleration_score

        self.slope_track[secid].append((now, current_slope))
        self.slope_track[secid] = [
            x for x in self.slope_track[secid]
            if now - x[0] <= self.DOMINANCE_WINDOW_SEC
        ]

        return acceleration_score

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
            probe = self.probe_state[index]
            legs_count = len(self.index_legs.get(index, set()))
            active_trade_present = secid in momentum_engine.active_trade
            in_cooldown = False
            displaced = False
            accel_state = self.accel_state.get(secid, {})
            acceleration_score = accel_state.get("acceleration_score", 0.0)
            if legs_count == 0:
                in_cooldown = (
                    probe["last_exit_ts"] > 0
                    and now - probe["last_exit_ts"] < self.PROBE_COOLDOWN_SEC
                )
                displaced = (
                    probe["last_entry_price"] > 0
                    and abs(ltp - probe["last_entry_price"]) >= probe["last_entry_price"] * self.COMPRESSION_PNL_RANGE
                )

            decision = "ACCEPT_ENTRY"
            reason = None
            if not active_trade_present:
                decision = "CANCEL_ENTRY"
                reason = "NO_ACTIVE_TRADE"
            else:
                allow_fresh_probe = (
                    not probe["active"]
                    and legs_count == 0
                    and probe["last_entry_price"] == 0
                )
                if legs_count == 0 and not allow_fresh_probe:
                    if in_cooldown:
                        decision = "CANCEL_ENTRY"
                        reason = "PROBE_COOLDOWN"
                    elif not probe["dominance_resolved"] and not displaced:
                        decision = "CANCEL_ENTRY"
                        reason = "PROBE_NOT_DISPLACED"

            self._log(
                "🧭 ENTRY_TRACE | "
                f"secid={secid} | tag={tag} | signal={signal} | ltp={ltp:.2f} | "
                f"active_trade={active_trade_present} | legs={legs_count} | "
                f"probe.active={probe['active']} | probe.last_exit_ts={probe['last_exit_ts']:.2f} | "
                f"probe.last_entry_price={probe['last_entry_price']:.2f} | "
                f"probe.dominance_resolved={probe['dominance_resolved']} | "
                f"in_cooldown={in_cooldown} | displaced={displaced} | "
                f"decision={decision}{f' | reason={reason}' if reason else ''}"
            )

            if decision == "CANCEL_ENTRY":
                self._log(
                    f"🏛️ ENTRY_CANCELLED | {tag} | reason={reason}"
                )
                momentum_engine.active_trade.pop(secid, None)
                return

            trade = momentum_engine.active_trade[secid]
            impulse_candidate = acceleration_score > self.ACCELERATION_ENTRY_THRESHOLD

            self.trade_ctx[secid] = {
                "index": index,
                "side": trade["side"],
                "entry": trade["entry"],
                "ts": trade["ts"],
                "last_ltp": ltp,
                "trade_phase": "PROBE",
                "impulse_candidate": impulse_candidate,
                "impulse_confirm_start": None,
                "accel_start_ts": None,
            }

            self.index_legs[index].add(secid)
            self.pnl_track[secid].clear()
            self.slope_track[secid].clear()

            # Start/refresh probe tracking for the index once a new probe is accepted.
            # WHY: ensures single active probe cycle per index with cooldown.
            if not probe["active"]:
                probe["active"] = True
                probe["last_entry_price"] = trade["entry"]
                probe["dominance_resolved"] = False

            self._log(
                f"🏛️ ENTRY_ACCEPTED | {tag} | side={trade['side']} | entry={trade['entry']:.2f} | "
                f"trade_phase=PROBE | impulse_candidate={impulse_candidate} | accel={acceleration_score:.4f}"
            )
            if secid not in paper_trader.positions:
                paper_trader.on_entry(
                    secid=secid,
                    tag=tag,
                    side=trade["side"],
                    ltp=trade["entry"],
                    lots=1,
                    reason=signal
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
                self.slope_track.pop(secid, None)
                self.accel_state.pop(secid, None)
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
        prev_accel = self.accel_state[secid].get("last_acceleration", 0.0)
        acceleration_score = self._update_acceleration(secid, now)
        current_slope = self.accel_state[secid]["current_slope"]
        peak_acceleration = self.accel_state[secid]["peak_acceleration"]

        if prev_accel >= 0 and acceleration_score < 0:
            self._log(
                f"📉 ACCELERATION_DECAY | secid={secid} | accel={acceleration_score:.4f}"
            )

        if ctx["trade_phase"] == "PROBE" and ctx.get("impulse_candidate"):
            if acceleration_score > 0:
                if ctx["accel_start_ts"] is None:
                    ctx["accel_start_ts"] = now
                if ctx["impulse_confirm_start"] is None:
                    ctx["impulse_confirm_start"] = ctx["accel_start_ts"]
                if now - ctx["accel_start_ts"] >= self.IMPULSE_CONFIRM_SEC:
                    ctx["trade_phase"] = "IMPULSE"
                    self._log(
                        f"🏁 IMPULSE_CONFIRMED | secid={secid} | accel={acceleration_score:.4f} | "
                        f"duration={now - ctx['accel_start_ts']:.2f}s"
                    )
            else:
                ctx["accel_start_ts"] = None
                ctx["impulse_confirm_start"] = None

        # ----------------------------------
        # 1️⃣ LEG DOMINANCE EXIT
        # ----------------------------------
        legs = list(self.index_legs[index])
        if len(legs) != 2:
            self.last_market_state.pop(index, None)
        if len(legs) == 2:
            dominance_allowed = True
            for s in legs:
                leg_ctx = self.trade_ctx.get(s, {})
                if leg_ctx.get("trade_phase") != "PROBE" or leg_ctx.get("impulse_candidate"):
                    dominance_allowed = False
                    break
        if len(legs) == 2 and dominance_allowed:
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
                    loser_ctx = self.trade_ctx.get(loser, {})
                    loser_phase = loser_ctx.get("trade_phase", "PROBE")
                    loser_accel = self.accel_state.get(loser, {}).get("acceleration_score", 0.0)
                    if loser_phase == "IMPULSE" and loser_accel >= 0:
                        self._log(
                            f"🏛️ IMPULSE_HOLD | {index} | skip LEG_DOMINANCE | "
                            f"secid={loser} | accel={loser_accel:.4f}"
                        )
                        self._log(
                            "🧯 IMPULSE_EXIT_BLOCKED | reason=ACCELERATION_ACTIVE"
                        )
                        return
                    if now - self.last_action_ts.get(loser, 0) > self.COOLDOWN_SEC:
                        self._log(
                            f"🏛️ LEG_DOMINANCE | {index} | EXIT weaker leg {loser}"
                        )
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
            if ctx["trade_phase"] == "IMPULSE" and acceleration_score >= 0:
                self._log(
                    f"🏛️ IMPULSE_HOLD | {tag} | skip TIME_EXIT | accel={acceleration_score:.4f}"
                )
                self._log(
                    "🧯 IMPULSE_EXIT_BLOCKED | reason=ACCELERATION_ACTIVE"
                )
                return
            self._log(
                f"⏱️ TIME_EXIT | {tag} | age={int(age)}s"
            )
            self._log(f"🧾 EXIT_ATTRIBUTION | TIME_EXIT | {tag}")
            exit_ltp = ctx.get("last_ltp", ltp)
            paper_trader.on_exit(secid, exit_ltp, reason="TIME_EXIT")
            momentum_engine.active_trade.pop(secid, None)
            self.index_legs[index].discard(secid)
            return

        # ----------------------------------
        # 4️⃣ EXHAUSTION / PROFIT PROTECT
        # ----------------------------------
        if (
            pnl > ctx["entry"] * self.EXHAUSTION_PROFIT_LOCK
            and peak_acceleration > 0
            and acceleration_score < 0
        ):
            ctx["trade_phase"] = "EXHAUSTION"
            self._log(
                f"🏁 EXHAUSTION_EXIT | {tag} | pnl={pnl:.2f} | accel={acceleration_score:.4f}"
            )
            self._log(f"🧾 EXIT_ATTRIBUTION | TREND_HOLD | {tag}")
            exit_ltp = ctx.get("last_ltp", ltp)
            paper_trader.on_exit(secid, exit_ltp, reason="EXHAUSTION")
            momentum_engine.active_trade.pop(secid, None)
            self.index_legs[index].discard(secid)
            return
