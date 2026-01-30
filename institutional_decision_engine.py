import time
from collections import defaultdict, deque


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
    HIGH_ACCEL_THRESHOLD = 0.02           # high peak accel to classify trend regime
    IMPULSE_CONFIRM_SEC = 3               # continuous accel duration to confirm impulse
    DECAY_TICKS = 3                       # consecutive negative accel ticks to allow exit
    MIN_SPEED_THRESHOLD_PCT = 0.02        # speed near zero threshold (% of entry)
    EXTENSION_FAIL_PNL_PCT = 0.15         # pnl growth threshold to confirm stall
    DISPLACEMENT_THRESHOLD_PCT = 0.15     # displacement needed for trend regime
    REGIME_DECISION_MIN_SEC = 10          # earliest regime decision window
    REGIME_DECISION_MAX_SEC = 25          # latest regime decision window
    STRUCTURE_LOOKBACK_SEC = 45           # price structure window
    STRUCTURE_MIN_POINTS = 6              # minimum points for structure gating
    STRUCTURE_NEAR_EXTREME_PCT = 0.05     # proximity to swing extreme
    STRUCTURE_SWING_MARGIN_PCT = 0.01     # HL/LH margin filter
    STRUCTURE_COMPRESSION_PCT = 0.12      # range compression threshold
    TURN_FAIL_BARS = 6                    # bars without continuation
    TURN_FAIL_SEC = 8                     # seconds without continuation
    TURN_ACCEL_CONFIRM_SEC = 3            # accel flip confirmation window
    TURN_DISPLACEMENT_PCT = 0.12          # displacement to confirm turn
    TURN_STRICT_MULTIPLIER = 1.35         # tighten turn confirmation under churn

    def __init__(self, debug=True):
        self.debug = debug
        self.debug_metrics = False

        # secid → trade meta
        self.trade_ctx = {}

        # index → active legs
        self.index_legs = defaultdict(set)

        # secid → pnl history
        self.pnl_track = defaultdict(list)
        # secid → slope history (rolling)
        self.slope_track = defaultdict(list)
        # secid → speed history (rolling)
        self.speed_track = defaultdict(list)
        # secid → acceleration state
        self.accel_state = defaultdict(lambda: {
            "last_slope": 0.0,
            "current_slope": 0.0,
            "acceleration_score": 0.0,
            "last_slope_ts": 0.0,
            "peak_acceleration": 0.0,
            "last_acceleration": 0.0,
            "negative_ticks": 0,
            "time_above_zero_accel": 0.0,
            "last_accel_ts": 0.0,
        })

        self.last_action_ts = {}
        self.last_market_state = {}
        self.price_track = defaultdict(deque)
        self.last_turn_log = {}
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

    def _update_price_history(self, secid: int, now: float, ltp: float) -> None:
        track = self.price_track[secid]
        track.append((now, ltp))
        while track and now - track[0][0] > self.STRUCTURE_LOOKBACK_SEC:
            track.popleft()

    def _extract_swings(self, prices):
        swings = []
        for i in range(1, len(prices) - 1):
            _, prev_p = prices[i - 1]
            ts, cur_p = prices[i]
            _, next_p = prices[i + 1]
            if cur_p > prev_p and cur_p > next_p:
                swings.append({"ts": ts, "price": cur_p, "type": "HIGH"})
            elif cur_p < prev_p and cur_p < next_p:
                swings.append({"ts": ts, "price": cur_p, "type": "LOW"})
        return swings

    def _structure_snapshot(self, secid: int):
        prices = list(self.price_track.get(secid, []))
        if len(prices) < self.STRUCTURE_MIN_POINTS:
            return None
        values = [p for _, p in prices]
        high = max(values)
        low = min(values)
        last_price = values[-1]
        price_range = high - low
        compression = price_range <= max(last_price, 1e-6) * self.STRUCTURE_COMPRESSION_PCT
        swings = self._extract_swings(prices)
        highs = [s for s in swings if s["type"] == "HIGH"]
        lows = [s for s in swings if s["type"] == "LOW"]
        last_high = highs[-1] if highs else None
        prev_high = highs[-2] if len(highs) > 1 else None
        last_low = lows[-1] if lows else None
        prev_low = lows[-2] if len(lows) > 1 else None
        return {
            "high": high,
            "low": low,
            "range": price_range,
            "compression": compression,
            "last_high": last_high,
            "prev_high": prev_high,
            "last_low": last_low,
            "prev_low": prev_low,
            "last_price": last_price,
        }

    def _churn_pressure(self, paper_trader) -> bool:
        entries = getattr(paper_trader, "entries_total", 0)
        exits = getattr(paper_trader, "exits_total", 0)
        if entries <= 0:
            return False
        churn_ratio = exits / max(entries, 1)
        return churn_ratio > 0.85

    def _entry_structure_ok(self, side: str, ltp: float, secid: int, churn_tighten: bool):
        struct = self._structure_snapshot(secid)
        if not struct:
            return False, "NO_STRUCTURE_DATA"

        price_range = struct["range"]
        compression = struct["compression"]
        last_high = struct["last_high"]
        prev_high = struct["prev_high"]
        last_low = struct["last_low"]
        prev_low = struct["prev_low"]

        if side == "LONG":
            near_low = bool(last_low) and ltp <= last_low["price"] * (1 + self.STRUCTURE_NEAR_EXTREME_PCT)
            higher_low = bool(last_low and prev_low) and last_low["price"] >= prev_low["price"] * (1 + self.STRUCTURE_SWING_MARGIN_PCT)
            failed_cont = (
                bool(last_low and prev_low)
                and last_low["price"] < prev_low["price"]
                and ltp > last_low["price"] + price_range * 0.4
            )
            if churn_tighten:
                ok = near_low and (higher_low or failed_cont)
            else:
                ok = near_low and (higher_low or failed_cont or compression)
            reason = "HL_OR_FAIL" if (higher_low or failed_cont) else "COMPRESSION"
        else:
            near_high = bool(last_high) and ltp >= last_high["price"] * (1 - self.STRUCTURE_NEAR_EXTREME_PCT)
            lower_high = bool(last_high and prev_high) and last_high["price"] <= prev_high["price"] * (1 - self.STRUCTURE_SWING_MARGIN_PCT)
            failed_cont = (
                bool(last_high and prev_high)
                and last_high["price"] > prev_high["price"]
                and ltp < last_high["price"] - price_range * 0.4
            )
            if churn_tighten:
                ok = near_high and (lower_high or failed_cont)
            else:
                ok = near_high and (lower_high or failed_cont or compression)
            reason = "LH_OR_FAIL" if (lower_high or failed_cont) else "COMPRESSION"

        if not ok:
            return False, "MID_IMPULSE"
        return True, reason

    def _structure_state(self, ctx, struct):
        if not struct:
            return "UNKNOWN"
        displacement = abs(struct["last_price"] - ctx["entry"])
        if struct["compression"]:
            return "SIDEWAYS"
        if displacement >= ctx["entry"] * self.DISPLACEMENT_THRESHOLD_PCT:
            return "TREND"
        return "NEUTRAL"

    def _turn_confirmed(self, ctx, side: str, ltp: float, now: float, acceleration_score: float, churn_tighten: bool):
        struct = self._structure_snapshot(ctx["secid"])
        if not struct:
            return False, "NO_STRUCTURE_DATA"

        last_high = struct["last_high"]
        last_low = struct["last_low"]
        fail_sec = self.TURN_FAIL_SEC
        accel_confirm = self.TURN_ACCEL_CONFIRM_SEC
        displacement_pct = self.TURN_DISPLACEMENT_PCT
        if churn_tighten:
            fail_sec *= self.TURN_STRICT_MULTIPLIER
            accel_confirm *= self.TURN_STRICT_MULTIPLIER
            displacement_pct *= self.TURN_STRICT_MULTIPLIER

        if side == "LONG":
            if ltp > ctx.get("trend_extreme", ctx["entry"]):
                ctx["trend_extreme"] = ltp
                ctx["last_trend_extreme_ts"] = now
                ctx["no_progress_ticks"] = 0
            else:
                ctx["no_progress_ticks"] = ctx.get("no_progress_ticks", 0) + 1
            swing_violation = bool(last_low) and ltp < last_low["price"]
            fail_to_continue = (
                now - ctx.get("last_trend_extreme_ts", ctx["ts"]) >= fail_sec
                or ctx.get("no_progress_ticks", 0) >= self.TURN_FAIL_BARS
            )
            opp_displacement = bool(last_low) and abs(ltp - last_low["price"]) >= ctx["entry"] * displacement_pct
            accel_opposite = acceleration_score < 0
        else:
            if ltp < ctx.get("trend_extreme", ctx["entry"]):
                ctx["trend_extreme"] = ltp
                ctx["last_trend_extreme_ts"] = now
                ctx["no_progress_ticks"] = 0
            else:
                ctx["no_progress_ticks"] = ctx.get("no_progress_ticks", 0) + 1
            swing_violation = bool(last_high) and ltp > last_high["price"]
            fail_to_continue = (
                now - ctx.get("last_trend_extreme_ts", ctx["ts"]) >= fail_sec
                or ctx.get("no_progress_ticks", 0) >= self.TURN_FAIL_BARS
            )
            opp_displacement = bool(last_high) and abs(ltp - last_high["price"]) >= ctx["entry"] * displacement_pct
            accel_opposite = acceleration_score > 0

        if accel_opposite:
            if ctx.get("accel_flip_ts") is None:
                ctx["accel_flip_ts"] = now
        else:
            ctx["accel_flip_ts"] = None

        accel_confirmed = False
        if ctx.get("accel_flip_ts") is not None:
            accel_confirmed = now - ctx["accel_flip_ts"] >= accel_confirm

        turn = swing_violation and fail_to_continue and opp_displacement and accel_confirmed
        reason = (
            f"violation={swing_violation} | stall={fail_to_continue} | "
            f"displacement={opp_displacement} | accel_flip={accel_confirmed}"
        )
        return turn, reason

    def _time_exit_allowed(self, ctx, struct_state, speed_near_zero):
        if struct_state != "SIDEWAYS":
            return False
        if not speed_near_zero:
            return False
        return True

    def _hold_limits(self, struct_state, churn_tighten):
        if struct_state == "SIDEWAYS":
            min_hold, max_hold = 30, 60
        elif struct_state == "TREND":
            min_hold, max_hold = 90, 180
        else:
            min_hold, max_hold = 60, 120
        if churn_tighten:
            min_hold *= 1.1
            max_hold *= 1.2
        return min_hold, max_hold

    def _finalize_exit(self, secid: int, index: str, now: float) -> None:
        self.trade_ctx.pop(secid, None)
        self.index_legs[index].discard(secid)
        if not self.index_legs[index]:
            self.index_legs.pop(index, None)
            self.last_market_state.pop(index, None)
            probe = self.probe_state[index]
            probe["active"] = False
            probe["last_exit_ts"] = now
        self.pnl_track.pop(secid, None)
        self.slope_track.pop(secid, None)
        self.speed_track.pop(secid, None)
        self.accel_state.pop(secid, None)
        self.last_action_ts.pop(secid, None)
        self.price_track.pop(secid, None)
        self.last_turn_log.pop(secid, None)

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
    def _update_speed(self, secid: int, now: float) -> float:
        pts = self.pnl_track.get(secid, [])
        if len(pts) >= 2:
            prev_ts, prev_pnl = pts[-2]
            dt = now - prev_ts
            dp = pts[-1][1] - prev_pnl
            speed = dp / max(dt, 1e-6)
        else:
            speed = 0.0

        self.speed_track[secid].append((now, speed))
        self.speed_track[secid] = [
            x for x in self.speed_track[secid]
            if now - x[0] <= self.DOMINANCE_WINDOW_SEC
        ]

        return speed

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
        self._update_price_history(secid, now, ltp)
        churn_tighten = self._churn_pressure(paper_trader)

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

            if decision == "CANCEL_ENTRY":
                momentum_engine.active_trade.pop(secid, None)
                return {"entry_allowed": False}

            trade = momentum_engine.active_trade[secid]
            structure_ok, structure_reason = self._entry_structure_ok(
                trade["side"], ltp, secid, churn_tighten
            )
            if not structure_ok:
                momentum_engine.active_trade.pop(secid, None)
                return {"entry_allowed": False}
            impulse_candidate = acceleration_score > self.ACCELERATION_ENTRY_THRESHOLD

            self.trade_ctx[secid] = {
                "index": index,
                "side": trade["side"],
                "secid": secid,
                "type": trade.get("type", "UNKNOWN"),
                "entry": trade["entry"],
                "ts": trade["ts"],
                "last_ltp": ltp,
                "trade_phase": "PROBE",
                "impulse_candidate": impulse_candidate,
                "impulse_confirm_start": None,
                "accel_start_ts": None,
                "trade_mode": None,
                "regime_decision_ts": None,
                "speed_near_zero_seen": False,
                "trend_extreme": trade["entry"],
                "last_trend_extreme_ts": trade["ts"],
                "no_progress_ticks": 0,
                "accel_flip_ts": None,
            }

            self.index_legs[index].add(secid)
            self.pnl_track[secid].clear()
            self.slope_track[secid].clear()
            self.speed_track[secid].clear()

            # Start/refresh probe tracking for the index once a new probe is accepted.
            # WHY: ensures single active probe cycle per index with cooldown.
            if not probe["active"]:
                probe["active"] = True
                probe["last_entry_price"] = trade["entry"]
                probe["dominance_resolved"] = False

            self._log(
                f"✅ ENTRY_COMMITTED | {tag} | side={trade['side']} | entry={trade['entry']:.2f} | "
                f"struct={structure_reason} | churn_tight={churn_tighten}"
            )
            if secid not in paper_trader.positions:
                paper_trader.on_entry(
                    secid=secid,
                    tag=tag,
                    side=trade["side"],
                    ltp=trade["entry"],
                    lots=1,
                    reason=f"STRUCT_{structure_reason}"
                )
            return {"entry_allowed": True}

        if signal == "EXIT":
            ctx = self.trade_ctx.get(secid)
            if not ctx:
                return {"exit_allowed": True}
            turn_confirmed, reason = self._turn_confirmed(
                ctx,
                ctx["side"],
                ltp,
                now,
                self.accel_state.get(secid, {}).get("acceleration_score", 0.0),
                churn_tighten,
            )
            if turn_confirmed and not self.last_turn_log.get(secid):
                self._log(f"🔁 TURN_CONFIRMED | {tag} | {reason}")
                self.last_turn_log[secid] = True
            if not turn_confirmed:
                momentum_engine.active_trade[secid] = {
                    "type": ctx.get("type", "STEER"),
                    "side": ctx["side"],
                    "entry": ctx["entry"],
                    "ts": ctx["ts"],
                }
                return {"exit_allowed": False}
            ctx = self.trade_ctx.get(secid)
            if ctx:
                self._finalize_exit(secid, ctx["index"], now)
            return {"exit_allowed": True, "exit_reason": "STRUCTURAL_TURN"}

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
        speed = self._update_speed(secid, now)
        acceleration_score = self._update_acceleration(secid, now)
        peak_acceleration = self.accel_state[secid]["peak_acceleration"]
        accel_state = self.accel_state[secid]
        last_accel_ts = accel_state.get("last_accel_ts", now)
        accel_dt = max(now - last_accel_ts, 0.0)
        if acceleration_score > 0:
            accel_state["time_above_zero_accel"] += accel_dt
        accel_state["last_accel_ts"] = now
        if acceleration_score < 0:
            self.accel_state[secid]["negative_ticks"] += 1
        else:
            self.accel_state[secid]["negative_ticks"] = 0

        min_speed_threshold = ctx["entry"] * self.MIN_SPEED_THRESHOLD_PCT
        speed_near_zero = abs(speed) <= min_speed_threshold
        if speed_near_zero:
            ctx["speed_near_zero_seen"] = True

        if ctx["trade_phase"] == "PROBE" and ctx.get("impulse_candidate"):
            if acceleration_score > 0:
                if ctx["accel_start_ts"] is None:
                    ctx["accel_start_ts"] = now
                if ctx["impulse_confirm_start"] is None:
                    ctx["impulse_confirm_start"] = ctx["accel_start_ts"]
                if now - ctx["accel_start_ts"] >= self.IMPULSE_CONFIRM_SEC:
                    ctx["trade_phase"] = "IMPULSE"
                    pass
            else:
                ctx["accel_start_ts"] = None
                ctx["impulse_confirm_start"] = None

        # ----------------------------------
        # REGIME DECISION (SCALP vs TREND)
        # ----------------------------------
        if ctx.get("trade_mode") is None:
            age_since_entry = now - ctx["ts"]
            within_window = (
                self.REGIME_DECISION_MIN_SEC
                <= age_since_entry
                <= self.REGIME_DECISION_MAX_SEC
            )
            decision_timeout = age_since_entry > self.REGIME_DECISION_MAX_SEC
            if within_window or decision_timeout:
                accel_positive_duration = 0.0
                if acceleration_score > 0 and ctx.get("accel_start_ts") is not None:
                    accel_positive_duration = now - ctx["accel_start_ts"]
                displacement = abs(ltp - ctx["entry"])
                displacement_threshold = ctx["entry"] * self.DISPLACEMENT_THRESHOLD_PCT
                regime_trend = (
                    peak_acceleration >= self.HIGH_ACCEL_THRESHOLD
                    and accel_positive_duration >= self.IMPULSE_CONFIRM_SEC
                    and displacement >= displacement_threshold
                    and not ctx.get("speed_near_zero_seen", False)
                )
                if regime_trend:
                    ctx["trade_mode"] = "TREND"
                    reason = "HIGH_PEAK_ACCEL"
                else:
                    ctx["trade_mode"] = "SCALP"
                    if peak_acceleration < self.HIGH_ACCEL_THRESHOLD:
                        reason = "LOW_ACCEL"
                    elif accel_positive_duration < self.IMPULSE_CONFIRM_SEC:
                        reason = "NO_IMPULSE_CONFIRM"
                    elif displacement < displacement_threshold:
                        reason = "LOW_DISPLACEMENT"
                    elif ctx.get("speed_near_zero_seen", False):
                        reason = "SPEED_REVERT"
                    else:
                        reason = "LOW_ACCEL"
                ctx["regime_decision_ts"] = now
                pass
                if hasattr(paper_trader, "note_regime_change"):
                    paper_trader.note_regime_change(
                        secid=secid,
                        tag=tag,
                        mode=ctx["trade_mode"],
                        reason=reason,
                    )

        # ----------------------------------
        # 0️⃣ STRUCTURAL TURN EXIT (HIGHEST PRIORITY)
        # ----------------------------------
        turn_confirmed, turn_reason = self._turn_confirmed(
            ctx, ctx["side"], ltp, now, acceleration_score, churn_tighten
        )
        if turn_confirmed:
            if not self.last_turn_log.get(secid):
                self._log(f"🔁 TURN_CONFIRMED | {tag} | {turn_reason}")
                self.last_turn_log[secid] = True
            self._log(f"🚪 EXIT_TURN | {tag} | reason=STRUCTURAL_TURN")
            exit_ltp = ctx.get("last_ltp", ltp)
            paper_trader.on_exit(secid, exit_ltp, reason="STRUCTURAL_TURN")
            momentum_engine.active_trade.pop(secid, None)
            self._finalize_exit(secid, index, now)
            return

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
                    loser_mode = loser_ctx.get("trade_mode")
                    if loser_mode == "TREND" and loser_accel >= 0:
                        return
                    if loser_phase == "IMPULSE" and loser_accel >= 0:
                        return
                    turn_confirmed, reason = self._turn_confirmed(
                        loser_ctx,
                        loser_ctx.get("side", "LONG"),
                        loser_ctx.get("last_ltp", ltp),
                        now,
                        loser_accel,
                        churn_tighten,
                    )
                    if turn_confirmed and not self.last_turn_log.get(loser):
                        self._log(f"🔁 TURN_CONFIRMED | {index} | {reason}")
                        self.last_turn_log[loser] = True
                    if not turn_confirmed:
                        return
                    if now - self.last_action_ts.get(loser, 0) > self.COOLDOWN_SEC:
                        self._log(f"🚪 EXIT_TURN | {index} | reason=STRUCTURAL_TURN")
                        loser_ltp = loser_ctx.get("last_ltp", ltp)
                        paper_trader.on_exit(loser, loser_ltp, reason="STRUCTURAL_TURN")
                        momentum_engine.active_trade.pop(loser, None)
                        self._finalize_exit(loser, index, now)
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
            else:
                state = "EXPANSION"
            if self.last_market_state.get(index) != state:
                self.last_market_state[index] = state

        # ----------------------------------
        # 3️⃣ TIME-BASED RISK GOVERNOR
        # ----------------------------------
        age = now - ctx["ts"]
        struct = self._structure_snapshot(secid)
        struct_state = self._structure_state(ctx, struct)
        min_hold, max_hold = self._hold_limits(struct_state, churn_tighten)
        if age < min_hold:
            return
        if age >= max_hold and self._time_exit_allowed(ctx, struct_state, speed_near_zero):
            self._log(
                f"⏱️ EXIT_TIME | {tag} | age={int(age)}s | state={struct_state}"
            )
            exit_ltp = ctx.get("last_ltp", ltp)
            paper_trader.on_exit(secid, exit_ltp, reason="TIME_EXIT")
            momentum_engine.active_trade.pop(secid, None)
            self._finalize_exit(secid, index, now)
            return

        # ----------------------------------
        # 4️⃣ EXHAUSTION / PROFIT PROTECT
        # ----------------------------------
        recent_speeds = [
            v for _, v in self.speed_track.get(secid, [])[-self.DECAY_TICKS:]
        ]
        speed_declining = (
            len(recent_speeds) >= self.DECAY_TICKS
            and all(
                recent_speeds[i] >= recent_speeds[i + 1]
                for i in range(len(recent_speeds) - 1)
            )
        )
        if len(self.pnl_track[secid]) >= 2:
            window_growth = pnl - self.pnl_track[secid][0][1]
        else:
            window_growth = 0.0
        extension_failed = window_growth < ctx["entry"] * self.EXTENSION_FAIL_PNL_PCT
        accel_exit_gate = (
            self.accel_state[secid]["negative_ticks"] >= self.DECAY_TICKS
            and (speed_near_zero or speed_declining)
            and extension_failed
        )
        trend_exit_gate = (
            self.accel_state[secid]["negative_ticks"] >= self.DECAY_TICKS
            and (speed_near_zero or speed_declining or extension_failed)
        )
        if ctx.get("trade_mode") == "TREND" and trend_exit_gate:
            ctx["trade_phase"] = "EXHAUSTION"
            turn_confirmed, reason = self._turn_confirmed(
                ctx, ctx["side"], ltp, now, acceleration_score, churn_tighten
            )
            if turn_confirmed and not self.last_turn_log.get(secid):
                self._log(f"🔁 TURN_CONFIRMED | {tag} | {reason}")
                self.last_turn_log[secid] = True
            if turn_confirmed:
                self._log(f"🚪 EXIT_TURN | {tag} | reason=STRUCTURAL_TURN")
                exit_ltp = ctx.get("last_ltp", ltp)
                paper_trader.on_exit(secid, exit_ltp, reason="STRUCTURAL_TURN")
                momentum_engine.active_trade.pop(secid, None)
                self._finalize_exit(secid, index, now)
                return
        if (
            pnl > ctx["entry"] * self.EXHAUSTION_PROFIT_LOCK
            and peak_acceleration > 0
            and accel_exit_gate
        ):
            ctx["trade_phase"] = "EXHAUSTION"
            turn_confirmed, reason = self._turn_confirmed(
                ctx, ctx["side"], ltp, now, acceleration_score, churn_tighten
            )
            if turn_confirmed and not self.last_turn_log.get(secid):
                self._log(f"🔁 TURN_CONFIRMED | {tag} | {reason}")
                self.last_turn_log[secid] = True
            if turn_confirmed:
                self._log(f"🚪 EXIT_TURN | {tag} | reason=STRUCTURAL_TURN")
                exit_ltp = ctx.get("last_ltp", ltp)
                paper_trader.on_exit(secid, exit_ltp, reason="STRUCTURAL_TURN")
                momentum_engine.active_trade.pop(secid, None)
                self._finalize_exit(secid, index, now)
                return
