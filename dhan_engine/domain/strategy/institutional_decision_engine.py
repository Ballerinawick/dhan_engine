import time
from collections import defaultdict, deque


class InstitutionalDecisionEngine:
    """
    INSTITUTIONAL DECISION ENGINE — STABLE (v2.2)

    Fixes:
    ✅ Fix deque slicing crash (on_opt_depth error)
    ✅ Clears phantom momentum trades on reject
    ✅ Allows same-side re-entry after cooldown
    ✅ Flip requires shadow confirmation
    """

    STRUCTURE_LOOKBACK_SEC = 45
    STRUCTURE_MIN_POINTS = 6
    STRUCTURE_COMPRESSION_PCT = 0.12
    BALANCE_LOOKBACK_SEC = 30

    DISPLACEMENT_THRESHOLD_PCT = 0.15
    HOLD_CONFIRM_SEC = 3
    POST_ENTRY_VALIDATE_MAX_SEC = 45

    MODE_DEFAULT = "SCALP"
    MODE_UPGRADE_CONFIRM_TICKS = 3

    FLIP_COOLDOWN_SEC = 12
    SHADOW_CONFIRM_TICKS = 3
    SHADOW_WINDOW_SEC = 30

    REENTRY_ALLOW_WITHOUT_STRUCT = True
    CONTINUATION_MAX_AGE_SEC = 12
    CONTINUATION_MIN_DOM = 0.15
    CONTINUATION_MIN_FLOW = 1000
    CONTINUATION_MIN_PRESSURE = 0.08

    def __init__(self, debug=True):
        self.debug = debug

        self.trade_ctx = {}
        self.price_track = defaultdict(deque)
        self.last_turn_signal = {}
        self.last_turn_ts = {}
        self.last_entry_ts = {}
        self.pending_turn_entry = {}

        self.index_active_side = {}
        self.index_active_secid = {}
        self.index_last_exit_ts = {}
        self.index_last_exit_side = {}

        self.shadow = defaultdict(lambda: {
            "CE": {"ticks": 0, "last_ts": 0.0, "structure_ok": False},
            "PE": {"ticks": 0, "last_ts": 0.0, "structure_ok": False},
        })

    # --------------------------------------------------
    def _log_event(self, **kwargs):
        if not getattr(self, "debug", True):
            return
        base = {
            "ts": int(time.time()),
            "engine": self.__class__.__name__,
        }
        base.update(kwargs)
        log_line = " | ".join([f"{k}={v}" for k, v in base.items()])
        print(f"🧠 {log_line}")

    def _index_from_tag(self, tag):
        return tag.split("_")[0].upper()

    def _side_from_tag(self, tag):
        return "CE" if "CE" in tag else "PE"

    # --------------------------------------------------
    def _update_price_history(self, secid, now, ltp):
        q = self.price_track[secid]
        q.append((now, float(ltp)))
        while q and now - q[0][0] > self.STRUCTURE_LOOKBACK_SEC:
            q.popleft()

    def _structure_ok(self, secid):
        pts = list(self.price_track[secid])
        if len(pts) < self.STRUCTURE_MIN_POINTS:
            return False

        prices = [p for _, p in pts]
        rng = max(prices) - min(prices)
        last = prices[-1]
        return rng <= max(last, 1e-6) * self.STRUCTURE_COMPRESSION_PCT

    # --------------------------------------------------
    def _shadow_update(self, index, side, now, struct_ok):
        s = self.shadow[index][side]
        if s["last_ts"] and now - s["last_ts"] > self.SHADOW_WINDOW_SEC:
            s["ticks"] = 0
        s["ticks"] += 1
        s["last_ts"] = now
        s["structure_ok"] = struct_ok

    def _shadow_confirmed(self, index, side, now):
        s = self.shadow[index][side]
        if now - s["last_ts"] > self.SHADOW_WINDOW_SEC:
            return False
        return s["ticks"] >= self.SHADOW_CONFIRM_TICKS and s["structure_ok"]

    def _cooldown_ok(self, index, now):
        ts = self.index_last_exit_ts.get(index)
        return ts is None or (now - ts) >= self.FLIP_COOLDOWN_SEC

    def _pressure_score(self, tick):
        score = 0

        imb = abs(float(tick.get("imbalance_5", 0) or 0))
        flow = abs(float(tick.get("flow", 0) or 0))
        absorb = float(tick.get("absorption_strength", 0) or 0)
        vac = bool(tick.get("vacuum_flag", False))
        spread = float(tick.get("spread", 0) or 0)

        if imb > 0.20:
            score += 2

        if flow > 400:
            score += 2

        if absorb > 0.20:
            score += 2

        if spread < 0.20:
            score += 1

        if vac:
            score -= 2

        return score

    # --------------------------------------------------
    def on_signal(self, *, secid, tag, ltp, signal, momentum_engine, paper_trader, snapshot=None):
        print("DECISION_ENGINE_INPUT →", {
            "secid": secid,
            "tag": tag,
            "ltp": ltp,
            "signal": signal,
            "has_snapshot": snapshot is not None,
        })
        now = time.time()
        index = self._index_from_tag(tag)
        side = self._side_from_tag(tag)
        snapshot = snapshot or {}
        signal_confidence = snapshot.get("confidence", 0.0)

        pending = self.pending_turn_entry.get(index)
        if pending and (now - float(pending.get("timestamp", 0.0))) > 3.0:
            self.pending_turn_entry.pop(index, None)
            self._log_event(
                event="TURN_CONFIRMATION_REJECT",
                index=index,
                secid=pending.get("secid"),
                reason="PENDING_EXPIRED",
            )

        if signal == "REAL_BULLISH_TURN":
            self.last_turn_signal[index] = signal
            self.last_turn_ts[index] = now
            self.pending_turn_entry[index] = {
                "index": index,
                "secid": secid,
                "tag": tag,
                "entry_side": "CE",
                "timestamp": now,
                "ltp": float(ltp),
                "confidence": float(signal_confidence or 0.0),
            }
            self._log_event(
                event="TURN_PENDING_CONFIRMATION",
                index=index,
                signal=signal,
            )
            return {"entry_allowed": False, "reason": "TURN_PENDING_CONFIRMATION"}
        elif signal == "REAL_BEARISH_TURN":
            self.last_turn_signal[index] = signal
            self.last_turn_ts[index] = now
            self.pending_turn_entry[index] = {
                "index": index,
                "secid": secid,
                "tag": tag,
                "entry_side": "PE",
                "timestamp": now,
                "ltp": float(ltp),
                "confidence": float(signal_confidence or 0.0),
            }
            self._log_event(
                event="TURN_PENDING_CONFIRMATION",
                index=index,
                signal=signal,
            )
            return {"entry_allowed": False, "reason": "TURN_PENDING_CONFIRMATION"}

        self._update_price_history(secid, now, ltp)
        struct_ok = self._structure_ok(secid)
        self._shadow_update(index, side, now, struct_ok)

        # ================= ENTRY =================
        if signal in (
            "REAL_BULLISH_TURN",
            "REAL_BEARISH_TURN",
            "BULLISH_CONTINUATION",
            "BEARISH_CONTINUATION",
        ):
            last_tick = momentum_engine.tick_buffer[secid][-1] if momentum_engine.tick_buffer[secid] else {}
            print("ENTRY_CONFIDENCE →", signal_confidence)
            if signal_confidence < 0.65:
                self._log_event(
                    event="ENTRY",
                    decision="REJECT",
                    reason="LOW_CONFIDENCE",
                    confidence=round(signal_confidence, 2)
                )
                print("DECISION_REJECT_REASON → LOW_CONFIDENCE")
                return {"entry_allowed": False}
            last_turn = self.last_turn_signal.get(index)
            last_turn_ts = self.last_turn_ts.get(index, 0.0)
            turn_age_sec = max(now - float(last_turn_ts or 0.0), 0.0)

            entry_side = None
            entry_reason = "TURN_CONTINUATION"
            if signal == "BULLISH_CONTINUATION" and last_turn == "REAL_BULLISH_TURN":
                entry_side = "CE"
            elif signal == "BEARISH_CONTINUATION" and last_turn == "REAL_BEARISH_TURN":
                entry_side = "PE"
            else:
                self._log_event(event="ENTRY", decision="REJECT", index=index, secid=secid, side=side, reason="TURN_NOT_MATCHED")
                print("DECISION_REJECT_REASON → TURN_NOT_MATCHED")
                return {"entry_allowed": False}

            if signal.endswith("CONTINUATION"):
                pending_turn = self.pending_turn_entry.get(index)
                if not pending_turn:
                    self._log_event(event="TURN_CONFIRMATION_REJECT", index=index, secid=secid, reason="NO_PENDING_TURN")
                    self._log_event(event="ENTRY", decision="REJECT", index=index, secid=secid, side=side, reason="CONTINUATION_WITHOUT_PENDING")
                    print("DECISION_REJECT_REASON → CONTINUATION_WITHOUT_PENDING")
                    return {"entry_allowed": False}
                if pending_turn.get("entry_side") != entry_side:
                    self._log_event(event="TURN_CONFIRMATION_REJECT", index=index, secid=secid, reason="DIRECTION_MISMATCH")
                    self._log_event(event="ENTRY", decision="REJECT", index=index, secid=secid, side=side, reason="CONTINUATION_DIRECTION_MISMATCH")
                    print("DECISION_REJECT_REASON → CONTINUATION_DIRECTION_MISMATCH")
                    return {"entry_allowed": False}
                pending_age = max(now - float(pending_turn.get("timestamp", 0.0)), 0.0)
                if pending_age > 3.0:
                    self.pending_turn_entry.pop(index, None)
                    self._log_event(event="TURN_CONFIRMATION_REJECT", index=index, secid=secid, reason="PENDING_TURN_STALE")
                    self._log_event(event="ENTRY", decision="REJECT", index=index, secid=secid, side=side, reason="CONTINUATION_STALE")
                    print("DECISION_REJECT_REASON → CONTINUATION_STALE")
                    return {"entry_allowed": False}
                if turn_age_sec > self.CONTINUATION_MAX_AGE_SEC:
                    self._log_event(event="TURN_CONFIRMATION_REJECT", index=index, secid=secid, reason="TURN_CONTEXT_STALE")
                    self._log_event(event="ENTRY", decision="REJECT", index=index, secid=secid, side=side, reason="CONTINUATION_STALE")
                    print("DECISION_REJECT_REASON → CONTINUATION_STALE")
                    return {"entry_allowed": False}
                if abs(snapshot.get("flow_diff", 0)) < self.CONTINUATION_MIN_FLOW:
                    self._log_event(event="TURN_CONFIRMATION_REJECT", index=index, secid=secid, reason="CONTINUATION_LOW_FLOW")
                    self._log_event(event="ENTRY", decision="REJECT", index=index, secid=secid, side=side, reason="CONTINUATION_LOW_FLOW")
                    print("DECISION_REJECT_REASON → CONTINUATION_LOW_FLOW")
                    return {"entry_allowed": False}
                if abs(snapshot.get("dominance_score", 0)) < self.CONTINUATION_MIN_DOM:
                    self._log_event(event="TURN_CONFIRMATION_REJECT", index=index, secid=secid, reason="CONTINUATION_LOW_DOM")
                    self._log_event(event="ENTRY", decision="REJECT", index=index, secid=secid, side=side, reason="CONTINUATION_LOW_DOM")
                    print("DECISION_REJECT_REASON → CONTINUATION_LOW_DOM")
                    return {"entry_allowed": False}
                if abs(snapshot.get("pressure_diff", 0)) < self.CONTINUATION_MIN_PRESSURE:
                    self._log_event(event="TURN_CONFIRMATION_REJECT", index=index, secid=secid, reason="CONTINUATION_LOW_PRESSURE")
                    self._log_event(event="ENTRY", decision="REJECT", index=index, secid=secid, side=side, reason="CONTINUATION_LOW_PRESSURE")
                    print("DECISION_REJECT_REASON → CONTINUATION_LOW_PRESSURE")
                    return {"entry_allowed": False}
                self._log_event(
                    event="TURN_CONFIRMATION_ACCEPT",
                    index=index,
                    secid=secid,
                    direction=entry_side,
                    age=round(pending_age, 2),
                )

            last_ts = self.last_entry_ts.get(index)
            if last_ts and (time.time() - last_ts) < 45:
                self._log_event(event="ENTRY", decision="REJECT", index=index, secid=secid, side=side, reason="ENTRY_COOLDOWN")
                print("DECISION_REJECT_REASON → ENTRY_COOLDOWN")
                return {"entry_allowed": False}

            print(
                "ENTRY_CHECK →",
                "regime=", snapshot.get("market_regime"),
                "flow=", snapshot.get("flow_diff"),
                "dom=", snapshot.get("dominance_score"),
                "pressure=", snapshot.get("pressure_diff")
            )

            # Allow strong trend inside compression
            if snapshot.get("market_regime") == "COMPRESSED":
                if abs(snapshot.get("dominance_score", 0)) < 0.20:
                    self._log_event(event="ENTRY_BLOCK", reason="WEAK_COMPRESSION", index=index)
                    print("DECISION_REJECT_REASON → LOW_DOM")
                    return {"entry_allowed": False}

            if abs(snapshot.get("flow_diff", 0)) < 1000:
                self._log_event(event="ENTRY_BLOCK", reason="LOW_FLOW", index=index)
                print("DECISION_REJECT_REASON → LOW_FLOW")
                return {"entry_allowed": False}

            if abs(snapshot.get("dominance_score", 0)) < 0.15:
                self._log_event(event="ENTRY_BLOCK", reason="LOW_DOM", index=index)
                print("DECISION_REJECT_REASON → LOW_DOM")
                return {"entry_allowed": False}

            if abs(snapshot.get("pressure_diff", 0)) < 0.08:
                self._log_event(event="ENTRY_BLOCK", reason="LOW_PRESSURE", index=index)
                print("DECISION_REJECT_REASON → LOW_PRESSURE")
                return {"entry_allowed": False}

            if index in self.index_active_secid and self.index_active_secid[index] in paper_trader.positions:
                self._log_event(
                    event="ENTRY",
                    decision="REJECT",
                    index=index,
                    tag=tag,
                    secid=secid,
                    side=entry_side,
                    reason="INDEX_LOCKED",
                )
                print("DECISION_REJECT_REASON → INDEX_LOCKED")
                return {"entry_allowed": False}

            if not self._cooldown_ok(index, now):
                self._log_event(
                    event="ENTRY",
                    decision="REJECT",
                    index=index,
                    tag=tag,
                    secid=secid,
                    side=entry_side,
                    reason="COOLDOWN",
                )
                print("DECISION_REJECT_REASON → COOLDOWN")
                return {"entry_allowed": False}

            last_exit_side = self.index_last_exit_side.get(index)
            is_flip = last_exit_side and last_exit_side != entry_side

            if is_flip and not self._shadow_confirmed(index, entry_side, now):
                self._log_event(
                    event="ENTRY",
                    decision="REJECT",
                    index=index,
                    tag=tag,
                    secid=secid,
                    side=entry_side,
                    reason="FLIP_NO_SHADOW",
                )
                print("DECISION_REJECT_REASON → FLIP_NO_SHADOW")
                return {"entry_allowed": False}

            if not is_flip and self.REENTRY_ALLOW_WITHOUT_STRUCT:
                struct_ok = True

            if not struct_ok:
                self._log_event(
                    event="ENTRY",
                    decision="REJECT",
                    index=index,
                    tag=tag,
                    secid=secid,
                    side=entry_side,
                    reason="STRUCT_NOT_OK",
                )
                print("DECISION_REJECT_REASON → STRUCT_NOT_OK")
                return {"entry_allowed": False}

            print("BEFORE_PAPER_ENTRY →", secid, tag, ltp)
            entry_accepted = paper_trader.on_entry(
                secid=secid,
                tag=tag,
                side="LONG",
                ltp=ltp,
                lots=1,
                reason=entry_reason
            )
            print("AFTER_PAPER_ENTRY →", entry_accepted)
            print("POSITIONS_NOW →", paper_trader.positions)
            if entry_accepted is False:
                self._log_event(
                    event="ENTRY",
                    decision="REJECT",
                    index=index,
                    tag=tag,
                    secid=secid,
                    side=entry_side,
                    reason="PAPER_TRADER_REJECT",
                )
                print("DECISION_REJECT_REASON → PAPER_TRADER_REJECT")
                return {"entry_allowed": False}

            trade = {
                "type": "TURN",
                "side": "LONG",
                "entry": float(ltp),
                "ts": now,
                "best_price": float(ltp),
                "worst_price": float(ltp),
                "mfe": 0.0,
                "mae": 0.0,
                "locked_price": None,
                "breakeven_armed": False,
                "profit_lock_armed": False,
                "entry_spread": float(last_tick.get("spread", 0) or 0),
            }
            if hasattr(momentum_engine, "register_trade"):
                momentum_engine.register_trade(secid, trade)
            else:
                momentum_engine.active_trade[secid] = trade

            self.index_active_side[index] = entry_side
            self.index_active_secid[index] = secid
            self.last_entry_ts[index] = time.time()

            self.trade_ctx[secid] = {
                "mode": self.MODE_DEFAULT,
                "accept": 0,
                "ts": now,
                "post_validate_until": now + self.POST_ENTRY_VALIDATE_MAX_SEC,
                "disp_start": None,
            }

            self._log_event(
                event="ENTRY",
                decision="ACCEPT",
                index=index,
                secid=secid,
                side=entry_side,
                flow=round(snapshot.get("flow_diff", 0), 2),
                dom=round(snapshot.get("dominance_score", 0), 2),
                pressure=round(snapshot.get("pressure_diff", 0), 2),
                confidence=round(signal_confidence, 2),
                reason=entry_reason
            )
            self.pending_turn_entry.pop(index, None)
            return {"entry_allowed": True}

        # ================= EXIT =================
        if signal == "EXIT":
            self.trade_ctx.pop(secid, None)

            if self.index_active_secid.get(index) == secid:
                self.index_last_exit_side[index] = self.index_active_side.get(index)
                self.index_active_side.pop(index, None)
                self.index_active_secid.pop(index, None)
                self.index_last_exit_ts[index] = now

            self._log_event(
                event="EXIT",
                decision="ACCEPT",
                index=index,
                tag=tag,
                secid=secid,
                ltp=f"{ltp:.2f}",
                reason="STRATEGY_EXIT",
            )
            return {"exit_allowed": True}

        # ================= POST ENTRY =================
        if secid not in paper_trader.positions:
            print("POST_ENTRY_SKIP → NO_OPEN_POSITION")
            return None

        ctx = self.trade_ctx.get(secid)
        if not ctx:
            print("POST_ENTRY_SKIP → NO_TRADE_CONTEXT")
            return None

        ctx["accept"] += 1
        if ctx["mode"] == "SCALP" and ctx["accept"] >= self.MODE_UPGRADE_CONFIRM_TICKS:
            ctx["mode"] = "TREND"
            self._log_event(
                event="MODE_SHIFT",
                index=index,
                tag=tag,
                secid=secid,
                mode="TREND",
            )

        if ctx["mode"] == "TREND":
            print("POST_ENTRY_HOLD → TREND_MODE")
            return None

        recent = list(self.price_track[secid])[-5:]
        bal = sum(p for _, p in recent) / len(recent) if recent else ltp
        disp = abs(ltp - bal) / max(bal, 1e-6)

        if disp >= self.DISPLACEMENT_THRESHOLD_PCT:
            ctx["disp_start"] = ctx["disp_start"] or now
        else:
            ctx["disp_start"] = None

        if ctx["disp_start"] and now - ctx["disp_start"] >= self.HOLD_CONFIRM_SEC:
            print("POST_ENTRY_HOLD → DISPLACEMENT_CONFIRM_WAIT")
            return None

        if now > ctx["post_validate_until"]:
            if (now - ctx["ts"]) < 45:
                print("POST_ENTRY_SKIP → HOLD_PROTECTED")
                return None
            paper_trader.on_exit(secid, ltp, reason="ENTRY_INVALIDATED")
            if hasattr(momentum_engine, "clear_trade"):
                momentum_engine.clear_trade(secid, "ENTRY_INVALIDATED")
            else:
                momentum_engine.active_trade.pop(secid, None)
            self.trade_ctx.pop(secid, None)

            if self.index_active_secid.get(index) == secid:
                self.index_last_exit_side[index] = self.index_active_side.get(index)
                self.index_active_side.pop(index, None)
                self.index_active_secid.pop(index, None)
                self.index_last_exit_ts[index] = now

            self._log_event(
                event="POST_KILL",
                index=index,
                tag=tag,
                secid=secid,
                reason="ENTRY_INVALIDATED",
            )
            print("DECISION_FELL_THROUGH →", {
                "secid": secid,
                "tag": tag,
                "signal": signal,
                "reason": "ENTRY_INVALIDATED",
            })
            return {"entry_allowed": False, "reason": "DECISION_FELL_THROUGH"}

        print("DECISION_NO_ACTION →", {
            "secid": secid,
            "tag": tag,
            "signal": signal
        })
        return None
