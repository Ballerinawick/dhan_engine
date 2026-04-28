import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque


class TurnDetectionEngine:
    HISTORY_MAX = 120
    MIN_HISTORY_FOR_TURN = 6
    MIN_HISTORY_FOR_CONTINUATION = 4
    SIGNAL_COOLDOWN_SEC = 5
    DOM_FLIP_THRESHOLD = 0.10
    FLOW_FLIP_THRESHOLD = 350.0
    PRESSURE_FLIP_THRESHOLD = 0.04
    COMPRESSION_THRESHOLD = 0.70
    EXHAUSTION_THRESHOLD = 0.65
    SUMMARY_INTERVAL_SEC = 30
    REGIME_SHIFT_COOLDOWN_SEC = 3
    MICRO_TURN_SCORE_THRESHOLD = 0.35
    SPOOF_RISK_BLOCK = 0.62
    TREND_FATIGUE_BLOCK = 0.72
    DECELERATION_CONFIRM = 0.18

    def __init__(self, debug=True):
        self.debug = debug
        self.history = defaultdict(lambda: deque(maxlen=self.HISTORY_MAX))
        self.last_signal = {}
        self.last_signal_ts = {}
        self.last_signal_confidence = {}
        self.last_turn_state = {}
        self.last_regime = {}
        self.last_regime_log_ts = {}
        self.active_trade = {}
        self.reject_stats = defaultdict(int)
        self.last_reject_stats_print_ts = 0.0

    def _log_event(self, **kwargs):
        if not getattr(self, "debug", True):
            return
        if kwargs.get("event") not in {
            "REGIME_SHIFT",
            "COMPRESSION",
            "COMPRESSION_WITH_BIAS",
            "EXHAUSTION",
            "REAL_BULLISH_TURN",
            "REAL_BEARISH_TURN",
            "BULLISH_CONTINUATION",
            "BEARISH_CONTINUATION",
        }:
            return
        base = {
            "ts": int(time.time()),
            "engine": self.__class__.__name__,
        }
        base.update(kwargs)
        log_line = " | ".join([f"{k}={v}" for k, v in base.items()])
        print(f"🔁 {log_line}")

    def _ist_time(self):
        ist = timezone(timedelta(hours=5, minutes=30))
        return datetime.now(ist).strftime("%H:%M:%S")

    def ist_time(self):
        return self._ist_time()

    def log_exit(self, index, entry_price, exit_price, qty, side):
        pnl = (exit_price - entry_price) * qty if side == "LONG" else (entry_price - exit_price) * qty
        print(
            f"🔴 EXIT | {self._ist_time()} | {index} | "
            f"Entry={entry_price} | Exit={exit_price} | Qty={qty} | "
            f"PnL={round(pnl, 2)}"
        )

    def _safe_float(self, value, default=0.0):
        try:
            if value is None:
                return float(default)
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    def _recent(self, index, n):
        if n <= 0:
            return []
        return list(self.history[index])[-n:]

    def _avg(self, field, rows):
        if not rows:
            return 0.0
        values = [self._safe_float(r.get(field)) for r in rows]
        return sum(values) / len(values)

    def _delta(self, field, rows):
        if len(rows) < 2:
            return 0.0
        return self._safe_float(rows[-1].get(field)) - self._safe_float(rows[0].get(field))

    def _slope(self, field, rows):
        if len(rows) < 2:
            return 0.0
        values = [self._safe_float(r.get(field)) for r in rows]
        x_vals = list(range(len(values)))
        x_avg = sum(x_vals) / len(x_vals)
        y_avg = sum(values) / len(values)
        denom = sum((x - x_avg) ** 2 for x in x_vals) or 1.0
        numer = sum((x - x_avg) * (y - y_avg) for x, y in zip(x_vals, values))
        return numer / denom

    def _bias_flip(self, rows, side):
        if len(rows) < 2:
            return False
        prev_bias = rows[-2].get("bias", "NEUTRAL")
        curr_bias = rows[-1].get("bias", "NEUTRAL")
        return prev_bias != side and curr_bias == side

    def _dominance_flip(self, rows, side):
        if len(rows) < 2:
            return False
        prev = self._safe_float(rows[-2].get("dominance_score"))
        curr = self._safe_float(rows[-1].get("dominance_score"))
        threshold = self.DOM_FLIP_THRESHOLD if side == "BULLISH" else -self.DOM_FLIP_THRESHOLD
        return (prev < threshold <= curr) if side == "BULLISH" else (prev > threshold >= curr)

    def _flow_flip(self, rows, side):
        if len(rows) < 2:
            return False
        prev = self._safe_float(rows[-2].get("flow_diff"))
        curr = self._safe_float(rows[-1].get("flow_diff"))
        threshold = self.FLOW_FLIP_THRESHOLD
        return (prev <= 0 < curr and curr >= threshold) if side == "BULLISH" else (prev >= 0 > curr and curr <= -threshold)

    def _is_compression(self, rows):
        if not rows:
            return False
        latest = rows[-1]
        return (
            latest.get("market_regime") == "COMPRESSED"
            or self._safe_float(latest.get("compression_score")) >= self.COMPRESSION_THRESHOLD
        )

    def _is_exhaustion(self, rows, side=None):
        if not rows:
            return False
        latest = rows[-1]
        score = self._safe_float(latest.get("exhaustion_score"))
        if score < self.EXHAUSTION_THRESHOLD:
            return False
        if side is None:
            return True
        return latest.get("bias") == side or latest.get("dominance_side") == side

    def _turn_reason(self, reasons):
        return "+".join(r for r in reasons if r)

    def _is_discontinuation(self, latest):
        displacement = float(latest.get("price_displacement_pct", 0.0) or 0.0)
        velocity = float(latest.get("velocity", 0.0) or 0.0)
        vol_expand = float(latest.get("volatility_expand", 1.0) or 1.0)
        flow_diff = self._safe_float(latest.get("flow_diff"))
        pressure_diff = self._safe_float(latest.get("pressure_diff"))
        dominance_score = self._safe_float(latest.get("dominance_score"))

        if abs(flow_diff) >= 350 or abs(pressure_diff) >= 0.04 or abs(dominance_score) >= 0.10:
            return True

        if displacement < 0.025:
            return False

        if abs(velocity) < 0.15:
            return False

        if vol_expand < 1.03:
            return False

        return True

    def _micro_turn_ready(self, latest, side):
        score_key = "bull_turn_score" if side == "BULLISH" else "bear_turn_score"
        score = self._safe_float(latest.get(score_key)) if score_key in latest else None

        if side == "BULLISH":
            fallback_confirm = (
                self._safe_float(latest.get("dominance_score")) >= 0.10
                or self._safe_float(latest.get("pressure_diff")) >= 0.05
                or self._safe_float(latest.get("flow_diff")) >= 250
            )
        else:
            fallback_confirm = (
                self._safe_float(latest.get("dominance_score")) <= -0.10
                or self._safe_float(latest.get("pressure_diff")) <= -0.05
                or self._safe_float(latest.get("flow_diff")) <= -250
            )

        if score is not None and score < self.MICRO_TURN_SCORE_THRESHOLD:
            return False
        if score is None and not fallback_confirm:
            return False

        if "spoof_risk" in latest and self._safe_float(latest.get("spoof_risk")) > self.SPOOF_RISK_BLOCK:
            return False

        if "trend_fatigue" in latest and self._safe_float(latest.get("trend_fatigue")) > self.TREND_FATIGUE_BLOCK:
            return False

        if "deceleration" in latest and self._safe_float(latest.get("deceleration")) < self.DECELERATION_CONFIRM:
            return False

        return True

    def _is_real_bullish_turn(self, rows):
        if len(rows) < self.MIN_HISTORY_FOR_TURN:
            return False, 0.0, []
        prev_rows = rows[:-1]
        latest = rows[-1]
        prev_state_bearish = (
            self._avg("dominance_score", prev_rows[-3:]) <= -0.12
            or prev_rows[-1].get("bias") == "BEARISH"
            or prev_rows[-1].get("dominance_side") == "BEARISH"
        )
        bias_flip = self._bias_flip(rows, "BULLISH")
        dom_flip = self._dominance_flip(rows, "BULLISH")
        flow_delta = self._delta("flow_diff", rows[-5:])
        pressure_delta = self._delta("pressure_diff", rows[-5:])
        flow_support = flow_delta >= self.FLOW_FLIP_THRESHOLD or self._slope("flow_diff", rows[-5:]) >= 400.0
        pressure_support = pressure_delta >= self.PRESSURE_FLIP_THRESHOLD or self._slope("pressure_diff", rows[-5:]) >= 0.04
        pre_exhaustion = self._safe_float(prev_rows[-1].get("exhaustion_score")) >= 0.50
        compression_release = self._is_compression(prev_rows[-3:]) and not self._is_compression(rows[-2:])
        weak_fake = self._safe_float(latest.get("dominance_score")) < self.DOM_FLIP_THRESHOLD or self._safe_float(latest.get("flow_diff")) < 0
        micro_ok = self._micro_turn_ready(latest, "BULLISH")
        if not prev_state_bearish:
            self.reject_stats["REAL_BULL_NO_PREV_STATE"] += 1
            return False, 0.0, []
        if not (bias_flip or dom_flip):
            self.reject_stats["REAL_BULL_NO_FLIP"] += 1
            return False, 0.0, []
        if not flow_support:
            self.reject_stats["REAL_BULL_NO_FLOW"] += 1
            return False, 0.0, []
        if not pressure_support:
            self.reject_stats["REAL_BULL_NO_PRESSURE"] += 1
            return False, 0.0, []
        if not micro_ok:
            self.reject_stats["REAL_BULL_MICRO_BLOCK"] += 1
            return False, 0.0, []
        if weak_fake:
            self.reject_stats["REAL_BULL_WEAK_FAKE"] += 1
            return False, 0.0, []
        reasons = []
        if bias_flip:
            reasons.append("bias_flip")
        if dom_flip:
            reasons.append("dominance_flip")
        if self._flow_flip(rows, "BULLISH") or flow_support:
            reasons.append("flow_flip")
        if compression_release:
            reasons.append("compression_release")
        elif pre_exhaustion:
            reasons.append("exhaustion_release")
        confidence = 0.58
        confidence += min(0.10, max(0.0, self._slope("dominance_score", rows[-5:]) * 2.0))
        confidence += min(0.10, max(0.0, self._slope("pressure_diff", rows[-5:]) * 1.5))
        confidence += min(0.10, max(0.0, self._slope("flow_diff", rows[-5:]) / 4000.0))
        if compression_release:
            confidence += 0.08
        if pre_exhaustion:
            confidence += 0.05
        if not self._is_discontinuation(latest):
            self.reject_stats["REAL_BULL_NO_DISCONTINUATION"] += 1
            return False, 0.0, ["NO_DISCONTINUATION"]
        return True, min(confidence, 0.95), reasons

    def _is_real_bearish_turn(self, rows):
        if len(rows) < self.MIN_HISTORY_FOR_TURN:
            return False, 0.0, []
        prev_rows = rows[:-1]
        latest = rows[-1]
        prev_state_bullish = (
            self._avg("dominance_score", prev_rows[-3:]) >= 0.12
            or prev_rows[-1].get("bias") == "BULLISH"
            or prev_rows[-1].get("dominance_side") == "BULLISH"
        )
        bias_flip = self._bias_flip(rows, "BEARISH")
        dom_flip = self._dominance_flip(rows, "BEARISH")
        flow_delta = self._delta("flow_diff", rows[-5:])
        pressure_delta = self._delta("pressure_diff", rows[-5:])
        flow_support = flow_delta <= -self.FLOW_FLIP_THRESHOLD or self._slope("flow_diff", rows[-5:]) <= -400.0
        pressure_support = pressure_delta <= -self.PRESSURE_FLIP_THRESHOLD or self._slope("pressure_diff", rows[-5:]) <= -0.04
        pre_exhaustion = self._safe_float(prev_rows[-1].get("exhaustion_score")) >= 0.50
        compression_release = self._is_compression(prev_rows[-3:]) and not self._is_compression(rows[-2:])
        weak_fake = self._safe_float(latest.get("dominance_score")) > -self.DOM_FLIP_THRESHOLD or self._safe_float(latest.get("flow_diff")) > 0
        micro_ok = self._micro_turn_ready(latest, "BEARISH")
        if not prev_state_bullish:
            self.reject_stats["REAL_BEAR_NO_PREV_STATE"] += 1
            return False, 0.0, []
        if not (bias_flip or dom_flip):
            self.reject_stats["REAL_BEAR_NO_FLIP"] += 1
            return False, 0.0, []
        if not flow_support:
            self.reject_stats["REAL_BEAR_NO_FLOW"] += 1
            return False, 0.0, []
        if not pressure_support:
            self.reject_stats["REAL_BEAR_NO_PRESSURE"] += 1
            return False, 0.0, []
        if not micro_ok:
            self.reject_stats["REAL_BEAR_MICRO_BLOCK"] += 1
            return False, 0.0, []
        if weak_fake:
            self.reject_stats["REAL_BEAR_WEAK_FAKE"] += 1
            return False, 0.0, []
        reasons = []
        if bias_flip:
            reasons.append("bias_flip")
        if dom_flip:
            reasons.append("dominance_flip")
        if self._flow_flip(rows, "BEARISH") or flow_support:
            reasons.append("flow_flip")
        if compression_release:
            reasons.append("compression_release")
        elif pre_exhaustion:
            reasons.append("exhaustion_release")
        confidence = 0.58
        confidence += min(0.10, max(0.0, -self._slope("dominance_score", rows[-5:]) * 2.0))
        confidence += min(0.10, max(0.0, -self._slope("pressure_diff", rows[-5:]) * 1.5))
        confidence += min(0.10, max(0.0, -self._slope("flow_diff", rows[-5:]) / 4000.0))
        if compression_release:
            confidence += 0.08
        if pre_exhaustion:
            confidence += 0.05
        if not self._is_discontinuation(latest):
            self.reject_stats["REAL_BEAR_NO_DISCONTINUATION"] += 1
            return False, 0.0, ["NO_DISCONTINUATION"]
        return True, min(confidence, 0.95), reasons

    def _is_fake_bullish_turn(self, rows):
        if len(rows) < 3:
            return False, 0.0, []
        latest = rows[-1]
        bias_flip = self._bias_flip(rows, "BULLISH") or latest.get("bias") == "BULLISH"
        weak_dom = self._safe_float(latest.get("dominance_score")) < self.DOM_FLIP_THRESHOLD
        weak_flow = self._safe_float(latest.get("flow_diff")) < self.FLOW_FLIP_THRESHOLD * 0.5
        exhaustion = self._safe_float(latest.get("exhaustion_score")) >= self.EXHAUSTION_THRESHOLD
        contradiction = self._safe_float(latest.get("pe_absorb")) > 0.55 or self._safe_float(latest.get("ce_vacuum")) > 0.55
        collapse = len(rows) >= 3 and rows[-2].get("bias") == "BULLISH" and latest.get("bias") != "BULLISH"
        if bias_flip and (weak_dom or weak_flow or exhaustion or contradiction or collapse):
            reasons = []
            if weak_flow:
                reasons.append("bias_flip_without_flow_support")
            if weak_dom:
                reasons.append("weak_dominance")
            if contradiction:
                reasons.append("bearish_absorption_contradiction")
            if exhaustion:
                reasons.append("bullish_exhaustion")
            if collapse:
                reasons.append("one_tick_collapse")
            confidence = 0.55 + (0.07 if weak_dom else 0.0) + (0.07 if weak_flow else 0.0) + (0.05 if contradiction else 0.0)
            return True, min(confidence, 0.85), reasons
        return False, 0.0, []

    def _is_fake_bearish_turn(self, rows):
        if len(rows) < 3:
            return False, 0.0, []
        latest = rows[-1]
        bias_flip = self._bias_flip(rows, "BEARISH") or latest.get("bias") == "BEARISH"
        weak_dom = self._safe_float(latest.get("dominance_score")) > -self.DOM_FLIP_THRESHOLD
        weak_flow = self._safe_float(latest.get("flow_diff")) > -self.FLOW_FLIP_THRESHOLD * 0.5
        exhaustion = self._safe_float(latest.get("exhaustion_score")) >= self.EXHAUSTION_THRESHOLD
        contradiction = self._safe_float(latest.get("ce_absorb")) > 0.55 or self._safe_float(latest.get("pe_vacuum")) > 0.55
        collapse = len(rows) >= 3 and rows[-2].get("bias") == "BEARISH" and latest.get("bias") != "BEARISH"
        if bias_flip and (weak_dom or weak_flow or exhaustion or contradiction or collapse):
            reasons = []
            if weak_flow:
                reasons.append("bias_flip_without_flow_support")
            if weak_dom:
                reasons.append("weak_dominance")
            if contradiction:
                reasons.append("bullish_absorption_contradiction")
            if exhaustion:
                reasons.append("bearish_exhaustion")
            if collapse:
                reasons.append("one_tick_collapse")
            confidence = 0.55 + (0.07 if weak_dom else 0.0) + (0.07 if weak_flow else 0.0) + (0.05 if contradiction else 0.0)
            return True, min(confidence, 0.85), reasons
        return False, 0.0, []

    def _is_bullish_continuation(self, rows):
        if len(rows) < self.MIN_HISTORY_FOR_CONTINUATION:
            return False, 0.0, []
        latest = rows[-1]
        strong_bias = latest.get("bias") == "BULLISH" or latest.get("dominance_side") == "BULLISH"
        flow_ok = self._avg("flow_diff", rows[-3:]) > 0
        dom_ok = self._avg("dominance_score", rows[-3:]) > 0.05
        not_tired = self._safe_float(latest.get("exhaustion_score")) < self.EXHAUSTION_THRESHOLD
        is_compressed = self._is_compression(rows[-2:])
        strong_trend_inside_compression = (
            latest.get("bias") == "BULLISH"
            and self._avg("dominance_score", rows[-3:]) > 0.08
            and self._avg("flow_diff", rows[-3:]) > 0
        )
        not_compressed = (not is_compressed) or strong_trend_inside_compression
        if strong_bias and flow_ok and dom_ok and not_tired and not_compressed:
            confidence = 0.58 + min(0.16, max(0.0, self._avg("dominance_score", rows[-3:])))
            if self._slope("flow_diff", rows[-4:]) > 0:
                confidence += 0.05
            return True, min(confidence, 0.88), ["bullish_structure_holding"]
        self.reject_stats["CONT_BULL_BLOCK"] += 1
        return False, 0.0, []

    def _is_bearish_continuation(self, rows):
        if len(rows) < self.MIN_HISTORY_FOR_CONTINUATION:
            return False, 0.0, []
        latest = rows[-1]
        strong_bias = latest.get("bias") == "BEARISH" or latest.get("dominance_side") == "BEARISH"
        flow_ok = self._avg("flow_diff", rows[-3:]) < 0
        dom_ok = self._avg("dominance_score", rows[-3:]) < -0.05
        not_tired = self._safe_float(latest.get("exhaustion_score")) < self.EXHAUSTION_THRESHOLD
        is_compressed = self._is_compression(rows[-2:])
        strong_trend_inside_compression = (
            latest.get("bias") == "BEARISH"
            and self._avg("dominance_score", rows[-3:]) < -0.08
            and self._avg("flow_diff", rows[-3:]) < 0
        )
        not_compressed = (not is_compressed) or strong_trend_inside_compression
        if strong_bias and flow_ok and dom_ok and not_tired and not_compressed:
            confidence = 0.58 + min(0.16, max(0.0, -self._avg("dominance_score", rows[-3:])))
            if self._slope("flow_diff", rows[-4:]) < 0:
                confidence += 0.05
            return True, min(confidence, 0.88), ["bearish_structure_holding"]
        self.reject_stats["CONT_BEAR_BLOCK"] += 1
        return False, 0.0, []

    def _emit(self, snapshot, signal, confidence, reason, state):
        index = snapshot.get("index", "UNKNOWN")
        return {
            "index": index,
            "ts": snapshot.get("ts"),
            "signal": signal,
            "confidence": round(max(0.0, min(confidence, 0.99)), 2),
            "reason": reason,
            "state": state,
        }

    def _track_entry(self, snapshot, signal):
        bullish_signal = signal in {"REAL_BULLISH_TURN", "BULLISH_CONTINUATION"}
        side = "LONG" if bullish_signal else "SHORT"
        premium_type = "CE" if bullish_signal else "PE"
        entry_price = self._safe_float(snapshot.get("ce_ltp" if bullish_signal else "pe_ltp"))
        trade = {
            "signal": signal,
            "side": side,
            "entry_price": entry_price,
            "entry_time_ist": self._ist_time(),
            "ts": int(self._safe_float(snapshot.get("ts"), time.time())),
            "qty": int(snapshot.get("qty", 1) or 1),
            "premium_type": premium_type,
        }
        self.active_trade[snapshot.get("index", "UNKNOWN")] = trade
        print(
            f"🟢 TURN_SIGNAL | {trade['entry_time_ist']} | {snapshot.get('index', 'UNKNOWN')} | "
            f"{signal} | {premium_type} | {entry_price}"
        )
        return trade

    def close_trade(self, index, exit_price, reason="manual_exit"):
        trade = self.active_trade.get(index)
        if not trade:
            return None
        exit_px = self._safe_float(exit_price)
        qty = int(trade.get("qty", 1) or 1)
        hold_sec = max(0, int(time.time()) - int(trade.get("ts", time.time())))
        if trade.get("side") == "LONG":
            pnl = (exit_px - trade.get("entry_price", 0.0)) * qty
        else:
            pnl = (trade.get("entry_price", 0.0) - exit_px) * qty
        print(
            f"🔴 EXIT | {self._ist_time()} | {index} | {trade.get('signal')} | "
            f"{trade.get('premium_type')} | {trade.get('entry_price')} | {exit_px} | "
            f"{round(pnl, 2)} | {hold_sec} | {reason}"
        )
        closed = dict(trade)
        closed.update({
            "exit_price": exit_px,
            "pnl": round(pnl, 2),
            "hold_sec": hold_sec,
            "reason": reason,
        })
        self.active_trade.pop(index, None)
        return closed

    def _should_emit_signal(self, index, signal, ts, confidence):
        last_signal = self.last_signal.get(index)
        last_ts = self.last_signal_ts.get(index, 0)
        last_conf = self.last_signal_confidence.get(index, 0.0)

        # New signal type always allowed
        if last_signal != signal:
            return True

        # Continuation signals need faster refresh
        if signal in ("BULLISH_CONTINUATION", "BEARISH_CONTINUATION"):
            # allow resend every 2 sec
            if ts - last_ts >= 2:
                return True

            # allow smaller confidence improvement
            return confidence >= (last_conf + 0.02)

        # Keep strict rules for turn signals / others
        if ts - last_ts >= self.SIGNAL_COOLDOWN_SEC:
            return True

        return confidence >= (last_conf + 0.08)

    def update(self, snapshot):
        if not snapshot or not isinstance(snapshot, dict):
            return None
        now_ts = time.time()
        if now_ts - self.last_reject_stats_print_ts >= self.SUMMARY_INTERVAL_SEC:
            print(f"TURN_REJECT_STATS => {dict(self.reject_stats)}")
            self.last_reject_stats_print_ts = now_ts

        index = snapshot.get("index")
        if not index:
            return None

        self.history[index].append(snapshot)
        rows = self._recent(index, self.HISTORY_MAX)
        ts = int(self._safe_float(snapshot.get("ts"), time.time()))

        regime = snapshot.get("market_regime", "BALANCED")
        prev_regime = self.last_regime.get(index)
        if prev_regime and prev_regime != regime:
            last_regime_log = self.last_regime_log_ts.get(index, 0)
            if ts - last_regime_log >= self.REGIME_SHIFT_COOLDOWN_SEC:
                self._log_event(
                    event="REGIME_SHIFT",
                    index=index,
                    from_regime=prev_regime,
                    to_regime=regime,
                )
                self.last_regime_log_ts[index] = ts
        self.last_regime[index] = regime


        if len(rows) < 2:
            return None

        if len(rows) < self.MIN_HISTORY_FOR_CONTINUATION:
            return None

        latest = rows[-1]

        compression_event = None
        if self._is_compression(rows[-3:]):
            score = self._safe_float(latest.get("compression_score"))
            if score >= self.COMPRESSION_THRESHOLD and self._should_emit_signal(index, "COMPRESSION_WARNING", ts, score):
                compression_event = self._emit(latest, "COMPRESSION_WARNING", score, "compression_building", {
                    "regime": regime,
                    "bias": latest.get("bias"),
                })
                self._log_event(
                    event="COMPRESSION",
                    index=index,
                    score=round(score, 2),
                    bias=latest.get("bias"),
                )
                if latest.get("bias") in ["BULLISH", "BEARISH"]:
                    self._log_event(
                        event="COMPRESSION_WITH_BIAS",
                        index=index,
                        bias=latest.get("bias"),
                        dom=round(self._safe_float(latest.get("dominance_score")), 2),
                        flow=round(self._safe_float(latest.get("flow_diff")), 2),
                    )
                self.last_signal[index] = compression_event["signal"]
                self.last_signal_ts[index] = ts
                self.last_signal_confidence[index] = compression_event["confidence"]
                self.last_turn_state[index] = compression_event

        trend_side = latest.get("bias") if latest.get("bias") in {"BULLISH", "BEARISH"} else latest.get("dominance_side", "NEUTRAL")
        if self._is_exhaustion(rows[-5:], trend_side):
            score = self._safe_float(latest.get("exhaustion_score"))
            if self._should_emit_signal(index, "EXHAUSTION_WARNING", ts, score):
                event = self._emit(latest, "EXHAUSTION_WARNING", score, "trend_exhaustion_rising", {
                    "side": trend_side,
                    "regime": regime,
                })
                self._log_event(
                    event="EXHAUSTION",
                    index=index,
                    side=trend_side,
                    score=round(score, 2),
                )
                self.last_signal[index] = event["signal"]
                self.last_signal_ts[index] = ts
                self.last_signal_confidence[index] = event["confidence"]
                self.last_turn_state[index] = event
                return event

        detectors = [
            ("REAL_BULLISH_TURN", self._is_real_bullish_turn),
            ("REAL_BEARISH_TURN", self._is_real_bearish_turn),
            ("FAKE_BULLISH_TURN", self._is_fake_bullish_turn),
            ("FAKE_BEARISH_TURN", self._is_fake_bearish_turn),
            ("BULLISH_CONTINUATION", self._is_bullish_continuation),
            ("BEARISH_CONTINUATION", self._is_bearish_continuation),
        ]

        for signal, fn in detectors:
            ok, confidence, reasons = fn(rows)
            if not ok:
                continue
            if not self._should_emit_signal(index, signal, ts, confidence):
                continue
            reason = self._turn_reason(reasons) or "signal_conditions_met"
            event = self._emit(latest, signal, confidence, reason, {
                "bias": latest.get("bias"),
                "dominance_side": latest.get("dominance_side"),
                "dominance_score": round(self._safe_float(latest.get("dominance_score")), 2),
                "regime": regime,
                "compression_score": round(self._safe_float(latest.get("compression_score")), 2),
                "exhaustion_score": round(self._safe_float(latest.get("exhaustion_score")), 2),
            })
            if signal.startswith("REAL_") or signal.endswith("_CONTINUATION"):
                print("TURN_SIGNAL →", signal)
                self._log_event(
                    event=signal,
                    index=index,
                    confidence=round(confidence, 2),
                    bias=latest.get("bias"),
                )
            self.last_signal[index] = signal
            self.last_signal_ts[index] = ts
            self.last_signal_confidence[index] = event["confidence"]
            self.last_turn_state[index] = event
            return event

        return compression_event
