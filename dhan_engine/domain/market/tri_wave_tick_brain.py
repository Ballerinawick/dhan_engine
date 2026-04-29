from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class TriWaveTick:
    index: str
    stream: str
    secid: int
    ltp: float
    ts: float
    features: dict


@dataclass
class TriWaveSignal:
    index: str
    action: str
    side: Optional[str]
    confidence: float
    reason: str
    ts: float
    state: dict


class TriWaveTickBrain:
    MAX_WINDOW_TICKS = 60
    CORE_WINDOW_TICKS = 30
    MIN_SYNC_TICKS = 12
    MAX_TICK_AGE_SEC = 45
    SIGNAL_COOLDOWN_SEC = 8

    ENTRY_CONFIDENCE = 0.68
    EXIT_CONFIDENCE = 0.62
    FLIP_CONFIDENCE = 0.72

    MIN_DYNAMIC_EXIT_HOLD_SEC = 20
    MAX_DYNAMIC_EXIT_HOLD_SEC = 180

    ADVERSE_EXIT_PCT = -2.0
    FAST_ADVERSE_EXIT_PCT = -4.0
    TIME_LOSS_EXIT_SEC = 120
    TIME_LOSS_EXIT_PCT = -1.0
    DEAD_TRADE_EXIT_SEC = 180
    DEAD_TRADE_MIN_PROFIT_PCT = 0.20

    TARGET_TOP_ZONE = 0.78
    TARGET_BREAKDOWN_LAST5_DELTA = -0.50
    TARGET_WEAK_STRENGTH = -0.20
    OPPOSITE_EXPAND_STRENGTH = 0.35
    FUTURE_REVERSAL_STRENGTH = 0.35

    EXIT_CONFIRM_TICKS = 2
    HEALTH_EXIT_CONFIRM_TICKS = 2
    FAST_EXIT_CONFIRM_TICKS = 1
    FLIP_CONFIRM_TICKS = 2
    MIN_HOLD_BEFORE_NORMAL_EXIT_SEC = MIN_DYNAMIC_EXIT_HOLD_SEC
    MIN_HOLD_BEFORE_FLIP_SEC = MIN_DYNAMIC_EXIT_HOLD_SEC
    ENTRY_COOLDOWN_SEC = 45
    REENTRY_SAME_SIDE_COOLDOWN_SEC = 60

    FUT_MIN_STRENGTH = 0.18
    OPTION_EXPAND_MIN_STRENGTH = 0.20
    OPTION_WEAKEN_MIN_STRENGTH = -0.12
    OPPOSITE_EXPAND_MIN_STRENGTH = 0.28
    CURRENT_WEAKEN_MIN_STRENGTH = -0.22
    ENTRY_PULLBACK_LOOKBACK_TICKS = 30
    ENTRY_MAX_POSITION_IN_RANGE = 0.72
    ENTRY_TURN_ZONE_MAX_POSITION = 0.45
    ENTRY_REQUIRE_RECENT_TURN = True
    ENTRY_MIN_RECOVERY_STRENGTH = 0.12
    ENTRY_MAX_CHASE_STRENGTH = 0.82
    ENTRY_MIN_OPPOSITE_WEAKEN = -0.10

    SPREAD_MAX_PCT = 0.025
    SPOOF_RISK_BLOCK = 0.72

    def __init__(self, debug: bool = False):
        self.debug = debug
        self.ticks = defaultdict(
            lambda: {
                "FUT": deque(maxlen=self.MAX_WINDOW_TICKS),
                "CE": deque(maxlen=self.MAX_WINDOW_TICKS),
                "PE": deque(maxlen=self.MAX_WINDOW_TICKS),
            }
        )
        self.last_signal_ts: Dict[str, float] = {}
        self.last_signal: Dict[str, str] = {}
        self.latest_state: Dict[str, dict] = {}
        self._last_state_log_ts: Dict[str, float] = {}
        self.exit_confirm = defaultdict(lambda: {"side": None, "count": 0, "first_ts": 0.0, "reason": None})
        self.last_entry_side: Dict[str, str] = {}
        self.last_entry_ts: Dict[str, float] = {}

    def _confirm_exit(self, index: str, side: str, condition: bool, reason: str, now_ts: float, required_ticks: int) -> bool:
        slot = self.exit_confirm[index]
        if not condition:
            self.exit_confirm[index] = {"side": None, "count": 0, "first_ts": 0.0, "reason": None}
            return False
        if slot["side"] != side or slot["reason"] != reason:
            slot["side"] = side
            slot["reason"] = reason
            slot["count"] = 1
            slot["first_ts"] = now_ts
        else:
            slot["count"] += 1
        if slot["count"] >= required_ticks:
            return True
        return False


    def _build_exit_candidate(self, side: str, hold_sec: float, pnl_pct: float, fs: dict, cs: dict, ps: dict) -> tuple[str, int]:
        if side == "CE":
            target = cs
            opposite = ps

            if pnl_pct <= self.FAST_ADVERSE_EXIT_PCT and hold_sec >= 10:
                return "FAST_ADVERSE_EXIT", self.FAST_EXIT_CONFIRM_TICKS

            if pnl_pct <= self.ADVERSE_EXIT_PCT and hold_sec >= self.MIN_DYNAMIC_EXIT_HOLD_SEC:
                return "ADVERSE_MOVE_CONFIRMED", self.HEALTH_EXIT_CONFIRM_TICKS

            if hold_sec >= self.TIME_LOSS_EXIT_SEC and pnl_pct <= self.TIME_LOSS_EXIT_PCT:
                return "TIME_LOSS_EXIT", self.HEALTH_EXIT_CONFIRM_TICKS

            if hold_sec >= self.DEAD_TRADE_EXIT_SEC and pnl_pct < self.DEAD_TRADE_MIN_PROFIT_PCT:
                return "DEAD_TRADE_TIMEOUT", self.HEALTH_EXIT_CONFIRM_TICKS

            if target["position_in_range"] >= self.TARGET_TOP_ZONE and target["turn_down"]:
                return "CE_TOP_TURN_EXIT", self.EXIT_CONFIRM_TICKS

            if target["last_5_delta"] <= self.TARGET_BREAKDOWN_LAST5_DELTA and target["strength"] <= self.TARGET_WEAK_STRENGTH:
                return "CE_PREMIUM_BREAKDOWN", self.EXIT_CONFIRM_TICKS

            if opposite["strength"] >= self.OPPOSITE_EXPAND_STRENGTH and target["strength"] <= 0:
                return "PE_EXPAND_CE_WEAKEN", self.EXIT_CONFIRM_TICKS

            if fs["strength"] <= -self.FUTURE_REVERSAL_STRENGTH and target["strength"] <= 0:
                return "FUTURE_REVERSAL_AGAINST_CE", self.EXIT_CONFIRM_TICKS

        if side == "PE":
            target = ps
            opposite = cs

            if pnl_pct <= self.FAST_ADVERSE_EXIT_PCT and hold_sec >= 10:
                return "FAST_ADVERSE_EXIT", self.FAST_EXIT_CONFIRM_TICKS

            if pnl_pct <= self.ADVERSE_EXIT_PCT and hold_sec >= self.MIN_DYNAMIC_EXIT_HOLD_SEC:
                return "ADVERSE_MOVE_CONFIRMED", self.HEALTH_EXIT_CONFIRM_TICKS

            if hold_sec >= self.TIME_LOSS_EXIT_SEC and pnl_pct <= self.TIME_LOSS_EXIT_PCT:
                return "TIME_LOSS_EXIT", self.HEALTH_EXIT_CONFIRM_TICKS

            if hold_sec >= self.DEAD_TRADE_EXIT_SEC and pnl_pct < self.DEAD_TRADE_MIN_PROFIT_PCT:
                return "DEAD_TRADE_TIMEOUT", self.HEALTH_EXIT_CONFIRM_TICKS

            if target["position_in_range"] >= self.TARGET_TOP_ZONE and target["turn_down"]:
                return "PE_TOP_TURN_EXIT", self.EXIT_CONFIRM_TICKS

            if target["last_5_delta"] <= self.TARGET_BREAKDOWN_LAST5_DELTA and target["strength"] <= self.TARGET_WEAK_STRENGTH:
                return "PE_PREMIUM_BREAKDOWN", self.EXIT_CONFIRM_TICKS

            if opposite["strength"] >= self.OPPOSITE_EXPAND_STRENGTH and target["strength"] <= 0:
                return "CE_EXPAND_PE_WEAKEN", self.EXIT_CONFIRM_TICKS

            if fs["strength"] >= self.FUTURE_REVERSAL_STRENGTH and target["strength"] <= 0:
                return "FUTURE_REVERSAL_AGAINST_PE", self.EXIT_CONFIRM_TICKS

        return "NONE", 0

    def on_future_tick(self, index: str, secid: int, ltp: float, quote: Optional[dict] = None) -> None:
        if not ltp or ltp <= 0:
            return
        payload = quote or {}
        self.ticks[index]["FUT"].append(TriWaveTick(index=index, stream="FUT", secid=int(secid), ltp=float(ltp), ts=time.time(), features=dict(payload)))

    def on_option_tick(self, index: str, side: str, secid: int, ltp: float, features: Optional[dict] = None) -> None:
        if side not in {"CE", "PE"} or not ltp or ltp <= 0:
            return
        self.ticks[index][side].append(TriWaveTick(index=index, stream=side, secid=int(secid), ltp=float(ltp), ts=time.time(), features=dict(features or {})))

    def _fresh(self, ticks: Deque[TriWaveTick], now_ts: float) -> List[TriWaveTick]:
        return [t for t in ticks if (now_ts - float(t.ts)) <= self.MAX_TICK_AGE_SEC]

    def _stream_stats(self, ticks: List[TriWaveTick]) -> dict:
        prices = [float(t.ltp) for t in ticks]
        n = len(prices)
        first, last = prices[0], prices[-1]
        delta = last - first
        min_price = min(prices)
        max_price = max(prices)
        rng = max(max_price - min_price, 0.05)
        position_in_range = (last - min_price) / rng
        recent = prices[-10:]
        recent_low = min(recent)
        recent_high = max(recent)
        from_recent_low_pct = ((last - recent_low) / max(recent_low, 1e-9)) * 100
        from_recent_high_pct = ((last - recent_high) / max(recent_high, 1e-9)) * 100
        last_5_delta = prices[-1] - prices[-5] if n >= 5 else 0.0
        previous_5_delta = prices[-5] - prices[-10] if n >= 10 else 0.0
        turn_up = previous_5_delta < 0 and last_5_delta > 0
        turn_down = previous_5_delta > 0 and last_5_delta < 0
        pullback_then_recover = turn_up and position_in_range <= self.ENTRY_TURN_ZONE_MAX_POSITION
        velocity = prices[-1] - prices[-2] if n >= 2 else 0.0
        prev_velocity = prices[-2] - prices[-3] if n >= 3 else 0.0
        accel = velocity - prev_velocity
        if n > 1:
            xm = (n - 1) / 2.0
            ym = sum(prices) / n
            num = sum((i - xm) * (p - ym) for i, p in enumerate(prices))
            den = sum((i - xm) ** 2 for i in range(n)) or 1.0
            slope = num / den
        else:
            slope = 0.0
        direction = "UP" if slope > 0 else "DOWN" if slope < 0 else "FLAT"
        latest = ticks[-1].features if ticks else {}
        return {
            "first": first,
            "last": last,
            "delta": delta,
            "slope": slope,
            "velocity": velocity,
            "prev_velocity": prev_velocity,
            "accel": accel,
            "direction": direction,
            "range": rng,
            "min_price": min_price,
            "max_price": max_price,
            "position_in_range": position_in_range,
            "recent_low": recent_low,
            "recent_high": recent_high,
            "from_recent_low_pct": from_recent_low_pct,
            "from_recent_high_pct": from_recent_high_pct,
            "last_5_delta": last_5_delta,
            "previous_5_delta": previous_5_delta,
            "turn_up": turn_up,
            "turn_down": turn_down,
            "pullback_then_recover": pullback_then_recover,
            "strength": delta / rng,
            "abs_strength": abs(delta / rng),
            "imbalance_5": latest.get("imbalance_5"),
            "flow": latest.get("flow"),
            "real_flow": latest.get("real_flow"),
            "ofi": latest.get("ofi"),
            "pressure": latest.get("pressure", latest.get("pressure_score")),
            "pressure_score": latest.get("pressure_score", latest.get("pressure")),
            "bull_turn_score": latest.get("bull_turn_score"),
            "bear_turn_score": latest.get("bear_turn_score"),
            "spoof_risk": float(latest.get("spoof_risk", 0.0) or 0.0),
            "trend_fatigue": latest.get("trend_fatigue"),
            "spread": latest.get("spread"),
            "bid_price": latest.get("bid_price"),
            "ask_price": latest.get("ask_price"),
        }

    def _entry_quality_ok(self, side: str, fs: dict, cs: dict, ps: dict) -> tuple[bool, str]:
        if side == "CE":
            target = cs
            opposite = ps
            if target["position_in_range"] > self.ENTRY_MAX_POSITION_IN_RANGE:
                return False, "CHASE_ENTRY_BLOCKED_CE_TOP_ZONE"
            if target["strength"] > self.ENTRY_MAX_CHASE_STRENGTH:
                return False, "CHASE_ENTRY_BLOCKED_CE_OVEREXTENDED"
            if self.ENTRY_REQUIRE_RECENT_TURN and not target["turn_up"]:
                return False, "NO_CE_BOTTOM_TURN"
            if target["last_5_delta"] < self.ENTRY_MIN_RECOVERY_STRENGTH:
                return False, "CE_RECOVERY_TOO_WEAK"
            if opposite["strength"] > self.ENTRY_MIN_OPPOSITE_WEAKEN:
                return False, "PE_NOT_WEAKENING_ENOUGH"
            return True, "CE_BOTTOM_TURN_CONFIRMED"

        if side == "PE":
            target = ps
            opposite = cs
            if target["position_in_range"] > self.ENTRY_MAX_POSITION_IN_RANGE:
                return False, "CHASE_ENTRY_BLOCKED_PE_TOP_ZONE"
            if target["strength"] > self.ENTRY_MAX_CHASE_STRENGTH:
                return False, "CHASE_ENTRY_BLOCKED_PE_OVEREXTENDED"
            if self.ENTRY_REQUIRE_RECENT_TURN and not target["turn_up"]:
                return False, "NO_PE_BOTTOM_TURN"
            if target["last_5_delta"] < self.ENTRY_MIN_RECOVERY_STRENGTH:
                return False, "PE_RECOVERY_TOO_WEAK"
            if opposite["strength"] > self.ENTRY_MIN_OPPOSITE_WEAKEN:
                return False, "CE_NOT_WEAKENING_ENOUGH"
            return True, "PE_BOTTOM_TURN_CONFIRMED"
        return False, "UNKNOWN_SIDE"

    def _signal(self, index: str, action: str, side: Optional[str], confidence: float, reason: str, state: dict) -> TriWaveSignal:
        sig = TriWaveSignal(index=index, action=action, side=side, confidence=float(confidence), reason=reason, ts=time.time(), state=state)
        self.latest_state[index] = state
        if action != "NO_TRADE":
            self.last_signal_ts[index] = sig.ts
            self.last_signal[index] = action
            logger.info("TRI_WAVE_SIGNAL | index=%s | action=%s | conf=%.2f | reason=%s", index, action, confidence, reason)
        return sig

    def evaluate(self, index: str, active_position: Optional[dict] = None) -> TriWaveSignal:
        now_ts = time.time()
        fut = self._fresh(self.ticks[index]["FUT"], now_ts)
        ce = self._fresh(self.ticks[index]["CE"], now_ts)
        pe = self._fresh(self.ticks[index]["PE"], now_ts)
        fut = fut[-self.CORE_WINDOW_TICKS :]
        ce = ce[-self.CORE_WINDOW_TICKS :]
        pe = pe[-self.CORE_WINDOW_TICKS :]
        if min(len(fut), len(ce), len(pe)) < self.MIN_SYNC_TICKS:
            logger.info("TRI_WAVE_REJECT | index=%s | reason=LOW_SYNC | fut=%s | ce=%s | pe=%s", index, len(fut), len(ce), len(pe))
            return self._signal(index, "NO_TRADE", None, 0.0, "LOW_SYNC", {"sync_ready": False, "reason": "LOW_SYNC", "bias": None})

        fs, cs, ps = self._stream_stats(fut), self._stream_stats(ce), self._stream_stats(pe)
        ce_spread = cs.get("spread")
        pe_spread = ps.get("spread")
        ce_sp = float(ce_spread if ce_spread is not None else (float(cs.get("ask_price") or 0) - float(cs.get("bid_price") or 0)))
        pe_sp = float(pe_spread if pe_spread is not None else (float(ps.get("ask_price") or 0) - float(ps.get("bid_price") or 0)))
        ce_sp_pct = ce_sp / max(float(cs["last"]), 1e-9)
        pe_sp_pct = pe_sp / max(float(ps["last"]), 1e-9)

        reason_parts: List[str] = []
        conf_adj = 0.0
        if ce_sp_pct > self.SPREAD_MAX_PCT or pe_sp_pct > self.SPREAD_MAX_PCT:
            reason_parts.append("WIDE_SPREAD")
            conf_adj -= 0.10
        if max(float(cs.get("spoof_risk", 0.0)), float(ps.get("spoof_risk", 0.0))) > self.SPOOF_RISK_BLOCK:
            logger.info("TRI_WAVE_REJECT | index=%s | reason=SPOOF_RISK", index)
            return self._signal(index, "NO_TRADE", None, 0.0, "SPOOF_RISK", {"sync_ready": True, "reason": "SPOOF_RISK", "bias": None})

        fut_up = fs["slope"] > 0 and fs["strength"] >= self.FUT_MIN_STRENGTH
        fut_down = fs["slope"] < 0 and fs["strength"] <= -self.FUT_MIN_STRENGTH
        fut_turn_up = fs["accel"] > 0 and fs["velocity"] > 0 and fs["prev_velocity"] < 0
        fut_turn_down = fs["accel"] < 0 and fs["velocity"] < 0 and fs["prev_velocity"] > 0

        ce_expand = cs["strength"] >= self.OPTION_EXPAND_MIN_STRENGTH
        pe_expand = ps["strength"] >= self.OPTION_EXPAND_MIN_STRENGTH
        ce_weaken = cs["strength"] <= self.OPTION_WEAKEN_MIN_STRENGTH
        pe_weaken = ps["strength"] <= self.OPTION_WEAKEN_MIN_STRENGTH

        bias = "CE" if cs["strength"] > ps["strength"] else "PE" if ps["strength"] > cs["strength"] else None
        state = {"sync_ready": True, "bias": bias, "reason": "+".join(reason_parts) if reason_parts else "SYNC_OK", "fut": fs, "ce": cs, "pe": ps, "active": (active_position or {}).get("side")}

        last_log = self._last_state_log_ts.get(index, 0.0)
        if now_ts - last_log >= 3:
            self._last_state_log_ts[index] = now_ts
            logger.info(
                "TRI_WAVE_STATE | index=%s | sync_ready=True | fut_dir=%s | fut_strength=%.2f | ce_strength=%.2f | pe_strength=%.2f | ce_pos=%.2f | pe_pos=%.2f | ce_turn_up=%s | pe_turn_up=%s | ce_last5=%.2f | pe_last5=%.2f | ce_prev5=%.2f | pe_prev5=%.2f | bias=%s | active=%s",
                index,
                fs["direction"],
                fs["strength"],
                cs["strength"],
                ps["strength"],
                cs["position_in_range"],
                ps["position_in_range"],
                cs["turn_up"],
                ps["turn_up"],
                cs["last_5_delta"],
                ps["last_5_delta"],
                cs["previous_5_delta"],
                ps["previous_5_delta"],
                bias,
                (active_position or {}).get("side"),
            )

        # entries
        if not active_position:
            if (fut_up or fut_turn_up) and ce_expand and pe_weaken:
                conf = 0.70 + conf_adj + (0.03 if cs.get("bull_turn_score", 0) and cs.get("bull_turn_score", 0) > 0 else 0)
                reasons = ["FUT_TURN_UP" if fut_turn_up else "FUT_TREND_UP", "CE_EXPAND", "PE_WEAKEN"]
                if conf >= self.ENTRY_CONFIDENCE:
                    quality_ok, quality_reason = self._entry_quality_ok("CE", fs, cs, ps)
                    if not quality_ok:
                        logger.info(
                            "TRI_WAVE_ENTRY_REJECT | index=%s | side=CE | reason=%s | fut_strength=%.2f | ce_strength=%.2f | pe_strength=%.2f | ce_pos=%.2f | pe_pos=%.2f | ce_turn_up=%s | pe_turn_up=%s | ce_last5=%.2f | pe_last5=%.2f",
                            index, quality_reason, fs["strength"], cs["strength"], ps["strength"],
                            cs["position_in_range"], ps["position_in_range"],
                            cs["turn_up"], ps["turn_up"], cs["last_5_delta"], ps["last_5_delta"]
                        )
                        return self._signal(index, "NO_TRADE", None, 0.0, quality_reason, state)
                    if (now_ts - self.last_signal_ts.get(index, 0.0)) < self.ENTRY_COOLDOWN_SEC:
                        logger.info("TRI_WAVE_ENTRY_COOLDOWN | index=%s | side=CE | remaining=%.2f", index, self.ENTRY_COOLDOWN_SEC - (now_ts - self.last_signal_ts.get(index, 0.0)))
                        return self._signal(index, "NO_TRADE", None, 0.0, "ENTRY_COOLDOWN", state)
                    if self.last_entry_side.get(index) == "CE" and (now_ts - self.last_entry_ts.get(index, 0.0)) < self.REENTRY_SAME_SIDE_COOLDOWN_SEC:
                        logger.info("TRI_WAVE_REENTRY_BLOCK | index=%s | side=CE | remaining=%.2f", index, self.REENTRY_SAME_SIDE_COOLDOWN_SEC - (now_ts - self.last_entry_ts.get(index, 0.0)))
                        return self._signal(index, "NO_TRADE", None, 0.0, "REENTRY_SAME_SIDE_COOLDOWN", state)
                    reasons.append(quality_reason)
                    self.last_entry_side[index] = "CE"
                    self.last_entry_ts[index] = now_ts
                    return self._signal(index, "BUY_CE", "CE", conf, "+".join(reasons + reason_parts), state)
            if (fut_down or fut_turn_down) and pe_expand and ce_weaken:
                conf = 0.70 + conf_adj + (0.03 if ps.get("bull_turn_score", 0) and ps.get("bull_turn_score", 0) > 0 else 0)
                reasons = ["FUT_TURN_DOWN" if fut_turn_down else "FUT_TREND_DOWN", "PE_EXPAND", "CE_WEAKEN"]
                if conf >= self.ENTRY_CONFIDENCE:
                    quality_ok, quality_reason = self._entry_quality_ok("PE", fs, cs, ps)
                    if not quality_ok:
                        logger.info(
                            "TRI_WAVE_ENTRY_REJECT | index=%s | side=PE | reason=%s | fut_strength=%.2f | ce_strength=%.2f | pe_strength=%.2f | ce_pos=%.2f | pe_pos=%.2f | ce_turn_up=%s | pe_turn_up=%s | ce_last5=%.2f | pe_last5=%.2f",
                            index, quality_reason, fs["strength"], cs["strength"], ps["strength"],
                            cs["position_in_range"], ps["position_in_range"],
                            cs["turn_up"], ps["turn_up"], cs["last_5_delta"], ps["last_5_delta"]
                        )
                        return self._signal(index, "NO_TRADE", None, 0.0, quality_reason, state)
                    if (now_ts - self.last_signal_ts.get(index, 0.0)) < self.ENTRY_COOLDOWN_SEC:
                        logger.info("TRI_WAVE_ENTRY_COOLDOWN | index=%s | side=PE | remaining=%.2f", index, self.ENTRY_COOLDOWN_SEC - (now_ts - self.last_signal_ts.get(index, 0.0)))
                        return self._signal(index, "NO_TRADE", None, 0.0, "ENTRY_COOLDOWN", state)
                    if self.last_entry_side.get(index) == "PE" and (now_ts - self.last_entry_ts.get(index, 0.0)) < self.REENTRY_SAME_SIDE_COOLDOWN_SEC:
                        logger.info("TRI_WAVE_REENTRY_BLOCK | index=%s | side=PE | remaining=%.2f", index, self.REENTRY_SAME_SIDE_COOLDOWN_SEC - (now_ts - self.last_entry_ts.get(index, 0.0)))
                        return self._signal(index, "NO_TRADE", None, 0.0, "REENTRY_SAME_SIDE_COOLDOWN", state)
                    reasons.append(quality_reason)
                    self.last_entry_side[index] = "PE"
                    self.last_entry_ts[index] = now_ts
                    return self._signal(index, "BUY_PE", "PE", conf, "+".join(reasons + reason_parts), state)
            logger.info("TRI_WAVE_REJECT | index=%s | reason=LOW_CONFIDENCE", index)
            return self._signal(index, "NO_TRADE", None, 0.0, "LOW_CONFIDENCE", state)

        hold_sec = now_ts - float(active_position.get("entry_ts", now_ts))
        pnl_pct = float(active_position.get("pnl_pct", 0.0))
        side = active_position.get("side")
        if side == "CE":
            reason_candidate, required_ticks = self._build_exit_candidate("CE", hold_sec, pnl_pct, fs, cs, ps)
            target = cs
            logger.info(
                "TRI_WAVE_EXIT_WATCH | index=%s | active_side=%s | hold_sec=%.2f | pnl_pct=%.2f | fut_strength=%.2f | ce_strength=%.2f | pe_strength=%.2f | target_pos=%.2f | target_turn_down=%s | target_last5_delta=%.2f | reason_candidate=%s | confirm_count=%s",
                index, side, hold_sec, pnl_pct, fs["strength"], cs["strength"], ps["strength"], target["position_in_range"], target["turn_down"], target["last_5_delta"], reason_candidate, self.exit_confirm[index]["count"]
            )
            if reason_candidate != "NONE":
                confirmed = self._confirm_exit(index, side, True, reason_candidate, now_ts, required_ticks)
                if not confirmed:
                    logger.info("TRI_WAVE_EXIT_BLOCKED | reason=CONFIRMATION_PENDING | candidate=%s | count=%s | required=%s", reason_candidate, self.exit_confirm[index]["count"], required_ticks)
                    return self._signal(index, "NO_TRADE", None, 0.0, "EXIT_CONFIRMATION_PENDING", state)
                action = "EXIT_CE"
                logger.info("TRI_WAVE_EXIT_CONFIRMED | action=%s | reason=TRI_WAVE_EXIT:%s | hold_sec=%.2f | pnl_pct=%.2f | confirm_count=%s", action, reason_candidate, hold_sec, pnl_pct, self.exit_confirm[index]["count"])
                return self._signal(index, action, side, 0.80, reason_candidate, state)

            ce_real_reversal = (fut_down or fut_turn_down) and ps["strength"] >= self.OPPOSITE_EXPAND_MIN_STRENGTH and cs["strength"] <= self.CURRENT_WEAKEN_MIN_STRENGTH
            ce_profit_protect = pnl_pct > 0.35 and cs["strength"] <= -0.10 and ps["strength"] >= 0.18
            ce_flip_candidate = ce_real_reversal

            ce_exit_reason = "REAL_REVERSAL" if ce_real_reversal else "PROFIT_PROTECT" if ce_profit_protect else "NONE"
            ce_confirmed = self._confirm_exit(index, "CE", ce_real_reversal or ce_profit_protect, ce_exit_reason, now_ts, self.EXIT_CONFIRM_TICKS)
            if not ce_confirmed and (ce_real_reversal or ce_profit_protect):
                logger.info("TRI_WAVE_EXIT_BLOCKED | reason=CONFIRMATION_NOT_MET | count=%s | required=%s", self.exit_confirm[index]["count"], self.EXIT_CONFIRM_TICKS)
            if hold_sec < self.MIN_HOLD_BEFORE_FLIP_SEC and ce_flip_candidate:
                logger.info("TRI_WAVE_EXIT_BLOCKED | reason=MIN_HOLD_NOT_MET | hold_sec=%.2f | required=%.2f", hold_sec, self.MIN_HOLD_BEFORE_FLIP_SEC)
            elif ce_flip_candidate:
                flip_confirmed = self._confirm_exit(index, "CE", ce_flip_candidate, "FLIP_TO_PE", now_ts, self.FLIP_CONFIRM_TICKS)
                if flip_confirmed:
                    conf = 0.74 + conf_adj
                    if conf >= self.FLIP_CONFIDENCE:
                        logger.info("TRI_WAVE_EXIT_CONFIRMED | action=FLIP_TO_PE | reason=TRI_WAVE_FLIP_EXIT:FUT_TURN_DOWN+PE_EXPAND+CE_WEAKEN | hold_sec=%.2f | pnl_pct=%.2f | confirm_count=%s", hold_sec, pnl_pct, self.exit_confirm[index]["count"])
                        return self._signal(index, "FLIP_TO_PE", "PE", conf, "FUT_TURN_DOWN+PE_EXPAND+CE_WEAKEN", state)
                else:
                    logger.info("TRI_WAVE_EXIT_BLOCKED | reason=CONFIRMATION_NOT_MET | count=%s | required=%s", self.exit_confirm[index]["count"], self.FLIP_CONFIRM_TICKS)

            if hold_sec < self.MIN_HOLD_BEFORE_NORMAL_EXIT_SEC and (ce_real_reversal or ce_profit_protect):
                logger.info("TRI_WAVE_EXIT_BLOCKED | reason=MIN_HOLD_NOT_MET | hold_sec=%.2f | required=%.2f", hold_sec, self.MIN_HOLD_BEFORE_NORMAL_EXIT_SEC)
            elif ce_confirmed:
                conf = 0.65 + conf_adj
                if conf >= self.EXIT_CONFIDENCE:
                    reason = "CE_REAL_REVERSAL_CONFIRMED" if ce_exit_reason == "REAL_REVERSAL" else "CE_PROFIT_PROTECT_CONFIRMED"
                    logger.info("TRI_WAVE_EXIT_CONFIRMED | action=EXIT_CE | reason=TRI_WAVE_EXIT:%s | hold_sec=%.2f | pnl_pct=%.2f | confirm_count=%s", reason, hold_sec, pnl_pct, self.exit_confirm[index]["count"])
                    return self._signal(index, "EXIT_CE", "CE", conf, reason, state)
        if side == "PE":
            reason_candidate, required_ticks = self._build_exit_candidate("PE", hold_sec, pnl_pct, fs, cs, ps)
            target = ps
            logger.info(
                "TRI_WAVE_EXIT_WATCH | index=%s | active_side=%s | hold_sec=%.2f | pnl_pct=%.2f | fut_strength=%.2f | ce_strength=%.2f | pe_strength=%.2f | target_pos=%.2f | target_turn_down=%s | target_last5_delta=%.2f | reason_candidate=%s | confirm_count=%s",
                index, side, hold_sec, pnl_pct, fs["strength"], cs["strength"], ps["strength"], target["position_in_range"], target["turn_down"], target["last_5_delta"], reason_candidate, self.exit_confirm[index]["count"]
            )
            if reason_candidate != "NONE":
                confirmed = self._confirm_exit(index, side, True, reason_candidate, now_ts, required_ticks)
                if not confirmed:
                    logger.info("TRI_WAVE_EXIT_BLOCKED | reason=CONFIRMATION_PENDING | candidate=%s | count=%s | required=%s", reason_candidate, self.exit_confirm[index]["count"], required_ticks)
                    return self._signal(index, "NO_TRADE", None, 0.0, "EXIT_CONFIRMATION_PENDING", state)
                action = "EXIT_PE"
                logger.info("TRI_WAVE_EXIT_CONFIRMED | action=%s | reason=TRI_WAVE_EXIT:%s | hold_sec=%.2f | pnl_pct=%.2f | confirm_count=%s", action, reason_candidate, hold_sec, pnl_pct, self.exit_confirm[index]["count"])
                return self._signal(index, action, side, 0.80, reason_candidate, state)

            pe_real_reversal = (fut_up or fut_turn_up) and cs["strength"] >= self.OPPOSITE_EXPAND_MIN_STRENGTH and ps["strength"] <= self.CURRENT_WEAKEN_MIN_STRENGTH
            pe_profit_protect = pnl_pct > 0.35 and ps["strength"] <= -0.10 and cs["strength"] >= 0.18
            pe_flip_candidate = pe_real_reversal

            pe_exit_reason = "REAL_REVERSAL" if pe_real_reversal else "PROFIT_PROTECT" if pe_profit_protect else "NONE"
            pe_confirmed = self._confirm_exit(index, "PE", pe_real_reversal or pe_profit_protect, pe_exit_reason, now_ts, self.EXIT_CONFIRM_TICKS)
            if not pe_confirmed and (pe_real_reversal or pe_profit_protect):
                logger.info("TRI_WAVE_EXIT_BLOCKED | reason=CONFIRMATION_NOT_MET | count=%s | required=%s", self.exit_confirm[index]["count"], self.EXIT_CONFIRM_TICKS)
            if hold_sec < self.MIN_HOLD_BEFORE_FLIP_SEC and pe_flip_candidate:
                logger.info("TRI_WAVE_EXIT_BLOCKED | reason=MIN_HOLD_NOT_MET | hold_sec=%.2f | required=%.2f", hold_sec, self.MIN_HOLD_BEFORE_FLIP_SEC)
            elif pe_flip_candidate:
                flip_confirmed = self._confirm_exit(index, "PE", pe_flip_candidate, "FLIP_TO_CE", now_ts, self.FLIP_CONFIRM_TICKS)
                if flip_confirmed:
                    conf = 0.74 + conf_adj
                    if conf >= self.FLIP_CONFIDENCE:
                        logger.info("TRI_WAVE_EXIT_CONFIRMED | action=FLIP_TO_CE | reason=TRI_WAVE_FLIP_EXIT:FUT_TURN_UP+CE_EXPAND+PE_WEAKEN | hold_sec=%.2f | pnl_pct=%.2f | confirm_count=%s", hold_sec, pnl_pct, self.exit_confirm[index]["count"])
                        return self._signal(index, "FLIP_TO_CE", "CE", conf, "FUT_TURN_UP+CE_EXPAND+PE_WEAKEN", state)
                else:
                    logger.info("TRI_WAVE_EXIT_BLOCKED | reason=CONFIRMATION_NOT_MET | count=%s | required=%s", self.exit_confirm[index]["count"], self.FLIP_CONFIRM_TICKS)

            if hold_sec < self.MIN_HOLD_BEFORE_NORMAL_EXIT_SEC and (pe_real_reversal or pe_profit_protect):
                logger.info("TRI_WAVE_EXIT_BLOCKED | reason=MIN_HOLD_NOT_MET | hold_sec=%.2f | required=%.2f", hold_sec, self.MIN_HOLD_BEFORE_NORMAL_EXIT_SEC)
            elif pe_confirmed:
                conf = 0.65 + conf_adj
                if conf >= self.EXIT_CONFIDENCE:
                    reason = "PE_REAL_REVERSAL_CONFIRMED" if pe_exit_reason == "REAL_REVERSAL" else "PE_PROFIT_PROTECT_CONFIRMED"
                    logger.info("TRI_WAVE_EXIT_CONFIRMED | action=EXIT_PE | reason=TRI_WAVE_EXIT:%s | hold_sec=%.2f | pnl_pct=%.2f | confirm_count=%s", reason, hold_sec, pnl_pct, self.exit_confirm[index]["count"])
                    return self._signal(index, "EXIT_PE", "PE", conf, reason, state)

        if self.last_signal.get(index) in {"BUY_CE", "BUY_PE"} and active_position:
            pass
        elif (now_ts - self.last_signal_ts.get(index, 0.0)) < self.SIGNAL_COOLDOWN_SEC:
            logger.info("TRI_WAVE_REJECT | index=%s | reason=COOLDOWN", index)
            return self._signal(index, "NO_TRADE", None, 0.0, "COOLDOWN", state)
        return self._signal(index, "NO_TRADE", None, 0.0, "LOW_CONFIDENCE", state)

    def get_latest_state(self, index: str) -> dict:
        return dict(self.latest_state.get(index, {}))
