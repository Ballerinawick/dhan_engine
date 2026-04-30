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

    MIN_EXIT_HOLD_SEC = 20
    MIN_BREATHING_HOLD_SEC = 20
    EARLY_FAST_ADVERSE_HOLD_SEC = 10
    EARLY_FAST_ADVERSE_PCT = -4.0
    NORMAL_EXIT_MIN_HOLD_SEC = 20
    PROFIT_EXIT_MIN_HOLD_SEC = 25
    BREAKEVEN_EXIT_MIN_HOLD_SEC = 35
    MAX_SCALP_HOLD_SEC = 600
    MIN_DYNAMIC_EXIT_HOLD_SEC = MIN_EXIT_HOLD_SEC
    MAX_DYNAMIC_EXIT_HOLD_SEC = MAX_SCALP_HOLD_SEC

    PROFIT_ARM_PCT = 1.5
    STRONG_PROFIT_PCT = 3.0
    BIG_PROFIT_PCT = 4.5

    TOP_ZONE_POSITION = 0.75
    EXTREME_TOP_ZONE_POSITION = 0.85

    PROFIT_GIVEBACK_EXIT_PCT = 0.35
    BIG_PROFIT_GIVEBACK_EXIT_PCT = 0.25
    BREAKEVEN_PROTECT_AFTER_MFE_PCT = 2.0
    BREAKEVEN_PROTECT_PNL_PCT = 0.25

    ADVERSE_EXIT_PCT = -2.0
    FAST_ADVERSE_EXIT_PCT = -4.0
    TIME_LOSS_EXIT_SEC = 120
    TIME_LOSS_EXIT_PCT = -1.0

    TARGET_TOP_ZONE = TOP_ZONE_POSITION
    TARGET_BREAKDOWN_STRENGTH = -0.25
    TARGET_BREAKDOWN_LAST5_DELTA = -0.50
    TARGET_WEAK_STRENGTH = -0.10
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
        self.trade_peak_state: Dict[str, dict] = {}

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


    def _evaluate_premium_scalp_exit(self, index, active_side, entry_price, current_price, fs, cs, ps, hold_sec):
        pnl_pct = ((current_price - max(entry_price, 1e-9)) / max(entry_price, 1e-9)) * 100.0
        target = cs if active_side == "CE" else ps
        opposite = ps if active_side == "CE" else cs
        now_ts = time.time()
        peak = self.trade_peak_state.get(index, {"entry_price": entry_price, "best_price": entry_price, "best_pnl_pct": 0.0, "mfe_pct": 0.0, "last_peak_ts": now_ts})
        if current_price >= peak["best_price"]:
            peak["best_price"] = current_price
            peak["best_pnl_pct"] = pnl_pct
            peak["mfe_pct"] = pnl_pct
            peak["last_peak_ts"] = now_ts
        peak["hold_sec"] = hold_sec
        peak["current_pnl_pct"] = pnl_pct
        peak["giveback_pct"] = max(0.0, peak["best_pnl_pct"] - pnl_pct)
        self.trade_peak_state[index] = peak

        if hold_sec < self.MIN_BREATHING_HOLD_SEC:
            if hold_sec >= self.EARLY_FAST_ADVERSE_HOLD_SEC and pnl_pct <= self.EARLY_FAST_ADVERSE_PCT:
                return {"exit": True, "reason": "FAST_ADVERSE_EXIT", "confirm_required": self.FAST_EXIT_CONFIRM_TICKS}
            return {"exit": False, "reason": "BREATHING_WINDOW_ACTIVE", "confirm_required": self.EXIT_CONFIRM_TICKS}

        reason = "NONE"
        confirm_required = self.EXIT_CONFIRM_TICKS
        if pnl_pct <= self.FAST_ADVERSE_EXIT_PCT and hold_sec >= self.EARLY_FAST_ADVERSE_HOLD_SEC:
            reason, confirm_required = "FAST_ADVERSE_EXIT", self.FAST_EXIT_CONFIRM_TICKS
        elif pnl_pct <= self.ADVERSE_EXIT_PCT and hold_sec >= self.NORMAL_EXIT_MIN_HOLD_SEC:
            reason = "ADVERSE_MOVE_CONFIRMED"
        elif hold_sec >= self.TIME_LOSS_EXIT_SEC and pnl_pct <= self.TIME_LOSS_EXIT_PCT:
            reason = "TIME_LOSS_EXIT"
        elif pnl_pct >= self.BIG_PROFIT_PCT and hold_sec >= self.EARLY_FAST_ADVERSE_HOLD_SEC and target["position_in_range"] >= self.EXTREME_TOP_ZONE_POSITION:
            reason, confirm_required = "EXTREME_TOP_PROFIT_EXIT", self.FAST_EXIT_CONFIRM_TICKS
        elif hold_sec >= self.PROFIT_EXIT_MIN_HOLD_SEC and pnl_pct >= self.PROFIT_ARM_PCT and target["position_in_range"] >= self.TOP_ZONE_POSITION and target["turn_down"] is True:
            reason = "PROFIT_TOP_TURN_EXIT"
        elif hold_sec >= self.PROFIT_EXIT_MIN_HOLD_SEC and pnl_pct >= self.STRONG_PROFIT_PCT and target["position_in_range"] >= self.TOP_ZONE_POSITION and target["last_5_delta"] <= 0:
            reason = "PROFIT_STALL_TOP_EXIT"
        elif hold_sec >= self.PROFIT_EXIT_MIN_HOLD_SEC and peak["best_pnl_pct"] >= self.BIG_PROFIT_PCT and peak["giveback_pct"] >= self.BIG_PROFIT_GIVEBACK_EXIT_PCT:
            reason = "BIG_PROFIT_GIVEBACK_EXIT"
        elif hold_sec >= self.PROFIT_EXIT_MIN_HOLD_SEC and peak["best_pnl_pct"] >= self.STRONG_PROFIT_PCT and peak["giveback_pct"] >= self.PROFIT_GIVEBACK_EXIT_PCT:
            reason = "PROFIT_GIVEBACK_EXIT"
        elif hold_sec >= self.BREAKEVEN_EXIT_MIN_HOLD_SEC and peak["best_pnl_pct"] >= self.BREAKEVEN_PROTECT_AFTER_MFE_PCT and pnl_pct <= self.BREAKEVEN_PROTECT_PNL_PCT:
            reason = "BREAKEVEN_PROTECT"
        elif hold_sec >= self.NORMAL_EXIT_MIN_HOLD_SEC and target["last_5_delta"] <= self.TARGET_BREAKDOWN_LAST5_DELTA and target["strength"] <= self.TARGET_BREAKDOWN_STRENGTH:
            reason = "TARGET_PREMIUM_BREAKDOWN"
        elif hold_sec >= self.NORMAL_EXIT_MIN_HOLD_SEC and opposite["strength"] >= self.OPPOSITE_EXPAND_STRENGTH and target["strength"] <= self.TARGET_WEAK_STRENGTH:
            reason = "OPPOSITE_EXPAND_TARGET_WEAKEN"
        elif hold_sec >= self.NORMAL_EXIT_MIN_HOLD_SEC and ((active_side == "CE" and fs["strength"] <= -self.FUTURE_REVERSAL_STRENGTH) or (active_side == "PE" and fs["strength"] >= self.FUTURE_REVERSAL_STRENGTH)) and target["strength"] <= self.TARGET_WEAK_STRENGTH:
            reason = "FUTURE_REVERSAL_TARGET_WEAKEN"
        elif hold_sec >= self.MAX_SCALP_HOLD_SEC and pnl_pct < self.STRONG_PROFIT_PCT:
            reason = "MAX_SCALP_HOLD_EXIT"
        return {"exit": reason != "NONE", "reason": reason, "confirm_required": confirm_required}

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

    def reset_trade_state(self, index: str, side: str, entry_price: float) -> None:
        now_ts = time.time()
        self.exit_confirm[index] = {"side": None, "count": 0, "first_ts": 0.0, "reason": None}
        self.last_entry_side[index] = side
        self.last_entry_ts[index] = now_ts
        self.trade_peak_state[index] = {
            "entry_price": float(entry_price),
            "best_price": float(entry_price),
            "best_pnl_pct": 0.0,
            "mfe_pct": 0.0,
            "last_peak_ts": now_ts,
        }

    def _guard_exit_or_flip(self, index: str, side: str, action: str, reason: str, hold_sec: float, pnl_pct: float, state: dict) -> Optional[TriWaveSignal]:
        if hold_sec < self.MIN_BREATHING_HOLD_SEC and reason != "FAST_ADVERSE_EXIT":
            logger.error(
                "TRI_WAVE_EXIT_BUG_BLOCKED | index=%s | side=%s | action=%s | reason=%s | hold_sec=%.2f | pnl_pct=%.2f",
                index, side, action, reason, hold_sec, pnl_pct
            )
            return self._signal(index, "NO_TRADE", None, 0.0, "EXIT_BUG_BLOCKED_BREATHING_WINDOW", state)
        return None

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
            self.trade_peak_state.pop(index, None)
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
                    self.exit_confirm[index] = {"side": None, "count": 0, "first_ts": 0.0, "reason": None}
                    self.trade_peak_state[index] = {"entry_price": float(cs["last"]), "best_price": float(cs["last"]), "best_pnl_pct": 0.0, "mfe_pct": 0.0, "last_peak_ts": now_ts}
                    logger.info("TRI_WAVE_TRADE_STATE_RESET | index=%s | side=%s | entry=%.2f", index, "CE", float(cs["last"]))
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
                    self.exit_confirm[index] = {"side": None, "count": 0, "first_ts": 0.0, "reason": None}
                    self.trade_peak_state[index] = {"entry_price": float(ps["last"]), "best_price": float(ps["last"]), "best_pnl_pct": 0.0, "mfe_pct": 0.0, "last_peak_ts": now_ts}
                    logger.info("TRI_WAVE_TRADE_STATE_RESET | index=%s | side=%s | entry=%.2f", index, "PE", float(ps["last"]))
                    return self._signal(index, "BUY_PE", "PE", conf, "+".join(reasons + reason_parts), state)
            logger.info("TRI_WAVE_REJECT | index=%s | reason=LOW_CONFIDENCE", index)
            return self._signal(index, "NO_TRADE", None, 0.0, "LOW_CONFIDENCE", state)

        hold_sec = now_ts - float(active_position.get("entry_ts", now_ts))
        entry_price = float(active_position.get("entry", 0.0) or 0.0)
        current_price = float(active_position.get("ltp", entry_price) or entry_price)
        pnl_pct = ((current_price - max(entry_price, 1e-9)) / max(entry_price, 1e-9)) * 100.0
        side = active_position.get("side")
        allow_emergency_exit = (
            hold_sec >= self.EARLY_FAST_ADVERSE_HOLD_SEC
            and pnl_pct <= self.EARLY_FAST_ADVERSE_PCT
        )
        if hold_sec < self.MIN_BREATHING_HOLD_SEC and not allow_emergency_exit:
            self.exit_confirm[index] = {"side": None, "count": 0, "first_ts": 0.0, "reason": None}
            logger.info(
                "TRI_WAVE_EXIT_GUARD | index=%s | side=%s | hold_sec=%.2f | pnl_pct=%.2f | guard=GLOBAL_BREATHING_WINDOW_ACTIVE",
                index, side, hold_sec, pnl_pct
            )
            return self._signal(index, "NO_TRADE", None, 0.0, "GLOBAL_BREATHING_WINDOW_ACTIVE", state)

        if side == "CE":
            scalp_eval = self._evaluate_premium_scalp_exit(index, "CE", entry_price, current_price, fs, cs, ps, hold_sec)
            reason_candidate, required_ticks = scalp_eval["reason"], scalp_eval["confirm_required"]
            target = cs
            peak = self.trade_peak_state.get(index, {})
            logger.info(
                "TRI_WAVE_EXIT_WATCH | index=%s | active_side=%s | hold_sec=%.2f | entry=%.2f | ltp=%.2f | pnl_pct=%.2f | best_pnl_pct=%.2f | giveback_pct=%.2f | fut_strength=%.2f | ce_strength=%.2f | pe_strength=%.2f | target_pos=%.2f | target_turn_down=%s | target_last5_delta=%.2f | reason_candidate=%s | confirm_count=%s",
                index, side, hold_sec, entry_price, current_price, pnl_pct, float(peak.get("best_pnl_pct", 0.0)), float(peak.get("giveback_pct", 0.0)), fs["strength"], cs["strength"], ps["strength"], target["position_in_range"], target["turn_down"], target["last_5_delta"], reason_candidate, self.exit_confirm[index]["count"]
            )
            if reason_candidate in {"NONE", "BREATHING_WINDOW_ACTIVE"}:
                self.exit_confirm[index] = {"side": None, "count": 0, "first_ts": 0.0, "reason": None}
            if reason_candidate == "BREATHING_WINDOW_ACTIVE":
                blocked_candidate = "FAST_ADVERSE_EXIT" if pnl_pct <= self.EARLY_FAST_ADVERSE_PCT else "NONE"
                logger.info("TRI_WAVE_EXIT_GUARD | index=%s | side=%s | hold_sec=%.2f | pnl_pct=%.2f | guard=BREATHING_WINDOW_ACTIVE | blocked_candidate=%s", index, side, hold_sec, pnl_pct, blocked_candidate)
            if scalp_eval["exit"]:
                confirmed = self._confirm_exit(index, side, True, reason_candidate, now_ts, required_ticks)
                if not confirmed:
                    logger.info("TRI_WAVE_EXIT_BLOCKED | reason=CONFIRMATION_PENDING | candidate=%s | count=%s | required=%s", reason_candidate, self.exit_confirm[index]["count"], required_ticks)
                    return self._signal(index, "NO_TRADE", None, 0.0, "EXIT_CONFIRMATION_PENDING", state)
                action = "EXIT_CE"
                logger.info("TRI_WAVE_EXIT_CONFIRMED | action=%s | reason=TRI_WAVE_EXIT:%s | hold_sec=%.2f | pnl_pct=%.2f | best_pnl_pct=%.2f | giveback_pct=%.2f | confirm_count=%s", action, reason_candidate, hold_sec, pnl_pct, float(peak.get("best_pnl_pct", 0.0)), float(peak.get("giveback_pct", 0.0)), self.exit_confirm[index]["count"])
                guard_signal = self._guard_exit_or_flip(index, side, action, reason_candidate, hold_sec, pnl_pct, state)
                if guard_signal:
                    return guard_signal
                return self._signal(index, action, side, 0.80, reason_candidate, state)

            ce_real_reversal = hold_sec >= self.NORMAL_EXIT_MIN_HOLD_SEC and (fut_down or fut_turn_down) and ps["strength"] >= self.OPPOSITE_EXPAND_MIN_STRENGTH and cs["strength"] <= self.CURRENT_WEAKEN_MIN_STRENGTH
            ce_profit_protect = hold_sec >= self.NORMAL_EXIT_MIN_HOLD_SEC and pnl_pct > 0.35 and cs["strength"] <= -0.10 and ps["strength"] >= 0.18
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
                        guard_signal = self._guard_exit_or_flip(index, side, "FLIP_TO_PE", "FUT_TURN_DOWN+PE_EXPAND+CE_WEAKEN", hold_sec, pnl_pct, state)
                        if guard_signal:
                            return guard_signal
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
                    guard_signal = self._guard_exit_or_flip(index, side, "EXIT_CE", reason, hold_sec, pnl_pct, state)
                    if guard_signal:
                        return guard_signal
                    return self._signal(index, "EXIT_CE", "CE", conf, reason, state)
        if side == "PE":
            scalp_eval = self._evaluate_premium_scalp_exit(index, "PE", entry_price, current_price, fs, cs, ps, hold_sec)
            reason_candidate, required_ticks = scalp_eval["reason"], scalp_eval["confirm_required"]
            target = ps
            peak = self.trade_peak_state.get(index, {})
            logger.info(
                "TRI_WAVE_EXIT_WATCH | index=%s | active_side=%s | hold_sec=%.2f | entry=%.2f | ltp=%.2f | pnl_pct=%.2f | best_pnl_pct=%.2f | giveback_pct=%.2f | fut_strength=%.2f | ce_strength=%.2f | pe_strength=%.2f | target_pos=%.2f | target_turn_down=%s | target_last5_delta=%.2f | reason_candidate=%s | confirm_count=%s",
                index, side, hold_sec, entry_price, current_price, pnl_pct, float(peak.get("best_pnl_pct", 0.0)), float(peak.get("giveback_pct", 0.0)), fs["strength"], cs["strength"], ps["strength"], target["position_in_range"], target["turn_down"], target["last_5_delta"], reason_candidate, self.exit_confirm[index]["count"]
            )
            if reason_candidate in {"NONE", "BREATHING_WINDOW_ACTIVE"}:
                self.exit_confirm[index] = {"side": None, "count": 0, "first_ts": 0.0, "reason": None}
            if reason_candidate == "BREATHING_WINDOW_ACTIVE":
                blocked_candidate = "FAST_ADVERSE_EXIT" if pnl_pct <= self.EARLY_FAST_ADVERSE_PCT else "NONE"
                logger.info("TRI_WAVE_EXIT_GUARD | index=%s | side=%s | hold_sec=%.2f | pnl_pct=%.2f | guard=BREATHING_WINDOW_ACTIVE | blocked_candidate=%s", index, side, hold_sec, pnl_pct, blocked_candidate)
            if scalp_eval["exit"]:
                confirmed = self._confirm_exit(index, side, True, reason_candidate, now_ts, required_ticks)
                if not confirmed:
                    logger.info("TRI_WAVE_EXIT_BLOCKED | reason=CONFIRMATION_PENDING | candidate=%s | count=%s | required=%s", reason_candidate, self.exit_confirm[index]["count"], required_ticks)
                    return self._signal(index, "NO_TRADE", None, 0.0, "EXIT_CONFIRMATION_PENDING", state)
                action = "EXIT_PE"
                logger.info("TRI_WAVE_EXIT_CONFIRMED | action=%s | reason=TRI_WAVE_EXIT:%s | hold_sec=%.2f | pnl_pct=%.2f | best_pnl_pct=%.2f | giveback_pct=%.2f | confirm_count=%s", action, reason_candidate, hold_sec, pnl_pct, float(peak.get("best_pnl_pct", 0.0)), float(peak.get("giveback_pct", 0.0)), self.exit_confirm[index]["count"])
                guard_signal = self._guard_exit_or_flip(index, side, action, reason_candidate, hold_sec, pnl_pct, state)
                if guard_signal:
                    return guard_signal
                return self._signal(index, action, side, 0.80, reason_candidate, state)

            pe_real_reversal = hold_sec >= self.NORMAL_EXIT_MIN_HOLD_SEC and (fut_up or fut_turn_up) and cs["strength"] >= self.OPPOSITE_EXPAND_MIN_STRENGTH and ps["strength"] <= self.CURRENT_WEAKEN_MIN_STRENGTH
            pe_profit_protect = hold_sec >= self.NORMAL_EXIT_MIN_HOLD_SEC and pnl_pct > 0.35 and ps["strength"] <= -0.10 and cs["strength"] >= 0.18
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
                        guard_signal = self._guard_exit_or_flip(index, side, "FLIP_TO_CE", "FUT_TURN_UP+CE_EXPAND+PE_WEAKEN", hold_sec, pnl_pct, state)
                        if guard_signal:
                            return guard_signal
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
                    guard_signal = self._guard_exit_or_flip(index, side, "EXIT_PE", reason, hold_sec, pnl_pct, state)
                    if guard_signal:
                        return guard_signal
                    return self._signal(index, "EXIT_PE", "PE", conf, reason, state)

        if self.last_signal.get(index) in {"BUY_CE", "BUY_PE"} and active_position:
            pass
        elif (now_ts - self.last_signal_ts.get(index, 0.0)) < self.SIGNAL_COOLDOWN_SEC:
            logger.info("TRI_WAVE_REJECT | index=%s | reason=COOLDOWN", index)
            return self._signal(index, "NO_TRADE", None, 0.0, "COOLDOWN", state)
        return self._signal(index, "NO_TRADE", None, 0.0, "LOW_CONFIDENCE", state)

    def get_latest_state(self, index: str) -> dict:
        return dict(self.latest_state.get(index, {}))
