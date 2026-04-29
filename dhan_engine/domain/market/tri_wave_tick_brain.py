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

    MIN_HOLD_BEFORE_NORMAL_EXIT_SEC = 25
    MIN_HOLD_BEFORE_FLIP_SEC = 35
    EXIT_CONFIRM_TICKS = 3
    ENTRY_COOLDOWN_SEC = 45
    REENTRY_SAME_SIDE_COOLDOWN_SEC = 60
    EMERGENCY_LOSS_PCT = -8.0

    FUT_MIN_STRENGTH = 0.18
    OPTION_EXPAND_MIN_STRENGTH = 0.20
    OPTION_WEAKEN_MIN_STRENGTH = -0.12
    OPPOSITE_EXPAND_MIN_STRENGTH = 0.28
    CURRENT_WEAKEN_MIN_STRENGTH = -0.22

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

    def _confirm_exit(self, index: str, side: str, condition: bool, reason: str, now_ts: float) -> bool:
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
        if slot["count"] >= self.EXIT_CONFIRM_TICKS:
            logger.info("TRI_WAVE_EXIT_CONFIRMED | index=%s | side=%s | reason=%s | count=%s", index, side, reason, slot["count"])
            return True
        return False

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
        rng = max(max(prices) - min(prices), 0.05)
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
            logger.info("TRI_WAVE_STATE | index=%s | sync_ready=True | fut_dir=%s | fut_strength=%.2f | ce_strength=%.2f | pe_strength=%.2f | bias=%s | active=%s", index, fs["direction"], fs["strength"], cs["strength"], ps["strength"], bias, (active_position or {}).get("side"))

        # entries
        if not active_position:
            if (fut_up or fut_turn_up) and ce_expand and pe_weaken:
                conf = 0.70 + conf_adj + (0.03 if cs.get("bull_turn_score", 0) and cs.get("bull_turn_score", 0) > 0 else 0)
                reasons = ["FUT_TURN_UP" if fut_turn_up else "FUT_TREND_UP", "CE_EXPAND", "PE_WEAKEN"]
                if conf >= self.ENTRY_CONFIDENCE:
                    if (now_ts - self.last_signal_ts.get(index, 0.0)) < self.ENTRY_COOLDOWN_SEC:
                        logger.info("TRI_WAVE_ENTRY_COOLDOWN | index=%s | side=CE | remaining=%.2f", index, self.ENTRY_COOLDOWN_SEC - (now_ts - self.last_signal_ts.get(index, 0.0)))
                        return self._signal(index, "NO_TRADE", None, 0.0, "ENTRY_COOLDOWN", state)
                    if self.last_entry_side.get(index) == "CE" and (now_ts - self.last_entry_ts.get(index, 0.0)) < self.REENTRY_SAME_SIDE_COOLDOWN_SEC:
                        logger.info("TRI_WAVE_REENTRY_BLOCK | index=%s | side=CE | remaining=%.2f", index, self.REENTRY_SAME_SIDE_COOLDOWN_SEC - (now_ts - self.last_entry_ts.get(index, 0.0)))
                        return self._signal(index, "NO_TRADE", None, 0.0, "REENTRY_SAME_SIDE_COOLDOWN", state)
                    self.last_entry_side[index] = "CE"
                    self.last_entry_ts[index] = now_ts
                    return self._signal(index, "BUY_CE", "CE", conf, "+".join(reasons + reason_parts), state)
            if (fut_down or fut_turn_down) and pe_expand and ce_weaken:
                conf = 0.70 + conf_adj + (0.03 if ps.get("bull_turn_score", 0) and ps.get("bull_turn_score", 0) > 0 else 0)
                reasons = ["FUT_TURN_DOWN" if fut_turn_down else "FUT_TREND_DOWN", "PE_EXPAND", "CE_WEAKEN"]
                if conf >= self.ENTRY_CONFIDENCE:
                    if (now_ts - self.last_signal_ts.get(index, 0.0)) < self.ENTRY_COOLDOWN_SEC:
                        logger.info("TRI_WAVE_ENTRY_COOLDOWN | index=%s | side=PE | remaining=%.2f", index, self.ENTRY_COOLDOWN_SEC - (now_ts - self.last_signal_ts.get(index, 0.0)))
                        return self._signal(index, "NO_TRADE", None, 0.0, "ENTRY_COOLDOWN", state)
                    if self.last_entry_side.get(index) == "PE" and (now_ts - self.last_entry_ts.get(index, 0.0)) < self.REENTRY_SAME_SIDE_COOLDOWN_SEC:
                        logger.info("TRI_WAVE_REENTRY_BLOCK | index=%s | side=PE | remaining=%.2f", index, self.REENTRY_SAME_SIDE_COOLDOWN_SEC - (now_ts - self.last_entry_ts.get(index, 0.0)))
                        return self._signal(index, "NO_TRADE", None, 0.0, "REENTRY_SAME_SIDE_COOLDOWN", state)
                    self.last_entry_side[index] = "PE"
                    self.last_entry_ts[index] = now_ts
                    return self._signal(index, "BUY_PE", "PE", conf, "+".join(reasons + reason_parts), state)
            logger.info("TRI_WAVE_REJECT | index=%s | reason=LOW_CONFIDENCE", index)
            return self._signal(index, "NO_TRADE", None, 0.0, "LOW_CONFIDENCE", state)

        hold_sec = now_ts - float(active_position.get("entry_ts", now_ts))
        pnl_pct = float(active_position.get("pnl_pct", 0.0))
        emergency = pnl_pct <= self.EMERGENCY_LOSS_PCT
        side = active_position.get("side")
        if side == "CE":
            ce_real_reversal = (fut_down or fut_turn_down) and ps["strength"] >= self.OPPOSITE_EXPAND_MIN_STRENGTH and cs["strength"] <= self.CURRENT_WEAKEN_MIN_STRENGTH
            ce_profit_protect = pnl_pct > 0.35 and cs["strength"] <= -0.10 and ps["strength"] >= 0.18
            ce_emergency = pnl_pct <= self.EMERGENCY_LOSS_PCT
            logger.info("TRI_WAVE_EXIT_WATCH | index=%s | side=CE | hold=%.2f | pnl_pct=%.2f | current_strength=%.2f | opposite_strength=%.2f | fut_strength=%.2f | confirm_count=%s", index, hold_sec, pnl_pct, cs["strength"], ps["strength"], fs["strength"], self.exit_confirm[index]["count"])
            ce_exit_reason = "REAL_REVERSAL" if ce_real_reversal else "PROFIT_PROTECT" if ce_profit_protect else "NONE"
            ce_confirmed = self._confirm_exit(index, "CE", ce_real_reversal or ce_profit_protect, ce_exit_reason, now_ts)
            if ce_emergency:
                return self._signal(index, "EXIT_CE", "CE", 0.65 + conf_adj, "EMERGENCY_LOSS", state)
            if hold_sec >= self.MIN_HOLD_BEFORE_FLIP_SEC and ce_real_reversal and ce_confirmed:
                conf = 0.74 + conf_adj
                if conf >= self.FLIP_CONFIDENCE:
                    return self._signal(index, "FLIP_TO_PE", "PE", conf, "FUT_TURN_DOWN+PE_EXPAND+CE_WEAKEN", state)
            if hold_sec >= self.MIN_HOLD_BEFORE_NORMAL_EXIT_SEC and ce_confirmed:
                conf = 0.65 + conf_adj
                if conf >= self.EXIT_CONFIDENCE:
                    reason = "CE_REAL_REVERSAL_CONFIRMED" if ce_exit_reason == "REAL_REVERSAL" else "CE_PROFIT_PROTECT_CONFIRMED"
                    return self._signal(index, "EXIT_CE", "CE", conf, reason, state)
        if side == "PE":
            pe_real_reversal = (fut_up or fut_turn_up) and cs["strength"] >= self.OPPOSITE_EXPAND_MIN_STRENGTH and ps["strength"] <= self.CURRENT_WEAKEN_MIN_STRENGTH
            pe_profit_protect = pnl_pct > 0.35 and ps["strength"] <= -0.10 and cs["strength"] >= 0.18
            pe_emergency = pnl_pct <= self.EMERGENCY_LOSS_PCT
            logger.info("TRI_WAVE_EXIT_WATCH | index=%s | side=PE | hold=%.2f | pnl_pct=%.2f | current_strength=%.2f | opposite_strength=%.2f | fut_strength=%.2f | confirm_count=%s", index, hold_sec, pnl_pct, ps["strength"], cs["strength"], fs["strength"], self.exit_confirm[index]["count"])
            pe_exit_reason = "REAL_REVERSAL" if pe_real_reversal else "PROFIT_PROTECT" if pe_profit_protect else "NONE"
            pe_confirmed = self._confirm_exit(index, "PE", pe_real_reversal or pe_profit_protect, pe_exit_reason, now_ts)
            if pe_emergency:
                return self._signal(index, "EXIT_PE", "PE", 0.65 + conf_adj, "EMERGENCY_LOSS", state)
            if hold_sec >= self.MIN_HOLD_BEFORE_FLIP_SEC and pe_real_reversal and pe_confirmed:
                conf = 0.74 + conf_adj
                if conf >= self.FLIP_CONFIDENCE:
                    return self._signal(index, "FLIP_TO_CE", "CE", conf, "FUT_TURN_UP+CE_EXPAND+PE_WEAKEN", state)
            if hold_sec >= self.MIN_HOLD_BEFORE_NORMAL_EXIT_SEC and pe_confirmed:
                conf = 0.65 + conf_adj
                if conf >= self.EXIT_CONFIDENCE:
                    reason = "PE_REAL_REVERSAL_CONFIRMED" if pe_exit_reason == "REAL_REVERSAL" else "PE_PROFIT_PROTECT_CONFIRMED"
                    return self._signal(index, "EXIT_PE", "PE", conf, reason, state)

        if self.last_signal.get(index) in {"BUY_CE", "BUY_PE"} and active_position:
            pass
        elif (now_ts - self.last_signal_ts.get(index, 0.0)) < self.SIGNAL_COOLDOWN_SEC:
            logger.info("TRI_WAVE_REJECT | index=%s | reason=COOLDOWN", index)
            return self._signal(index, "NO_TRADE", None, 0.0, "COOLDOWN", state)
        return self._signal(index, "NO_TRADE", None, 0.0, "LOW_CONFIDENCE", state)

    def get_latest_state(self, index: str) -> dict:
        return dict(self.latest_state.get(index, {}))
