from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional

from dhan_engine.domain.intelligence.scalp_swing_brain import evaluate_long_opportunity


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def _pct_change(current: float, previous: float) -> float:
    if current <= 0 or previous <= 0:
        return 0.0
    return ((current - previous) / previous) * 100.0


@dataclass(frozen=True)
class CommodityPercentSignal:
    symbol: str
    action: str
    score: float
    ltp: float
    reason: str
    features: Dict[str, float]


COMMODITY_PROFILES = {
    "GOLD": {
        "scalp_score": 58.0,
        "swing_score": 70.0,
        "ret5": 0.015,
        "ret30": -0.010,
        "swing_ret30": 0.025,
        "ret120": -0.020,
        "swing_ret120": 0.030,
        "vwap": -0.080,
        "max_spread": 0.060,
        "clean": 40.0,
        "spoof": 65.0,
        "min_day_position": 18.0,
        "max_day_position": 96.0,
        "min_orderflow": 35.0,
    },
    "CRUDEOIL": {
        "scalp_score": 58.0,
        "swing_score": 72.0,
        "ret5": 0.015,
        "ret30": -0.015,
        "swing_ret30": 0.040,
        "ret120": -0.020,
        "swing_ret120": 0.030,
        "vwap": -0.050,
        "max_spread": 0.060,
        "clean": 35.0,
        "spoof": 70.0,
        "min_day_position": 25.0,
        "max_day_position": 97.0,
        "min_orderflow": 40.0,
    },
    "NATURALGAS": {
        "scalp_score": 59.0,
        "swing_score": 74.0,
        "ret5": 0.020,
        "ret30": -0.020,
        "swing_ret30": 0.080,
        "ret120": -0.020,
        "swing_ret120": 0.050,
        "vwap": -0.050,
        "max_spread": 0.080,
        "clean": 45.0,
        "spoof": 60.0,
        "min_day_position": 22.0,
        "max_day_position": 94.0,
        "min_orderflow": 45.0,
    },
}

DEFAULT_PROFILE = {
    "scalp_score": 59.0,
    "swing_score": 72.0,
    "ret5": 0.020,
    "ret30": -0.010,
    "swing_ret30": 0.060,
    "ret120": -0.020,
    "swing_ret120": 0.040,
    "vwap": 0.000,
    "max_spread": 0.080,
    "clean": 40.0,
    "spoof": 65.0,
    "min_day_position": 20.0,
    "max_day_position": 96.0,
    "min_orderflow": 40.0,
}


class PercentNormalizedCommodityEngine:
    """Price-independent long-only MCX commodity paper signal engine."""

    def __init__(
        self,
        *,
        entry_score: float = 68.0,
        exit_score: float = 38.0,
        min_samples: int = 12,
        max_samples: int = 1800,
        max_spread_pct: float = 0.22,
    ):
        self.entry_score = float(entry_score)
        self.exit_score = float(exit_score)
        self.min_samples = max(int(min_samples), 3)
        self.max_spread_pct = float(max_spread_pct)
        self.history: Dict[str, Deque[dict]] = defaultdict(lambda: deque(maxlen=max_samples))

    @staticmethod
    def _profile(symbol: str) -> Dict[str, float]:
        profile = dict(DEFAULT_PROFILE)
        profile.update(COMMODITY_PROFILES.get(str(symbol).upper().strip(), {}))
        return profile

    @staticmethod
    def _price_at_or_before(history: Deque[dict], target_ts: float) -> float:
        for sample in reversed(history):
            if float(sample["ts"]) <= target_ts:
                return float(sample["ltp"])
        return float(history[0]["ltp"]) if history else 0.0

    def on_tick(
        self,
        symbol: str,
        ltp: float,
        raw_features: Optional[dict],
        ts: float,
        *,
        in_position: bool,
    ) -> CommodityPercentSignal:
        root = str(symbol).upper().strip()
        price = float(ltp)
        features = dict(raw_features or {})
        samples = self.history[root]
        previous = float(samples[-1]["ltp"]) if samples else price
        samples.append({"ts": float(ts), "ltp": price})

        return_1tick = _pct_change(price, previous)
        return_5s = _pct_change(price, self._price_at_or_before(samples, ts - 5.0))
        return_30s = _pct_change(price, self._price_at_or_before(samples, ts - 30.0))
        return_120s = _pct_change(price, self._price_at_or_before(samples, ts - 120.0))
        intraday_return = float(features.get("intraday_return_pct", 0.0) or 0.0)
        ltp_vs_avg = float(features.get("ltp_vs_avg_pct", 0.0) or 0.0)
        day_position = _clamp(float(features.get("day_position", 0.5) or 0.5) * 100.0)
        depth_imbalance = max(-1.0, min(1.0, float(features.get("depth_imbalance_5", 0.0) or 0.0)))
        market_imbalance = max(-1.0, min(1.0, float(features.get("market_queue_imbalance", 0.0) or 0.0)))
        spread_pct = max(float(features.get("spread_pct", 0.0) or 0.0), 0.0)
        clean_trade = _clamp(float(features.get("clean_trade_score", 0.50) or 0.50) * 100.0)
        spoof_risk = _clamp(float(features.get("spoof_risk", 0.0) or 0.0) * 100.0)
        top_depth_imbalance = max(-1.0, min(1.0, float(features.get("top_depth_imbalance", 0.0) or 0.0)))
        recovery_score = _clamp(float(features.get("recovery_score", 0.0) or 0.0) * 100.0)
        exhaustion_score = _clamp(float(features.get("exhaustion_score", 0.0) or 0.0) * 100.0)
        volume_change_tick = float(features.get("volume_change_tick", 0.0) or 0.0)
        oi_change_tick = float(features.get("oi_change_tick", 0.0) or 0.0)

        fast_momentum = _clamp(50.0 + (return_5s * 190.0))
        slow_momentum = _clamp(50.0 + (return_30s * 120.0))
        session_momentum = _clamp(50.0 + (return_120s * 70.0))
        intraday_trend = _clamp(50.0 + (intraday_return * 30.0))
        vwap_bias = _clamp(50.0 + (ltp_vs_avg * 70.0))
        orderflow = _clamp(
            50.0
            + (depth_imbalance * 24.0)
            + (top_depth_imbalance * 12.0)
            + (market_imbalance * 16.0)
            + (recovery_score * 0.08)
            - (exhaustion_score * 0.08)
        )
        liquidity = _clamp(100.0 - ((spread_pct / max(self.max_spread_pct, 1e-9)) * 100.0))
        profile = self._profile(root)
        effective_scalp_score = float(profile["scalp_score"])
        effective_swing_score = max(float(self.entry_score), float(profile["swing_score"]))
        effective_max_spread = min(float(self.max_spread_pct), float(profile["max_spread"]))

        score = _clamp(
            (0.22 * fast_momentum)
            + (0.18 * slow_momentum)
            + (0.10 * session_momentum)
            + (0.12 * intraday_trend)
            + (0.12 * vwap_bias)
            + (0.13 * orderflow)
            + (0.06 * day_position)
            + (0.05 * liquidity)
            + (0.04 * clean_trade)
            - (0.04 * spoof_risk)
        )
        brain = evaluate_long_opportunity(
            base_score=score,
            return_1tick=return_1tick,
            return_5s=return_5s,
            return_30s=return_30s,
            return_120s=return_120s,
            ltp_vs_avg=ltp_vs_avg,
            day_position=day_position,
            orderflow=orderflow,
            liquidity=liquidity,
            clean_trade=clean_trade,
            spoof_risk=spoof_risk,
            spread_pct=spread_pct,
            max_spread_pct=effective_max_spread,
            profile=profile,
            exhaustion=exhaustion_score,
            recovery=recovery_score,
        )
        normalized = {
            "return_1tick_pct": return_1tick,
            "return_5s_pct": return_5s,
            "return_30s_pct": return_30s,
            "return_120s_pct": return_120s,
            "intraday_return_pct": intraday_return,
            "ltp_vs_avg_pct": ltp_vs_avg,
            "day_position_pct": day_position,
            "depth_imbalance_pct": depth_imbalance * 100.0,
            "top_depth_imbalance_pct": top_depth_imbalance * 100.0,
            "market_imbalance_pct": market_imbalance * 100.0,
            "spread_pct": spread_pct,
            "clean_trade_pct": clean_trade,
            "spoof_risk_pct": spoof_risk,
            "recovery_pct": recovery_score,
            "exhaustion_pct": exhaustion_score,
            "volume_change_tick": volume_change_tick,
            "oi_change_tick": oi_change_tick,
            "orderflow_score": orderflow,
            "liquidity_score": liquidity,
            "effective_scalp_score": effective_scalp_score,
            "effective_swing_score": effective_swing_score,
            "effective_max_spread_pct": effective_max_spread,
            "profile_ret5_min": float(profile["ret5"]),
            "profile_ret30_min": float(profile["ret30"]),
            "profile_ret120_min": float(profile["ret120"]),
            "profile_swing_ret30_min": float(profile["swing_ret30"]),
            "profile_swing_ret120_min": float(profile["swing_ret120"]),
            "profile_vwap_min": float(profile["vwap"]),
            "profile_clean_min": float(profile["clean"]),
            "profile_spoof_max": float(profile["spoof"]),
            "score": score,
            "sample_count": float(len(samples)),
        }
        normalized.update(brain.as_features())

        if len(samples) < self.min_samples:
            return CommodityPercentSignal(root, "HOLD", score, price, "WARMUP", normalized)
        if spread_pct > effective_max_spread:
            return CommodityPercentSignal(root, "HOLD", score, price, "SPREAD_TOO_WIDE", normalized)
        if in_position and score <= self.exit_score:
            return CommodityPercentSignal(root, "EXIT", score, price, "COMMODITY_SCORE_BREAKDOWN", normalized)
        if in_position:
            return CommodityPercentSignal(root, "HOLD", score, price, "POSITION_HELD", normalized)

        shared_checks = [
            ("CLEAN_TRADE_WEAK", clean_trade >= float(profile["clean"])),
            ("SPOOF_RISK_HIGH", spoof_risk <= float(profile["spoof"])),
            ("DAY_POSITION_LOW", day_position >= float(profile["min_day_position"])),
            ("DAY_POSITION_EXTENDED", day_position <= float(profile["max_day_position"])),
            ("ORDERFLOW_WEAK", orderflow >= float(profile["min_orderflow"])),
            ("SMART_RISK_HIGH", brain.risk_score <= 68.0),
        ]
        for reason, passed in shared_checks:
            if not passed:
                return CommodityPercentSignal(root, "HOLD", score, price, reason, normalized)

        swing_checks = [
            ("SWING_SCORE_BELOW_PROFILE", brain.swing_confidence >= effective_swing_score),
            ("SWING_RAW_SCORE_BELOW_PROFILE", score >= effective_swing_score),
            ("SWING_RET30_WEAK", return_30s >= float(profile["swing_ret30"])),
            ("SWING_RET120_WEAK", return_120s >= float(profile["swing_ret120"])),
            ("SWING_VWAP_BIAS_WEAK", ltp_vs_avg >= max(0.0, float(profile["vwap"]))),
        ]
        if all(passed for _, passed in swing_checks):
            normalized["entry_mode"] = 2.0
            return CommodityPercentSignal(root, "ENTRY", score, price, "COMMODITY_SWING_MOMENTUM_ALIGNMENT", normalized)

        scalp_checks = [
            ("SCALP_SCORE_BELOW_PROFILE", brain.scalp_confidence >= effective_scalp_score),
            ("SCALP_RET5_WEAK", return_5s >= float(profile["ret5"])),
            ("SCALP_RET30_WEAK", return_30s >= float(profile["ret30"])),
            ("SCALP_RET120_WEAK", return_120s >= float(profile["ret120"])),
            ("SCALP_VWAP_BIAS_WEAK", ltp_vs_avg >= float(profile["vwap"])),
            ("SCALP_TICK_NOT_POSITIVE", return_1tick >= 0.0),
        ]
        if all(passed for _, passed in scalp_checks):
            normalized["entry_mode"] = 1.0
            return CommodityPercentSignal(root, "ENTRY", score, price, "COMMODITY_SCALP_MOMENTUM_ALIGNMENT", normalized)

        for reason, passed in scalp_checks + swing_checks:
            if not passed:
                return CommodityPercentSignal(root, "HOLD", score, price, reason, normalized)
        return CommodityPercentSignal(root, "HOLD", score, price, "NO_CONFIRMED_EDGE", normalized)
