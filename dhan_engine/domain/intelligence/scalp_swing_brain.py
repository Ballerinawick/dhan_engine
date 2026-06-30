from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


REGIME_CODE = {
    "CHOP": 0.0,
    "PULLBACK_RECLAIM": 1.0,
    "SCALP_EXPANSION": 2.0,
    "SWING_TREND": 3.0,
    "EXHAUSTION": 4.0,
    "HIGH_RISK": 5.0,
}

EXIT_PLAN_CODE = {
    "FAST_SCALP_EXIT": 1.0,
    "TRAILING_SWING_EXIT": 2.0,
    "DEFENSIVE_NO_TRADE": 3.0,
}


@dataclass(frozen=True)
class ScalpSwingDecision:
    regime: str
    exit_plan: str
    opportunity_score: float
    risk_score: float
    scalp_confidence: float
    swing_confidence: float
    ghost_candidate: bool

    def as_features(self) -> dict:
        return {
            "regime_code": REGIME_CODE[self.regime],
            "exit_plan_code": EXIT_PLAN_CODE[self.exit_plan],
            "opportunity_score": self.opportunity_score,
            "risk_score": self.risk_score,
            "scalp_confidence": self.scalp_confidence,
            "swing_confidence": self.swing_confidence,
            "ghost_candidate": 1.0 if self.ghost_candidate else 0.0,
        }


def evaluate_long_opportunity(
    *,
    base_score: float,
    return_1tick: float,
    return_5s: float,
    return_30s: float,
    return_120s: float,
    ltp_vs_avg: float,
    day_position: float,
    orderflow: float,
    liquidity: float,
    clean_trade: float,
    spoof_risk: float,
    spread_pct: float,
    max_spread_pct: float,
    profile: Mapping[str, float],
    exhaustion: float = 0.0,
    recovery: float = 0.0,
) -> ScalpSwingDecision:
    """Evaluate a long-only scalp/swing opportunity from normalized microstructure.

    This brain intentionally separates opportunity from risk. A low raw score can
    still be a valid scalp when microstructure is clean, but high risk can block
    even a high score.
    """

    spread_pressure = clamp((spread_pct / max(max_spread_pct, 1e-9)) * 100.0)
    extension_risk = max(0.0, day_position - float(profile.get("max_day_position", 96.0))) * 2.0
    low_range_risk = max(0.0, float(profile.get("min_day_position", 15.0)) - day_position) * 1.5
    risk_score = clamp(
        (0.30 * spoof_risk)
        + (0.22 * (100.0 - liquidity))
        + (0.18 * exhaustion)
        + (0.16 * spread_pressure)
        + (0.08 * extension_risk)
        + (0.06 * low_range_risk)
    )

    impulse = clamp(50.0 + (return_5s * 550.0) + (return_1tick * 250.0))
    trend = clamp(50.0 + (return_30s * 300.0) + (return_120s * 180.0))
    vwap_reclaim = clamp(50.0 + (ltp_vs_avg * 120.0))
    range_quality = clamp(100.0 - abs(day_position - 58.0) * 1.25)
    flow_quality = clamp((0.65 * orderflow) + (0.35 * clean_trade))

    opportunity_score = clamp(
        (0.28 * base_score)
        + (0.22 * impulse)
        + (0.16 * trend)
        + (0.15 * flow_quality)
        + (0.08 * liquidity)
        + (0.06 * vwap_reclaim)
        + (0.05 * range_quality)
        - (0.18 * risk_score)
    )

    scalp_confidence = clamp(
        (0.40 * opportunity_score)
        + (0.22 * impulse)
        + (0.18 * orderflow)
        + (0.10 * liquidity)
        + (0.10 * recovery)
        - (0.20 * risk_score)
    )
    swing_confidence = clamp(
        (0.34 * opportunity_score)
        + (0.30 * trend)
        + (0.14 * vwap_reclaim)
        + (0.12 * clean_trade)
        + (0.10 * liquidity)
        - (0.16 * risk_score)
    )

    if risk_score >= 70.0:
        regime = "HIGH_RISK"
        exit_plan = "DEFENSIVE_NO_TRADE"
    elif exhaustion >= 55.0 and day_position >= 88.0:
        regime = "EXHAUSTION"
        exit_plan = "FAST_SCALP_EXIT"
    elif swing_confidence >= float(profile.get("swing_score", 70.0)) and return_30s >= 0:
        regime = "SWING_TREND"
        exit_plan = "TRAILING_SWING_EXIT"
    elif scalp_confidence >= float(profile.get("scalp_score", 55.0)):
        regime = "SCALP_EXPANSION" if return_5s >= 0 else "PULLBACK_RECLAIM"
        exit_plan = "FAST_SCALP_EXIT"
    else:
        regime = "CHOP"
        exit_plan = "DEFENSIVE_NO_TRADE"

    threshold = min(float(profile.get("scalp_score", 55.0)), float(profile.get("swing_score", 70.0)))
    ghost_candidate = (
        risk_score < 68.0
        and max(scalp_confidence, swing_confidence) >= threshold - 8.0
    )

    return ScalpSwingDecision(
        regime=regime,
        exit_plan=exit_plan,
        opportunity_score=opportunity_score,
        risk_score=risk_score,
        scalp_confidence=scalp_confidence,
        swing_confidence=swing_confidence,
        ghost_candidate=ghost_candidate,
    )
