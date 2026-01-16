from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Literal, Optional, Tuple

Side = Literal["LONG", "SHORT"]
Signal = Literal["A_ENTRY", "B_ENTRY", "EXIT", "NO_TRADE"]


@dataclass(frozen=True)
class ScoringInputs:
    """
    Structured inputs for a formal scoring model.

    Expected keys are designed to align with DepthMicroFeatureBuilder output
    and the OptionsMomentumEngine candle stats.
    """

    # microstructure
    imbalance_5: float
    flow: float
    vacuum_flag: bool
    absorption_flag: bool
    absorption_strength: float

    # price action / volatility
    price_speed: float
    avg_range: float
    avg_volume: float
    last_volume: float

    # regime awareness
    day_index: int
    time_regime: str


@dataclass(frozen=True)
class ScoreResult:
    score: float
    components: Dict[str, float]


def score_momentum(inputs: ScoringInputs) -> ScoreResult:
    """
    Formally defined scoring model.

    Score is a weighted sum of normalized components.
    Positive => bullish momentum, negative => bearish momentum.
    """
    vol_ratio = (inputs.last_volume / inputs.avg_volume) if inputs.avg_volume > 0 else 0.0
    speed_ratio = (inputs.price_speed / inputs.avg_range) if inputs.avg_range > 0 else 0.0

    components = {
        "imbalance": 0.35 * inputs.imbalance_5,
        "flow": 0.20 * (inputs.flow / max(inputs.avg_range, 1e-9)),
        "speed": 0.25 * speed_ratio,
        "volume": 0.20 * vol_ratio,
        "vacuum": -0.30 if inputs.vacuum_flag else 0.0,
        "absorption": 0.15 * inputs.absorption_strength if inputs.absorption_flag else 0.0,
    }

    # regime dampener (late day reduces conviction)
    day_decay = 1.0 - min(max(inputs.day_index, 0), 5) * 0.05
    regime_decay = 0.9 if inputs.time_regime in {"CLOSE"} else 1.0

    raw_score = sum(components.values())
    score = raw_score * day_decay * regime_decay
    return ScoreResult(score=score, components=components)


def expected_move(
    avg_range: float,
    avg_volume: float,
    last_volume: float,
    time_regime: str,
    day_index: int,
) -> float:
    """
    Expected Move (EM) formula:

    EM = avg_range * volume_factor * regime_factor * day_factor

    - volume_factor: min(2.0, last_volume / avg_volume) to avoid blow-up
    - regime_factor: OPEN 1.15, MID 1.0, TREND 1.05, CLOSE 0.9
    - day_factor: 1 - 0.05 * day_index (caps at 0.75 on day5)
    """
    if avg_range <= 0:
        return 0.0

    volume_factor = (last_volume / avg_volume) if avg_volume > 0 else 1.0
    volume_factor = min(2.0, max(0.5, volume_factor))

    regime_factor = {
        "OPEN": 1.15,
        "MID": 1.0,
        "TREND": 1.05,
        "CLOSE": 0.9,
    }.get(time_regime, 1.0)

    day_factor = max(0.75, 1.0 - (0.05 * max(day_index, 0)))

    return avg_range * volume_factor * regime_factor * day_factor


def zone_stop_target(
    entry: float,
    side: Side,
    expected_move_points: float,
    zone: Literal["tight", "normal", "wide"] = "normal",
) -> Tuple[float, float]:
    """
    Zone-based stop/target calculator.

    - tight: 0.6x EM stop, 1.0x EM target
    - normal: 0.8x EM stop, 1.4x EM target
    - wide: 1.0x EM stop, 1.8x EM target
    """
    if expected_move_points <= 0:
        return entry, entry

    stop_mult, target_mult = {
        "tight": (0.6, 1.0),
        "normal": (0.8, 1.4),
        "wide": (1.0, 1.8),
    }[zone]

    stop_dist = expected_move_points * stop_mult
    target_dist = expected_move_points * target_mult

    if side == "LONG":
        stop = entry - stop_dist
        target = entry + target_dist
    else:
        stop = entry + stop_dist
        target = entry - target_dist

    return stop, target


def decision_tree(
    score: float,
    expected_move_points: float,
    has_position: bool,
    cooldown_active: bool,
    min_em: float = 0.5,
    entry_threshold: float = 1.0,
    exit_threshold: float = 0.2,
) -> Signal:
    """
    Decision tree compatible with the current engine flow.

    - If cooldown active => NO_TRADE
    - If no position:
        * require expected move >= min_em
        * score >= entry_threshold => A_ENTRY
        * score <= -entry_threshold => B_ENTRY (short bias)
    - If has position:
        * abs(score) <= exit_threshold => EXIT
        * else NO_TRADE
    """
    if cooldown_active:
        return "NO_TRADE"

    if expected_move_points < min_em:
        return "NO_TRADE"

    if not has_position:
        if score >= entry_threshold:
            return "A_ENTRY"
        if score <= -entry_threshold:
            return "B_ENTRY"
        return "NO_TRADE"

    if abs(score) <= exit_threshold:
        return "EXIT"
    return "NO_TRADE"
