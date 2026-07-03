from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import math


@dataclass(frozen=True)
class FiveMinuteZoneDecision:
    cycle_start: float
    cycle_end: float
    origin_price: float
    current_price: float
    displacement: float
    normalized_displacement: float
    velocity_per_sec: float
    zone: str
    direction: str


@dataclass
class _CycleState:
    cycle_start: float
    origin_price: float
    high: float
    low: float
    samples: deque[tuple[float, float]] = field(default_factory=lambda: deque(maxlen=4096))
    decision_emitted: bool = False


class FiveMinuteZoneTracker:
    """Observe the first half of a fixed cycle, then emit one zone decision."""

    def __init__(
        self,
        *,
        cycle_sec: float = 300.0,
        observe_sec: float = 150.0,
        confirm_sec: float = 10.0,
        entry_window_sec: float = 30.0,
        middle_zone_ratio: float = 0.25,
        strong_zone_ratio: float = 0.65,
        min_scale_ratio: float = 0.0005,
    ):
        self.cycle_sec = max(60.0, float(cycle_sec))
        self.observe_sec = max(1.0, min(float(observe_sec), self.cycle_sec - 2.0))
        self.confirm_sec = max(1.0, float(confirm_sec))
        self.entry_window_sec = max(1.0, float(entry_window_sec))
        self.middle_zone_ratio = max(0.0, float(middle_zone_ratio))
        self.strong_zone_ratio = max(self.middle_zone_ratio, float(strong_zone_ratio))
        self.min_scale_ratio = max(0.000001, float(min_scale_ratio))
        self._states: dict[str, _CycleState] = {}

    def cycle_start(self, now: float) -> float:
        return math.floor(float(now) / self.cycle_sec) * self.cycle_sec

    def cycle_end(self, now: float) -> float:
        return self.cycle_start(now) + self.cycle_sec

    def phase(self, now: float) -> str:
        elapsed = float(now) - self.cycle_start(now)
        if elapsed < self.observe_sec:
            return "OBSERVE"
        if elapsed < self.observe_sec + self.confirm_sec:
            return "CONFIRM"
        if elapsed <= self.observe_sec + self.confirm_sec + self.entry_window_sec:
            return "ENTRY"
        return "MANAGE"

    def update(self, key: str, price: float, now: float) -> FiveMinuteZoneDecision | None:
        price = float(price)
        now = float(now)
        if price <= 0:
            return None
        cycle_start = self.cycle_start(now)
        elapsed = now - cycle_start
        state = self._states.get(key)
        if state is None or state.cycle_start != cycle_start:
            # A cycle must be observed from its first half. Starting late cannot
            # manufacture history and therefore cannot produce an entry.
            if elapsed >= self.observe_sec:
                self._states.pop(key, None)
                return None
            state = _CycleState(cycle_start, price, price, price)
            self._states[key] = state

        state.high = max(state.high, price)
        state.low = min(state.low, price)
        state.samples.append((now, price))

        entry_start = self.observe_sec + self.confirm_sec
        entry_end = entry_start + self.entry_window_sec
        if elapsed < entry_start or elapsed > entry_end or state.decision_emitted:
            return None

        confirmation_start = cycle_start + self.observe_sec
        confirmation = [(ts, value) for ts, value in state.samples if ts >= confirmation_start]
        if len(confirmation) < 2:
            return None

        scale = max(
            abs(state.high - state.origin_price),
            abs(state.low - state.origin_price),
            abs(state.origin_price) * self.min_scale_ratio,
        )
        displacement = price - state.origin_price
        normalized = displacement / scale
        first_ts, first_price = confirmation[0]
        duration = max(now - first_ts, 0.001)
        velocity = (price - first_price) / duration
        sign = 1 if displacement > 0 else -1 if displacement < 0 else 0
        same_side = sum(
            1
            for _, value in confirmation
            if (value - state.origin_price > 0 and sign > 0)
            or (value - state.origin_price < 0 and sign < 0)
        )
        hold_ratio = same_side / len(confirmation)

        direction = "NEUTRAL"
        if normalized >= self.middle_zone_ratio and velocity > 0 and hold_ratio >= 0.70:
            direction = "POSITIVE"
        elif normalized <= -self.middle_zone_ratio and velocity < 0 and hold_ratio >= 0.70:
            direction = "NEGATIVE"

        magnitude = abs(normalized)
        zone = "NEAR"
        if magnitude >= self.strong_zone_ratio:
            zone = "STRONG"
        elif magnitude >= self.middle_zone_ratio:
            zone = "MIDDLE"

        state.decision_emitted = True
        return FiveMinuteZoneDecision(
            cycle_start=cycle_start,
            cycle_end=cycle_start + self.cycle_sec,
            origin_price=state.origin_price,
            current_price=price,
            displacement=displacement,
            normalized_displacement=normalized,
            velocity_per_sec=velocity,
            zone=zone,
            direction=direction,
        )

