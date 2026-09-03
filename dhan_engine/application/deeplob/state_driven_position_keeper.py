from __future__ import annotations

import math
import statistics
from collections import deque
from dataclasses import asdict, dataclass
from typing import Mapping


def _clip(value: float) -> float:
    return max(-1.0, min(1.0, float(value)))


def _relative(value: float, scale: float) -> float:
    return _clip(float(value) / max(abs(float(scale)), 0.0001))


@dataclass(frozen=True)
class KeeperDecision:
    action: str
    phase: str
    context: str
    reason: str
    support_score: float
    fast_support: float
    context_support: float
    gross_pnl: float
    net_pnl: float
    best_observed_pnl: float
    worst_observed_pnl: float
    surrendered_mfe: float
    mfe_capture_ratio: float | None
    observations: int

    def as_dict(self) -> dict:
        return asdict(self)


class StateDrivenPositionKeeper:
    """Retain an option position while its executable market state remains valid.

    Normal decisions are based on the trajectory of market-state evidence, not on
    a fixed hold time, target, stop, or trailing percentage. The caller remains
    responsible for market-close and catastrophic safeguards.
    """

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.secid: int | None = None
        self.side = ""
        self.phase = "OBSERVING"
        self.best_observed_pnl = float("-inf")
        self.worst_observed_pnl = float("inf")
        self.best_observed_bid = 0.0
        self.best_observed_ts = 0.0
        self.worst_observed_ts = 0.0
        self._support = deque(maxlen=256)
        self._last_decision: KeeperDecision | None = None
        self._challenge_support: float | None = None
        self._recovery_support: float | None = None
        self._recovery_pnl = float("-inf")

    @property
    def snapshot(self) -> dict:
        result = self._last_decision.as_dict() if self._last_decision else {}
        result.update(
            best_observed_pnl=(
                self.best_observed_pnl
                if self.best_observed_pnl != float("-inf")
                else None
            ),
            worst_observed_pnl=(
                self.worst_observed_pnl
                if self.worst_observed_pnl != float("inf")
                else None
            ),
            best_observed_bid=self.best_observed_bid,
            best_observed_ts=self.best_observed_ts,
            worst_observed_ts=self.worst_observed_ts,
        )
        return result

    def mark_excursion(
        self,
        *,
        position: Mapping,
        side: str,
        bid: float,
        received_ts: float,
    ) -> None:
        secid = int(position.get("secid", 0) or 0)
        if self.secid != secid:
            self.reset()
            self.secid = secid
            self.side = str(side).upper()
        entry = float(position.get("entry", 0.0) or 0.0)
        qty = max(int(position.get("qty", 0) or 0), 1)
        self._record_excursion(
            (float(bid) - entry) * qty,
            float(bid),
            float(received_ts),
        )

    def observe(
        self,
        *,
        position: Mapping,
        side: str,
        bid: float,
        received_ts: float,
        evidence: Mapping,
        stable_state: str,
        instant_state: str,
        round_trip_fee: float,
    ) -> KeeperDecision:
        secid = int(position.get("secid", 0) or 0)
        entry = float(position.get("entry", 0.0) or 0.0)
        qty = max(int(position.get("qty", 0) or 0), 1)
        gross_pnl = (float(bid) - entry) * qty
        net_pnl = gross_pnl - float(round_trip_fee)
        self.mark_excursion(
            position=position,
            side=side,
            bid=bid,
            received_ts=received_ts,
        )

        support_score = self._aligned_support(
            self.side, evidence, stable_state, instant_state
        )
        previous_support = self._support[-1][1] if self._support else support_score
        previous_pnl = self._support[-1][2] if self._support else gross_pnl
        self._support.append((float(received_ts), support_score, gross_pnl))
        fast_support, context_support = self._support_layers()
        context = self._context(fast_support, context_support)
        previous_phase = self.phase

        supportive = support_score > 0.0 and fast_support > 0.0 and context_support >= 0.0
        opposing_state = self._is_opposing_state(self.side, stable_state)
        exhaustion_state = self._is_matching_exhaustion(self.side, stable_state)
        opposing = (
            support_score < 0.0
            and fast_support < 0.0
            and (opposing_state or exhaustion_state)
        )
        earned_move = self.best_observed_pnl > float(round_trip_fee)
        state_and_price_weakening = (
            support_score < previous_support and gross_pnl < previous_pnl
        )

        action = "HOLD"
        reason = "STATE_SUPPORTED"
        if earned_move and state_and_price_weakening:
            if previous_phase in {"PULLBACK", "RECOVERY", "CHALLENGED"}:
                self.phase = "FAILED_RECOVERY"
                action = "EXIT"
                reason = "EARNED_MOVE_STATE_DECAY"
            else:
                self.phase = "PULLBACK"
                action = "DEFEND"
                reason = "EARNED_MOVE_PULLBACK"
        elif supportive:
            recovery_confirmed = previous_phase == "RECOVERY" and (
                support_score >= float(self._recovery_support or 0.0)
                and gross_pnl >= self._recovery_pnl
            )
            if recovery_confirmed:
                self.phase = "SUPPORTED"
                self._challenge_support = None
                self._recovery_support = None
                self._recovery_pnl = float("-inf")
                reason = "STATE_RECOVERY_CONFIRMED"
            elif previous_phase in {"PULLBACK", "CHALLENGED", "RECOVERY"}:
                self.phase = "RECOVERY"
                self._recovery_support = support_score
                self._recovery_pnl = max(self._recovery_pnl, gross_pnl)
                reason = "STATE_RECOVERY"
            else:
                self.phase = "SUPPORTED"
        elif opposing:
            failed_recovery = previous_phase == "RECOVERY" and (
                support_score < float(self._recovery_support or 0.0)
                or gross_pnl < self._recovery_pnl
            )
            continued_challenge = previous_phase == "CHALLENGED" and (
                support_score <= float(self._challenge_support or 0.0)
            )
            if failed_recovery or continued_challenge:
                self.phase = "FAILED_RECOVERY"
                action = "EXIT"
                reason = (
                    "FAILED_STATE_RECOVERY"
                    if failed_recovery
                    else "OPPOSING_STATE_ACCEPTED"
                )
            else:
                self.phase = "CHALLENGED"
                self._challenge_support = support_score
                action = "DEFEND"
                reason = "OPPOSING_STATE_CHALLENGE"
        elif context_support > 0.0:
            self.phase = "PULLBACK"
            action = "DEFEND"
            reason = "CONTEXT_SUPPORTED_PULLBACK"
        else:
            self.phase = "CHALLENGED"
            self._challenge_support = support_score
            action = "DEFEND"
            reason = "STATE_TRANSITION"

        surrendered = max(0.0, self.best_observed_pnl - gross_pnl)
        capture_ratio = None
        if self.best_observed_pnl > 0.0:
            capture_ratio = gross_pnl / self.best_observed_pnl
        decision = KeeperDecision(
            action=action,
            phase=self.phase,
            context=context,
            reason=reason,
            support_score=support_score,
            fast_support=fast_support,
            context_support=context_support,
            gross_pnl=gross_pnl,
            net_pnl=net_pnl,
            best_observed_pnl=self.best_observed_pnl,
            worst_observed_pnl=self.worst_observed_pnl,
            surrendered_mfe=surrendered,
            mfe_capture_ratio=capture_ratio,
            observations=len(self._support),
        )
        self._last_decision = decision
        return decision

    def _record_excursion(self, gross_pnl: float, bid: float, received_ts: float) -> None:
        if gross_pnl > self.best_observed_pnl:
            self.best_observed_pnl = gross_pnl
            self.best_observed_bid = bid
            self.best_observed_ts = received_ts
        if gross_pnl < self.worst_observed_pnl:
            self.worst_observed_pnl = gross_pnl
            self.worst_observed_ts = received_ts

    def _support_layers(self) -> tuple[float, float]:
        values = [item[1] for item in self._support]
        if not values:
            return 0.0, 0.0
        width = max(2, min(len(values), int(math.sqrt(len(values))) + 1))
        fast = statistics.median(values[-width:])
        context_values = values[-(width * 3) : -width]
        context = statistics.median(context_values or values)
        return float(fast), float(context)

    @staticmethod
    def _context(fast_support: float, context_support: float) -> str:
        if fast_support > 0.0 and context_support > 0.0:
            return "TREND_WITH_POSITION"
        if fast_support < 0.0 and context_support < 0.0:
            return "TREND_AGAINST_POSITION"
        return "TRANSITION"

    @staticmethod
    def _is_opposing_state(side: str, state: str) -> bool:
        return (side == "CE" and state == "BEARISH_EXPANSION") or (
            side == "PE" and state == "BULLISH_EXPANSION"
        )

    @staticmethod
    def _is_matching_exhaustion(side: str, state: str) -> bool:
        return (side == "CE" and state == "BULLISH_EXHAUSTION") or (
            side == "PE" and state == "BEARISH_EXHAUSTION"
        )

    @staticmethod
    def _aligned_support(
        side: str,
        evidence: Mapping,
        stable_state: str,
        instant_state: str,
    ) -> float:
        sign = 1.0 if side == "CE" else -1.0
        ce = evidence.get("ce", {})
        pe = evidence.get("pe", {})
        future = evidence.get("future", {})
        books = evidence.get("v1_books", {})

        option_scale = max(
            abs(float(ce.get("range_pct", 0.0) or 0.0))
            + abs(float(pe.get("range_pct", 0.0) or 0.0)),
            0.0001,
        )
        velocity_scale = max(
            abs(float(ce.get("velocity_pct_sec", 0.0) or 0.0))
            + abs(float(pe.get("velocity_pct_sec", 0.0) or 0.0)),
            0.0001,
        )
        acceleration_scale = max(
            abs(float(ce.get("acceleration", 0.0) or 0.0))
            + abs(float(pe.get("acceleration", 0.0) or 0.0)),
            0.0001,
        )
        book_scale = max(
            abs(float(books.get("long_ce_pct", 0.0) or 0.0))
            + abs(float(books.get("long_pe_pct", 0.0) or 0.0)),
            0.0001,
        )
        stable_vote = StateDrivenPositionKeeper._state_vote(side, stable_state)
        instant_vote = StateDrivenPositionKeeper._state_vote(side, instant_state)
        components = (
            _clip(float(books.get("direction_score", 0.0) or 0.0) * sign),
            _clip(float(evidence.get("pressure", 0.0) or 0.0) * sign),
            _relative(float(evidence.get("velocity_spread", 0.0) or 0.0) * sign, velocity_scale),
            _relative(float(evidence.get("acceleration_spread", 0.0) or 0.0) * sign, acceleration_scale),
            _relative(float(future.get("change_pct", 0.0) or 0.0) * sign, future.get("range_pct", 0.0)),
            _relative(float(evidence.get("directional_pct", 0.0) or 0.0) * sign, option_scale),
            _relative(
                (
                    float(books.get("long_ce_pct", 0.0) or 0.0)
                    - float(books.get("long_pe_pct", 0.0) or 0.0)
                )
                * sign,
                book_scale,
            ),
            _clip(float(future.get("context_direction", 0.0) or 0.0) * sign),
            stable_vote,
            instant_vote,
        )
        return sum(components) / len(components)

    @staticmethod
    def _state_vote(side: str, state: str) -> float:
        if (side == "CE" and state == "BULLISH_EXPANSION") or (
            side == "PE" and state == "BEARISH_EXPANSION"
        ):
            return 1.0
        if StateDrivenPositionKeeper._is_opposing_state(side, state):
            return -1.0
        if StateDrivenPositionKeeper._is_matching_exhaustion(side, state):
            return -0.5
        return 0.0
