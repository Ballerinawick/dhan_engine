from __future__ import annotations

import math
import statistics
from collections import deque
from dataclasses import asdict, dataclass, replace
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
    timeframe_short_support: float
    timeframe_medium_support: float
    timeframe_long_support: float
    timeframe_overall_support: float
    timeframe_ready_groups: int
    premium_short_direction: float
    premium_medium_direction: float
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
        self._support = deque(maxlen=8192)
        self._price = deque(maxlen=16384)
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

    def observe_quote(
        self,
        *,
        position: Mapping,
        side: str,
        bid: float,
        received_ts: float,
        round_trip_fee: float,
    ) -> KeeperDecision | None:
        """Act on live premium continuation after model state enters defence."""
        self.mark_excursion(
            position=position,
            side=side,
            bid=bid,
            received_ts=received_ts,
        )
        previous = self._last_decision
        if previous is None or previous.action != "DEFEND":
            return None
        gross_pnl = self._gross_pnl(position, bid)
        premium_short, short_ready = self._price_group(received_ts, (1.0, 5.0, 10.0, 30.0))
        premium_medium, medium_ready = self._price_group(received_ts, (60.0, 240.0))
        premium_adverse = short_ready and premium_short < 0.0 and (
            not medium_ready or premium_medium <= 0.0
        )
        state_defensive = (
            previous.timeframe_short_support < 0.0
            or previous.timeframe_overall_support < 0.0
            or previous.reason
            in {
                "EARNED_MOVE_PULLBACK",
                "MULTITIMEFRAME_STATE_CHALLENGE",
                "PRICE_STATE_DIVERGENCE",
                "OPPOSING_STATE_CHALLENGE",
            }
        )
        if not (
            premium_adverse
            and state_defensive
            and gross_pnl < previous.gross_pnl
        ):
            return None

        earned_move = self.best_observed_pnl > float(round_trip_fee)
        self.phase = "FAILED_RECOVERY"
        decision = replace(
            previous,
            action="EXIT",
            phase=self.phase,
            reason=(
                "QUOTE_CONFIRMED_EARNED_MOVE_DECAY"
                if earned_move
                else "QUOTE_CONFIRMED_ENTRY_THESIS_FAILURE"
            ),
            premium_short_direction=premium_short,
            premium_medium_direction=premium_medium,
            gross_pnl=gross_pnl,
            net_pnl=gross_pnl - float(round_trip_fee),
            best_observed_pnl=self.best_observed_pnl,
            worst_observed_pnl=self.worst_observed_pnl,
            surrendered_mfe=max(0.0, self.best_observed_pnl - gross_pnl),
            mfe_capture_ratio=(
                gross_pnl / self.best_observed_pnl
                if self.best_observed_pnl > 0.0
                else None
            ),
        )
        self._last_decision = decision
        return decision

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
        gross_pnl = self._gross_pnl(position, bid)
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
        timeframe = self._timeframe_support(self.side, evidence, support_score)
        previous_support = self._support[-1][1] if self._support else support_score
        previous_pnl = self._support[-1][2] if self._support else gross_pnl
        previous_short = self._support[-1][3] if self._support else timeframe["short"]
        previous_medium = self._support[-1][4] if self._support else timeframe["medium"]
        previous_overall = self._support[-1][6] if self._support else timeframe["overall"]
        self._support.append(
            (
                float(received_ts),
                support_score,
                gross_pnl,
                timeframe["short"],
                timeframe["medium"],
                timeframe["long"],
                timeframe["overall"],
            )
        )
        fast_support, context_support = self._support_layers()
        context = self._context(fast_support, context_support)
        previous_phase = self.phase
        premium_short, premium_short_ready = self._price_group(
            received_ts, (1.0, 5.0, 10.0, 30.0)
        )
        premium_medium, premium_medium_ready = self._price_group(
            received_ts, (60.0, 240.0)
        )

        short_supportive = not timeframe["short_ready"] or timeframe["short"] > 0.0
        overall_supportive = (
            not timeframe["overall_ready"] or timeframe["overall"] >= 0.0
        )
        supportive = (
            support_score > 0.0
            and fast_support > 0.0
            and context_support >= 0.0
            and short_supportive
            and overall_supportive
        )
        opposing_state = self._is_opposing_state(self.side, stable_state)
        exhaustion_state = self._is_matching_exhaustion(self.side, stable_state)
        short_opposing = timeframe["short_ready"] and timeframe["short"] < 0.0
        medium_opposing = timeframe["medium_ready"] and timeframe["medium"] < 0.0
        overall_opposing = timeframe["overall_ready"] and timeframe["overall"] < 0.0
        timeframe_reversal = short_opposing and (medium_opposing or overall_opposing)
        opposing = (
            support_score < 0.0
            and fast_support < 0.0
            and (opposing_state or exhaustion_state)
        )
        earned_move = self.best_observed_pnl > float(round_trip_fee)
        support_weakening = support_score < previous_support
        price_weakening = gross_pnl < previous_pnl
        timeframe_weakening = (
            timeframe["short"] < previous_short
            and (
                timeframe["medium"] < previous_medium
                or timeframe["overall"] < previous_overall
            )
        )
        state_and_price_weakening = price_weakening and (
            support_weakening or timeframe_weakening or timeframe_reversal
        )
        premium_adverse = premium_short_ready and premium_short < 0.0 and (
            not premium_medium_ready or premium_medium <= 0.0
        )
        price_state_divergence = (
            not earned_move
            and gross_pnl < 0.0
            and price_weakening
            and premium_adverse
            and (supportive or timeframe_reversal)
        )
        continued_price_divergence = (
            price_state_divergence and previous_phase == "PRICE_DIVERGENCE"
        )
        challenge_floor = float(
            self._challenge_support
            if self._challenge_support is not None
            else previous_support
        )
        transition_failed = (
            not earned_move
            and previous_phase == "CHALLENGED"
            and gross_pnl < 0.0
            and price_weakening
            and support_score <= challenge_floor
            and (
                fast_support < 0.0
                or context_support < 0.0
                or timeframe_reversal
            )
        )

        action = "HOLD"
        reason = "STATE_SUPPORTED"
        if earned_move and state_and_price_weakening:
            if previous_phase in {
                "PULLBACK",
                "RECOVERY",
                "CHALLENGED",
                "PRICE_DIVERGENCE",
            }:
                self.phase = "FAILED_RECOVERY"
                action = "EXIT"
                reason = "EARNED_MOVE_STATE_DECAY"
            else:
                self.phase = "PULLBACK"
                action = "DEFEND"
                reason = "EARNED_MOVE_PULLBACK"
        elif timeframe_reversal and premium_adverse:
            if previous_phase in {"CHALLENGED", "PULLBACK", "PRICE_DIVERGENCE"}:
                self.phase = "FAILED_RECOVERY"
                action = "EXIT"
                reason = "MULTITIMEFRAME_REVERSAL_ACCEPTED"
            else:
                self.phase = "CHALLENGED"
                self._challenge_support = support_score
                action = "DEFEND"
                reason = "MULTITIMEFRAME_STATE_CHALLENGE"
        elif continued_price_divergence:
            self.phase = "FAILED_RECOVERY"
            action = "EXIT"
            reason = "PRICE_STATE_DIVERGENCE_ACCEPTED"
        elif price_state_divergence:
            self.phase = "PRICE_DIVERGENCE"
            self._challenge_support = support_score
            action = "DEFEND"
            reason = "PRICE_STATE_DIVERGENCE"
        elif transition_failed:
            self.phase = "FAILED_RECOVERY"
            action = "EXIT"
            reason = "ENTRY_THESIS_INVALIDATED"
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
            elif previous_phase in {
                "PULLBACK",
                "CHALLENGED",
                "RECOVERY",
                "PRICE_DIVERGENCE",
            }:
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
            timeframe_short_support=timeframe["short"],
            timeframe_medium_support=timeframe["medium"],
            timeframe_long_support=timeframe["long"],
            timeframe_overall_support=timeframe["overall"],
            timeframe_ready_groups=timeframe["ready_groups"],
            premium_short_direction=premium_short,
            premium_medium_direction=premium_medium,
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
        point = (float(received_ts), float(gross_pnl))
        if self._price and self._price[-1][0] == point[0]:
            self._price[-1] = point
        else:
            self._price.append(point)
        if gross_pnl > self.best_observed_pnl:
            self.best_observed_pnl = gross_pnl
            self.best_observed_bid = bid
            self.best_observed_ts = received_ts
        if gross_pnl < self.worst_observed_pnl:
            self.worst_observed_pnl = gross_pnl
            self.worst_observed_ts = received_ts

    @staticmethod
    def _gross_pnl(position: Mapping, bid: float) -> float:
        entry = float(position.get("entry", 0.0) or 0.0)
        qty = max(int(position.get("qty", 0) or 0), 1)
        return (float(bid) - entry) * qty

    def _price_direction(self, received_ts: float, window_sec: float) -> float | None:
        if len(self._price) < 2:
            return None
        cutoff = float(received_ts) - float(window_sec)
        start_index = 0
        for index, item in enumerate(self._price):
            if item[0] <= cutoff:
                start_index = index
            else:
                break
        window = list(self._price)[start_index:]
        if len(window) < 2:
            return None
        observed = window[-1][0] - window[0][0]
        if observed < float(window_sec) * 0.8:
            return None
        travelled = sum(
            abs(current[1] - previous[1])
            for previous, current in zip(window, window[1:])
        )
        return _clip((window[-1][1] - window[0][1]) / max(travelled, 0.0001))

    def _price_group(
        self, received_ts: float, windows: tuple[float, ...]
    ) -> tuple[float, bool]:
        directions = [
            direction
            for seconds in windows
            if (direction := self._price_direction(received_ts, seconds)) is not None
        ]
        if not directions:
            return 0.0, False
        return float(statistics.median(directions)), True

    @staticmethod
    def _timeframe_support(side: str, evidence: Mapping, fallback: float) -> dict:
        sign = 1.0 if side == "CE" else -1.0
        groups = evidence.get("timeframe_state", {}).get("groups", {})

        def aligned(name: str) -> tuple[float, bool]:
            group = groups.get(name, {})
            ready = bool(group.get("ready"))
            value = float(group.get("direction", 0.0)) * sign if ready else fallback
            return value, ready

        short, short_ready = aligned("short")
        medium, medium_ready = aligned("medium")
        long, long_ready = aligned("long")
        overall, overall_ready = aligned("overall")
        return {
            "short": short,
            "medium": medium,
            "long": long,
            "overall": overall,
            "short_ready": short_ready,
            "medium_ready": medium_ready,
            "long_ready": long_ready,
            "overall_ready": overall_ready,
            "ready_groups": sum((short_ready, medium_ready, long_ready)),
        }

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
