from __future__ import annotations

import math
from collections import OrderedDict
from types import MappingProxyType
from typing import Any

from .models import (
    ExecutableQuote,
    LegLifecycle,
    LegSnapshot,
    LegSpec,
    MarkRejection,
    RecentMovement,
    StrategyLifecycle,
    StrategySnapshot,
    TradeSide,
    ValuationState,
)
from .valuation import (
    entry_cash_flow,
    entry_executable_price,
    executable_liquidation_value,
    exit_cash_flow,
    gross_realized_pnl,
    gross_unrealized_pnl,
    mark_executable_price,
    validate_quote,
)


class StrategyAccountingBook:
    """Deterministic accounting for one isolated, caller-synchronized strategy."""

    def __init__(self, strategy_instance_id: str):
        self.strategy_instance_id = strategy_instance_id
        self._legs: OrderedDict[str, LegSpec] = OrderedDict()
        self._leg_state: dict[str, LegLifecycle] = {}
        self._history: list[StrategySnapshot] = []
        self._current: StrategySnapshot | None = None
        self._last_accepted_timestamp_ns: int | None = None
        self._strategy_lifecycle = StrategyLifecycle.PLANNED
        self._last_rejection: MarkRejection | None = None

    @property
    def lifecycle(self) -> str:
        return self._strategy_lifecycle.value

    @property
    def latest_snapshot(self) -> StrategySnapshot | None:
        return self._current

    @property
    def last_rejection(self) -> MarkRejection | None:
        return self._last_rejection

    @property
    def history(self) -> tuple[StrategySnapshot, ...]:
        return tuple(self._history)

    def add_leg(self, leg: LegSpec) -> None:
        if leg.strategy_instance_id != self.strategy_instance_id:
            raise ValueError("leg belongs to another strategy instance")
        if leg.leg_id in self._legs:
            raise ValueError(f"leg already present: {leg.leg_id}")
        if self._strategy_lifecycle not in {StrategyLifecycle.PLANNED, StrategyLifecycle.PENDING}:
            raise ValueError("cannot add legs after initialization")
        self._legs[leg.leg_id] = leg
        self._leg_state[leg.leg_id] = LegLifecycle.PLANNED

    def initialize(self, quotes: dict[str, Any], *, timestamp_ns: int, entry_fees: float = 0.0) -> StrategySnapshot | None:
        if self._current is not None or not self._legs or not self._valid_fee(entry_fees):
            return None
        quote_map = self._resolve_quote_batch(quotes, list(self._legs), timestamp_ns, "initialize")
        if quote_map is None:
            return None
        fee_allocation = self._allocate_fees(float(entry_fees), list(self._legs.values()))
        snapshots: dict[str, LegSnapshot] = {}
        for leg_id, leg in self._legs.items():
            quote = quote_map[leg_id]
            entry_price = entry_executable_price(leg.side, quote)
            mark_price = mark_executable_price(leg.side, quote)
            unrealized = gross_unrealized_pnl(leg.side, entry_price, mark_price, leg.quantity)
            fee = fee_allocation[leg_id]
            snapshots[leg_id] = self._leg_snapshot(
                leg, LegLifecycle.OPEN, entry_price, mark_price,
                entry_cash_flow(leg.side, leg.quantity, entry_price), 0.0,
                0.0, unrealized, fee, unrealized - fee,
                max(0.0, unrealized - fee), min(0.0, unrealized - fee), timestamp_ns,
            )
            self._leg_state[leg_id] = LegLifecycle.OPEN
        return self._accept_snapshot(snapshots, timestamp_ns, StrategyLifecycle.INITIALIZED)

    def mark(self, quotes: dict[str, Any], *, timestamp_ns: int, stale: bool = False, ages_ns: dict[str, int] | None = None, max_age_ns: int | None = None) -> StrategySnapshot | None:
        if self._current is None or self._strategy_lifecycle == StrategyLifecycle.CLOSED:
            return None
        rejection = self._timestamp_rejection(timestamp_ns)
        if rejection is not None:
            return self._reject(*rejection)
        open_ids = [leg_id for leg_id, state in self._leg_state.items() if state == LegLifecycle.OPEN]
        if not open_ids:
            return None
        if stale or self._has_stale_age(open_ids, ages_ns, max_age_ns):
            return self._reject(ValuationState.STALE_QUOTE, "caller marked quote batch stale", timestamp_ns)
        quote_map = self._resolve_quote_batch(quotes, open_ids, timestamp_ns, "mark")
        if quote_map is None:
            return None

        previous = self._current
        snapshots = dict(previous.leg_snapshots)
        for leg_id in open_ids:
            leg = self._legs[leg_id]
            prior = previous.leg_snapshots[leg_id]
            mark_price = mark_executable_price(leg.side, quote_map[leg_id])
            unrealized = gross_unrealized_pnl(leg.side, prior.entry_price, mark_price, leg.quantity)
            net = unrealized - prior.fees_incurred
            snapshots[leg_id] = self._leg_snapshot(
                leg, LegLifecycle.OPEN, prior.entry_price, mark_price,
                prior.entry_cash_flow, 0.0, 0.0, unrealized, prior.fees_incurred,
                net, max(prior.mfe, net), min(prior.mae, net), timestamp_ns,
            )
        lifecycle = (
            StrategyLifecycle.PARTIALLY_CLOSED
            if self._strategy_lifecycle == StrategyLifecycle.PARTIALLY_CLOSED
            else StrategyLifecycle.ACTIVE
        )
        return self._accept_snapshot(snapshots, timestamp_ns, lifecycle)

    def close_legs(self, leg_ids: list[str], quotes: dict[str, Any], *, timestamp_ns: int, exit_fees: float = 0.0) -> StrategySnapshot | None:
        selected = list(dict.fromkeys(leg_ids))
        if self._current is None or not selected or not self._valid_fee(exit_fees):
            return None
        if self._strategy_lifecycle == StrategyLifecycle.CLOSED or any(self._leg_state.get(leg_id) != LegLifecycle.OPEN for leg_id in selected):
            return None
        rejection = self._timestamp_rejection(timestamp_ns)
        if rejection is not None:
            return self._reject(*rejection)
        open_ids = [leg_id for leg_id, state in self._leg_state.items() if state == LegLifecycle.OPEN]
        quote_map = self._resolve_quote_batch(quotes, open_ids, timestamp_ns, "close")
        if quote_map is None:
            return None

        previous = self._current
        fee_allocation = self._allocate_fees(float(exit_fees), [self._legs[leg_id] for leg_id in selected])
        snapshots = dict(previous.leg_snapshots)
        for leg_id in open_ids:
            leg = self._legs[leg_id]
            prior = previous.leg_snapshots[leg_id]
            mark_price = mark_executable_price(leg.side, quote_map[leg_id])
            if leg_id in selected:
                realized = gross_realized_pnl(leg.side, prior.entry_price, mark_price, leg.quantity)
                fees = prior.fees_incurred + fee_allocation[leg_id]
                net = realized - fees
                snapshots[leg_id] = self._leg_snapshot(
                    leg, LegLifecycle.CLOSED, prior.entry_price, mark_price,
                    prior.entry_cash_flow, exit_cash_flow(leg.side, leg.quantity, mark_price),
                    realized, 0.0, fees, net, max(prior.mfe, net), min(prior.mae, net), timestamp_ns,
                )
                self._leg_state[leg_id] = LegLifecycle.CLOSED
            else:
                unrealized = gross_unrealized_pnl(leg.side, prior.entry_price, mark_price, leg.quantity)
                net = unrealized - prior.fees_incurred
                snapshots[leg_id] = self._leg_snapshot(
                    leg, LegLifecycle.OPEN, prior.entry_price, mark_price,
                    prior.entry_cash_flow, 0.0, 0.0, unrealized, prior.fees_incurred,
                    net, max(prior.mfe, net), min(prior.mae, net), timestamp_ns,
                )
        lifecycle = StrategyLifecycle.CLOSED if all(state == LegLifecycle.CLOSED for state in self._leg_state.values()) else StrategyLifecycle.PARTIALLY_CLOSED
        return self._accept_snapshot(snapshots, timestamp_ns, lifecycle)

    def close(self, quotes: dict[str, Any], *, timestamp_ns: int, exit_fees: float = 0.0) -> StrategySnapshot | None:
        open_ids = [leg_id for leg_id, state in self._leg_state.items() if state == LegLifecycle.OPEN]
        return self.close_legs(open_ids, quotes, timestamp_ns=timestamp_ns, exit_fees=exit_fees)

    def snapshot(self) -> StrategySnapshot | None:
        return self._current

    def recent_movement(self, *, window_ns: int) -> RecentMovement | None:
        if self._current is None or len(self._history) < 2 or window_ns < 0:
            return None
        current = self._current
        anchor = next((item for item in reversed(self._history[:-1]) if current.timestamp_ns - item.timestamp_ns >= window_ns), None)
        if anchor is None:
            return None
        return RecentMovement(
            strategy_instance_id=self.strategy_instance_id,
            start_ts_ns=anchor.timestamp_ns,
            end_ts_ns=current.timestamp_ns,
            window_ns=window_ns,
            net_pnl_delta=current.net_pnl - anchor.net_pnl,
            gross_realized_delta=current.gross_realized_pnl - anchor.gross_realized_pnl,
            gross_unrealized_delta=current.gross_unrealized_pnl - anchor.gross_unrealized_pnl,
            liquidation_value_delta=current.liquidation_value - anchor.liquidation_value,
            ready=True,
        )

    def _accept_snapshot(self, leg_snapshots: dict[str, LegSnapshot], timestamp_ns: int, lifecycle: StrategyLifecycle) -> StrategySnapshot:
        previous = self._current
        realized = sum(item.gross_realized_pnl for item in leg_snapshots.values())
        unrealized = sum(item.gross_unrealized_pnl for item in leg_snapshots.values())
        fees = sum(item.fees_incurred for item in leg_snapshots.values())
        entry_cash = sum(item.entry_cash_flow for item in leg_snapshots.values())
        exit_cash = sum(item.exit_cash_flow for item in leg_snapshots.values())
        liquidation = sum(item.executable_liquidation_value for item in leg_snapshots.values())
        net = realized + unrealized - fees
        prior_mfe = previous.mfe if previous else 0.0
        prior_mae = previous.mae if previous else 0.0
        snapshot = StrategySnapshot(
            strategy_instance_id=self.strategy_instance_id,
            strategy_lifecycle=lifecycle,
            valuation_state=ValuationState.VALID,
            timestamp_ns=timestamp_ns,
            entry_cash_flow=entry_cash,
            exit_cash_flow=exit_cash,
            gross_realized_pnl=realized,
            gross_unrealized_pnl=unrealized,
            fees=fees,
            net_pnl=net,
            liquidation_value=liquidation,
            mfe=max(prior_mfe, net),
            mae=min(prior_mae, net),
            lifetime_net_pnl=net,
            leg_snapshots=MappingProxyType(dict(leg_snapshots)),
        )
        self._history.append(snapshot)
        self._current = snapshot
        self._last_accepted_timestamp_ns = timestamp_ns
        self._strategy_lifecycle = lifecycle
        return snapshot

    @staticmethod
    def _leg_snapshot(leg: LegSpec, lifecycle: LegLifecycle, entry_price: float, mark_price: float, entry_cash: float, exit_cash: float, realized: float, unrealized: float, fees: float, net: float, mfe: float, mae: float, timestamp_ns: int) -> LegSnapshot:
        return LegSnapshot(
            strategy_instance_id=leg.strategy_instance_id,
            leg_id=leg.leg_id,
            instrument=leg.instrument,
            option_type=leg.option_type,
            strike=leg.strike,
            expiry=leg.expiry,
            side=leg.side,
            quantity=leg.quantity,
            lifecycle=lifecycle,
            entry_price=entry_price,
            current_mark=mark_price,
            entry_cash_flow=entry_cash,
            exit_cash_flow=exit_cash,
            gross_realized_pnl=realized,
            gross_unrealized_pnl=unrealized,
            fees_incurred=fees,
            net_pnl=net,
            executable_liquidation_value=0.0 if lifecycle == LegLifecycle.CLOSED else executable_liquidation_value(leg.side, mark_price, leg.quantity),
            mfe=mfe,
            mae=mae,
            valuation_state=ValuationState.VALID,
            timestamp_ns=timestamp_ns,
        )

    def _resolve_quote_batch(self, quotes: dict[str, Any], required_ids: list[str], timestamp_ns: int, operation: str) -> dict[str, ExecutableQuote] | None:
        if not isinstance(timestamp_ns, int) or timestamp_ns < 0 or not isinstance(quotes, dict):
            self._reject(ValuationState.INVALID_QUOTE, f"invalid {operation} timestamp or batch", timestamp_ns)
            return None
        result: dict[str, ExecutableQuote] = {}
        for leg_id in required_ids:
            quote = validate_quote(quotes.get(leg_id))
            if quote is None:
                self._reject(ValuationState.MISSING_QUOTE if leg_id not in quotes else ValuationState.INVALID_QUOTE, f"invalid or missing quote for {leg_id}", timestamp_ns)
                return None
            if quote.timestamp_ns != timestamp_ns:
                self._reject(ValuationState.UNCOHERENT_BATCH, f"quote timestamp mismatch for {leg_id}", timestamp_ns)
                return None
            result[leg_id] = quote
        return result

    def _timestamp_rejection(self, timestamp_ns: int) -> tuple[ValuationState, str, int] | None:
        if not isinstance(timestamp_ns, int) or timestamp_ns < 0:
            return ValuationState.INVALID_QUOTE, "invalid timestamp", timestamp_ns
        if self._last_accepted_timestamp_ns is not None:
            if timestamp_ns == self._last_accepted_timestamp_ns:
                return ValuationState.DUPLICATE_TIMESTAMP, "duplicate accepted timestamp", timestamp_ns
            if timestamp_ns < self._last_accepted_timestamp_ns:
                return ValuationState.OUT_OF_ORDER_TIMESTAMP, "out-of-order timestamp", timestamp_ns
        return None

    def _reject(self, state: ValuationState, reason: str, timestamp_ns: int) -> None:
        safe_timestamp = timestamp_ns if isinstance(timestamp_ns, int) else 0
        self._last_rejection = MarkRejection(self.strategy_instance_id, state, reason, safe_timestamp)
        return None

    @staticmethod
    def _has_stale_age(leg_ids: list[str], ages_ns: dict[str, int] | None, max_age_ns: int | None) -> bool:
        return ages_ns is not None and max_age_ns is not None and any(leg_id not in ages_ns or ages_ns[leg_id] > max_age_ns for leg_id in leg_ids)

    @staticmethod
    def _valid_fee(value: float) -> bool:
        try:
            return math.isfinite(float(value)) and float(value) >= 0.0
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _allocate_fees(total_fees: float, legs: list[LegSpec]) -> dict[str, float]:
        quantity_total = sum(leg.quantity for leg in legs)
        return {leg.leg_id: total_fees * leg.quantity / quantity_total for leg in legs} if quantity_total else {leg.leg_id: 0.0 for leg in legs}
