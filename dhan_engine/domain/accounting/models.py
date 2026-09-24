from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping


class StrategyLifecycle(str, Enum):
    PLANNED = "PLANNED"
    PENDING = "PENDING"
    INITIALIZED = "INITIALIZED"
    ACTIVE = "ACTIVE"
    PARTIALLY_CLOSED = "PARTIALLY_CLOSED"
    CLOSED = "CLOSED"


class LegLifecycle(str, Enum):
    PLANNED = "PLANNED"
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class TradeSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OptionType(str, Enum):
    CALL = "CALL"
    PUT = "PUT"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class ValuationState(str, Enum):
    UNVALUED = "UNVALUED"
    VALID = "VALID"
    MISSING_QUOTE = "MISSING_QUOTE"
    INVALID_QUOTE = "INVALID_QUOTE"
    STALE_QUOTE = "STALE_QUOTE"
    DUPLICATE_TIMESTAMP = "DUPLICATE_TIMESTAMP"
    OUT_OF_ORDER_TIMESTAMP = "OUT_OF_ORDER_TIMESTAMP"
    UNCOHERENT_BATCH = "UNCOHERENT_BATCH"


@dataclass(frozen=True)
class InstrumentIdentity:
    security_id: int | str
    exchange_segment: str
    symbol: str


@dataclass(frozen=True)
class LegSpec:
    strategy_instance_id: str
    leg_id: str
    instrument: InstrumentIdentity
    option_type: OptionType
    strike: float
    expiry: str
    side: TradeSide
    quantity: int

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")


@dataclass(frozen=True)
class ExecutableQuote:
    bid: float
    ask: float
    timestamp_ns: int

    def __post_init__(self) -> None:
        if not self._finite(self.bid) or not self._finite(self.ask) or not self._finite(self.timestamp_ns):
            raise ValueError("quote values must be finite")
        if self.bid <= 0.0 or self.ask <= 0.0:
            raise ValueError("quote values must be positive")
        if self.ask < self.bid:
            raise ValueError("ask must be >= bid")

    @staticmethod
    def _finite(value: Any) -> bool:
        import math

        return isinstance(value, (int, float)) and math.isfinite(float(value))


@dataclass(frozen=True)
class LegSnapshot:
    strategy_instance_id: str
    leg_id: str
    instrument: InstrumentIdentity
    option_type: OptionType
    strike: float
    expiry: str
    side: TradeSide
    quantity: int
    lifecycle: LegLifecycle
    entry_price: float
    current_mark: float
    entry_cash_flow: float
    exit_cash_flow: float
    gross_realized_pnl: float
    gross_unrealized_pnl: float
    fees_incurred: float
    net_pnl: float
    executable_liquidation_value: float
    mfe: float
    mae: float
    valuation_state: ValuationState
    timestamp_ns: int


@dataclass(frozen=True)
class StrategySnapshot:
    strategy_instance_id: str
    strategy_lifecycle: StrategyLifecycle
    valuation_state: ValuationState
    timestamp_ns: int
    entry_cash_flow: float
    exit_cash_flow: float
    gross_realized_pnl: float
    gross_unrealized_pnl: float
    fees: float
    net_pnl: float
    liquidation_value: float
    mfe: float
    mae: float
    lifetime_net_pnl: float
    leg_snapshots: Mapping[str, LegSnapshot] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True)
class RecentMovement:
    strategy_instance_id: str
    start_ts_ns: int
    end_ts_ns: int
    window_ns: int
    net_pnl_delta: float
    gross_realized_delta: float
    gross_unrealized_delta: float
    liquidation_value_delta: float
    ready: bool


@dataclass(frozen=True)
class MarkRejection:
    strategy_instance_id: str
    valuation_state: ValuationState
    reason: str
    timestamp_ns: int
