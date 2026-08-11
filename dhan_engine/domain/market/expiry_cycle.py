from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Iterable


@dataclass(frozen=True)
class ExpiryCycleContext:
    trade_date: str
    expiry_date: str
    cycle_day: int
    cycle_label: str
    sessions_to_expiry: int
    premium_regime: str

    def as_dict(self) -> dict:
        return asdict(self)


def _as_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def expiry_cycle_context(
    trade_date,
    expiry_date,
    *,
    holidays: Iterable[date | str] = (),
) -> ExpiryCycleContext:
    """Return the NIFTY weekly session number using the selected expiry.

    Counting actual weekdays backwards from the selected expiry gives the
    normal Wednesday=Day 1 through Tuesday=Day 5 cycle while allowing known
    exchange holidays to be excluded by callers.
    """

    trading_day = _as_date(trade_date)
    expiry = _as_date(expiry_date)
    holiday_dates = {_as_date(value) for value in holidays}
    if trading_day > expiry:
        raise ValueError("trade_date cannot be after option expiry")

    sessions_to_expiry = 0
    cursor = trading_day
    while cursor < expiry:
        cursor += timedelta(days=1)
        if cursor.weekday() < 5 and cursor not in holiday_dates:
            sessions_to_expiry += 1

    cycle_day = max(1, min(5, 5 - sessions_to_expiry))
    regimes = {
        1: "HIGH_TIME_VALUE",
        2: "EARLY_DECAY",
        3: "BALANCED_GAMMA_DECAY",
        4: "HIGH_GAMMA",
        5: "EXPIRY_GAMMA_DECAY",
    }
    return ExpiryCycleContext(
        trade_date=trading_day.isoformat(),
        expiry_date=expiry.isoformat(),
        cycle_day=cycle_day,
        cycle_label=f"DAY_{cycle_day}",
        sessions_to_expiry=sessions_to_expiry,
        premium_regime=regimes[cycle_day],
    )

