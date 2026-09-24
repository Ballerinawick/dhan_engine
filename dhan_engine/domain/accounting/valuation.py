from __future__ import annotations

import math
from typing import Any

from .models import ExecutableQuote, TradeSide


def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def entry_executable_price(side: TradeSide, quote: ExecutableQuote) -> float:
    if side == TradeSide.BUY:
        return float(quote.ask)
    return float(quote.bid)


def mark_executable_price(side: TradeSide, quote: ExecutableQuote) -> float:
    if side == TradeSide.BUY:
        return float(quote.bid)
    return float(quote.ask)


def entry_cash_flow(side: TradeSide, quantity: int, price: float) -> float:
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    if side == TradeSide.BUY:
        return -float(price) * float(quantity)
    return float(price) * float(quantity)


def exit_cash_flow(side: TradeSide, quantity: int, price: float) -> float:
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    if side == TradeSide.BUY:
        return float(price) * float(quantity)
    return -float(price) * float(quantity)


def gross_realized_pnl(side: TradeSide, entry_price: float, exit_price: float, quantity: int) -> float:
    if side == TradeSide.BUY:
        return (float(exit_price) - float(entry_price)) * float(quantity)
    return (float(entry_price) - float(exit_price)) * float(quantity)


def gross_unrealized_pnl(side: TradeSide, entry_price: float, mark_price: float, quantity: int) -> float:
    return gross_realized_pnl(side, entry_price, mark_price, quantity)


def executable_liquidation_value(side: TradeSide, mark_price: float, quantity: int) -> float:
    if side == TradeSide.BUY:
        return float(mark_price) * float(quantity)
    return -float(mark_price) * float(quantity)


def validate_quote(raw: Any) -> ExecutableQuote | None:
    if isinstance(raw, ExecutableQuote):
        quote = raw
    elif isinstance(raw, dict):
        try:
            bid = float(raw.get("bid"))
            ask = float(raw.get("ask"))
            ts = int(raw.get("timestamp_ns"))
        except Exception:
            return None
        try:
            quote = ExecutableQuote(bid=bid, ask=ask, timestamp_ns=ts)
        except ValueError:
            return None
    else:
        return None

    if not _finite(quote.bid) or not _finite(quote.ask) or not _finite(quote.timestamp_ns):
        return None
    if quote.bid <= 0.0 or quote.ask <= 0.0 or quote.ask < quote.bid:
        return None
    return quote
