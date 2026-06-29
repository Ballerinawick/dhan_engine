from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class CommodityPaperPosition:
    secid: int
    symbol: str
    trading_symbol: str
    qty: int
    entry: float
    entry_ts: float
    last_ltp: float
    last_tick_ts: float
    peak_ltp: float
    entry_score: float


class CommodityPaperPortfolio:
    """Independent MCX commodity paper ledger with bounded exposure."""

    def __init__(
        self,
        *,
        capital: float,
        notional_per_trade: float,
        max_positions: int,
        round_trip_fee: float,
    ):
        self.initial_capital = float(capital)
        self.cash = float(capital)
        self.notional_per_trade = float(notional_per_trade)
        self.max_positions = max(int(max_positions), 1)
        self.round_trip_fee = max(float(round_trip_fee), 0.0)
        self.positions: Dict[int, CommodityPaperPosition] = {}
        self.realized_pnl = 0.0
        self.closed_trades = 0

    def enter(
        self,
        secid: int,
        symbol: str,
        trading_symbol: str,
        ltp: float,
        score: float,
        now: Optional[float] = None,
    ) -> bool:
        now = time.time() if now is None else float(now)
        price = float(ltp)
        if int(secid) in self.positions or len(self.positions) >= self.max_positions or price <= 0:
            return False
        budget = min(self.notional_per_trade, self.cash)
        qty = int(math.floor(budget / price))
        if qty <= 0:
            return False
        cost = qty * price
        self.cash -= cost
        self.positions[int(secid)] = CommodityPaperPosition(
            secid=int(secid),
            symbol=str(symbol),
            trading_symbol=str(trading_symbol),
            qty=qty,
            entry=price,
            entry_ts=now,
            last_ltp=price,
            last_tick_ts=now,
            peak_ltp=price,
            entry_score=float(score),
        )
        return True

    def mark(self, secid: int, ltp: float, now: Optional[float] = None) -> None:
        position = self.positions.get(int(secid))
        if position is None:
            return
        now = time.time() if now is None else float(now)
        position.last_ltp = float(ltp)
        position.last_tick_ts = now
        position.peak_ltp = max(position.peak_ltp, float(ltp))

    def exit(self, secid: int, ltp: float, reason: str, now: Optional[float] = None) -> Optional[dict]:
        position = self.positions.pop(int(secid), None)
        if position is None:
            return None
        now = time.time() if now is None else float(now)
        price = float(ltp)
        gross = (price - position.entry) * position.qty
        net = gross - self.round_trip_fee
        self.cash += price * position.qty
        self.realized_pnl += net
        self.closed_trades += 1
        return {
            "symbol": position.symbol,
            "trading_symbol": position.trading_symbol,
            "secid": position.secid,
            "qty": position.qty,
            "entry": position.entry,
            "exit": price,
            "gross_pnl": gross,
            "fee": self.round_trip_fee,
            "net_pnl": net,
            "hold_sec": now - position.entry_ts,
            "reason": str(reason),
        }

    def unrealized_pnl(self) -> float:
        return sum((p.last_ltp - p.entry) * p.qty for p in self.positions.values())

    def equity(self) -> float:
        marked_value = sum(p.last_ltp * p.qty for p in self.positions.values())
        return self.cash + marked_value
