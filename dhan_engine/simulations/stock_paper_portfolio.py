from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Dict, Optional

from dhan_engine.domain.stocks.equity_charges import NseIntradayChargeCalculator


@dataclass
class StockPaperPosition:
    secid: int
    symbol: str
    qty: int
    entry: float
    entry_ts: float
    last_ltp: float
    last_tick_ts: float
    peak_ltp: float
    entry_score: float
    margin_used: float = 0.0


class StockPaperPortfolio:
    """Independent cash-equity paper ledger with bounded exposure."""

    def __init__(
        self,
        *,
        capital: float,
        notional_per_trade: float,
        max_positions: int,
        round_trip_fee: float,
        charge_calculator: Optional[NseIntradayChargeCalculator] = None,
        leverage: float = 1.0,
    ):
        self.initial_capital = float(capital)
        self.cash = float(capital)
        self.notional_per_trade = float(notional_per_trade)
        self.max_positions = max(int(max_positions), 1)
        self.round_trip_fee = max(float(round_trip_fee), 0.0)
        self.charge_calculator = charge_calculator
        self.leverage = max(float(leverage), 1.0)
        self.positions: Dict[int, StockPaperPosition] = {}
        self.realized_pnl = 0.0
        self.closed_trades = 0

    def enter(self, secid: int, symbol: str, ltp: float, score: float, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else float(now)
        price = float(ltp)
        if int(secid) in self.positions or len(self.positions) >= self.max_positions or price <= 0:
            return False
        exposure_budget = min(self.notional_per_trade, self.cash * self.leverage)
        qty = int(math.floor(exposure_budget / price))
        if qty <= 0:
            return False
        cost = qty * price
        margin_used = cost / self.leverage
        self.cash -= margin_used
        self.positions[int(secid)] = StockPaperPosition(
            secid=int(secid), symbol=str(symbol), qty=qty, entry=price,
            entry_ts=now, last_ltp=price, last_tick_ts=now, peak_ltp=price,
            entry_score=float(score),
            margin_used=margin_used,
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
        charges = (
            self.charge_calculator.estimate(position.entry, price, position.qty)
            if self.charge_calculator is not None
            else None
        )
        fee = charges.total if charges is not None else self.round_trip_fee
        net = gross - fee
        self.cash += position.margin_used + gross - fee
        self.realized_pnl += net
        self.closed_trades += 1
        return {
            "symbol": position.symbol, "secid": position.secid, "qty": position.qty,
            "entry": position.entry, "exit": price, "gross_pnl": gross,
            "fee": fee, "net_pnl": net,
            "hold_sec": now - position.entry_ts, "reason": str(reason),
            "fee_estimated": charges is not None,
            "fee_breakdown": (
                {
                    "brokerage": charges.brokerage,
                    "exchange": charges.exchange,
                    "stt": charges.stt,
                    "sebi": charges.sebi,
                    "ipft": charges.ipft,
                    "stamp_duty": charges.stamp_duty,
                    "gst": charges.gst,
                }
                if charges is not None
                else {"fixed": fee}
            ),
        }

    def unrealized_pnl(self) -> float:
        return sum((p.last_ltp - p.entry) * p.qty for p in self.positions.values())

    def estimate_round_trip_fee(self, position: StockPaperPosition, exit_price: float) -> float:
        if self.charge_calculator is None:
            return self.round_trip_fee
        return self.charge_calculator.estimate(
            position.entry,
            float(exit_price),
            position.qty,
        ).total

    def equity(self) -> float:
        blocked_margin = sum(p.margin_used for p in self.positions.values())
        return self.cash + blocked_margin + self.unrealized_pnl()
