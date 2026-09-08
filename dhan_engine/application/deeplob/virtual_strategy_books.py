from __future__ import annotations

from dataclasses import dataclass
from collections import deque
import math


@dataclass(frozen=True)
class MarketMark:
    received_ts: float
    future_ltp: float
    ce_bid: float
    ce_ask: float
    pe_bid: float
    pe_ask: float

    def is_executable(self) -> bool:
        return self.ce_bid <= self.ce_ask and self.pe_bid <= self.pe_ask and all(
            math.isfinite(value) and value > 0.0
            for value in (
                self.received_ts,
                self.future_ltp,
                self.ce_bid,
                self.ce_ask,
                self.pe_bid,
                self.pe_ask,
            )
        )


@dataclass
class VirtualBook:
    name: str
    entry_value: float
    opened_ts: float
    pnl: float = 0.0
    pnl_pct: float = 0.0
    mfe_pct: float = 0.0
    mae_pct: float = 0.0
    updates: int = 0

    def mark(self, pnl: float) -> None:
        self.pnl = float(pnl)
        self.pnl_pct = self.pnl / max(abs(self.entry_value), 0.01) * 100.0
        self.mfe_pct = max(self.mfe_pct, self.pnl_pct)
        self.mae_pct = min(self.mae_pct, self.pnl_pct)
        self.updates += 1

    def snapshot(self, now_ts: float) -> dict:
        return {
            "pnl": self.pnl,
            "pnl_pct": self.pnl_pct,
            "mfe_pct": self.mfe_pct,
            "mae_pct": self.mae_pct,
            "updates": self.updates,
            "age_sec": max(0.0, now_ts - self.opened_ts),
        }


class ExecutableStrategyLedger:
    """Paper-only strategy books marked using executable bid/ask prices."""

    def __init__(self) -> None:
        self._entry: MarketMark | None = None
        self._books: dict[str, VirtualBook] = {}
        self._marks: deque[MarketMark] = deque(maxlen=16384)

    def reset(self) -> None:
        self._entry = None
        self._books = {}
        self._marks.clear()

    @property
    def initialized(self) -> bool:
        return self._entry is not None

    def update(self, mark: MarketMark) -> dict:
        if not mark.is_executable() or (
            self._marks and mark.received_ts <= self._marks[-1].received_ts
        ):
            return self.snapshot(mark.received_ts)
        if self._entry is None:
            self._open(mark)
        self._mark(mark)
        self._marks.append(mark)
        return self.snapshot(mark.received_ts)

    def recent_changes(self, window_sec: float) -> dict:
        """Change in the existing open books, separate from lifetime trade P&L."""
        if len(self._marks) < 2:
            return {"ready": False, "books": {}}
        current = self._marks[-1]
        cutoff = current.received_ts - window_sec
        anchor = self._marks[0]
        for mark in self._marks:
            if mark.received_ts > cutoff:
                break
            anchor = mark
        observed = current.received_ts - anchor.received_ts
        changes = self._pnl(current, anchor)
        opening_spread = self._pnl(anchor, anchor)
        values = self._entry_values(anchor)
        return {
            "ready": observed >= window_sec * 0.8,
            "observed_sec": observed,
            "start_ts": anchor.received_ts,
            "end_ts": current.received_ts,
            "books": {
                name: {
                    "change_pct": (pnl - opening_spread[name]) / values[name] * 100.0,
                    "executable_pnl": pnl,
                }
                for name, pnl in changes.items()
            },
        }

    def snapshot(self, now_ts: float) -> dict:
        if self._entry is None:
            return {
                "ready": False,
                "updates": 0,
                "age_sec": 0.0,
                "books": {},
                "coverage": [],
            }
        updates = min((book.updates for book in self._books.values()), default=0)
        return {
            "ready": True,
            "updates": updates,
            "age_sec": max(0.0, now_ts - self._entry.received_ts),
            "books": {
                name: book.snapshot(now_ts) for name, book in self._books.items()
            },
            "coverage": sorted(self._books),
            "valuation": {
                "options": "EXECUTABLE_BID_ASK",
                "future": "LTP_PROXY",
                "paper_only_short_books": True,
            },
        }

    def _open(self, mark: MarketMark) -> None:
        self._entry = mark
        self._books = {
            name: VirtualBook(name, value, mark.received_ts)
            for name, value in self._entry_values(mark).items()
        }

    @staticmethod
    def _entry_values(mark: MarketMark) -> dict:
        return {
            "future_long": mark.future_ltp,
            "future_short": mark.future_ltp,
            "long_ce": mark.ce_ask,
            "long_pe": mark.pe_ask,
            "synthetic_long": mark.ce_ask + mark.pe_bid,
            "synthetic_short": mark.pe_ask + mark.ce_bid,
            "long_straddle": mark.ce_ask + mark.pe_ask,
            "short_straddle": mark.ce_bid + mark.pe_bid,
        }

    def _mark(self, mark: MarketMark) -> None:
        entry = self._entry
        if entry is None:
            return
        for name, value in self._pnl(mark, entry).items():
            self._books[name].mark(value)

    @staticmethod
    def _pnl(mark: MarketMark, entry: MarketMark) -> dict:
        return {
            "future_long": mark.future_ltp - entry.future_ltp,
            "future_short": entry.future_ltp - mark.future_ltp,
            "long_ce": mark.ce_bid - entry.ce_ask,
            "long_pe": mark.pe_bid - entry.pe_ask,
            "synthetic_long": (mark.ce_bid - entry.ce_ask)
            + (entry.pe_bid - mark.pe_ask),
            "synthetic_short": (mark.pe_bid - entry.pe_ask)
            + (entry.ce_bid - mark.ce_ask),
            "long_straddle": (mark.ce_bid + mark.pe_bid)
            - (entry.ce_ask + entry.pe_ask),
            "short_straddle": (entry.ce_bid + entry.pe_bid)
            - (mark.ce_ask + mark.pe_ask),
        }
