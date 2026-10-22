"""Daily persistence and one-second stepping for the isolated cross book."""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from dhan_engine.simulations.nifty_cross_book import (
    Book, Close, Pair, Trade, apply_tick, atm_pair, ist, reduce_atm,
)


def _trade(value: dict | None) -> Trade | None:
    if value is None:
        return None
    result = dict(value)
    result["entry_t"] = datetime.fromisoformat(result["entry_t"])
    if result.get("exit_t"):
        result["exit_t"] = datetime.fromisoformat(result["exit_t"])
    return Trade(**result)


class CrossSession:
    def __init__(self, path: Path, expiry: str, pairs: list[Pair], *, save_delay: float = 5.0):
        self.path = Path(path)
        self.expiry = expiry
        self.pairs = list(pairs)
        self.save_delay = save_delay
        self.book = Book(expiry=expiry)
        self._dirty_since: float | None = None
        self._last_step: int | None = None

    def load(self, now: datetime) -> None:
        if not self.path.exists():
            return
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        if raw.get("day") != ist(now).date().isoformat() or raw.get("expiry") != self.expiry:
            return
        self.book = Book(
            day=raw["day"], expiry=raw["expiry"],
            minutes={int(k): [Close(datetime.fromisoformat(p["t"]), p["price"]) for p in v]
                     for k, v in raw.get("minutes", {}).items()},
            open=_trade(raw.get("open")),
            closed=[_trade(trade) for trade in raw.get("closed", [])],
            seen=list(raw.get("seen", [])),
        )

    def tick(self, security_id: int, price: float, now: datetime) -> None:
        apply_tick(self.book, security_id, price, now)
        self._dirty_since = time.monotonic()

    def step(self, future: float, now: datetime) -> Book:
        second = int(ist(now).timestamp())
        if second == self._last_step:
            return self.book
        self._last_step = second
        if self.book.day != ist(now).date().isoformat():
            self.book = Book(day=ist(now).date().isoformat(), expiry=self.expiry)
        if self.pairs:
            reduce_atm(self.book, atm_pair(future, self.pairs), now)
        self._dirty_since = self._dirty_since or time.monotonic()
        self.save_if_due()
        return self.book

    def save_if_due(self, *, force: bool = False) -> None:
        if self._dirty_since is None or (not force and time.monotonic() - self._dirty_since < self.save_delay):
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(json.dumps(asdict(self.book), default=lambda value: value.isoformat()), encoding="utf-8")
        temporary.replace(self.path)
        self._dirty_since = None

