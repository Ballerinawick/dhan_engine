from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Mapping, Sequence

from dhan_engine.domain.market.full_depth_microstructure import BookSnapshot


@dataclass(frozen=True)
class LiquidityEventEvidence:
    score: float = 0.0
    near_score: float = 0.0
    deep_score: float = 0.0
    aggressive_buy_qty: int = 0
    aggressive_sell_qty: int = 0
    bid_consumed_qty: int = 0
    ask_consumed_qty: int = 0
    bid_cancelled_qty: int = 0
    ask_cancelled_qty: int = 0
    bid_replenished_qty: int = 0
    ask_replenished_qty: int = 0
    bid_persistence: float = 0.0
    ask_persistence: float = 0.0
    persistence: float = 0.0
    volatility_bps_sec: float = 0.0
    evidence_quality: float = 0.0


def _levels(rows: Sequence, start: int, stop: int) -> dict[float, int]:
    return {
        float(row.price): max(0, int(row.qty))
        for row in rows[start:stop]
        if float(row.price) > 0
    }


def _changes(previous: dict[float, int], current: dict[float, int]) -> tuple[int, int]:
    removed = added = 0
    for price in previous.keys() | current.keys():
        delta = current.get(price, 0) - previous.get(price, 0)
        removed += max(0, -delta)
        added += max(0, delta)
    return removed, added


def _persistence(previous: dict[float, int], current: dict[float, int]) -> float:
    union = sum(max(previous.get(price, 0), current.get(price, 0)) for price in previous.keys() | current.keys())
    overlap = sum(min(previous.get(price, 0), current.get(price, 0)) for price in previous.keys() & current.keys())
    return overlap / union if union else 0.0


class LiquidityEventTracker:
    """Causal MBP event classifier; it never claims order IDs or FIFO knowledge."""

    def __init__(self, history_size: int = 120):
        self._previous_book: BookSnapshot | None = None
        self._previous_quote: Mapping[str, object] | None = None
        self._history = deque(maxlen=max(16, history_size))

    @staticmethod
    def _trade_delta(current: Mapping[str, object], previous: Mapping[str, object]) -> int:
        volume = int(float(current.get("volume", 0) or 0))
        old_volume = int(float(previous.get("volume", 0) or 0))
        if volume >= old_volume > 0:
            return volume - old_volume
        current_ns = int(current.get("received_ns", 0) or 0)
        previous_ns = int(previous.get("received_ns", 0) or 0)
        return int(float(current.get("ltq", 0) or 0)) if current_ns != previous_ns else 0

    def update(
        self,
        book: BookSnapshot,
        quote: Mapping[str, object],
    ) -> LiquidityEventEvidence:
        previous_book, previous_quote = self._previous_book, self._previous_quote
        self._previous_book, self._previous_quote = book, dict(quote)
        if previous_book is None or previous_quote is None:
            return LiquidityEventEvidence()

        bid_near_old, bid_near = _levels(previous_book.bids, 0, 20), _levels(book.bids, 0, 20)
        ask_near_old, ask_near = _levels(previous_book.asks, 0, 20), _levels(book.asks, 0, 20)
        bid_deep_old, bid_deep = _levels(previous_book.bids, 20, 200), _levels(book.bids, 20, 200)
        ask_deep_old, ask_deep = _levels(previous_book.asks, 20, 200), _levels(book.asks, 20, 200)
        bid_removed, bid_added = _changes(bid_near_old, bid_near)
        ask_removed, ask_added = _changes(ask_near_old, ask_near)
        deep_bid_removed, deep_bid_added = _changes(bid_deep_old, bid_deep)
        deep_ask_removed, deep_ask_added = _changes(ask_deep_old, ask_deep)

        traded = self._trade_delta(quote, previous_quote)
        ltp = float(quote.get("ltp", 0.0) or 0.0)
        previous_ltp = float(previous_quote.get("ltp", 0.0) or 0.0)
        old_bid = float(previous_book.bids[0].price)
        old_ask = float(previous_book.asks[0].price)
        aggressive_buy = aggressive_sell = 0
        if traded > 0:
            if ltp >= old_ask or ltp > previous_ltp:
                aggressive_buy = traded
            elif ltp <= old_bid or ltp < previous_ltp:
                aggressive_sell = traded

        ask_consumed = min(ask_removed, aggressive_buy)
        bid_consumed = min(bid_removed, aggressive_sell)
        ask_cancelled = max(0, ask_removed - ask_consumed)
        bid_cancelled = max(0, bid_removed - bid_consumed)
        near_scale = max(
            ask_consumed + bid_consumed + ask_cancelled + bid_cancelled + bid_added + ask_added,
            1,
        )
        near_score = (
            ask_consumed - bid_consumed
            + bid_added - ask_added
            + ask_cancelled - bid_cancelled
        ) / near_scale
        deep_scale = max(deep_bid_removed + deep_ask_removed + deep_bid_added + deep_ask_added, 1)
        deep_score = (
            deep_ask_removed - deep_bid_removed + deep_bid_added - deep_ask_added
        ) / deep_scale
        bid_persistence = 0.65 * _persistence(bid_near_old, bid_near) + 0.35 * _persistence(bid_deep_old, bid_deep)
        ask_persistence = 0.65 * _persistence(ask_near_old, ask_near) + 0.35 * _persistence(ask_deep_old, ask_deep)
        score = max(-1.0, min(1.0, 0.72 * near_score + 0.28 * deep_score))

        mid = (float(book.bids[0].price) + float(book.asks[0].price)) / 2.0
        self._history.append((float(book.received_mono), mid, score))
        rows = list(self._history)
        signs = [1 if row[2] > 0.04 else -1 if row[2] < -0.04 else 0 for row in rows]
        target = 1 if score > 0.04 else -1 if score < -0.04 else 0
        persistence = sum(value == target for value in signs) / len(signs) if target else 0.0
        volatility = 0.0
        if len(rows) >= 3:
            moves = []
            for left, right in zip(rows, rows[1:]):
                dt = right[0] - left[0]
                if dt > 0 and left[1] > 0:
                    moves.append(abs(right[1] - left[1]) / left[1] * 10_000.0 / dt)
            volatility = math.sqrt(sum(value * value for value in moves) / len(moves)) if moves else 0.0
        classified = aggressive_buy + aggressive_sell
        evidence_quality = min(1.0, 0.45 * (classified / max(traded, 1)) + 0.55 * ((bid_persistence + ask_persistence) / 2.0))
        return LiquidityEventEvidence(
            score=score,
            near_score=max(-1.0, min(1.0, near_score)),
            deep_score=max(-1.0, min(1.0, deep_score)),
            aggressive_buy_qty=aggressive_buy,
            aggressive_sell_qty=aggressive_sell,
            bid_consumed_qty=bid_consumed,
            ask_consumed_qty=ask_consumed,
            bid_cancelled_qty=bid_cancelled,
            ask_cancelled_qty=ask_cancelled,
            bid_replenished_qty=bid_added,
            ask_replenished_qty=ask_added,
            bid_persistence=bid_persistence,
            ask_persistence=ask_persistence,
            persistence=persistence,
            volatility_bps_sec=volatility,
            evidence_quality=evidence_quality,
        )


def adaptive_horizon_seconds(
    evidence: LiquidityEventEvidence | None,
    *,
    minimum: int,
    maximum: int,
    profile: str,
) -> int:
    """Select a causal continuous horizon from persistence and current volatility."""
    if evidence is None:
        return minimum
    span = max(0, maximum - minimum)
    persistence = max(0.0, min(1.0, evidence.persistence))
    quality = max(0.0, min(1.0, evidence.evidence_quality))
    volatility_penalty = 1.0 / (1.0 + evidence.volatility_bps_sec / (1.2 if profile == "scalp" else 0.6))
    maturity = persistence * (0.35 + 0.65 * quality) * volatility_penalty
    return int(round(minimum + span * maturity))
