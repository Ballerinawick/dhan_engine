from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping, Sequence

from dhan_engine.domain.market.full_depth_microstructure import BookSnapshot


@dataclass(frozen=True)
class ExecutionEstimate:
    side: str
    requested_qty: int
    filled_qty: int
    average_price: float
    worst_price: float
    slippage_bps: float
    fill_ratio: float


@dataclass(frozen=True)
class MarketByPriceFeatures:
    mid: float
    spread_bps: float
    microprice_bps: float
    imbalance_5: float
    imbalance_20: float
    imbalance_50: float
    weighted_imbalance_20: float
    order_imbalance_20: float
    ofi_top: float
    bid_depletion: float
    ask_depletion: float
    bid_replenishment: float
    ask_replenishment: float
    buy_slippage_bps: float
    sell_slippage_bps: float
    pressure_score: float

    def as_vector(self) -> list[float]:
        return [
            self.spread_bps,
            self.microprice_bps,
            self.imbalance_5,
            self.imbalance_20,
            self.imbalance_50,
            self.weighted_imbalance_20,
            self.order_imbalance_20,
            self.ofi_top,
            self.bid_depletion,
            self.ask_depletion,
            self.bid_replenishment,
            self.ask_replenishment,
            self.buy_slippage_bps,
            self.sell_slippage_bps,
            self.pressure_score,
        ]


@dataclass(frozen=True)
class CompositeMarketSnapshot:
    book: BookSnapshot
    full_quote: Mapping[str, object]
    quote_age_ms: float
    features: MarketByPriceFeatures


def validate_composite_snapshot(
    book: BookSnapshot,
    full_quote: Mapping[str, object] | None,
    *,
    max_quote_age_ms: float,
    max_spread_bps: float,
) -> tuple[bool, str, float]:
    if not book.bids or not book.asks:
        return False, "INCOMPLETE_200_DEPTH", math.inf
    if any(left.price < right.price for left, right in zip(book.bids, book.bids[1:])):
        return False, "BIDS_NOT_SORTED", math.inf
    if any(left.price > right.price for left, right in zip(book.asks, book.asks[1:])):
        return False, "ASKS_NOT_SORTED", math.inf
    best_bid = float(book.bids[0].price)
    best_ask = float(book.asks[0].price)
    if best_bid <= 0 or best_ask <= best_bid:
        return False, "CROSSED_OR_INVALID_BOOK", math.inf
    mid = (best_bid + best_ask) / 2.0
    spread_bps = (best_ask - best_bid) / mid * 10_000.0
    if spread_bps > max_spread_bps:
        return False, "SPREAD_TOO_WIDE", math.inf
    if not full_quote:
        return False, "FULLQUOTE_MISSING", math.inf
    quote_ns = int(full_quote.get("received_ns", 0) or 0)
    if quote_ns <= 0:
        return False, "FULLQUOTE_TIMESTAMP_MISSING", math.inf
    book_ns = int(float(book.received_ts) * 1_000_000_000)
    quote_age_ms = max(0.0, (book_ns - quote_ns) / 1_000_000.0)
    if quote_age_ms > max_quote_age_ms:
        return False, "FULLQUOTE_STALE", quote_age_ms
    ltp = float(full_quote.get("ltp", 0.0) or 0.0)
    if ltp <= 0:
        return False, "FULLQUOTE_LTP_INVALID", quote_age_ms
    return True, "OK", quote_age_ms


def estimate_market_execution(
    levels: Sequence,
    *,
    side: str,
    quantity: int,
    reference_price: float,
) -> ExecutionEstimate:
    requested = max(0, int(quantity))
    remaining = requested
    notional = 0.0
    filled = 0
    worst = 0.0
    for level in levels:
        available = max(0, int(level.qty))
        take = min(remaining, available)
        if take <= 0:
            continue
        price = float(level.price)
        notional += take * price
        filled += take
        remaining -= take
        worst = price
        if remaining == 0:
            break
    average = notional / filled if filled else 0.0
    direction = 1.0 if side.upper() == "BUY" else -1.0
    slippage = (
        direction * (average - reference_price) / reference_price * 10_000.0
        if average > 0 and reference_price > 0
        else math.inf
    )
    return ExecutionEstimate(
        side=side.upper(),
        requested_qty=requested,
        filled_qty=filled,
        average_price=average,
        worst_price=worst,
        slippage_bps=slippage,
        fill_ratio=filled / requested if requested else 0.0,
    )


def _imbalance(bids: Sequence, asks: Sequence, levels: int, *, use_orders: bool = False) -> float:
    attr = "orders" if use_orders else "qty"
    bid = sum(max(0, int(getattr(row, attr))) for row in bids[:levels])
    ask = sum(max(0, int(getattr(row, attr))) for row in asks[:levels])
    total = bid + ask
    return (bid - ask) / total if total else 0.0


def _weighted_imbalance(bids: Sequence, asks: Sequence, levels: int) -> float:
    bid = sum(max(0, int(row.qty)) / (index + 1) for index, row in enumerate(bids[:levels]))
    ask = sum(max(0, int(row.qty)) / (index + 1) for index, row in enumerate(asks[:levels]))
    total = bid + ask
    return (bid - ask) / total if total else 0.0


def derive_market_by_price_features(
    current: BookSnapshot,
    previous: BookSnapshot | None = None,
    *,
    probe_quantity: int = 65,
) -> MarketByPriceFeatures:
    bid = current.bids[0]
    ask = current.asks[0]
    mid = (float(bid.price) + float(ask.price)) / 2.0
    spread_bps = (float(ask.price) - float(bid.price)) / mid * 10_000.0
    top_total = max(int(bid.qty) + int(ask.qty), 1)
    microprice = (
        float(ask.price) * int(bid.qty) + float(bid.price) * int(ask.qty)
    ) / top_total
    microprice_bps = (microprice - mid) / mid * 10_000.0

    ofi_top = bid_depletion = ask_depletion = bid_replenishment = ask_replenishment = 0.0
    if previous and previous.bids and previous.asks:
        old_bid, old_ask = previous.bids[0], previous.asks[0]
        bid_delta = int(bid.qty) - int(old_bid.qty) if bid.price == old_bid.price else int(bid.qty)
        ask_delta = int(ask.qty) - int(old_ask.qty) if ask.price == old_ask.price else int(ask.qty)
        scale = max(abs(bid_delta) + abs(ask_delta), 1)
        ofi_top = (bid_delta - ask_delta) / scale
        bid_depletion = max(0, -bid_delta) / max(int(old_bid.qty), 1)
        ask_depletion = max(0, -ask_delta) / max(int(old_ask.qty), 1)
        bid_replenishment = max(0, bid_delta) / max(int(old_bid.qty), 1)
        ask_replenishment = max(0, ask_delta) / max(int(old_ask.qty), 1)

    buy = estimate_market_execution(
        current.asks, side="BUY", quantity=probe_quantity, reference_price=mid
    )
    sell = estimate_market_execution(
        current.bids, side="SELL", quantity=probe_quantity, reference_price=mid
    )
    imbalance_5 = _imbalance(current.bids, current.asks, 5)
    imbalance_20 = _imbalance(current.bids, current.asks, 20)
    imbalance_50 = _imbalance(current.bids, current.asks, 50)
    weighted = _weighted_imbalance(current.bids, current.asks, 20)
    order_imbalance = _imbalance(current.bids, current.asks, 20, use_orders=True)
    pressure_score = max(
        -1.0,
        min(
            1.0,
            0.20 * imbalance_5
            + 0.20 * imbalance_20
            + 0.10 * imbalance_50
            + 0.20 * weighted
            + 0.10 * order_imbalance
            + 0.10 * ofi_top
            + 0.05 * (ask_depletion - bid_depletion)
            + 0.05 * (bid_replenishment - ask_replenishment),
        ),
    )
    return MarketByPriceFeatures(
        mid=mid,
        spread_bps=spread_bps,
        microprice_bps=microprice_bps,
        imbalance_5=imbalance_5,
        imbalance_20=imbalance_20,
        imbalance_50=imbalance_50,
        weighted_imbalance_20=weighted,
        order_imbalance_20=order_imbalance,
        ofi_top=ofi_top,
        bid_depletion=bid_depletion,
        ask_depletion=ask_depletion,
        bid_replenishment=bid_replenishment,
        ask_replenishment=ask_replenishment,
        buy_slippage_bps=buy.slippage_bps,
        sell_slippage_bps=sell.slippage_bps,
        pressure_score=pressure_score,
    )

