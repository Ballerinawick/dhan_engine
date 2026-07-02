from __future__ import annotations

import math
import statistics
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class BookLevel:
    price: float
    qty: int
    orders: int


@dataclass(frozen=True)
class BookSnapshot:
    security_id: int
    name: str
    bids: Tuple[BookLevel, ...]
    asks: Tuple[BookLevel, ...]
    received_ts: float
    received_mono: float

    @classmethod
    def build(cls, security_id: int, name: str, bids: Iterable, asks: Iterable,
              received_ts: Optional[float] = None, received_mono: Optional[float] = None):
        def normalize(rows, reverse):
            by_price: Dict[float, BookLevel] = {}
            for row in rows:
                if isinstance(row, BookLevel):
                    level = row
                else:
                    level = BookLevel(float(row[0]), int(row[1]), int(row[2]))
                if level.price > 0 and level.qty >= 0 and level.orders >= 0:
                    by_price[level.price] = level
            return tuple(sorted(by_price.values(), key=lambda x: x.price, reverse=reverse))
        now = time.time() if received_ts is None else float(received_ts)
        mono = time.monotonic() if received_mono is None else float(received_mono)
        return cls(int(security_id), str(name), normalize(bids, True), normalize(asks, False), now, mono)


@dataclass(frozen=True)
class TradeObservation:
    ltp: float
    ltq: int = 0
    ltt: str = ""
    received_mono: float = field(default_factory=time.monotonic)


@dataclass
class MicrostructureState:
    name: str
    ready: bool = False
    mid: float = 0.0
    spread: float = 0.0
    microprice: float = 0.0
    qty_imbalance_5: float = 0.0
    qty_imbalance_20: float = 0.0
    qty_imbalance_all: float = 0.0
    order_imbalance_20: float = 0.0
    bid_depletion: int = 0
    ask_depletion: int = 0
    bid_refill: int = 0
    ask_refill: int = 0
    bid_order_depletion: int = 0
    ask_order_depletion: int = 0
    bid_order_refill: int = 0
    ask_order_refill: int = 0
    bid_wall_created: int = 0
    ask_wall_created: int = 0
    bid_wall_removed: int = 0
    ask_wall_removed: int = 0
    inferred_buy_qty: int = 0
    inferred_sell_qty: int = 0
    trade_inference: str = "UNKNOWN"
    pressure: float = 0.0
    persistence: float = 0.0
    velocity_points_sec: float = 0.0
    spoof_risk: float = 0.0
    received_mono: float = 0.0


def _imbalance(bids: Tuple[BookLevel, ...], asks: Tuple[BookLevel, ...], depth: int,
               attr: str = "qty") -> float:
    b = sum(getattr(x, attr) / math.sqrt(i + 1) for i, x in enumerate(bids[:depth]))
    a = sum(getattr(x, attr) / math.sqrt(i + 1) for i, x in enumerate(asks[:depth]))
    return (b - a) / (b + a) if b + a else 0.0


class InstrumentBookAnalyzer:
    """Stateful, causal analysis of aggregated depth snapshots."""

    def __init__(self, history_size: int = 30):
        self.previous: Optional[BookSnapshot] = None
        self.history: Deque[Tuple[float, float, float]] = deque(maxlen=max(5, history_size))
        self._recent_walls: Dict[Tuple[str, float], float] = {}
        self._last_trade_key = None

    @staticmethod
    def _changes(previous, current, attr="qty"):
        old = {x.price: x for x in previous}
        new = {x.price: x for x in current}
        depletion = refill = 0
        for price in old.keys() | new.keys():
            delta = getattr(new.get(price, BookLevel(price, 0, 0)), attr) - getattr(old.get(price, BookLevel(price, 0, 0)), attr)
            refill += max(0, delta)
            depletion += max(0, -delta)
        return depletion, refill

    def update(self, snapshot: BookSnapshot, trade: Optional[TradeObservation] = None) -> MicrostructureState:
        if not snapshot.bids or not snapshot.asks:
            return MicrostructureState(name=snapshot.name, received_mono=snapshot.received_mono)
        bid, ask = snapshot.bids[0], snapshot.asks[0]
        mid = (bid.price + ask.price) / 2
        microprice = (ask.price * bid.qty + bid.price * ask.qty) / max(1, bid.qty + ask.qty)
        state = MicrostructureState(
            name=snapshot.name, mid=mid, spread=max(0.0, ask.price - bid.price), microprice=microprice,
            qty_imbalance_5=_imbalance(snapshot.bids, snapshot.asks, 5),
            qty_imbalance_20=_imbalance(snapshot.bids, snapshot.asks, 20),
            qty_imbalance_all=_imbalance(snapshot.bids, snapshot.asks, max(len(snapshot.bids), len(snapshot.asks))),
            order_imbalance_20=_imbalance(snapshot.bids, snapshot.asks, 20, "orders"),
            received_mono=snapshot.received_mono,
        )
        if self.previous:
            state.ready = True
            state.bid_depletion, state.bid_refill = self._changes(self.previous.bids, snapshot.bids)
            state.ask_depletion, state.ask_refill = self._changes(self.previous.asks, snapshot.asks)
            state.bid_order_depletion, state.bid_order_refill = self._changes(self.previous.bids, snapshot.bids, "orders")
            state.ask_order_depletion, state.ask_order_refill = self._changes(self.previous.asks, snapshot.asks, "orders")
            self._wall_changes(self.previous, snapshot, state)
        trade_key = (trade.ltt, trade.received_mono) if trade else None
        if trade and trade.ltq > 0 and trade_key != self._last_trade_key:
            if trade.ltp >= ask.price:
                state.inferred_buy_qty, state.trade_inference = trade.ltq, "AGGRESSIVE_BUY_INFERRED"
            elif trade.ltp <= bid.price:
                state.inferred_sell_qty, state.trade_inference = trade.ltq, "AGGRESSIVE_SELL_INFERRED"
            self._last_trade_key = trade_key
        flow = (state.ask_depletion + state.bid_refill) - (state.bid_depletion + state.ask_refill)
        scale = state.ask_depletion + state.bid_refill + state.bid_depletion + state.ask_refill
        flow_score = flow / scale if scale else 0.0
        trade_score = (state.inferred_buy_qty - state.inferred_sell_qty) / max(1, state.inferred_buy_qty + state.inferred_sell_qty)
        state.pressure = max(-1.0, min(1.0, .40 * state.qty_imbalance_20 + .20 * state.order_imbalance_20 + .25 * flow_score + .15 * trade_score))
        self.history.append((snapshot.received_mono, mid, state.pressure))
        if len(self.history) >= 2:
            dt = self.history[-1][0] - self.history[0][0]
            state.velocity_points_sec = (self.history[-1][1] - self.history[0][1]) / dt if dt > 0 else 0.0
            signs = [1 if x[2] > .05 else -1 if x[2] < -.05 else 0 for x in self.history]
            target = 1 if state.pressure > .05 else -1 if state.pressure < -.05 else 0
            state.persistence = sum(1 for x in signs if x == target) / len(signs) if target else 0.0
        self.previous = snapshot
        return state

    def _wall_changes(self, previous: BookSnapshot, current: BookSnapshot, state: MicrostructureState):
        now = current.received_mono
        for side, old_rows, new_rows in (("bid", previous.bids, current.bids), ("ask", previous.asks, current.asks)):
            quantities = [x.qty for x in new_rows if x.qty > 0]
            threshold = max(1.0, statistics.median(quantities) * 4) if quantities else float("inf")
            old = {x.price: x.qty for x in old_rows}
            new = {x.price: x.qty for x in new_rows}
            created = {p for p, q in new.items() if q >= threshold and old.get(p, 0) < threshold}
            removed = {p for p, q in old.items() if q >= threshold and new.get(p, 0) < threshold}
            setattr(state, f"{side}_wall_created", len(created))
            setattr(state, f"{side}_wall_removed", len(removed))
            for price in created:
                self._recent_walls[(side, price)] = now
            quick = sum(1 for price in removed if now - self._recent_walls.pop((side, price), -999) <= 3.0)
            state.spoof_risk = max(state.spoof_risk, min(1.0, quick / 2.0))


@dataclass(frozen=True)
class CrossMarketDecision:
    action: str
    direction: str
    confidence: float
    expected_move_points: float
    expected_net: float
    reason: str


class CrossInstrumentDepthEngine:
    """Confirms FUT direction with CE and inverse PE, then applies executable costs."""

    def __init__(self, lot_size: int, round_trip_fee: float, slippage_points: float = .5,
                 horizon_sec: float = 5.0, min_confidence: float = .60, stale_after_sec: float = 2.0):
        self.lot_size, self.round_trip_fee = int(lot_size), float(round_trip_fee)
        self.slippage_points, self.horizon_sec = float(slippage_points), float(horizon_sec)
        self.min_confidence, self.stale_after_sec = float(min_confidence), float(stale_after_sec)
        self.states: Dict[str, MicrostructureState] = {}

    def update(self, leg: str, state: MicrostructureState, now_mono: Optional[float] = None) -> CrossMarketDecision:
        self.states[leg.upper()] = state
        now = time.monotonic() if now_mono is None else float(now_mono)
        if set(self.states) < {"FUT", "CE", "PE"}:
            return CrossMarketDecision("NO_TRADE", "NONE", 0, 0, -self.round_trip_fee, "WAITING_FOR_THREE_BOOKS")
        legs = [self.states[x] for x in ("FUT", "CE", "PE")]
        if not all(x.ready for x in legs):
            return CrossMarketDecision("NO_TRADE", "NONE", 0, 0, -self.round_trip_fee, "WARMUP")
        if any(now - x.received_mono > self.stale_after_sec for x in legs):
            return CrossMarketDecision("NO_TRADE", "NONE", 0, 0, -self.round_trip_fee, "STALE_BOOK")
        fut, ce, pe = legs
        signed = (fut.pressure, ce.pressure, -pe.pressure)
        score = sum(signed) / 3
        agreement = sum(1 for x in signed if x * score > 0) / 3 if score else 0
        confidence = min(1.0, abs(score) * .65 + agreement * .35)
        direction = "UP" if score > 0 else "DOWN" if score < 0 else "NONE"
        selected = ce if direction == "UP" else pe
        expected_move = max(0.0, selected.velocity_points_sec) * self.horizon_sec + max(0.0, selected.microprice - selected.mid)
        expected_net = (expected_move - selected.spread - self.slippage_points) * self.lot_size - self.round_trip_fee
        if agreement < 1.0:
            reason = "CROSS_LEG_CONFLICT"
        elif confidence < self.min_confidence:
            reason = "CONFIDENCE_BELOW_THRESHOLD"
        elif expected_net <= 0:
            reason = "EXPECTED_MOVE_BELOW_COST"
        else:
            reason = "COST_ADJUSTED_EDGE_CONFIRMED"
        return CrossMarketDecision("ELIGIBLE" if reason.endswith("CONFIRMED") else "NO_TRADE", direction,
                                   confidence, expected_move, expected_net, reason)
