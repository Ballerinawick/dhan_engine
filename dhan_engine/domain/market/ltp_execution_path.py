from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

from dhan_engine.domain.market.market_by_price_execution import CompositeMarketSnapshot


@dataclass(frozen=True)
class LtpExecutionSample:
    received_mono: float
    ltp: float
    mid: float
    event_score: float
    evidence_quality: float
    aggressive_buy_qty: int
    aggressive_sell_qty: int
    bid_consumed_qty: int
    ask_consumed_qty: int
    bid_cancelled_qty: int
    ask_cancelled_qty: int
    bid_replenished_qty: int
    ask_replenished_qty: int


@dataclass(frozen=True)
class LtpExecutionPath:
    ltp_now: float = 0.0
    ltp_return_bps: float = 0.0
    velocity_bps_sec: float = 0.0
    recent_velocity_bps_sec: float = 0.0
    acceleration_bps_sec2: float = 0.0
    classified_buy_qty: int = 0
    classified_sell_qty: int = 0
    execution_imbalance: float = 0.0
    liquidity_imbalance: float = 0.0
    path_alignment: float = 0.0
    evidence_quality: float = 0.0
    strength: float = 0.0

    def forecast_bps(self, horizon_sec: int, *, pressure: float) -> float:
        """Causal MBP forecast anchored to traded LTP, not displayed mid alone."""
        horizon = max(1.0, float(horizon_sec))
        # Recent velocity matters most, but decays as the forecast extends. The
        # acceleration contribution is intentionally capped because two MBP
        # snapshots cannot establish a stable second derivative.
        decay = 1.0 / math.sqrt(max(1.0, horizon / 10.0))
        velocity = (0.72 * self.recent_velocity_bps_sec + 0.28 * self.velocity_bps_sec)
        acceleration = max(-0.08, min(0.08, self.acceleration_bps_sec2))
        flow = 0.60 * self.strength + 0.40 * max(-1.0, min(1.0, pressure))
        forecast = velocity * horizon * decay + 0.5 * acceleration * horizon * horizon * decay
        forecast += flow * (2.0 + math.sqrt(horizon))
        return max(-50.0, min(50.0, forecast))


def sample_ltp_execution_path(
    received_mono: float,
    composite: CompositeMarketSnapshot,
) -> LtpExecutionSample:
    event = getattr(composite, "event_evidence", None)
    mid = float(getattr(composite.features, "mid", 0.0) or 0.0)
    full_quote = getattr(composite, "full_quote", {}) or {}
    ltp = float(full_quote.get("ltp", 0.0) or 0.0)
    if ltp <= 0:
        ltp = mid
    return LtpExecutionSample(
        received_mono=float(received_mono),
        ltp=ltp,
        mid=mid,
        event_score=float(getattr(event, "score", 0.0) or 0.0),
        evidence_quality=float(getattr(event, "evidence_quality", 0.0) or 0.0),
        aggressive_buy_qty=int(getattr(event, "aggressive_buy_qty", 0) or 0),
        aggressive_sell_qty=int(getattr(event, "aggressive_sell_qty", 0) or 0),
        bid_consumed_qty=int(getattr(event, "bid_consumed_qty", 0) or 0),
        ask_consumed_qty=int(getattr(event, "ask_consumed_qty", 0) or 0),
        bid_cancelled_qty=int(getattr(event, "bid_cancelled_qty", 0) or 0),
        ask_cancelled_qty=int(getattr(event, "ask_cancelled_qty", 0) or 0),
        bid_replenished_qty=int(getattr(event, "bid_replenished_qty", 0) or 0),
        ask_replenished_qty=int(getattr(event, "ask_replenished_qty", 0) or 0),
    )


def summarize_ltp_execution_path(
    samples: Sequence[LtpExecutionSample],
) -> LtpExecutionPath:
    rows = [sample for sample in samples if sample.ltp > 0]
    if not rows:
        return LtpExecutionPath()
    if len(rows) == 1:
        return LtpExecutionPath(ltp_now=rows[-1].ltp)

    elapsed = max(rows[-1].received_mono - rows[0].received_mono, 0.001)
    ltp_now = rows[-1].ltp
    ltp_return = (ltp_now - rows[0].ltp) / rows[0].ltp * 10_000.0
    velocity = ltp_return / elapsed

    pivot = max(1, len(rows) // 2)
    recent_start = rows[pivot - 1]
    recent_elapsed = max(rows[-1].received_mono - recent_start.received_mono, 0.001)
    recent_return = (ltp_now - recent_start.ltp) / recent_start.ltp * 10_000.0
    recent_velocity = recent_return / recent_elapsed
    old_elapsed = max(recent_start.received_mono - rows[0].received_mono, 0.001)
    old_velocity = (
        (recent_start.ltp - rows[0].ltp) / rows[0].ltp * 10_000.0 / old_elapsed
    )
    acceleration = (recent_velocity - old_velocity) / max((recent_elapsed + old_elapsed) / 2.0, 0.001)

    buy_qty = sum(row.aggressive_buy_qty for row in rows)
    sell_qty = sum(row.aggressive_sell_qty for row in rows)
    execution_imbalance = (buy_qty - sell_qty) / max(buy_qty + sell_qty, 1)

    bullish_liquidity = sum(
        row.ask_consumed_qty + row.bid_replenished_qty + row.ask_cancelled_qty
        for row in rows
    )
    bearish_liquidity = sum(
        row.bid_consumed_qty + row.ask_replenished_qty + row.bid_cancelled_qty
        for row in rows
    )
    liquidity_imbalance = (bullish_liquidity - bearish_liquidity) / max(
        bullish_liquidity + bearish_liquidity, 1
    )

    agreements = total_directional = 0
    for left, right in zip(rows, rows[1:]):
        price_direction = 1 if right.ltp > left.ltp else -1 if right.ltp < left.ltp else 0
        evidence_direction = 1 if right.event_score > 0.02 else -1 if right.event_score < -0.02 else 0
        if price_direction and evidence_direction:
            total_directional += 1
            agreements += int(price_direction == evidence_direction)
    alignment = agreements / total_directional if total_directional else 0.0
    quality = sum(row.evidence_quality for row in rows) / len(rows)
    velocity_component = max(-1.0, min(1.0, recent_velocity / 1.5))
    event_mean = sum(row.event_score for row in rows) / len(rows)
    strength = max(
        -1.0,
        min(
            1.0,
            0.34 * velocity_component
            + 0.30 * execution_imbalance
            + 0.20 * liquidity_imbalance
            + 0.16 * event_mean,
        ),
    )
    return LtpExecutionPath(
        ltp_now=ltp_now,
        ltp_return_bps=ltp_return,
        velocity_bps_sec=velocity,
        recent_velocity_bps_sec=recent_velocity,
        acceleration_bps_sec2=acceleration,
        classified_buy_qty=buy_qty,
        classified_sell_qty=sell_qty,
        execution_imbalance=execution_imbalance,
        liquidity_imbalance=liquidity_imbalance,
        path_alignment=alignment,
        evidence_quality=quality,
        strength=strength,
    )
