from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class GateDecision:
    allowed: bool
    reason: str = "OK"
    fields: dict | None = None

    @classmethod
    def allow(cls, **fields) -> "GateDecision":
        return cls(True, "OK", fields or {})

    @classmethod
    def block(cls, reason: str, **fields) -> "GateDecision":
        return cls(False, reason, fields or {})


@dataclass(frozen=True)
class EntryGateConfig:
    min_support_score: float
    max_risk_score: float
    min_dynamic_edge: float
    min_expected_net_rupees: float
    min_expected_move_pct: float


@dataclass(frozen=True)
class ScaleInGateConfig:
    enabled: bool
    max_lots: int
    min_profit_pct: float
    min_support_score: float
    max_risk_score: float
    min_edge: float
    cooldown_sec: float
    fresh_ltp_max_age_sec: float


def entry_expected_edge(
    *,
    stats: dict,
    ltp: float,
    lot_size: int,
    fee: float,
    min_expected_move_pct: float,
) -> dict:
    recent_high = float(stats.get("recent_high", ltp) or ltp)
    last_5_delta = max(0.0, float(stats.get("last_5_delta", 0.0) or 0.0))
    breakout_budget = last_5_delta * 2.0
    retest_budget = max(0.0, recent_high - float(ltp))
    min_pct_budget = float(ltp) * (float(min_expected_move_pct) / 100.0)
    expected_points = max(breakout_budget, retest_budget, min_pct_budget)
    expected_gross = expected_points * int(lot_size)
    expected_net = expected_gross - float(fee)
    return {
        "lot_size": int(lot_size),
        "fee": float(fee),
        "expected_points": expected_points,
        "expected_gross": expected_gross,
        "expected_net": expected_net,
        "last_5_delta": last_5_delta,
        "recent_high": recent_high,
    }


def evaluate_entry_quality(
    *,
    stats: dict,
    ltp: float,
    lot_size: int,
    fee: float,
    phase: str,
    config: EntryGateConfig,
) -> GateDecision:
    support = float(stats.get("dynamic_support_score", 0.0) or 0.0)
    risk = float(stats.get("dynamic_risk_score", 0.0) or 0.0)
    edge = float(stats.get("dynamic_edge", 0.0) or 0.0)

    if support < config.min_support_score or risk > config.max_risk_score or edge < config.min_dynamic_edge:
        return GateDecision.block(
            "WEAK_ENTRY_EDGE",
            support=support,
            risk=risk,
            edge=edge,
            phase=phase,
            required_support=config.min_support_score,
            max_risk=config.max_risk_score,
            required_edge=config.min_dynamic_edge,
        )

    expected = entry_expected_edge(
        stats=stats,
        ltp=float(ltp),
        lot_size=int(lot_size),
        fee=float(fee),
        min_expected_move_pct=config.min_expected_move_pct,
    )
    if expected["expected_net"] < config.min_expected_net_rupees:
        return GateDecision.block(
            "EXPECTED_NET_BELOW_FEES",
            expected_points=expected["expected_points"],
            expected_net=expected["expected_net"],
            required_net=config.min_expected_net_rupees,
            lot_size=expected["lot_size"],
            fee=expected["fee"],
            last_5_delta=expected["last_5_delta"],
        )

    return GateDecision.allow(
        support=support,
        risk=risk,
        edge=edge,
        phase=phase,
        **expected,
    )


def evaluate_scale_in_quality(
    *,
    position: dict,
    ltp: float,
    stats: dict,
    ltp_age: Optional[float],
    now_ts: float,
    config: ScaleInGateConfig,
) -> GateDecision:
    if not config.enabled:
        return GateDecision.block("SCALE_IN_DISABLED")

    current_lots = int(position.get("lots", 0) or 0)
    if current_lots >= config.max_lots:
        return GateDecision.block("SCALE_IN_MAX_LOTS", lots=current_lots, max_lots=config.max_lots)

    last_add_ts = float(position.get("last_add_ts", position.get("entry_ts", 0.0)) or 0.0)
    elapsed = float(now_ts) - last_add_ts if last_add_ts else None
    if elapsed is not None and elapsed < config.cooldown_sec:
        return GateDecision.block("SCALE_IN_COOLDOWN", elapsed=round(elapsed, 2), required=config.cooldown_sec)

    if ltp_age is None or ltp_age > config.fresh_ltp_max_age_sec:
        return GateDecision.block("SCALE_IN_STALE_LTP", ltp_age=ltp_age, max_age=config.fresh_ltp_max_age_sec)

    entry = float(position.get("entry", 0.0) or 0.0)
    pnl_pct = ((float(ltp) - entry) / entry) * 100.0 if entry > 0 else 0.0
    if pnl_pct < config.min_profit_pct:
        return GateDecision.block("SCALE_IN_NOT_IN_PROFIT", pnl_pct=round(pnl_pct, 3), required=config.min_profit_pct)

    support = float(stats.get("dynamic_support_score", 0.0) or 0.0)
    risk = float(stats.get("dynamic_risk_score", 0.0) or 0.0)
    edge = float(stats.get("dynamic_edge", 0.0) or 0.0)
    if support < config.min_support_score or risk > config.max_risk_score or edge < config.min_edge:
        return GateDecision.block(
            "SCALE_IN_WEAK_EDGE",
            support=support,
            risk=risk,
            edge=edge,
            required_support=config.min_support_score,
            max_risk=config.max_risk_score,
            required_edge=config.min_edge,
        )

    return GateDecision.allow(
        lots=current_lots,
        pnl_pct=pnl_pct,
        support=support,
        risk=risk,
        edge=edge,
    )

