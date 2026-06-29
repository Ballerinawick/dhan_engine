from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def _pct_change(current: float, previous: float) -> float:
    if current <= 0 or previous <= 0:
        return 0.0
    return ((current - previous) / previous) * 100.0


@dataclass(frozen=True)
class CommodityPercentSignal:
    symbol: str
    action: str
    score: float
    ltp: float
    reason: str
    features: Dict[str, float]


class PercentNormalizedCommodityEngine:
    """Price-independent long-only MCX commodity paper signal engine."""

    def __init__(
        self,
        *,
        entry_score: float = 68.0,
        exit_score: float = 38.0,
        min_samples: int = 12,
        max_samples: int = 1800,
        max_spread_pct: float = 0.22,
    ):
        self.entry_score = float(entry_score)
        self.exit_score = float(exit_score)
        self.min_samples = max(int(min_samples), 3)
        self.max_spread_pct = float(max_spread_pct)
        self.history: Dict[str, Deque[dict]] = defaultdict(lambda: deque(maxlen=max_samples))

    @staticmethod
    def _price_at_or_before(history: Deque[dict], target_ts: float) -> float:
        for sample in reversed(history):
            if float(sample["ts"]) <= target_ts:
                return float(sample["ltp"])
        return float(history[0]["ltp"]) if history else 0.0

    def on_tick(
        self,
        symbol: str,
        ltp: float,
        raw_features: Optional[dict],
        ts: float,
        *,
        in_position: bool,
    ) -> CommodityPercentSignal:
        root = str(symbol).upper().strip()
        price = float(ltp)
        features = dict(raw_features or {})
        samples = self.history[root]
        previous = float(samples[-1]["ltp"]) if samples else price
        samples.append({"ts": float(ts), "ltp": price})

        return_1tick = _pct_change(price, previous)
        return_5s = _pct_change(price, self._price_at_or_before(samples, ts - 5.0))
        return_30s = _pct_change(price, self._price_at_or_before(samples, ts - 30.0))
        return_120s = _pct_change(price, self._price_at_or_before(samples, ts - 120.0))
        intraday_return = float(features.get("intraday_return_pct", 0.0) or 0.0)
        ltp_vs_avg = float(features.get("ltp_vs_avg_pct", 0.0) or 0.0)
        day_position = _clamp(float(features.get("day_position", 0.5) or 0.5) * 100.0)
        depth_imbalance = max(-1.0, min(1.0, float(features.get("depth_imbalance_5", 0.0) or 0.0)))
        market_imbalance = max(-1.0, min(1.0, float(features.get("market_queue_imbalance", 0.0) or 0.0)))
        spread_pct = max(float(features.get("spread_pct", 0.0) or 0.0), 0.0)
        clean_trade = _clamp(float(features.get("clean_trade_score", 0.50) or 0.50) * 100.0)
        spoof_risk = _clamp(float(features.get("spoof_risk", 0.0) or 0.0) * 100.0)

        fast_momentum = _clamp(50.0 + (return_5s * 190.0))
        slow_momentum = _clamp(50.0 + (return_30s * 120.0))
        session_momentum = _clamp(50.0 + (return_120s * 70.0))
        intraday_trend = _clamp(50.0 + (intraday_return * 30.0))
        vwap_bias = _clamp(50.0 + (ltp_vs_avg * 70.0))
        orderflow = _clamp(50.0 + (depth_imbalance * 28.0) + (market_imbalance * 18.0))
        liquidity = _clamp(100.0 - ((spread_pct / max(self.max_spread_pct, 1e-9)) * 100.0))

        score = _clamp(
            (0.22 * fast_momentum)
            + (0.18 * slow_momentum)
            + (0.10 * session_momentum)
            + (0.12 * intraday_trend)
            + (0.12 * vwap_bias)
            + (0.13 * orderflow)
            + (0.06 * day_position)
            + (0.05 * liquidity)
            + (0.04 * clean_trade)
            - (0.04 * spoof_risk)
        )
        normalized = {
            "return_1tick_pct": return_1tick,
            "return_5s_pct": return_5s,
            "return_30s_pct": return_30s,
            "return_120s_pct": return_120s,
            "intraday_return_pct": intraday_return,
            "ltp_vs_avg_pct": ltp_vs_avg,
            "day_position_pct": day_position,
            "depth_imbalance_pct": depth_imbalance * 100.0,
            "market_imbalance_pct": market_imbalance * 100.0,
            "spread_pct": spread_pct,
            "clean_trade_pct": clean_trade,
            "spoof_risk_pct": spoof_risk,
            "score": score,
            "sample_count": float(len(samples)),
        }

        if len(samples) < self.min_samples:
            return CommodityPercentSignal(root, "HOLD", score, price, "WARMUP", normalized)
        if spread_pct > self.max_spread_pct:
            return CommodityPercentSignal(root, "HOLD", score, price, "SPREAD_TOO_WIDE", normalized)
        if in_position and score <= self.exit_score:
            return CommodityPercentSignal(root, "EXIT", score, price, "COMMODITY_SCORE_BREAKDOWN", normalized)
        if (
            not in_position
            and score >= self.entry_score
            and return_5s >= 0.03
            and return_30s >= 0.06
            and ltp_vs_avg >= -0.02
            and clean_trade >= 30.0
            and spoof_risk <= 75.0
        ):
            return CommodityPercentSignal(root, "ENTRY", score, price, "COMMODITY_MOMENTUM_ALIGNMENT", normalized)
        return CommodityPercentSignal(root, "HOLD", score, price, "NO_CONFIRMED_EDGE", normalized)
