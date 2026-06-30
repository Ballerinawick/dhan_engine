from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional

from dhan_engine.domain.intelligence.scalp_swing_brain import evaluate_long_opportunity
from dhan_engine.domain.stocks.tick_state import StockLiveTickStore


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def _pct_change(current: float, previous: float) -> float:
    if current <= 0 or previous <= 0:
        return 0.0
    return ((current - previous) / previous) * 100.0


@dataclass(frozen=True)
class StockPercentSignal:
    symbol: str
    action: str
    score: float
    ltp: float
    reason: str
    features: Dict[str, float]


STOCK_PROFILES = {
    "RELIANCE": {"scalp_score": 52.0, "swing_score": 66.0, "scalp_ret5": 0.012, "scalp_ret30": -0.020, "vwap": -0.35},
    "ICICIBANK": {"scalp_score": 52.0, "swing_score": 66.0, "scalp_ret5": 0.012, "scalp_ret30": -0.020, "vwap": -0.25},
    "SBIN": {"scalp_score": 52.0, "swing_score": 65.0, "scalp_ret5": 0.012, "scalp_ret30": -0.020, "vwap": -0.18},
    "HDFCBANK": {"scalp_score": 50.0, "swing_score": 65.0, "scalp_ret5": 0.010, "scalp_ret30": -0.015, "vwap": -0.45},
}

DEFAULT_PROFILE = {
    "scalp_score": 53.0,
    "swing_score": 67.0,
    "scalp_ret5": 0.015,
    "scalp_ret30": -0.010,
    "swing_ret30": 0.080,
    "swing_ret120": 0.120,
    "vwap": -0.25,
    "min_orderflow": 44.0,
    "min_liquidity": 55.0,
    "min_clean": 35.0,
    "max_spoof": 65.0,
    "min_day_position": 18.0,
    "max_day_position": 96.0,
}


class PercentNormalizedStockEngine:
    """Price-independent long-only paper signal engine.

    Every input is converted to a percentage or a bounded 0..100 component.
    This makes scores comparable across stocks with very different prices while
    retaining each symbol's independent history.
    """

    def __init__(
        self,
        *,
        entry_score: float = 72.0,
        exit_score: float = 40.0,
        min_samples: int = 20,
        max_samples: int = 900,
        max_spread_pct: float = 0.18,
    ):
        self.entry_score = float(entry_score)
        self.exit_score = float(exit_score)
        self.min_samples = max(int(min_samples), 3)
        self.max_spread_pct = float(max_spread_pct)
        self.history: Dict[str, Deque[dict]] = defaultdict(lambda: deque(maxlen=max_samples))
        self.live_store = StockLiveTickStore(max_samples=max_samples, max_spread_pct=max_spread_pct)

    @staticmethod
    def _profile(symbol: str) -> Dict[str, float]:
        profile = dict(DEFAULT_PROFILE)
        profile.update(STOCK_PROFILES.get(str(symbol).upper().strip(), {}))
        return profile

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
    ) -> StockPercentSignal:
        ticker = str(symbol).upper().strip()
        price = float(ltp)
        features = dict(raw_features or {})
        live_state = self.live_store.update(ticker, price, features, ts)
        samples = self.history[ticker]
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
        profile = self._profile(ticker)

        fast_momentum = _clamp(50.0 + (return_5s * 160.0))
        slow_momentum = _clamp(50.0 + (return_30s * 90.0))
        intraday_trend = _clamp(50.0 + (intraday_return * 35.0))
        vwap_bias = _clamp(50.0 + (ltp_vs_avg * 80.0))
        orderflow = _clamp(50.0 + (depth_imbalance * 30.0) + (market_imbalance * 20.0))
        liquidity = _clamp(100.0 - ((spread_pct / max(self.max_spread_pct, 1e-9)) * 100.0))

        score = _clamp(
            (0.22 * fast_momentum)
            + (0.18 * slow_momentum)
            + (0.14 * intraday_trend)
            + (0.14 * vwap_bias)
            + (0.15 * orderflow)
            + (0.08 * day_position)
            + (0.06 * liquidity)
            + (0.03 * clean_trade)
            - (0.05 * spoof_risk)
        )
        brain = evaluate_long_opportunity(
            base_score=score,
            return_1tick=return_1tick,
            return_5s=return_5s,
            return_30s=return_30s,
            return_120s=return_120s,
            ltp_vs_avg=ltp_vs_avg,
            day_position=day_position,
            orderflow=orderflow,
            liquidity=liquidity,
            clean_trade=clean_trade,
            spoof_risk=spoof_risk,
            spread_pct=spread_pct,
            max_spread_pct=self.max_spread_pct,
            profile=profile,
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
            "orderflow_score": orderflow,
            "liquidity_score": liquidity,
            "profile_scalp_score": float(profile["scalp_score"]),
            "profile_swing_score": float(profile["swing_score"]),
            "score": score,
            "sample_count": float(len(samples)),
        }
        normalized.update(live_state.as_features())
        normalized.update(brain.as_features())

        if len(samples) < self.min_samples:
            return StockPercentSignal(ticker, "HOLD", score, price, "WARMUP", normalized)
        if spread_pct > self.max_spread_pct:
            return StockPercentSignal(ticker, "HOLD", score, price, "SPREAD_TOO_WIDE", normalized)
        if in_position and score <= self.exit_score:
            return StockPercentSignal(ticker, "EXIT", score, price, "PERCENT_SCORE_BREAKDOWN", normalized)
        if in_position:
            return StockPercentSignal(ticker, "HOLD", score, price, "POSITION_HELD", normalized)

        shared_checks = [
            ("SPREAD_TOO_WIDE", spread_pct <= self.max_spread_pct),
            ("CLEAN_TRADE_WEAK", clean_trade >= float(profile["min_clean"])),
            ("SPOOF_RISK_HIGH", spoof_risk <= float(profile["max_spoof"])),
            ("ORDERFLOW_WEAK", orderflow >= float(profile["min_orderflow"])),
            ("LIQUIDITY_WEAK", liquidity >= float(profile["min_liquidity"])),
            ("SMART_RISK_HIGH", brain.risk_score <= 68.0),
            ("SUPPORT_WATCH_NO_RECLAIM", not live_state.support_watch),
            ("LIVE_TICK_NOT_READY", live_state.long_entry_ready),
            ("DAY_POSITION_LOW", day_position >= float(profile["min_day_position"])),
            ("DAY_POSITION_EXTENDED", day_position <= float(profile["max_day_position"])),
        ]
        for reason, passed in shared_checks:
            if not passed:
                return StockPercentSignal(ticker, "HOLD", score, price, reason, normalized)

        swing_checks = [
            ("SWING_SCORE_BELOW_PROFILE", brain.swing_confidence >= max(float(profile["swing_score"]), self.entry_score - 6.0)),
            ("SWING_RAW_SCORE_BELOW_PROFILE", score >= max(float(profile["swing_score"]), self.entry_score - 6.0)),
            ("SWING_RET30_WEAK", return_30s >= float(profile["swing_ret30"])),
            ("SWING_RET120_WEAK", return_120s >= float(profile["swing_ret120"])),
            ("SWING_VWAP_BIAS_WEAK", ltp_vs_avg >= 0.0),
            ("SWING_INTRADAY_TREND_WEAK", intraday_return >= 0.0),
        ]
        if all(passed for _, passed in swing_checks):
            normalized["entry_mode"] = 2.0
            return StockPercentSignal(ticker, "ENTRY", score, price, "STOCK_SWING_MOMENTUM_ALIGNMENT", normalized)

        scalp_checks = [
            ("SCALP_SCORE_BELOW_PROFILE", brain.scalp_confidence >= float(profile["scalp_score"])),
            ("SCALP_RET5_WEAK", return_5s >= float(profile["scalp_ret5"])),
            ("SCALP_RET30_WEAK", return_30s >= float(profile["scalp_ret30"])),
            ("SCALP_VWAP_BIAS_WEAK", ltp_vs_avg >= float(profile["vwap"])),
            ("SCALP_TICK_NOT_POSITIVE", return_1tick >= 0.0),
        ]
        if all(passed for _, passed in scalp_checks):
            normalized["entry_mode"] = 1.0
            return StockPercentSignal(ticker, "ENTRY", score, price, "STOCK_SCALP_MOMENTUM_ALIGNMENT", normalized)

        for reason, passed in scalp_checks + swing_checks:
            if not passed:
                return StockPercentSignal(ticker, "HOLD", score, price, reason, normalized)
        return StockPercentSignal(ticker, "HOLD", score, price, "NO_CONFIRMED_EDGE", normalized)
