from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def _float(features: dict, key: str, default: float = 0.0) -> float:
    try:
        return float(features.get(key, default) or default)
    except Exception:
        return float(default)


def _pct_change(current: float, previous: float) -> float:
    if current <= 0 or previous <= 0:
        return 0.0
    return ((current - previous) / previous) * 100.0


@dataclass(frozen=True)
class StockTickState:
    """In-memory live market state derived only from received stock tick fields."""

    symbol: str
    ltp: float
    ts: float
    sample_count: int
    return_1tick_pct: float
    return_3s_pct: float
    return_5s_pct: float
    return_15s_pct: float
    return_30s_pct: float
    return_120s_pct: float
    ltp_vs_avg_pct: float
    day_position_pct: float
    intraday_return_pct: float
    spread_pct: float
    clean_trade_pct: float
    spoof_risk_pct: float
    depth_imbalance_pct: float
    top_depth_imbalance_pct: float
    market_imbalance_pct: float
    pressure_score: float
    orderflow_score: float
    liquidity_score: float
    price_location_score: float
    depth_support_score: float
    top_book_score: float
    flow_confirmation_score: float
    momentum_score: float
    quality_score: float
    reclaim_confirmed: bool
    support_watch: bool
    long_entry_ready: bool
    market_state_code: float

    def as_features(self) -> Dict[str, float]:
        return {
            "return_1tick_pct": self.return_1tick_pct,
            "return_3s_pct": self.return_3s_pct,
            "return_5s_pct": self.return_5s_pct,
            "return_15s_pct": self.return_15s_pct,
            "return_30s_pct": self.return_30s_pct,
            "return_120s_pct": self.return_120s_pct,
            "ltp_vs_avg_pct": self.ltp_vs_avg_pct,
            "day_position_pct": self.day_position_pct,
            "intraday_return_pct": self.intraday_return_pct,
            "spread_pct": self.spread_pct,
            "clean_trade_pct": self.clean_trade_pct,
            "spoof_risk_pct": self.spoof_risk_pct,
            "depth_imbalance_pct": self.depth_imbalance_pct,
            "top_depth_imbalance_pct": self.top_depth_imbalance_pct,
            "market_imbalance_pct": self.market_imbalance_pct,
            "pressure_score": self.pressure_score,
            "orderflow_score": self.orderflow_score,
            "liquidity_score": self.liquidity_score,
            "price_location_score": self.price_location_score,
            "depth_support_score": self.depth_support_score,
            "top_book_score": self.top_book_score,
            "flow_confirmation_score": self.flow_confirmation_score,
            "momentum_score": self.momentum_score,
            "quality_score": self.quality_score,
            "reclaim_confirmed": 1.0 if self.reclaim_confirmed else 0.0,
            "support_watch": 1.0 if self.support_watch else 0.0,
            "long_entry_ready": 1.0 if self.long_entry_ready else 0.0,
            "market_state_code": self.market_state_code,
            "sample_count": float(self.sample_count),
        }


class StockLiveTickEntity:
    """Rolling per-symbol memory fed only by live received stock tick payload fields."""

    def __init__(self, *, max_samples: int = 900, max_spread_pct: float = 0.18):
        self.samples: Deque[dict] = deque(maxlen=max(int(max_samples), 30))
        self.max_spread_pct = float(max_spread_pct)
        self.reclaim_since_ts: Optional[float] = None

    @staticmethod
    def _price_at_or_before(samples: Deque[dict], target_ts: float) -> float:
        for sample in reversed(samples):
            if float(sample["ts"]) <= target_ts:
                return float(sample["ltp"])
        return float(samples[0]["ltp"]) if samples else 0.0

    @staticmethod
    def _avg_recent(samples: Deque[dict], ts: float, window_sec: float, key: str, default: float = 0.0) -> float:
        values = [float(sample.get(key, default) or default) for sample in samples if ts - float(sample["ts"]) <= window_sec]
        return sum(values) / len(values) if values else float(default)

    def update(self, symbol: str, ltp: float, raw_features: Optional[dict], ts: float) -> StockTickState:
        ticker = str(symbol).upper().strip()
        price = float(ltp)
        features = dict(raw_features or {})
        previous = float(self.samples[-1]["ltp"]) if self.samples else price

        ltp_vs_avg = _float(features, "ltp_vs_avg_pct")
        day_position = _clamp(_float(features, "day_position", 0.5) * 100.0)
        intraday_return = _float(features, "intraday_return_pct")
        depth_imbalance = max(-1.0, min(1.0, _float(features, "depth_imbalance_5")))
        top_depth_imbalance = max(-1.0, min(1.0, _float(features, "top_depth_imbalance")))
        market_imbalance = max(-1.0, min(1.0, _float(features, "market_queue_imbalance")))
        pressure = max(-1.0, min(1.0, _float(features, "pressure_score", _float(features, "pressure"))))
        spread_pct = max(_float(features, "spread_pct"), 0.0)
        clean_trade = _clamp(_float(features, "clean_trade_score", 0.50) * 100.0)
        spoof_risk = _clamp(_float(features, "spoof_risk") * 100.0)
        buy_sell_qty_ratio = _float(features, "buy_sell_qty_ratio", 1.0)

        self.samples.append(
            {
                "ts": float(ts),
                "ltp": price,
                "depth_imbalance": depth_imbalance,
                "top_depth_imbalance": top_depth_imbalance,
                "market_imbalance": market_imbalance,
                "pressure": pressure,
            }
        )

        return_1tick = _pct_change(price, previous)
        return_3s = _pct_change(price, self._price_at_or_before(self.samples, ts - 3.0))
        return_5s = _pct_change(price, self._price_at_or_before(self.samples, ts - 5.0))
        return_15s = _pct_change(price, self._price_at_or_before(self.samples, ts - 15.0))
        return_30s = _pct_change(price, self._price_at_or_before(self.samples, ts - 30.0))
        return_120s = _pct_change(price, self._price_at_or_before(self.samples, ts - 120.0))

        avg_top_5s = self._avg_recent(self.samples, ts, 5.0, "top_depth_imbalance", top_depth_imbalance)
        avg_market_5s = self._avg_recent(self.samples, ts, 5.0, "market_imbalance", market_imbalance)
        avg_pressure_5s = self._avg_recent(self.samples, ts, 5.0, "pressure", pressure)

        liquidity = _clamp(100.0 - ((spread_pct / max(self.max_spread_pct, 1e-9)) * 100.0))
        price_location = _clamp(
            50.0
            + (ltp_vs_avg * 80.0)
            + (intraday_return * 20.0)
            + ((day_position - 50.0) * 0.20)
        )
        depth_support = _clamp(50.0 + (depth_imbalance * 35.0) + (max(buy_sell_qty_ratio - 1.0, -1.0) * 15.0))
        top_book = _clamp(50.0 + (top_depth_imbalance * 35.0) + (avg_top_5s * 15.0))
        flow_confirmation = _clamp(
            50.0
            + (market_imbalance * 22.0)
            + (pressure * 18.0)
            + (avg_market_5s * 8.0)
            + (avg_pressure_5s * 8.0)
        )
        momentum = _clamp(50.0 + (return_3s * 120.0) + (return_5s * 100.0) + (return_15s * 60.0))
        quality = _clamp((0.45 * clean_trade) + (0.35 * liquidity) + (0.20 * (100.0 - spoof_risk)))

        if ltp_vs_avg >= 0:
            if self.reclaim_since_ts is None:
                self.reclaim_since_ts = float(ts)
        else:
            self.reclaim_since_ts = None
        reclaim_age = 0.0 if self.reclaim_since_ts is None else max(0.0, float(ts) - self.reclaim_since_ts)
        reclaim_confirmed = (
            ltp_vs_avg >= 0.0
            and reclaim_age >= 2.0
            and return_5s >= 0.0
            and flow_confirmation >= 52.0
        ) or (
            ltp_vs_avg > -0.05
            and return_5s > 0.02
            and return_15s >= 0.0
            and top_book >= 50.0
            and flow_confirmation >= 52.0
        )
        support_watch = (
            day_position <= 30.0
            and ltp_vs_avg < 0.0
            and depth_support >= 58.0
            and (top_book < 50.0 or flow_confirmation < 52.0)
        )
        tradable = spread_pct <= self.max_spread_pct and clean_trade >= 35.0 and spoof_risk <= 65.0
        long_entry_ready = (
            tradable
            and not support_watch
            and reclaim_confirmed
            and momentum >= 52.0
            and top_book >= 50.0
            and flow_confirmation >= 52.0
            and quality >= 60.0
        )
        if long_entry_ready:
            state_code = 3.0
        elif support_watch:
            state_code = 1.0
        elif ltp_vs_avg < 0.0 or flow_confirmation < 50.0:
            state_code = 0.0
        else:
            state_code = 2.0

        return StockTickState(
            symbol=ticker,
            ltp=price,
            ts=float(ts),
            sample_count=len(self.samples),
            return_1tick_pct=return_1tick,
            return_3s_pct=return_3s,
            return_5s_pct=return_5s,
            return_15s_pct=return_15s,
            return_30s_pct=return_30s,
            return_120s_pct=return_120s,
            ltp_vs_avg_pct=ltp_vs_avg,
            day_position_pct=day_position,
            intraday_return_pct=intraday_return,
            spread_pct=spread_pct,
            clean_trade_pct=clean_trade,
            spoof_risk_pct=spoof_risk,
            depth_imbalance_pct=depth_imbalance * 100.0,
            top_depth_imbalance_pct=top_depth_imbalance * 100.0,
            market_imbalance_pct=market_imbalance * 100.0,
            pressure_score=pressure,
            orderflow_score=flow_confirmation,
            liquidity_score=liquidity,
            price_location_score=price_location,
            depth_support_score=depth_support,
            top_book_score=top_book,
            flow_confirmation_score=flow_confirmation,
            momentum_score=momentum,
            quality_score=quality,
            reclaim_confirmed=reclaim_confirmed,
            support_watch=support_watch,
            long_entry_ready=long_entry_ready,
            market_state_code=state_code,
        )


class StockLiveTickStore:
    """Railway-process in-memory stock state store; no external DB dependency."""

    def __init__(self, *, max_samples: int = 900, max_spread_pct: float = 0.18):
        self.max_samples = int(max_samples)
        self.max_spread_pct = float(max_spread_pct)
        self.entities: Dict[str, StockLiveTickEntity] = {}
        self.latest: Dict[str, StockTickState] = {}

    def update(self, symbol: str, ltp: float, raw_features: Optional[dict], ts: float) -> StockTickState:
        ticker = str(symbol).upper().strip()
        entity = self.entities.get(ticker)
        if entity is None:
            entity = StockLiveTickEntity(max_samples=self.max_samples, max_spread_pct=self.max_spread_pct)
            self.entities[ticker] = entity
        state = entity.update(ticker, ltp, raw_features, ts)
        self.latest[ticker] = state
        return state

    def get(self, symbol: str) -> Optional[StockTickState]:
        return self.latest.get(str(symbol).upper().strip())
