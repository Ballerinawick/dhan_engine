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
class CommodityTickState:
    """In-memory commodity intelligence state derived only from received tick payload fields."""

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
    exhaustion_pct: float
    recovery_pct: float
    depth_imbalance_pct: float
    top_depth_imbalance_pct: float
    market_imbalance_pct: float
    pressure_score: float
    oi_change_tick: float
    volume_change_tick: float
    trend_score: float
    breakout_score: float
    pullback_reclaim_score: float
    orderflow_score: float
    liquidity_score: float
    trap_risk_score: float
    hold_quality_score: float
    exit_pressure_score: float
    entry_quality_score: float
    breakout_watch: bool
    pullback_reclaim_ready: bool
    long_entry_ready: bool
    hold_ready: bool
    exit_ready: bool
    market_state_code: float
    commodity_mode_code: float

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
            "exhaustion_pct": self.exhaustion_pct,
            "recovery_pct": self.recovery_pct,
            "depth_imbalance_pct": self.depth_imbalance_pct,
            "top_depth_imbalance_pct": self.top_depth_imbalance_pct,
            "market_imbalance_pct": self.market_imbalance_pct,
            "pressure_score": self.pressure_score,
            "oi_change_tick": self.oi_change_tick,
            "volume_change_tick": self.volume_change_tick,
            "trend_score": self.trend_score,
            "breakout_score": self.breakout_score,
            "pullback_reclaim_score": self.pullback_reclaim_score,
            "orderflow_score": self.orderflow_score,
            "liquidity_score": self.liquidity_score,
            "trap_risk_score": self.trap_risk_score,
            "hold_quality_score": self.hold_quality_score,
            "exit_pressure_score": self.exit_pressure_score,
            "entry_quality_score": self.entry_quality_score,
            "breakout_watch": 1.0 if self.breakout_watch else 0.0,
            "pullback_reclaim_ready": 1.0 if self.pullback_reclaim_ready else 0.0,
            "long_entry_ready": 1.0 if self.long_entry_ready else 0.0,
            "hold_ready": 1.0 if self.hold_ready else 0.0,
            "exit_ready": 1.0 if self.exit_ready else 0.0,
            "market_state_code": self.market_state_code,
            "commodity_mode_code": self.commodity_mode_code,
            "sample_count": float(self.sample_count),
        }


class CommodityLiveTickEntity:
    """Rolling per-contract memory fed only by live received commodity tick payload fields."""

    def __init__(self, *, symbol: str, max_samples: int = 1200, max_spread_pct: float = 0.22):
        self.symbol = str(symbol).upper().strip()
        self.samples: Deque[dict] = deque(maxlen=max(int(max_samples), 30))
        self.max_spread_pct = float(max_spread_pct)

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

    @staticmethod
    def _mode_code(symbol: str) -> float:
        ticker = str(symbol).upper().strip()
        if ticker == "GOLD":
            return 1.0
        if ticker == "CRUDEOIL":
            return 2.0
        if ticker == "NATURALGAS":
            return 3.0
        return 0.0

    def update(self, symbol: str, ltp: float, raw_features: Optional[dict], ts: float) -> CommodityTickState:
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
        exhaustion = _clamp(_float(features, "exhaustion_score") * 100.0)
        recovery = _clamp(_float(features, "recovery_score") * 100.0)
        buy_sell_qty_ratio = _float(features, "buy_sell_qty_ratio", 1.0)
        oi_change_tick = _float(features, "oi_change_tick")
        volume_change_tick = _float(features, "volume_change_tick")
        ask_wall_ratio = max(_float(features, "ask_wall_ratio"), 0.0)
        bid_wall_ratio = max(_float(features, "bid_wall_ratio"), 0.0)

        self.samples.append(
            {
                "ts": float(ts),
                "ltp": price,
                "depth_imbalance": depth_imbalance,
                "top_depth_imbalance": top_depth_imbalance,
                "market_imbalance": market_imbalance,
                "pressure": pressure,
                "exhaustion": exhaustion,
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
        avg_exhaustion_5s = self._avg_recent(self.samples, ts, 5.0, "exhaustion", exhaustion)

        liquidity = _clamp(100.0 - ((spread_pct / max(self.max_spread_pct, 1e-9)) * 100.0))
        orderflow = _clamp(
            50.0
            + (depth_imbalance * 20.0)
            + (top_depth_imbalance * 14.0)
            + (market_imbalance * 22.0)
            + (pressure * 18.0)
            + (avg_market_5s * 8.0)
            + (avg_pressure_5s * 8.0)
            + (max(min(buy_sell_qty_ratio - 1.0, 1.0), -1.0) * 10.0)
        )
        trend = _clamp(
            50.0
            + (return_5s * 130.0)
            + (return_15s * 90.0)
            + (return_30s * 70.0)
            + (return_120s * 35.0)
            + (ltp_vs_avg * 35.0)
            + (intraday_return * 12.0)
        )
        breakout = _clamp(
            35.0
            + ((day_position - 50.0) * 0.45)
            + max(ltp_vs_avg, 0.0) * 30.0
            + max(intraday_return, 0.0) * 10.0
            + (orderflow - 50.0) * 0.55
            + min(max(volume_change_tick, 0.0), 100.0) * 0.03
        )
        pullback_reclaim = _clamp(
            35.0
            + max(return_30s, -0.08) * 120.0
            + max(return_120s, -0.10) * 70.0
            + max(ltp_vs_avg, -0.12) * 85.0
            + (orderflow - 45.0) * 0.80
            + (recovery * 0.18)
            - max(return_5s * -100.0, 0.0) * 0.15
        )
        wall_flip_risk = max(ask_wall_ratio - bid_wall_ratio, 0.0) * 12.0
        trap_risk = _clamp(
            (spoof_risk * 0.40)
            + (exhaustion * 0.32)
            + (max(avg_exhaustion_5s - 35.0, 0.0) * 0.30)
            + (max(spread_pct - (self.max_spread_pct * 0.40), 0.0) * 180.0)
            + wall_flip_risk
            + (max(-top_depth_imbalance, 0.0) * 16.0)
        )
        entry_quality = _clamp(
            (trend * 0.28)
            + (breakout * 0.18)
            + (pullback_reclaim * 0.16)
            + (orderflow * 0.20)
            + (liquidity * 0.10)
            + (clean_trade * 0.08)
            - (trap_risk * 0.18)
        )
        hold_quality = _clamp(
            (trend * 0.30)
            + (orderflow * 0.24)
            + (liquidity * 0.14)
            + (clean_trade * 0.12)
            + (100.0 - trap_risk) * 0.20
        )
        exit_pressure = _clamp(
            25.0
            + max(-return_5s, 0.0) * 260.0
            + max(-return_15s, 0.0) * 180.0
            + max(-ltp_vs_avg, 0.0) * 75.0
            + max(50.0 - orderflow, 0.0) * 0.85
            + trap_risk * 0.45
            + max(-top_depth_imbalance, 0.0) * 18.0
        )

        mode = self._mode_code(ticker)
        if ticker == "GOLD":
            min_entry_quality, min_orderflow, max_trap = 58.0, 52.0, 42.0
        elif ticker == "NATURALGAS":
            min_entry_quality, min_orderflow, max_trap = 55.0, 50.0, 48.0
        else:
            min_entry_quality, min_orderflow, max_trap = 56.0, 50.0, 45.0

        breakout_watch = day_position >= 78.0 and ltp_vs_avg >= 0.0 and orderflow >= min_orderflow and trap_risk <= max_trap
        pullback_reclaim_ready = (
            return_5s >= -0.025
            and return_30s >= -0.015
            and return_120s >= -0.020
            and ltp_vs_avg >= -0.03
            and orderflow >= min_orderflow
            and pullback_reclaim >= 55.0
            and trap_risk <= max_trap
        )
        tradable = spread_pct <= self.max_spread_pct and clean_trade >= 35.0 and liquidity >= 50.0
        long_entry_ready = (
            tradable
            and len(self.samples) >= 3
            and entry_quality >= min_entry_quality
            and orderflow >= min_orderflow
            and trend >= 52.0
            and trap_risk <= max_trap
            and (
                (breakout_watch and return_1tick >= 0.0 and return_5s >= 0.0)
                or pullback_reclaim_ready
                or (return_5s >= 0.015 and return_15s >= 0.0 and ltp_vs_avg >= -0.02)
            )
        )
        hold_ready = hold_quality >= 50.0 and exit_pressure < 62.0 and trap_risk <= max(max_trap + 12.0, 55.0)
        exit_ready = exit_pressure >= 65.0 or (hold_quality < 42.0 and trap_risk >= max_trap)

        if exit_ready:
            state_code = 4.0
        elif long_entry_ready:
            state_code = 3.0
        elif breakout_watch:
            state_code = 2.0
        elif pullback_reclaim_ready:
            state_code = 1.5
        elif orderflow < 45.0 or trend < 45.0:
            state_code = 0.0
        else:
            state_code = 1.0

        return CommodityTickState(
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
            exhaustion_pct=exhaustion,
            recovery_pct=recovery,
            depth_imbalance_pct=depth_imbalance * 100.0,
            top_depth_imbalance_pct=top_depth_imbalance * 100.0,
            market_imbalance_pct=market_imbalance * 100.0,
            pressure_score=pressure,
            oi_change_tick=oi_change_tick,
            volume_change_tick=volume_change_tick,
            trend_score=trend,
            breakout_score=breakout,
            pullback_reclaim_score=pullback_reclaim,
            orderflow_score=orderflow,
            liquidity_score=liquidity,
            trap_risk_score=trap_risk,
            hold_quality_score=hold_quality,
            exit_pressure_score=exit_pressure,
            entry_quality_score=entry_quality,
            breakout_watch=breakout_watch,
            pullback_reclaim_ready=pullback_reclaim_ready,
            long_entry_ready=long_entry_ready,
            hold_ready=hold_ready,
            exit_ready=exit_ready,
            market_state_code=state_code,
            commodity_mode_code=mode,
        )


class CommodityLiveTickStore:
    """Railway-process in-memory commodity intelligence store; no external DB dependency."""

    def __init__(self, *, max_samples: int = 1200, max_spread_pct: float = 0.22):
        self.max_samples = int(max_samples)
        self.max_spread_pct = float(max_spread_pct)
        self.entities: Dict[str, CommodityLiveTickEntity] = {}
        self.latest: Dict[str, CommodityTickState] = {}

    def update(self, symbol: str, ltp: float, raw_features: Optional[dict], ts: float) -> CommodityTickState:
        ticker = str(symbol).upper().strip()
        entity = self.entities.get(ticker)
        if entity is None:
            entity = CommodityLiveTickEntity(symbol=ticker, max_samples=self.max_samples, max_spread_pct=self.max_spread_pct)
            self.entities[ticker] = entity
        state = entity.update(ticker, ltp, raw_features, ts)
        self.latest[ticker] = state
        return state

    def get(self, symbol: str) -> Optional[CommodityTickState]:
        return self.latest.get(str(symbol).upper().strip())
