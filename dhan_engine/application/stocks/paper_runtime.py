from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import Dict, Tuple
from zoneinfo import ZoneInfo

from dhan_engine.application.market_data import FutureQuoteStream
from dhan_engine.domain.market.five_minute_zone import FiveMinuteZoneDecision, FiveMinuteZoneTracker
from dhan_engine.domain.stocks.equity_charges import NseIntradayChargeCalculator
from dhan_engine.domain.stocks.percent_engine import PercentNormalizedStockEngine
from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
from dhan_engine.infrastructure.mongo.trade_summary_sink import get_trade_summary_sink
from dhan_engine.simulations.stock_paper_portfolio import StockPaperPortfolio


logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")
SYMBOL_ALIASES = {"HDFC": "HDFCBANK", "SBI": "SBIN", "ICICI": "ICICIBANK"}


def _env_symbols() -> Tuple[str, ...]:
    raw = os.getenv(
        "STOCK_PAPER_SYMBOLS",
        "RELIANCE,ICICIBANK,SBIN,HDFCBANK,AXISBANK,INFY,TCS,KOTAKBANK",
    )
    symbols = []
    for value in raw.split(","):
        symbol = SYMBOL_ALIASES.get(value.strip().upper(), value.strip().upper())
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return tuple(symbols)


@dataclass(frozen=True)
class StockPaperSettings:
    client_id: str
    access_token: str
    csv_file: str
    symbols: Tuple[str, ...]
    capital: float
    notional_per_trade: float
    max_positions: int
    round_trip_fee: float
    dynamic_charges_enabled: bool
    leverage: float
    entry_score: float
    exit_score: float
    max_spread_pct: float
    stop_loss_pct: float
    take_profit_pct: float
    trail_arm_pct: float
    trail_giveback_pct: float
    max_hold_sec: float
    entry_cooldown_sec: float
    score_exit_min_hold_sec: float
    score_exit_confirmations: int
    score_exit_fee_guard_sec: float
    score_exit_min_adverse_fee_ratio: float
    adaptive_exit_enabled: bool
    profit_lock_min_hold_sec: float
    profit_lock_min_fee_multiple: float
    profit_lock_giveback_fee_multiple: float
    dead_trade_sec: float
    dead_trade_fee_ratio: float
    dead_trade_max_score: float
    dead_trade_min_net_fee_multiple: float
    stale_tick_sec: float
    max_daily_loss: float
    max_daily_trades: int
    heartbeat_sec: float
    five_minute_cycle_enabled: bool = False
    cycle_sec: float = 300.0
    observe_sec: float = 150.0
    confirm_sec: float = 10.0
    entry_window_sec: float = 30.0
    middle_zone_ratio: float = 0.25
    strong_zone_ratio: float = 0.65
    positive_exit_min_hold_sec: float = 5.0
    force_cycle_trade_enabled: bool = False
    cycle_selection_grace_sec: float = 2.0
    fixed_cycle_qty: int = 1
    market_start: dtime = dtime(9, 15)
    market_end: dtime = dtime(15, 25)

    @classmethod
    def from_env(cls) -> "StockPaperSettings":
        client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
        access_token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
        if not client_id or not access_token:
            raise RuntimeError("Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN")
        return cls(
            client_id=client_id,
            access_token=access_token,
            csv_file=os.getenv("CSV_FILE", "api-scrip-master.csv").strip() or "api-scrip-master.csv",
            symbols=_env_symbols(),
            capital=float(os.getenv("STOCK_PAPER_CAPITAL", "500000") or 500000),
            notional_per_trade=float(os.getenv("STOCK_PAPER_NOTIONAL", "75000") or 75000),
            max_positions=int(os.getenv("STOCK_PAPER_MAX_POSITIONS", "2") or 2),
            round_trip_fee=float(os.getenv("STOCK_PAPER_ROUND_TRIP_FEE", "40") or 40),
            dynamic_charges_enabled=os.getenv("STOCK_DYNAMIC_CHARGES_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            leverage=max(float(os.getenv("STOCK_PAPER_LEVERAGE", "1") or 1), 1.0),
            entry_score=float(os.getenv("STOCK_PERCENT_ENTRY_SCORE", "72") or 72),
            exit_score=float(os.getenv("STOCK_PERCENT_EXIT_SCORE", "40") or 40),
            max_spread_pct=float(os.getenv("STOCK_PERCENT_MAX_SPREAD_PCT", "0.18") or 0.18),
            stop_loss_pct=float(os.getenv("STOCK_PAPER_STOP_LOSS_PCT", "0.35") or 0.35),
            take_profit_pct=float(os.getenv("STOCK_PAPER_TAKE_PROFIT_PCT", "0.80") or 0.80),
            trail_arm_pct=float(os.getenv("STOCK_PAPER_TRAIL_ARM_PCT", "0.40") or 0.40),
            trail_giveback_pct=float(os.getenv("STOCK_PAPER_TRAIL_GIVEBACK_PCT", "0.25") or 0.25),
            max_hold_sec=float(os.getenv("STOCK_PAPER_MAX_HOLD_SEC", "900") or 900),
            entry_cooldown_sec=float(os.getenv("STOCK_PAPER_ENTRY_COOLDOWN_SEC", "90") or 90),
            score_exit_min_hold_sec=float(os.getenv("STOCK_SCORE_EXIT_MIN_HOLD_SEC", "25") or 25),
            score_exit_confirmations=max(1, int(os.getenv("STOCK_SCORE_EXIT_CONFIRMATIONS", "2") or 2)),
            score_exit_fee_guard_sec=float(os.getenv("STOCK_SCORE_EXIT_FEE_GUARD_SEC", "90") or 90),
            score_exit_min_adverse_fee_ratio=float(os.getenv("STOCK_SCORE_EXIT_MIN_ADVERSE_FEE_RATIO", "0.75") or 0.75),
            adaptive_exit_enabled=os.getenv("STOCK_ADAPTIVE_EXIT_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            profit_lock_min_hold_sec=float(os.getenv("STOCK_PROFIT_LOCK_MIN_HOLD_SEC", "90") or 90),
            profit_lock_min_fee_multiple=float(os.getenv("STOCK_PROFIT_LOCK_MIN_FEE_MULTIPLE", "1.60") or 1.60),
            profit_lock_giveback_fee_multiple=float(os.getenv("STOCK_PROFIT_LOCK_GIVEBACK_FEE_MULTIPLE", "0.80") or 0.80),
            dead_trade_sec=float(os.getenv("STOCK_DEAD_TRADE_SEC", "360") or 360),
            dead_trade_fee_ratio=float(os.getenv("STOCK_DEAD_TRADE_FEE_RATIO", "0.85") or 0.85),
            dead_trade_max_score=float(os.getenv("STOCK_DEAD_TRADE_MAX_SCORE", "45") or 45),
            dead_trade_min_net_fee_multiple=float(os.getenv("STOCK_DEAD_TRADE_MIN_NET_FEE_MULTIPLE", "0.50") or 0.50),
            stale_tick_sec=float(os.getenv("STOCK_PAPER_STALE_TICK_SEC", "10") or 10),
            max_daily_loss=float(os.getenv("STOCK_PAPER_MAX_DAILY_LOSS", "3000") or 3000),
            max_daily_trades=int(os.getenv("STOCK_PAPER_MAX_DAILY_TRADES", "20") or 20),
            heartbeat_sec=float(os.getenv("STOCK_PAPER_HEARTBEAT_SEC", "10") or 10),
            five_minute_cycle_enabled=os.getenv("STOCK_FIVE_MINUTE_CYCLE_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            cycle_sec=float(os.getenv("STOCK_CYCLE_SEC", "300") or 300),
            observe_sec=float(os.getenv("STOCK_CYCLE_OBSERVE_SEC", "150") or 150),
            confirm_sec=float(os.getenv("STOCK_CYCLE_CONFIRM_SEC", "10") or 10),
            entry_window_sec=float(os.getenv("STOCK_CYCLE_ENTRY_WINDOW_SEC", "30") or 30),
            middle_zone_ratio=float(os.getenv("STOCK_CYCLE_MIDDLE_ZONE_RATIO", "0.25") or 0.25),
            strong_zone_ratio=float(os.getenv("STOCK_CYCLE_STRONG_ZONE_RATIO", "0.65") or 0.65),
            positive_exit_min_hold_sec=float(os.getenv("STOCK_CYCLE_POSITIVE_EXIT_MIN_HOLD_SEC", "5") or 5),
            force_cycle_trade_enabled=os.getenv("STOCK_CYCLE_FORCE_TRADE_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            cycle_selection_grace_sec=max(0.0, float(os.getenv("STOCK_CYCLE_SELECTION_GRACE_SEC", "2") or 2)),
            fixed_cycle_qty=max(1, int(os.getenv("STOCK_PAPER_FIXED_QTY", "1") or 1)),
        )


class StockPaperRuntime:
    """Standalone NSE cash-equity paper runtime."""

    def __init__(self, settings: StockPaperSettings, master: InstrumentMaster):
        self.settings = settings
        self.master = master
        self.engine = PercentNormalizedStockEngine(
            entry_score=settings.entry_score,
            exit_score=settings.exit_score,
            max_spread_pct=settings.max_spread_pct,
        )
        charge_calculator = NseIntradayChargeCalculator() if settings.dynamic_charges_enabled else None
        effective_max_positions = max(settings.max_positions, len(settings.symbols)) if settings.force_cycle_trade_enabled else settings.max_positions
        self.portfolio = StockPaperPortfolio(
            capital=settings.capital,
            notional_per_trade=settings.notional_per_trade,
            max_positions=effective_max_positions,
            round_trip_fee=settings.round_trip_fee,
            charge_calculator=charge_calculator,
            leverage=settings.leverage,
        )
        self.instrument_by_secid: Dict[int, dict] = {}
        self.secid_by_symbol: Dict[str, int] = {}
        self.last_tick_ts: Dict[str, float] = defaultdict(float)
        self.last_score_log_ts: Dict[str, float] = defaultdict(float)
        self.last_ghost_log_ts: Dict[str, float] = defaultdict(float)
        self.last_exit_ts: Dict[str, float] = defaultdict(float)
        self.score_exit_weak_count: Dict[int, int] = defaultdict(int)
        self.last_score_exit_guard_log_ts: Dict[int, float] = defaultdict(float)
        self.first_tick_logged_symbols: set[str] = set()
        self.first_tick_log_enabled = os.getenv("STOCK_FIRST_TICK_LOG_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
        self.last_health_ts = 0.0
        self.daily_trades = 0
        self.daily_date = datetime.now(IST).date()
        self.daily_realized_start = 0.0
        self.cycle_candidates: Dict[float, dict[str, tuple]] = defaultdict(dict)
        self.completed_trade_cycles: set[float] = set()
        self.trade_summary_sink = get_trade_summary_sink()
        self.zone_tracker = FiveMinuteZoneTracker(
            cycle_sec=settings.cycle_sec,
            observe_sec=settings.observe_sec,
            confirm_sec=settings.confirm_sec,
            entry_window_sec=settings.entry_window_sec,
            middle_zone_ratio=settings.middle_zone_ratio,
            strong_zone_ratio=settings.strong_zone_ratio,
        )
        self.stream = FutureQuoteStream(
            client_id=settings.client_id,
            token=settings.access_token,
            exchange_segment="NSE_EQ",
            on_quote=self.on_quote,
            debug=False,
            shard_count=1,
        )

    def _register_instruments(self) -> list[tuple[int, str]]:
        subscriptions = []
        for symbol in self.settings.symbols:
            instrument = self.master.get_equity(symbol)
            secid = int(instrument["security_id"])
            self.instrument_by_secid[secid] = instrument
            self.secid_by_symbol[symbol] = secid
            subscriptions.append((secid, f"{symbol}_EQ"))
            logger.info("STOCK_PROFILE_REGISTERED | symbol=%s | secid=%s", symbol, secid)
        return subscriptions

    def _market_open(self, now: float) -> bool:
        local = datetime.fromtimestamp(now, IST)
        return local.weekday() < 5 and self.settings.market_start <= local.time() <= self.settings.market_end

    def _reset_day_if_needed(self, now: float) -> None:
        current = datetime.fromtimestamp(now, IST).date()
        if current != self.daily_date:
            self.daily_date = current
            self.daily_trades = 0
            self.daily_realized_start = self.portfolio.realized_pnl

    def _entry_allowed(self, symbol: str, now: float) -> bool:
        if not self._market_open(now):
            return False
        if now - float(self.last_tick_ts.get(symbol, 0.0) or 0.0) > self.settings.stale_tick_sec:
            return False
        if now - float(self.last_exit_ts.get(symbol, 0.0) or 0.0) < self.settings.entry_cooldown_sec:
            return False
        if not getattr(self.settings, "force_cycle_trade_enabled", False):
            if self.daily_trades >= self.settings.max_daily_trades:
                return False
            daily_realized = self.portfolio.realized_pnl - self.daily_realized_start
            if daily_realized <= -abs(self.settings.max_daily_loss):
                return False
        return True

    def _estimated_round_trip_fee(self, position, exit_price: float) -> float:
        portfolio = getattr(self, "portfolio", None)
        if portfolio is not None:
            return portfolio.estimate_round_trip_fee(position, float(exit_price))
        return max(float(getattr(self.settings, "round_trip_fee", 0.0) or 0.0), 0.0)

    def _position_exit_reason(self, position, signal, now: float) -> str | None:
        direction_sign = float(getattr(position, "direction_sign", 1.0))
        pnl_pct = ((signal.ltp - position.entry) / position.entry) * 100.0 * direction_sign
        peak_pct = ((position.peak_ltp - position.entry) / position.entry) * 100.0
        drawdown_pct = ((position.peak_ltp - signal.ltp) / position.peak_ltp) * 100.0
        hold_sec = now - float(position.entry_ts)
        if getattr(self.settings, "five_minute_cycle_enabled", False):
            qty = max(int(position.qty), 1)
            estimated_fee = self._estimated_round_trip_fee(position, float(signal.ltp))
            net_after_fee = position.gross_pnl(float(signal.ltp)) - estimated_fee
            if hold_sec >= float(getattr(self.settings, "positive_exit_min_hold_sec", 5.0)) and net_after_fee > 0:
                self.score_exit_weak_count[int(position.secid)] = 0
                return "STOCK_FIVE_MINUTE_POSITIVE_NET_EXIT"
            if now >= self.zone_tracker.cycle_end(float(position.entry_ts)):
                self.score_exit_weak_count[int(position.secid)] = 0
                return "STOCK_FIVE_MINUTE_CYCLE_TIMEOUT"
            return None
        if pnl_pct <= -abs(self.settings.stop_loss_pct):
            self.score_exit_weak_count[int(position.secid)] = 0
            return "STOCK_PERCENT_STOP_LOSS"
        if pnl_pct >= self.settings.take_profit_pct:
            self.score_exit_weak_count[int(position.secid)] = 0
            return "STOCK_PERCENT_TAKE_PROFIT"
        if peak_pct >= self.settings.trail_arm_pct and drawdown_pct >= self.settings.trail_giveback_pct:
            self.score_exit_weak_count[int(position.secid)] = 0
            return "STOCK_PERCENT_TRAIL"
        adaptive_reason = self._adaptive_exit_reason(position, signal, hold_sec)
        if adaptive_reason:
            self.score_exit_weak_count[int(position.secid)] = 0
            return adaptive_reason
        if hold_sec >= self.settings.max_hold_sec:
            self.score_exit_weak_count[int(position.secid)] = 0
            return "STOCK_PERCENT_MAX_HOLD"
        if signal.action == "EXIT":
            if signal.reason != "PERCENT_SCORE_BREAKDOWN":
                return signal.reason
            return self._score_breakdown_exit_reason(position, signal, hold_sec, pnl_pct, now)
        self.score_exit_weak_count[int(position.secid)] = 0
        return None

    def _zone_entry_reason(self, decision: FiveMinuteZoneDecision, signal) -> str | None:
        if decision.direction != "POSITIVE" or decision.zone not in {"MIDDLE", "STRONG"}:
            return None
        features = signal.features or {}
        if float(features.get("spread_pct", 999.0) or 999.0) > self.settings.max_spread_pct:
            return None
        if float(features.get("quality_score", 0.0) or 0.0) < 55.0:
            return None
        if float(features.get("flow_confirmation_score", 0.0) or 0.0) < 45.0:
            return None
        if float(features.get("risk_score", 100.0) or 100.0) > 35.0:
            return None
        return f"STOCK_FIVE_MINUTE_FUTURE_POSITIVE_{decision.zone}"

    def _record_cycle_candidate(self, decision, signal, secid: int, symbol: str, ltp: float, now: float) -> None:
        cycle_start = float(decision.cycle_start)
        if cycle_start in self.completed_trade_cycles:
            return
        score = abs(float(decision.normalized_displacement))
        self.cycle_candidates[cycle_start][symbol] = (score, decision, signal, int(secid), float(ltp), now)
        candidates = self.cycle_candidates[cycle_start]
        grace_elapsed = now >= cycle_start + self.settings.observe_sec + self.settings.confirm_sec + self.settings.cycle_selection_grace_sec
        if len(candidates) < len(self.settings.symbols) and not grace_elapsed:
            return
        self.completed_trade_cycles.add(cycle_start)
        opened = 0
        for _, selected, selected_signal, selected_secid, selected_ltp, selected_now in sorted(
            candidates.values(), key=lambda item: item[1].current_price
        ):
            selected_symbol = str(self.instrument_by_secid[selected_secid]["symbol"])
            side = "SHORT" if selected.direction == "NEGATIVE" else "LONG"
            if selected.direction == "NEUTRAL":
                side = "SHORT" if selected.displacement < 0 else "LONG"
            reason = f"STOCK_FORCED_CYCLE_{side}_{selected.zone}"
            if not self._entry_allowed(selected_symbol, selected_now):
                logger.info("STOCK_FORCED_CYCLE_SKIPPED | cycle_start=%.0f | symbol=%s | reason=ENTRY_SAFETY_GATE", cycle_start, selected_symbol)
                continue
            entry_score = max(float(selected_signal.score), abs(selected.normalized_displacement) * 100.0)
            if self.portfolio.enter(
                selected_secid, selected_symbol, selected_ltp, entry_score, selected_now,
                side=side, qty_override=self.settings.fixed_cycle_qty,
            ):
                opened += 1
                self.daily_trades += 1
                position = self.portfolio.positions[selected_secid]
                logger.info(
                    "STOCK_FORCED_CYCLE_ENTRY | cycle_start=%.0f | symbol=%s | side=%s | qty=%s | entry=%.2f | normalized=%+.3f | zone=%s | candidates=%s | reason=%s",
                    cycle_start, selected_symbol, side, position.qty, selected_ltp,
                    selected.normalized_displacement, selected.zone, len(candidates), reason,
                )
            else:
                logger.info("STOCK_FORCED_CYCLE_SKIPPED | cycle_start=%.0f | symbol=%s | side=%s | reason=PORTFOLIO_REJECTED", cycle_start, selected_symbol, side)
        logger.info(
            "STOCK_FORCED_CYCLE_BATCH | cycle_start=%.0f | candidates=%s | opened=%s | fixed_qty=%s | paper=true",
            cycle_start, len(candidates), opened, self.settings.fixed_cycle_qty,
        )

    def _score_breakdown_exit_reason(self, position, signal, hold_sec: float, pnl_pct: float, now: float) -> str | None:
        secid = int(position.secid)
        if hold_sec < self.settings.score_exit_min_hold_sec:
            self._log_score_exit_guard(
                position,
                "MIN_HOLD",
                hold_sec=hold_sec,
                pnl_pct=pnl_pct,
                score=signal.score,
                required=self.settings.score_exit_min_hold_sec,
            )
            return None

        self.score_exit_weak_count[secid] += 1
        weak_count = int(self.score_exit_weak_count[secid])
        if weak_count < self.settings.score_exit_confirmations:
            self._log_score_exit_guard(
                position,
                "WAIT_CONFIRMATION",
                hold_sec=hold_sec,
                pnl_pct=pnl_pct,
                score=signal.score,
                weak_count=weak_count,
                required=self.settings.score_exit_confirmations,
            )
            return None

        gross_pnl = (float(signal.ltp) - float(position.entry)) * int(position.qty)
        estimated_fee = self._estimated_round_trip_fee(position, float(signal.ltp))
        fee_guard = abs(gross_pnl) < (estimated_fee * self.settings.score_exit_min_adverse_fee_ratio)
        still_inside_discovery = hold_sec < self.settings.score_exit_fee_guard_sec
        not_real_risk_yet = pnl_pct > -(abs(self.settings.stop_loss_pct) * 0.70)
        if gross_pnl < 0 and fee_guard and still_inside_discovery and not_real_risk_yet:
            self._log_score_exit_guard(
                position,
                "FEE_GUARD",
                hold_sec=hold_sec,
                pnl_pct=pnl_pct,
                score=signal.score,
                gross_pnl=gross_pnl,
                fee=estimated_fee,
                weak_count=weak_count,
            )
            return None

        self.score_exit_weak_count[secid] = 0
        return "PERCENT_SCORE_BREAKDOWN_CONFIRMED"

    def _adaptive_exit_reason(self, position, signal, hold_sec: float) -> str | None:
        if not getattr(self.settings, "adaptive_exit_enabled", True):
            return None

        qty = max(int(position.qty), 1)
        current_gross = (float(signal.ltp) - float(position.entry)) * qty
        peak_gross = (float(position.peak_ltp) - float(position.entry)) * qty
        giveback_gross = max(0.0, peak_gross - current_gross)
        fee = self._estimated_round_trip_fee(position, float(signal.ltp))
        net_after_fee = current_gross - fee
        features = getattr(signal, "features", {}) or {}
        score = float(getattr(signal, "score", 0.0) or 0.0)
        ret5 = float(features.get("return_5s_pct", 0.0) or 0.0)
        ret30 = float(features.get("return_30s_pct", 0.0) or 0.0)
        orderflow = float(features.get("orderflow_score", 50.0) or 50.0)
        scalp_conf = float(features.get("scalp_confidence", 0.0) or 0.0)
        exit_plan = float(features.get("exit_plan_code", 0.0) or 0.0)

        if (
            hold_sec >= float(getattr(self.settings, "profit_lock_min_hold_sec", 90.0) or 90.0)
            and fee > 0
            and peak_gross >= fee * float(getattr(self.settings, "profit_lock_min_fee_multiple", 1.60) or 1.60)
            and giveback_gross >= fee * float(getattr(self.settings, "profit_lock_giveback_fee_multiple", 0.80) or 0.80)
            and net_after_fee > 0
        ):
            return "STOCK_ADAPTIVE_PROFIT_LOCK"

        weak_edge = (
            score <= float(getattr(self.settings, "dead_trade_max_score", 45.0) or 45.0)
            or scalp_conf < 45.0
            or exit_plan >= 1.0
        )
        flow_faded = orderflow < 48.0 or (ret5 <= 0.0 and ret30 <= 0.0)
        dead_zone = abs(current_gross) <= fee * float(getattr(self.settings, "dead_trade_fee_ratio", 0.85) or 0.85)
        real_loss_after_fee = net_after_fee <= -(fee * float(getattr(self.settings, "dead_trade_min_net_fee_multiple", 0.50) or 0.50))
        if (
            hold_sec >= float(getattr(self.settings, "dead_trade_sec", 360.0) or 360.0)
            and fee > 0
            and weak_edge
            and flow_faded
            and (dead_zone or real_loss_after_fee)
        ):
            return "STOCK_ADAPTIVE_DEAD_SCALP_EXIT"

        return None

    def _log_score_exit_guard(self, position, reason: str, **fields) -> None:
        now = time.time()
        secid = int(position.secid)
        if now - self.last_score_exit_guard_log_ts[secid] < self.settings.heartbeat_sec:
            return
        self.last_score_exit_guard_log_ts[secid] = now
        details = " | ".join(f"{key}={value:.2f}" if isinstance(value, float) else f"{key}={value}" for key, value in fields.items())
        logger.info(
            "STOCK_SCORE_EXIT_GUARD | symbol=%s | secid=%s | reason=%s%s%s",
            position.symbol,
            secid,
            reason,
            " | " if details else "",
            details,
        )

    @staticmethod
    def _json_safe(value):
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, dict):
            return {str(key): StockPaperRuntime._json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [StockPaperRuntime._json_safe(item) for item in value]
        return str(value)

    def _log_first_full_tick(self, *, symbol: str, secid: int, tag: str, ltp: float, depth) -> None:
        if not self.first_tick_log_enabled or symbol in self.first_tick_logged_symbols:
            return
        self.first_tick_logged_symbols.add(symbol)
        payload = {
            "segment": "stocks",
            "symbol": symbol,
            "secid": int(secid),
            "tag": str(tag),
            "ltp": float(ltp),
            "depth": {
                "bid_price": list(getattr(depth, "bid_price", []) or []),
                "bid_qty": list(getattr(depth, "bid_qty", []) or []),
                "ask_price": list(getattr(depth, "ask_price", []) or []),
                "ask_qty": list(getattr(depth, "ask_qty", []) or []),
                "ts": float(getattr(depth, "ts", time.time()) or time.time()),
            },
            "features": dict(getattr(depth, "features", None) or {}),
            "raw": dict(getattr(depth, "raw", None) or {}),
            "diag": dict(getattr(depth, "diag", None) or {}),
        }
        logger.info(
            "STOCK_FIRST_FULL_TICK | symbol=%s | secid=%s | payload=%s",
            symbol,
            int(secid),
            json.dumps(self._json_safe(payload), sort_keys=True, separators=(",", ":")),
        )

    def on_quote(self, secid: int, tag: str, ltp: float, depth) -> None:
        now = time.time()
        instrument = self.instrument_by_secid.get(int(secid))
        if instrument is None or float(ltp) <= 0:
            return
        symbol = str(instrument["symbol"])
        self._log_first_full_tick(symbol=symbol, secid=int(secid), tag=str(tag), ltp=float(ltp), depth=depth)
        self._reset_day_if_needed(now)
        self.last_tick_ts[symbol] = now
        self.portfolio.mark(secid, ltp, now)
        position = self.portfolio.positions.get(int(secid))
        signal = self.engine.on_tick(
            symbol,
            float(ltp),
            getattr(depth, "features", None),
            now,
            in_position=position is not None,
        )
        zone_decision = None
        if getattr(self.settings, "five_minute_cycle_enabled", False):
            zone_decision = self.zone_tracker.update(symbol, float(ltp), now)
            if zone_decision is not None:
                logger.info(
                    "STOCK_FIVE_MINUTE_ZONE | symbol=%s | cycle_start=%.0f | origin=%.2f | current=%.2f | displacement=%+.4f | normalized=%+.3f | velocity=%+.5f | zone=%s | direction=%s",
                    symbol,
                    zone_decision.cycle_start,
                    zone_decision.origin_price,
                    zone_decision.current_price,
                    zone_decision.displacement,
                    zone_decision.normalized_displacement,
                    zone_decision.velocity_per_sec,
                    zone_decision.zone,
                    zone_decision.direction,
                )
                if getattr(self.settings, "force_cycle_trade_enabled", False):
                    self._record_cycle_candidate(zone_decision, signal, secid, symbol, ltp, now)

        if now - self.last_score_log_ts[symbol] >= self.settings.heartbeat_sec:
            self.last_score_log_ts[symbol] = now
            logger.info(
                "STOCK_PERCENT_STATE | symbol=%s | ltp=%.2f | score=%.1f | state=%.0f | support_watch=%.0f | reclaim=%.0f | entry_ready=%.0f | price_loc=%.1f | depth=%.1f | top_book=%.1f | flow=%.1f | momentum=%.1f | quality=%.1f | opp=%.1f | risk=%.1f | scalp=%.1f | swing=%.1f | regime=%.0f | exit_plan=%.0f | ret5=%.3f%% | ret15=%.3f%% | ret30=%.3f%% | vwap=%.3f%% | spread=%.3f%% | action=%s | reason=%s",
                symbol, float(ltp), signal.score,
                signal.features.get("market_state_code", 0.0),
                signal.features.get("support_watch", 0.0),
                signal.features.get("reclaim_confirmed", 0.0),
                signal.features.get("long_entry_ready", 0.0),
                signal.features.get("price_location_score", 0.0),
                signal.features.get("depth_support_score", 0.0),
                signal.features.get("top_book_score", 0.0),
                signal.features.get("flow_confirmation_score", 0.0),
                signal.features.get("momentum_score", 0.0),
                signal.features.get("quality_score", 0.0),
                signal.features.get("opportunity_score", 0.0),
                signal.features.get("risk_score", 0.0),
                signal.features.get("scalp_confidence", 0.0),
                signal.features.get("swing_confidence", 0.0),
                signal.features.get("regime_code", 0.0),
                signal.features.get("exit_plan_code", 0.0),
                signal.features["return_5s_pct"],
                signal.features.get("return_15s_pct", 0.0),
                signal.features["return_30s_pct"],
                signal.features["ltp_vs_avg_pct"], signal.features["spread_pct"],
                signal.action, signal.reason,
            )

        if (
            signal.action == "HOLD"
            and signal.features.get("ghost_candidate", 0.0) >= 1.0
            and now - self.last_ghost_log_ts[symbol] >= self.settings.heartbeat_sec
        ):
            self.last_ghost_log_ts[symbol] = now
            logger.info(
                "STOCK_GHOST_OPPORTUNITY | symbol=%s | ltp=%.2f | opp=%.1f | risk=%.1f | scalp=%.1f | swing=%.1f | regime=%.0f | blocked_by=%s",
                symbol,
                float(ltp),
                signal.features.get("opportunity_score", 0.0),
                signal.features.get("risk_score", 0.0),
                signal.features.get("scalp_confidence", 0.0),
                signal.features.get("swing_confidence", 0.0),
                signal.features.get("regime_code", 0.0),
                signal.reason,
            )

        if position is not None:
            reason = self._position_exit_reason(position, signal, now)
            if reason:
                trade = self.portfolio.exit(secid, ltp, reason, now)
                self.last_exit_ts[symbol] = now
                if trade:
                    self.trade_summary_sink.record(
                        "stocks",
                        {
                            "symbol": symbol,
                            "secid": int(secid),
                            "qty": int(trade["qty"]),
                            "side": trade.get("side", "LONG"),
                            "entry": float(trade["entry"]),
                            "exit": float(trade["exit"]),
                            "gross_pnl": float(trade["gross_pnl"]),
                            "fee": float(trade["fee"]),
                            "fee_estimated": bool(trade.get("fee_estimated", False)),
                            "fee_breakdown": dict(trade.get("fee_breakdown", {})),
                            "net_pnl": float(trade["net_pnl"]),
                            "hold_sec": float(trade["hold_sec"]),
                            "exit_reason": trade["reason"],
                            "runtime": "stock_percent_paper",
                            "strategy": "percent_normalized_v1",
                        },
                    )
                    logger.info(
                        "STOCK_TRADE_SUMMARY | %s | Side:%s | Qty:%s | Entry:%.2f | Exit:%.2f | GrossPnL:%+.2f | Fee:%.2f | NetPnL:%+.2f | Hold:%.1fs | ExitReason:%s",
                        symbol, trade.get("side", "LONG"), trade["qty"], trade["entry"], trade["exit"],
                        trade["gross_pnl"], trade["fee"], trade["net_pnl"],
                        trade["hold_sec"], trade["reason"],
                    )
                    breakdown = trade.get("fee_breakdown", {})
                    if trade.get("fee_estimated"):
                        logger.info(
                            "STOCK_FEE_BREAKDOWN | %s | Brokerage:%.2f | Exchange:%.2f | STT:%.2f | SEBI:%.2f | IPFT:%.2f | Stamp:%.2f | GST:%.2f | Total:%.2f",
                            symbol,
                            float(breakdown.get("brokerage", 0.0)),
                            float(breakdown.get("exchange", 0.0)),
                            float(breakdown.get("stt", 0.0)),
                            float(breakdown.get("sebi", 0.0)),
                            float(breakdown.get("ipft", 0.0)),
                            float(breakdown.get("stamp_duty", 0.0)),
                            float(breakdown.get("gst", 0.0)),
                            float(trade["fee"]),
                        )
            return

        entry_reason = None
        entry_score = float(signal.score)
        if getattr(self.settings, "five_minute_cycle_enabled", False):
            if zone_decision is not None and not getattr(self.settings, "force_cycle_trade_enabled", False):
                entry_reason = self._zone_entry_reason(zone_decision, signal)
                entry_score = max(entry_score, abs(zone_decision.normalized_displacement) * 100.0)
        elif signal.action == "ENTRY":
            entry_reason = signal.reason

        if entry_reason and self._entry_allowed(symbol, now):
            if self.portfolio.enter(secid, symbol, ltp, entry_score, now):
                self.daily_trades += 1
                position = self.portfolio.positions[int(secid)]
                logger.info(
                    "STOCK_ENTRY_COMMITTED | symbol=%s | secid=%s | qty=%s | entry=%.2f | score=%.1f | opp=%.1f | risk=%.1f | mode=%.0f | exit_plan=%.0f | reason=%s",
                    symbol,
                    secid,
                    position.qty,
                    float(ltp),
                    entry_score,
                    signal.features.get("opportunity_score", 0.0),
                    signal.features.get("risk_score", 0.0),
                    signal.features.get("entry_mode", 0.0),
                    signal.features.get("exit_plan_code", 0.0),
                    entry_reason,
                )
            else:
                logger.info(
                    "STOCK_ENTRY_REJECTED | symbol=%s | secid=%s | reason=PORTFOLIO_REJECTED | cash=%.2f | open_positions=%s | max_positions=%s | ltp=%.2f",
                    symbol,
                    secid,
                    self.portfolio.cash,
                    len(self.portfolio.positions),
                    self.settings.max_positions,
                    float(ltp),
                )

    def _health(self, now: float) -> None:
        if now - self.last_health_ts < self.settings.heartbeat_sec:
            return
        self.last_health_ts = now
        ages = ",".join(
            f"{symbol}:{(now - ts):.1f}s" for symbol, ts in sorted(self.last_tick_ts.items())
        ) or "waiting"
        stale = [symbol for symbol, ts in self.last_tick_ts.items() if now - ts > self.settings.stale_tick_sec]
        unrealized = self.portfolio.unrealized_pnl()
        realized = self.portfolio.realized_pnl
        daily_realized = realized - self.daily_realized_start
        net_pnl = realized + unrealized
        logger.info(
            "STOCK_FEED_HEALTH | symbols=%s | tick_ages=%s | stale=%s | open_positions=%s | cash=%.2f | equity=%.2f | realized=%+.2f | unrealized=%+.2f | trades_today=%s",
            ",".join(self.settings.symbols), ages, ",".join(stale) or "none",
            len(self.portfolio.positions), self.portfolio.cash, self.portfolio.equity(),
            realized, unrealized, self.daily_trades,
        )
        self.trade_summary_sink.record_portfolio(
            "stocks",
            {
                "symbols": list(self.settings.symbols),
                "capital": self.settings.capital,
                "cash": self.portfolio.cash,
                "equity": self.portfolio.equity(),
                "realized_pnl": realized,
                "unrealized_pnl": unrealized,
                "net_pnl": net_pnl,
                "daily_realized_pnl": daily_realized,
                "daily_unrealized_pnl": unrealized,
                "daily_net_pnl": daily_realized + unrealized,
                "open_positions": len(self.portfolio.positions),
                "trades_today": self.daily_trades,
                "closed_trades": self.portfolio.closed_trades,
                "runtime": "stock_percent_paper",
                "strategy": "percent_normalized_v1",
            },
        )

    def run(self) -> None:
        subscriptions = self._register_instruments()
        self.stream.replace_subscriptions(subscriptions, reason="stock_profiles_startup")
        self.stream.start()
        logger.info(
            "STOCK_PAPER_RUNTIME_ACTIVE | symbols=%s | subscriptions=%s | capital=%.2f | notional=%.2f | max_positions=%s | leverage=%.2fx | fee_model=%s | strategy=percent_normalized_v1 | five_minute_cycle=%s | cycle=%.0fs | observe=%.0fs | confirm=%.0fs | force_cycle_trade=%s | fixed_qty=%s | paper=true",
            ",".join(self.settings.symbols), len(subscriptions), self.settings.capital,
            self.settings.notional_per_trade, self.portfolio.max_positions,
            self.settings.leverage,
            "NSE_INTRADAY_DYNAMIC" if self.settings.dynamic_charges_enabled else "FIXED_FALLBACK",
            self.settings.five_minute_cycle_enabled, self.settings.cycle_sec,
            self.settings.observe_sec, self.settings.confirm_sec,
            self.settings.force_cycle_trade_enabled, self.settings.fixed_cycle_qty,
        )
        try:
            while True:
                now = time.time()
                self._health(now)
                time.sleep(1.0)
        except KeyboardInterrupt:
            logger.info("STOCK_PAPER_RUNTIME_STOPPED | reason=keyboard_interrupt")
        finally:
            self.stream.close()


def build_stock_paper_runtime(settings: StockPaperSettings) -> StockPaperRuntime:
    master = InstrumentMaster(settings.csv_file, debug=False)
    return StockPaperRuntime(settings, master)
