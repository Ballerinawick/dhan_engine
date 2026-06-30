from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import Dict, Tuple
from zoneinfo import ZoneInfo

from dhan_engine.application.market_data import FutureQuoteStream
from dhan_engine.domain.stocks.percent_engine import PercentNormalizedStockEngine
from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
from dhan_engine.simulations.stock_paper_portfolio import StockPaperPortfolio


logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")
SYMBOL_ALIASES = {"HDFC": "HDFCBANK", "SBI": "SBIN", "ICICI": "ICICIBANK"}


def _env_symbols() -> Tuple[str, ...]:
    raw = os.getenv("STOCK_PAPER_SYMBOLS", "RELIANCE,ICICIBANK,SBIN,HDFCBANK")
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
    entry_score: float
    exit_score: float
    max_spread_pct: float
    stop_loss_pct: float
    take_profit_pct: float
    trail_arm_pct: float
    trail_giveback_pct: float
    max_hold_sec: float
    entry_cooldown_sec: float
    stale_tick_sec: float
    max_daily_loss: float
    max_daily_trades: int
    heartbeat_sec: float
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
            entry_score=float(os.getenv("STOCK_PERCENT_ENTRY_SCORE", "72") or 72),
            exit_score=float(os.getenv("STOCK_PERCENT_EXIT_SCORE", "40") or 40),
            max_spread_pct=float(os.getenv("STOCK_PERCENT_MAX_SPREAD_PCT", "0.18") or 0.18),
            stop_loss_pct=float(os.getenv("STOCK_PAPER_STOP_LOSS_PCT", "0.35") or 0.35),
            take_profit_pct=float(os.getenv("STOCK_PAPER_TAKE_PROFIT_PCT", "0.80") or 0.80),
            trail_arm_pct=float(os.getenv("STOCK_PAPER_TRAIL_ARM_PCT", "0.40") or 0.40),
            trail_giveback_pct=float(os.getenv("STOCK_PAPER_TRAIL_GIVEBACK_PCT", "0.25") or 0.25),
            max_hold_sec=float(os.getenv("STOCK_PAPER_MAX_HOLD_SEC", "900") or 900),
            entry_cooldown_sec=float(os.getenv("STOCK_PAPER_ENTRY_COOLDOWN_SEC", "90") or 90),
            stale_tick_sec=float(os.getenv("STOCK_PAPER_STALE_TICK_SEC", "10") or 10),
            max_daily_loss=float(os.getenv("STOCK_PAPER_MAX_DAILY_LOSS", "3000") or 3000),
            max_daily_trades=int(os.getenv("STOCK_PAPER_MAX_DAILY_TRADES", "20") or 20),
            heartbeat_sec=float(os.getenv("STOCK_PAPER_HEARTBEAT_SEC", "10") or 10),
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
        self.portfolio = StockPaperPortfolio(
            capital=settings.capital,
            notional_per_trade=settings.notional_per_trade,
            max_positions=settings.max_positions,
            round_trip_fee=settings.round_trip_fee,
        )
        self.instrument_by_secid: Dict[int, dict] = {}
        self.secid_by_symbol: Dict[str, int] = {}
        self.last_tick_ts: Dict[str, float] = defaultdict(float)
        self.last_score_log_ts: Dict[str, float] = defaultdict(float)
        self.last_ghost_log_ts: Dict[str, float] = defaultdict(float)
        self.last_exit_ts: Dict[str, float] = defaultdict(float)
        self.last_health_ts = 0.0
        self.daily_trades = 0
        self.daily_date = datetime.now(IST).date()
        self.daily_realized_start = 0.0
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
        if self.daily_trades >= self.settings.max_daily_trades:
            return False
        daily_realized = self.portfolio.realized_pnl - self.daily_realized_start
        if daily_realized <= -abs(self.settings.max_daily_loss):
            return False
        return True

    def _position_exit_reason(self, position, signal, now: float) -> str | None:
        pnl_pct = ((signal.ltp - position.entry) / position.entry) * 100.0
        peak_pct = ((position.peak_ltp - position.entry) / position.entry) * 100.0
        drawdown_pct = ((position.peak_ltp - signal.ltp) / position.peak_ltp) * 100.0
        if pnl_pct <= -abs(self.settings.stop_loss_pct):
            return "STOCK_PERCENT_STOP_LOSS"
        if pnl_pct >= self.settings.take_profit_pct:
            return "STOCK_PERCENT_TAKE_PROFIT"
        if peak_pct >= self.settings.trail_arm_pct and drawdown_pct >= self.settings.trail_giveback_pct:
            return "STOCK_PERCENT_TRAIL"
        if now - position.entry_ts >= self.settings.max_hold_sec:
            return "STOCK_PERCENT_MAX_HOLD"
        if signal.action == "EXIT":
            return signal.reason
        return None

    def on_quote(self, secid: int, tag: str, ltp: float, depth) -> None:
        now = time.time()
        instrument = self.instrument_by_secid.get(int(secid))
        if instrument is None or float(ltp) <= 0:
            return
        symbol = str(instrument["symbol"])
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

        if now - self.last_score_log_ts[symbol] >= self.settings.heartbeat_sec:
            self.last_score_log_ts[symbol] = now
            logger.info(
                "STOCK_PERCENT_STATE | symbol=%s | ltp=%.2f | score=%.1f | opp=%.1f | risk=%.1f | scalp=%.1f | swing=%.1f | regime=%.0f | exit_plan=%.0f | ret5=%.3f%% | ret30=%.3f%% | vwap=%.3f%% | spread=%.3f%% | action=%s | reason=%s",
                symbol, float(ltp), signal.score,
                signal.features.get("opportunity_score", 0.0),
                signal.features.get("risk_score", 0.0),
                signal.features.get("scalp_confidence", 0.0),
                signal.features.get("swing_confidence", 0.0),
                signal.features.get("regime_code", 0.0),
                signal.features.get("exit_plan_code", 0.0),
                signal.features["return_5s_pct"], signal.features["return_30s_pct"],
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
                    logger.info(
                        "STOCK_TRADE_SUMMARY | %s | Qty:%s | Entry:%.2f | Exit:%.2f | GrossPnL:%+.2f | Fee:%.2f | NetPnL:%+.2f | Hold:%.1fs | ExitReason:%s",
                        symbol, trade["qty"], trade["entry"], trade["exit"],
                        trade["gross_pnl"], trade["fee"], trade["net_pnl"],
                        trade["hold_sec"], trade["reason"],
                    )
            return

        if signal.action == "ENTRY" and self._entry_allowed(symbol, now):
            if self.portfolio.enter(secid, symbol, ltp, signal.score, now):
                self.daily_trades += 1
                position = self.portfolio.positions[int(secid)]
                logger.info(
                    "STOCK_ENTRY_COMMITTED | symbol=%s | secid=%s | qty=%s | entry=%.2f | score=%.1f | opp=%.1f | risk=%.1f | mode=%.0f | exit_plan=%.0f | reason=%s",
                    symbol,
                    secid,
                    position.qty,
                    float(ltp),
                    signal.score,
                    signal.features.get("opportunity_score", 0.0),
                    signal.features.get("risk_score", 0.0),
                    signal.features.get("entry_mode", 0.0),
                    signal.features.get("exit_plan_code", 0.0),
                    signal.reason,
                )

    def _health(self, now: float) -> None:
        if now - self.last_health_ts < self.settings.heartbeat_sec:
            return
        self.last_health_ts = now
        ages = ",".join(
            f"{symbol}:{(now - ts):.1f}s" for symbol, ts in sorted(self.last_tick_ts.items())
        ) or "waiting"
        stale = [symbol for symbol, ts in self.last_tick_ts.items() if now - ts > self.settings.stale_tick_sec]
        logger.info(
            "STOCK_FEED_HEALTH | symbols=%s | tick_ages=%s | stale=%s | open_positions=%s | cash=%.2f | equity=%.2f | realized=%+.2f | unrealized=%+.2f | trades_today=%s",
            ",".join(self.settings.symbols), ages, ",".join(stale) or "none",
            len(self.portfolio.positions), self.portfolio.cash, self.portfolio.equity(),
            self.portfolio.realized_pnl, self.portfolio.unrealized_pnl(), self.daily_trades,
        )

    def run(self) -> None:
        subscriptions = self._register_instruments()
        self.stream.replace_subscriptions(subscriptions, reason="stock_profiles_startup")
        self.stream.start()
        logger.info(
            "STOCK_PAPER_RUNTIME_ACTIVE | symbols=%s | subscriptions=%s | capital=%.2f | notional=%.2f | max_positions=%s | strategy=percent_normalized_v1",
            ",".join(self.settings.symbols), len(subscriptions), self.settings.capital,
            self.settings.notional_per_trade, self.settings.max_positions,
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
