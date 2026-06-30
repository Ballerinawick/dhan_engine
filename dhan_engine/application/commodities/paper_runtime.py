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
from dhan_engine.domain.commodities.percent_engine import PercentNormalizedCommodityEngine
from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
from dhan_engine.simulations.commodity_paper_portfolio import CommodityPaperPortfolio


logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")
SYMBOL_ALIASES = {
    "CRUDE": "CRUDEOIL",
    "NATGAS": "NATURALGAS",
    "NATURAL GAS": "NATURALGAS",
}


def _env_symbols() -> Tuple[str, ...]:
    raw = os.getenv("COMMODITY_PAPER_SYMBOLS", "GOLD,CRUDEOIL,NATURALGAS")
    symbols = []
    for value in raw.split(","):
        symbol = SYMBOL_ALIASES.get(value.strip().upper(), value.strip().upper())
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return tuple(symbols)


def _env_time(name: str, default: str) -> dtime:
    value = os.getenv(name, default).strip() or default
    hour, minute = value.split(":", 1)
    return dtime(int(hour), int(minute))


@dataclass(frozen=True)
class CommodityPaperSettings:
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
    tick_audit_sec: float
    market_start: dtime
    market_end: dtime

    @classmethod
    def from_env(cls) -> "CommodityPaperSettings":
        client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
        access_token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
        if not client_id or not access_token:
            raise RuntimeError("Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN")
        return cls(
            client_id=client_id,
            access_token=access_token,
            csv_file=os.getenv("CSV_FILE", "api-scrip-master.csv").strip() or "api-scrip-master.csv",
            symbols=_env_symbols(),
            capital=float(os.getenv("COMMODITY_PAPER_CAPITAL", "500000") or 500000),
            notional_per_trade=float(os.getenv("COMMODITY_PAPER_NOTIONAL", "75000") or 75000),
            max_positions=int(os.getenv("COMMODITY_PAPER_MAX_POSITIONS", "2") or 2),
            round_trip_fee=float(os.getenv("COMMODITY_PAPER_ROUND_TRIP_FEE", "60") or 60),
            entry_score=float(os.getenv("COMMODITY_PERCENT_ENTRY_SCORE", "68") or 68),
            exit_score=float(os.getenv("COMMODITY_PERCENT_EXIT_SCORE", "38") or 38),
            max_spread_pct=float(os.getenv("COMMODITY_PERCENT_MAX_SPREAD_PCT", "0.22") or 0.22),
            stop_loss_pct=float(os.getenv("COMMODITY_PAPER_STOP_LOSS_PCT", "0.22") or 0.22),
            take_profit_pct=float(os.getenv("COMMODITY_PAPER_TAKE_PROFIT_PCT", "0.45") or 0.45),
            trail_arm_pct=float(os.getenv("COMMODITY_PAPER_TRAIL_ARM_PCT", "0.25") or 0.25),
            trail_giveback_pct=float(os.getenv("COMMODITY_PAPER_TRAIL_GIVEBACK_PCT", "0.16") or 0.16),
            max_hold_sec=float(os.getenv("COMMODITY_PAPER_MAX_HOLD_SEC", "900") or 900),
            entry_cooldown_sec=float(os.getenv("COMMODITY_PAPER_ENTRY_COOLDOWN_SEC", "60") or 60),
            stale_tick_sec=float(os.getenv("COMMODITY_PAPER_STALE_TICK_SEC", "12") or 12),
            max_daily_loss=float(os.getenv("COMMODITY_PAPER_MAX_DAILY_LOSS", "3000") or 3000),
            max_daily_trades=int(os.getenv("COMMODITY_PAPER_MAX_DAILY_TRADES", "20") or 20),
            heartbeat_sec=float(os.getenv("COMMODITY_PAPER_HEARTBEAT_SEC", "10") or 10),
            tick_audit_sec=float(os.getenv("COMMODITY_TICK_AUDIT_SEC", "15") or 15),
            market_start=_env_time("COMMODITY_MARKET_START", "09:00"),
            market_end=_env_time("COMMODITY_MARKET_END", "23:25"),
        )


class CommodityPaperRuntime:
    """Standalone MCX commodity futures paper runtime."""

    def __init__(self, settings: CommodityPaperSettings, master: InstrumentMaster):
        self.settings = settings
        self.master = master
        self.engine = PercentNormalizedCommodityEngine(
            entry_score=settings.entry_score,
            exit_score=settings.exit_score,
            max_spread_pct=settings.max_spread_pct,
        )
        self.portfolio = CommodityPaperPortfolio(
            capital=settings.capital,
            notional_per_trade=settings.notional_per_trade,
            max_positions=settings.max_positions,
            round_trip_fee=settings.round_trip_fee,
        )
        self.instrument_by_secid: Dict[int, dict] = {}
        self.secid_by_symbol: Dict[str, int] = {}
        self.last_tick_ts: Dict[str, float] = defaultdict(float)
        self.last_score_log_ts: Dict[str, float] = defaultdict(float)
        self.last_audit_log_ts: Dict[str, float] = defaultdict(float)
        self.last_exit_ts: Dict[str, float] = defaultdict(float)
        self.last_health_ts = 0.0
        self.daily_trades = 0
        self.daily_date = datetime.now(IST).date()
        self.daily_realized_start = 0.0
        self.stream = FutureQuoteStream(
            client_id=settings.client_id,
            token=settings.access_token,
            exchange_segment="MCX_COMM",
            on_quote=self.on_quote,
            debug=False,
            shard_count=1,
        )

    @staticmethod
    def _feature(features: dict, key: str, default: float = 0.0) -> float:
        try:
            return float(features.get(key, default) or default)
        except Exception:
            return float(default)

    def _register_instruments(self) -> list[tuple[int, str]]:
        subscriptions = []
        for symbol in self.settings.symbols:
            instrument = self.master.get_nearest_commodity_future(symbol)
            secid = int(instrument["security_id"])
            self.instrument_by_secid[secid] = instrument
            self.secid_by_symbol[symbol] = secid
            subscriptions.append((secid, f"{symbol}_FUT"))
            logger.info(
                "COMMODITY_PROFILE_REGISTERED | symbol=%s | trading_symbol=%s | secid=%s | expiry=%s | lot_size=%s",
                symbol,
                instrument["trading_symbol"],
                secid,
                instrument["expiry"],
                instrument["lot_size"],
            )
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
            return "COMMODITY_PERCENT_STOP_LOSS"
        if pnl_pct >= self.settings.take_profit_pct:
            return "COMMODITY_PERCENT_TAKE_PROFIT"
        if peak_pct >= self.settings.trail_arm_pct and drawdown_pct >= self.settings.trail_giveback_pct:
            return "COMMODITY_PERCENT_TRAIL"
        if now - position.entry_ts >= self.settings.max_hold_sec:
            return "COMMODITY_PERCENT_MAX_HOLD"
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
        raw_features = dict(getattr(depth, "features", None) or {})
        signal = self.engine.on_tick(
            symbol,
            float(ltp),
            raw_features,
            now,
            in_position=position is not None,
        )

        if now - self.last_score_log_ts[symbol] >= self.settings.heartbeat_sec:
            self.last_score_log_ts[symbol] = now
            logger.info(
                "COMMODITY_PERCENT_STATE | symbol=%s | contract=%s | ltp=%.2f | score=%.1f | ret5=%.3f%% | ret30=%.3f%% | ret120=%.3f%% | vwap=%.3f%% | spread=%.3f%% | action=%s | reason=%s",
                symbol,
                instrument["trading_symbol"],
                float(ltp),
                signal.score,
                signal.features["return_5s_pct"],
                signal.features["return_30s_pct"],
                signal.features["return_120s_pct"],
                signal.features["ltp_vs_avg_pct"],
                signal.features["spread_pct"],
                signal.action,
                signal.reason,
            )

        if now - self.last_audit_log_ts[symbol] >= self.settings.tick_audit_sec:
            self.last_audit_log_ts[symbol] = now
            logger.info(
                "COMMODITY_TICK_AUDIT | symbol=%s | contract=%s | ltp=%.2f | open=%.2f | prev_close=%.2f | high=%.2f | low=%.2f | day_pos=%.1f%% | avg=%.2f | vwap_bias=%.3f%% | bid=%.2f | ask=%.2f | spread=%.3f%% | bid_qty5=%s | ask_qty5=%s | depth_imb=%.1f%% | top_imb=%.1f%% | buy_qty=%s | sell_qty=%s | queue_imb=%.1f%% | volume=%s | vol_chg=%+.0f | oi=%s | oi_chg=%+.0f | clean=%.1f%% | spoof=%.1f%% | recovery=%.1f%% | exhaustion=%.1f%% | orderflow=%.1f | liquidity=%.1f | score=%.1f | action=%s | reason=%s",
                symbol,
                instrument["trading_symbol"],
                float(ltp),
                self._feature(raw_features, "open_price"),
                self._feature(raw_features, "prev_close"),
                self._feature(raw_features, "day_high"),
                self._feature(raw_features, "day_low"),
                signal.features["day_position_pct"],
                self._feature(raw_features, "avg_price"),
                signal.features["ltp_vs_avg_pct"],
                self._feature(raw_features, "best_bid"),
                self._feature(raw_features, "best_ask"),
                signal.features["spread_pct"],
                int(self._feature(raw_features, "bid_qty_5")),
                int(self._feature(raw_features, "ask_qty_5")),
                signal.features["depth_imbalance_pct"],
                signal.features["top_depth_imbalance_pct"],
                int(self._feature(raw_features, "total_buy_quantity")),
                int(self._feature(raw_features, "total_sell_quantity")),
                signal.features["market_imbalance_pct"],
                int(self._feature(raw_features, "volume")),
                signal.features["volume_change_tick"],
                int(self._feature(raw_features, "oi")),
                signal.features["oi_change_tick"],
                signal.features["clean_trade_pct"],
                signal.features["spoof_risk_pct"],
                signal.features["recovery_pct"],
                signal.features["exhaustion_pct"],
                signal.features["orderflow_score"],
                signal.features["liquidity_score"],
                signal.score,
                signal.action,
                signal.reason,
            )

        if position is not None:
            reason = self._position_exit_reason(position, signal, now)
            if reason:
                trade = self.portfolio.exit(secid, ltp, reason, now)
                self.last_exit_ts[symbol] = now
                if trade:
                    logger.info(
                        "COMMODITY_TRADE_SUMMARY | %s | Contract:%s | Qty:%s | Entry:%.2f | Exit:%.2f | GrossPnL:%+.2f | Fee:%.2f | NetPnL:%+.2f | Hold:%.1fs | ExitReason:%s",
                        symbol,
                        trade["trading_symbol"],
                        trade["qty"],
                        trade["entry"],
                        trade["exit"],
                        trade["gross_pnl"],
                        trade["fee"],
                        trade["net_pnl"],
                        trade["hold_sec"],
                        trade["reason"],
                    )
            return

        if signal.action == "ENTRY" and self._entry_allowed(symbol, now):
            if self.portfolio.enter(
                secid,
                symbol,
                instrument["trading_symbol"],
                ltp,
                signal.score,
                now,
            ):
                self.daily_trades += 1
                position = self.portfolio.positions[int(secid)]
                logger.info(
                    "COMMODITY_ENTRY_COMMITTED | symbol=%s | contract=%s | secid=%s | qty=%s | entry=%.2f | score=%.1f | reason=%s",
                    symbol,
                    instrument["trading_symbol"],
                    secid,
                    position.qty,
                    float(ltp),
                    signal.score,
                    signal.reason,
                )
        elif signal.action == "ENTRY":
            logger.info(
                "COMMODITY_ENTRY_REJECTED | symbol=%s | contract=%s | reason=RUNTIME_GUARD | market_open=%s | trades_today=%s | open_positions=%s | stale_age=%.1fs | cooldown_left=%.1fs",
                symbol,
                instrument["trading_symbol"],
                self._market_open(now),
                self.daily_trades,
                len(self.portfolio.positions),
                now - float(self.last_tick_ts.get(symbol, 0.0) or 0.0),
                max(0.0, self.settings.entry_cooldown_sec - (now - float(self.last_exit_ts.get(symbol, 0.0) or 0.0))),
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
            "COMMODITY_FEED_HEALTH | symbols=%s | tick_ages=%s | stale=%s | open_positions=%s | cash=%.2f | equity=%.2f | realized=%+.2f | unrealized=%+.2f | trades_today=%s",
            ",".join(self.settings.symbols),
            ages,
            ",".join(stale) or "none",
            len(self.portfolio.positions),
            self.portfolio.cash,
            self.portfolio.equity(),
            self.portfolio.realized_pnl,
            self.portfolio.unrealized_pnl(),
            self.daily_trades,
        )

    def run(self) -> None:
        subscriptions = self._register_instruments()
        self.stream.replace_subscriptions(subscriptions, reason="commodity_profiles_startup")
        self.stream.start()
        logger.info(
            "COMMODITY_PAPER_RUNTIME_ACTIVE | symbols=%s | subscriptions=%s | capital=%.2f | notional=%.2f | max_positions=%s | market=%s-%s | strategy=commodity_percent_normalized_v1",
            ",".join(self.settings.symbols),
            len(subscriptions),
            self.settings.capital,
            self.settings.notional_per_trade,
            self.settings.max_positions,
            self.settings.market_start.strftime("%H:%M"),
            self.settings.market_end.strftime("%H:%M"),
        )
        try:
            while True:
                now = time.time()
                self._health(now)
                time.sleep(1.0)
        except KeyboardInterrupt:
            logger.info("COMMODITY_PAPER_RUNTIME_STOPPED | reason=keyboard_interrupt")
        finally:
            self.stream.close()


def build_commodity_paper_runtime(settings: CommodityPaperSettings) -> CommodityPaperRuntime:
    master = InstrumentMaster(settings.csv_file, debug=False)
    return CommodityPaperRuntime(settings, master)
