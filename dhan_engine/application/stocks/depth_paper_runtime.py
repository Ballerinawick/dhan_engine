from __future__ import annotations

import logging
import math
import os
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, time as clock_time
from typing import Callable, Mapping
from zoneinfo import ZoneInfo

from dhan_engine.application.deeplob.premodel_paper_runtime import (
    MarketByPricePaperRuntime,
    MarketByPricePaperSettings,
)
from dhan_engine.application.deeplob.trade_summary_s3 import (
    TradeSummaryS3Settings,
    TradeSummaryS3Sink,
)
from dhan_engine.domain.market.liquidity_event_state import LiquidityEventTracker
from dhan_engine.domain.market.market_by_price_execution import (
    CompositeMarketSnapshot,
    derive_market_by_price_features,
    validate_composite_snapshot,
)
from dhan_engine.domain.stocks.equity_charges import NseIntradayChargeCalculator
from dhan_engine.simulations.stock_paper_portfolio import StockPaperPortfolio

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


def _clock(value: str, default: str) -> clock_time:
    text = (value or default).strip()
    try:
        hour, minute = (int(part) for part in text.split(":", 1))
        return clock_time(hour=hour, minute=minute)
    except (TypeError, ValueError):
        hour, minute = (int(part) for part in default.split(":", 1))
        return clock_time(hour=hour, minute=minute)


@dataclass(frozen=True)
class StockDepthPaperSettings:
    client_id: str
    access_token: str
    csv_file: str
    symbols: tuple[str, ...]
    capital: float
    notional_per_trade: float
    max_positions: int
    leverage: float
    fixed_qty: int
    entry_confirmations: int
    exit_confirmations: int
    uncertain_exit_confirmations: int
    min_confidence: float
    min_edge_strength: float
    min_forecast_reliability: float
    max_quote_age_sec: float
    max_future_spread_bps: float
    max_cash_spread_bps: float
    cash_beta: float
    min_cost_multiple: float
    market_start: clock_time
    entry_cutoff: clock_time
    market_end: clock_time
    health_interval_sec: float
    trade_s3_bucket: str
    trade_s3_prefix: str
    trade_s3_queue_size: int

    @classmethod
    def from_env(cls) -> "StockDepthPaperSettings":
        symbols = tuple(
            dict.fromkeys(
                item.strip().upper()
                for item in os.getenv(
                    "STOCK_DEPTH_SYMBOLS", "RELIANCE,HDFCBANK"
                ).split(",")
                if item.strip()
            )
        )
        if not symbols:
            raise ValueError("STOCK_DEPTH_SYMBOLS must contain at least one symbol")
        if len(symbols) > 5:
            raise ValueError(
                "STOCK_DEPTH_SYMBOLS supports at most five symbols because Dhan "
                "Full Market Depth allows five concurrent instrument connections"
            )
        return cls(
            client_id=os.getenv("DHAN_CLIENT_ID", "").strip(),
            access_token=os.getenv("DHAN_ACCESS_TOKEN", "").strip(),
            csv_file=os.getenv("CSV_FILE", "api-scrip-master.csv").strip(),
            symbols=symbols,
            capital=max(1.0, float(os.getenv("STOCK_DEPTH_CAPITAL", "500000"))),
            notional_per_trade=max(
                1.0, float(os.getenv("STOCK_DEPTH_NOTIONAL_PER_TRADE", "50000"))
            ),
            max_positions=max(1, int(os.getenv("STOCK_DEPTH_MAX_POSITIONS", "2"))),
            leverage=max(1.0, float(os.getenv("STOCK_DEPTH_LEVERAGE", "5"))),
            fixed_qty=max(0, int(os.getenv("STOCK_DEPTH_FIXED_QTY", "0"))),
            entry_confirmations=max(
                1, int(os.getenv("STOCK_DEPTH_SIGNAL_CONFIRMATIONS", "3"))
            ),
            exit_confirmations=max(
                1, int(os.getenv("STOCK_DEPTH_EXIT_CONFIRMATIONS", "3"))
            ),
            uncertain_exit_confirmations=max(
                1, int(os.getenv("STOCK_DEPTH_UNCERTAIN_EXIT_CONFIRMATIONS", "5"))
            ),
            min_confidence=max(
                0.0, min(1.0, float(os.getenv("STOCK_DEPTH_MIN_CONFIDENCE", "0.65")))
            ),
            min_edge_strength=max(
                0.0, float(os.getenv("STOCK_DEPTH_MIN_EDGE_STRENGTH", "0.08"))
            ),
            min_forecast_reliability=max(
                0.0,
                min(
                    1.0,
                    float(os.getenv("STOCK_DEPTH_MIN_FORECAST_RELIABILITY", "0.35")),
                ),
            ),
            max_quote_age_sec=max(
                0.1, float(os.getenv("STOCK_DEPTH_MAX_QUOTE_AGE_SEC", "2"))
            ),
            max_future_spread_bps=max(
                0.1, float(os.getenv("STOCK_DEPTH_MAX_FUTURE_SPREAD_BPS", "25"))
            ),
            max_cash_spread_bps=max(
                0.1, float(os.getenv("STOCK_DEPTH_MAX_CASH_SPREAD_BPS", "20"))
            ),
            cash_beta=max(0.0, float(os.getenv("STOCK_DEPTH_CASH_BETA", "1"))),
            min_cost_multiple=max(
                1.0, float(os.getenv("STOCK_DEPTH_MIN_COST_MULTIPLE", "1.5"))
            ),
            market_start=_clock(os.getenv("STOCK_DEPTH_MARKET_START", "09:15"), "09:15"),
            entry_cutoff=_clock(
                os.getenv("STOCK_DEPTH_ENTRY_CUTOFF", "15:10"), "15:10"
            ),
            market_end=_clock(os.getenv("STOCK_DEPTH_MARKET_END", "15:15"), "15:15"),
            health_interval_sec=max(
                5.0, float(os.getenv("STOCK_DEPTH_HEALTH_INTERVAL_SEC", "30"))
            ),
            trade_s3_bucket=os.getenv("DEEPLOB_S3_BUCKET", "").strip(),
            trade_s3_prefix=os.getenv(
                "STOCK_DEPTH_TRADE_S3_PREFIX", "paper-trades/stock-depth"
            ).strip().strip("/"),
            trade_s3_queue_size=max(
                16, int(os.getenv("STOCK_DEPTH_TRADE_S3_QUEUE_SIZE", "256"))
            ),
        )


@dataclass(frozen=True)
class StockProfile:
    root: str
    future_tag: str
    future_secid: int
    future_symbol: str
    future_expiry: str
    cash_tag: str
    cash_secid: int
    cash_symbol: str


@dataclass(frozen=True)
class CashQuote:
    ltp: float
    bid: float
    ask: float
    received_ts: float

    @property
    def spread_bps(self) -> float:
        mid = (self.bid + self.ask) / 2.0
        return (self.ask - self.bid) / mid * 10_000.0 if mid > 0 else math.inf

    def executable_entry(self, side: str) -> float:
        return self.ask if side == "LONG" else self.bid

    def executable_exit(self, side: str) -> float:
        return self.bid if side == "LONG" else self.ask


@dataclass
class SignalState:
    direction: str = "FLAT"
    streak: int = 0
    uncertain_streak: int = 0
    last_signal_ts: float = 0.0
    last_confidence: float = 0.0
    last_metadata: dict = field(default_factory=dict)


class StockDepthPaperExecutor:
    """State-driven cash-equity paper execution from FUTSTK 200-depth evidence."""

    profile = "stock_depth_v1"
    strategy = "futstk_200depth_cash_intraday_v1"

    def __init__(
        self,
        settings: StockDepthPaperSettings,
        portfolio: StockPaperPortfolio,
        trade_summary_sink: TradeSummaryS3Sink | None = None,
        *,
        clock_fn: Callable[[], float] = time.time,
    ):
        self.settings = settings
        self.portfolio = portfolio
        self.trade_summary_sink = trade_summary_sink
        self._profiles_by_future_tag: dict[str, StockProfile] = {}
        self._profiles_by_cash_secid: dict[int, StockProfile] = {}
        self._cash_quotes: dict[int, CashQuote] = {}
        self._signals: dict[str, SignalState] = defaultdict(SignalState)
        self._entry_context: dict[int, dict] = {}
        self._lock = threading.RLock()
        self._clock_fn = clock_fn
        self._charge_calculator = NseIntradayChargeCalculator()
        self._entries = 0
        self._exits = 0
        self._blocked = defaultdict(int)
        self._last_block_log: dict[tuple[str, str], float] = {}

    def register(self, profile: StockProfile) -> None:
        with self._lock:
            self._profiles_by_future_tag[profile.future_tag] = profile
            self._profiles_by_cash_secid[profile.cash_secid] = profile

    def on_cash_quote(
        self,
        secid: int,
        ltp: float,
        bid: float,
        ask: float,
        received_ts: float,
    ) -> None:
        if min(float(ltp), float(bid), float(ask)) <= 0 or ask < bid:
            return
        quote = CashQuote(float(ltp), float(bid), float(ask), float(received_ts))
        with self._lock:
            self._cash_quotes[int(secid)] = quote
            position = self.portfolio.positions.get(int(secid))
            if position is not None:
                self.portfolio.mark(
                    int(secid), quote.executable_exit(position.side), received_ts
                )

    @staticmethod
    def _direction(paper_action: str) -> str:
        if paper_action == "BUY_CE":
            return "LONG"
        if paper_action == "BUY_PE":
            return "SHORT"
        return "FLAT"

    def _market_phase(self, now: float) -> str:
        local = datetime.fromtimestamp(now, IST).time().replace(tzinfo=None)
        if local < self.settings.market_start:
            return "PRE_MARKET"
        if local >= self.settings.market_end:
            return "CLOSED"
        if local >= self.settings.entry_cutoff:
            return "EXIT_ONLY"
        return "OPEN"

    def _block(self, profile: StockProfile, reason: str, **values) -> None:
        self._blocked[reason] += 1
        now = self._clock_fn()
        key = (profile.root, reason)
        if now - self._last_block_log.get(key, 0.0) < 10.0:
            return
        self._last_block_log[key] = now
        extras = " | ".join(f"{key}={value}" for key, value in values.items())
        logger.info(
            "STOCK_DEPTH_ENTRY_BLOCKED | symbol=%s | reason=%s%s",
            profile.root,
            reason,
            f" | {extras}" if extras else "",
        )

    def on_prediction(
        self,
        *,
        paper_action: str,
        confidence: float,
        composite: CompositeMarketSnapshot,
        probability_down: float,
        probability_flat: float,
        probability_up: float,
        model_version: str,
        horizon_sec: int,
        signal_metadata: Mapping[str, object] | None = None,
    ) -> None:
        now = self._clock_fn()
        metadata = dict(signal_metadata or {})
        tag = str(composite.book.name)
        direction = self._direction(str(paper_action))
        with self._lock:
            profile = self._profiles_by_future_tag.get(tag)
            if profile is None:
                return
            state = self._signals[tag]
            if direction == state.direction:
                state.streak += 1
            else:
                state.direction = direction
                state.streak = 1
            state.uncertain_streak = (
                state.uncertain_streak + 1 if direction == "FLAT" else 0
            )
            state.last_signal_ts = now
            state.last_confidence = float(confidence)
            state.last_metadata = metadata

            logger.info(
                "STOCK_DEPTH_SIGNAL | symbol=%s | future=%s | action=%s | "
                "state=%s | streak=%s | confidence=%.4f | edge=%+.4f | "
                "reliability=%.3f | ltp=%s | expected_future_bps=%s | "
                "horizon_sec=%s | orders=false",
                profile.root,
                profile.future_symbol,
                paper_action,
                direction,
                state.streak,
                confidence,
                float(metadata.get("edge_strength", 0.0) or 0.0),
                float(metadata.get("forecast_reliability", 0.0) or 0.0),
                metadata.get("ltp_now", composite.full_quote.get("ltp")),
                metadata.get("expected_future_move_bps"),
                horizon_sec,
            )

            position = self.portfolio.positions.get(profile.cash_secid)
            if position is not None:
                if direction in {"LONG", "SHORT"} and direction != position.side:
                    if state.streak >= self.settings.exit_confirmations:
                        self._exit_locked(
                            profile,
                            f"FUTURE_STATE_OPPOSITE:{direction}",
                            now,
                            model_version=model_version,
                            metadata=metadata,
                        )
                    return
                weak_state = (
                    direction == "FLAT"
                    and not bool(metadata.get("edge_active", False))
                    and float(metadata.get("edge_strength", 0.0) or 0.0)
                    < self.settings.min_edge_strength
                )
                if weak_state and state.uncertain_streak >= self.settings.uncertain_exit_confirmations:
                    self._exit_locked(
                        profile,
                        "FUTURE_STATE_EXHAUSTED",
                        now,
                        model_version=model_version,
                        metadata=metadata,
                    )
                return

            if direction == "FLAT" or state.streak < self.settings.entry_confirmations:
                return
            self._try_entry_locked(
                profile,
                direction,
                float(confidence),
                metadata,
                now,
                model_version=model_version,
                horizon_sec=int(horizon_sec),
                probabilities={
                    "down": float(probability_down),
                    "flat": float(probability_flat),
                    "up": float(probability_up),
                },
            )

    def _try_entry_locked(
        self,
        profile: StockProfile,
        direction: str,
        confidence: float,
        metadata: dict,
        now: float,
        *,
        model_version: str,
        horizon_sec: int,
        probabilities: dict,
    ) -> None:
        phase = self._market_phase(now)
        if phase != "OPEN":
            self._block(profile, f"MARKET_{phase}")
            return
        quote = self._cash_quotes.get(profile.cash_secid)
        if quote is None:
            self._block(profile, "CASH_QUOTE_MISSING")
            return
        quote_age = max(0.0, now - quote.received_ts)
        if quote_age > self.settings.max_quote_age_sec:
            self._block(profile, "CASH_QUOTE_STALE", age_sec=round(quote_age, 3))
            return
        if quote.spread_bps > self.settings.max_cash_spread_bps:
            self._block(
                profile, "CASH_SPREAD_TOO_WIDE", spread_bps=round(quote.spread_bps, 3)
            )
            return
        edge_strength = abs(float(metadata.get("edge_strength", 0.0) or 0.0))
        reliability = float(metadata.get("forecast_reliability", 0.0) or 0.0)
        if confidence < self.settings.min_confidence:
            self._block(profile, "CONFIDENCE_LOW", value=round(confidence, 4))
            return
        if edge_strength < self.settings.min_edge_strength:
            self._block(profile, "EDGE_WEAK", value=round(edge_strength, 4))
            return
        if reliability < self.settings.min_forecast_reliability:
            self._block(profile, "FORECAST_RELIABILITY_LOW", value=round(reliability, 4))
            return
        expected_bps = abs(
            float(metadata.get("expected_future_move_bps", 0.0) or 0.0)
        ) * self.settings.cash_beta
        entry_price = quote.executable_entry(direction)
        qty = self.settings.fixed_qty or int(
            self.settings.notional_per_trade // entry_price
        )
        qty = max(qty, 0)
        if qty <= 0:
            self._block(profile, "QTY_ZERO", price=entry_price)
            return
        estimated_fee = self._charge_calculator.estimate(
            entry_price, entry_price, qty
        ).total
        expected_gross = entry_price * expected_bps / 10_000.0 * qty
        required_gross = estimated_fee * self.settings.min_cost_multiple
        if expected_gross < required_gross:
            self._block(
                profile,
                "EXPECTED_GROSS_BELOW_COST_BUFFER",
                expected_gross=round(expected_gross, 2),
                required_gross=round(required_gross, 2),
                qty=qty,
            )
            return
        entered = self.portfolio.enter(
            profile.cash_secid,
            profile.cash_symbol,
            entry_price,
            confidence,
            now=now,
            side=direction,
            qty_override=qty,
        )
        if not entered:
            self._block(profile, "PORTFOLIO_REJECTED")
            return
        self._entry_context[profile.cash_secid] = {
            "future_secid": profile.future_secid,
            "future_symbol": profile.future_symbol,
            "future_expiry": profile.future_expiry,
            "model_version": model_version,
            "horizon_sec": horizon_sec,
            "probabilities": probabilities,
            "signal_metadata": metadata,
            "expected_future_move_bps": expected_bps,
            "expected_gross": expected_gross,
            "required_gross": required_gross,
        }
        self._entries += 1
        logger.info(
            "STOCK_DEPTH_PAPER_ENTRY | symbol=%s | side=%s | qty=%s | entry=%.2f | "
            "future=%s | confidence=%.4f | expected_gross=%.2f | fee_estimate=%.2f | "
            "horizon_sec=%s | orders=false",
            profile.root,
            direction,
            qty,
            entry_price,
            profile.future_symbol,
            confidence,
            expected_gross,
            estimated_fee,
            horizon_sec,
        )

    def _exit_locked(
        self,
        profile: StockProfile,
        reason: str,
        now: float,
        *,
        model_version: str = "",
        metadata: Mapping[str, object] | None = None,
    ) -> dict | None:
        position = self.portfolio.positions.get(profile.cash_secid)
        quote = self._cash_quotes.get(profile.cash_secid)
        if position is None or quote is None:
            return None
        entry_ts = position.entry_ts
        entry_context = dict(self._entry_context.get(profile.cash_secid, {}))
        exit_price = quote.executable_exit(position.side)
        trade = self.portfolio.exit(
            profile.cash_secid, exit_price, reason, now=now
        )
        if trade is None:
            return None
        self._entry_context.pop(profile.cash_secid, None)
        self._exits += 1
        summary = {
            **trade,
            "trade_id": (
                f"stock-depth:{profile.root}:{int(entry_ts * 1_000_000_000)}:"
                f"{int(now * 1_000_000_000)}"
            ),
            "index": "STOCKS",
            "tag": profile.root,
            "profile": self.profile,
            "strategy": self.strategy,
            "entry_ts": entry_ts,
            "exit_ts": now,
            "future_secid": profile.future_secid,
            "future_symbol": profile.future_symbol,
            "future_expiry": profile.future_expiry,
            "entry_context": entry_context,
            "exit_model_version": model_version,
            "exit_signal_metadata": dict(metadata or {}),
            "orders": False,
        }
        if self.trade_summary_sink is not None:
            self.trade_summary_sink.record(summary)
        logger.info(
            "STOCK_DEPTH_TRADE_SUMMARY | symbol=%s | side=%s | qty=%s | "
            "entry=%.2f | exit=%.2f | gross=%+.2f | fee=%.2f | net=%+.2f | "
            "hold_sec=%.1f | reason=%s | s3_queued=%s",
            profile.root,
            trade["side"],
            trade["qty"],
            trade["entry"],
            trade["exit"],
            trade["gross_pnl"],
            trade["fee"],
            trade["net_pnl"],
            trade["hold_sec"],
            reason,
            self.trade_summary_sink is not None,
        )
        return summary

    def heartbeat(self, now: float | None = None) -> None:
        current = self._clock_fn() if now is None else float(now)
        with self._lock:
            phase = self._market_phase(current)
            for secid, position in list(self.portfolio.positions.items()):
                profile = self._profiles_by_cash_secid[secid]
                quote = self._cash_quotes.get(secid)
                signal = self._signals.get(profile.future_tag)
                if phase == "CLOSED":
                    self._exit_locked(profile, "MARKET_CLOSE_1515", current)
                elif quote is None or current - quote.received_ts > self.settings.max_quote_age_sec:
                    if quote is not None:
                        self._exit_locked(profile, "STALE_CASH_MARKET_DATA", current)
                elif (
                    signal is None
                    or signal.last_signal_ts <= 0.0
                    or current - signal.last_signal_ts > self.settings.max_quote_age_sec
                ):
                    self._exit_locked(profile, "STALE_FUTURE_DEPTH_SIGNAL", current)

    def health(self) -> dict:
        with self._lock:
            return {
                "profiles": len(self._profiles_by_future_tag),
                "cash_quotes": len(self._cash_quotes),
                "open_positions": len(self.portfolio.positions),
                "entries": self._entries,
                "exits": self._exits,
                "realized_pnl": round(self.portfolio.realized_pnl, 2),
                "unrealized_pnl": round(self.portfolio.unrealized_pnl(), 2),
                "equity": round(self.portfolio.equity(), 2),
                "blocked": dict(self._blocked),
            }


class StockDepthPaperRuntime:
    def __init__(
        self,
        settings: StockDepthPaperSettings,
        master,
        depth_adapter,
        fullquote_feed,
        inference: MarketByPricePaperRuntime,
        executor: StockDepthPaperExecutor,
        trade_summary_sink: TradeSummaryS3Sink,
    ):
        self.settings = settings
        self.master = master
        self.depth_adapter = depth_adapter
        self.fullquote_feed = fullquote_feed
        self.inference = inference
        self.executor = executor
        self.trade_summary_sink = trade_summary_sink
        self._profiles_by_future_tag: dict[str, StockProfile] = {}
        self._profiles_by_future_secid: dict[int, StockProfile] = {}
        self._profiles_by_cash_secid: dict[int, StockProfile] = {}
        self._future_quotes: dict[int, dict] = {}
        self._previous_books = {}
        self._events = defaultdict(LiquidityEventTracker)
        self._lock = threading.Lock()
        self._received_depth = 0
        self._received_fullquote = 0
        self._quality_rejections = defaultdict(int)

    @staticmethod
    def _pick(raw: Mapping[str, object], *names: str, default=0):
        for name in names:
            if name in raw and raw[name] is not None:
                return raw[name]
        return default

    def _quote_payload(self, ltp, depth) -> dict:
        raw = depth.raw or {}
        received_ts = float(depth.ts or time.time())
        return {
            "ltp": float(ltp),
            "ltq": self._pick(raw, "LTQ", "ltq"),
            "ltt": self._pick(raw, "LTT", "ltt", default=""),
            "volume": self._pick(raw, "volume", "Volume"),
            "oi": self._pick(raw, "OI", "oi"),
            "average_price": self._pick(raw, "avg_price", "average_price"),
            "open": self._pick(raw, "open", "Open"),
            "high": self._pick(raw, "high", "High"),
            "low": self._pick(raw, "low", "Low"),
            "close": self._pick(raw, "close", "Close"),
            "total_buy_qty": self._pick(raw, "total_buy_quantity", "total_buy_qty"),
            "total_sell_qty": self._pick(raw, "total_sell_quantity", "total_sell_qty"),
            "received_ns": int(received_ts * 1_000_000_000),
            "received_ts": received_ts,
            "best_bid": float(depth.bid_price[0]) if depth.bid_price else 0.0,
            "best_ask": float(depth.ask_price[0]) if depth.ask_price else 0.0,
        }

    def on_fullquote(self, secid, tag, ltp, depth) -> None:
        quote = self._quote_payload(ltp, depth)
        with self._lock:
            self._received_fullquote += 1
            if int(secid) in self._profiles_by_future_secid:
                self._future_quotes[int(secid)] = quote
        if int(secid) in self._profiles_by_cash_secid:
            self.executor.on_cash_quote(
                int(secid),
                float(ltp),
                quote["best_bid"],
                quote["best_ask"],
                quote["received_ts"],
            )

    def on_book(self, tag, snapshot) -> None:
        with self._lock:
            self._received_depth += 1
            quote = dict(self._future_quotes.get(int(snapshot.security_id), {}))
        valid, reason, quote_age_ms = validate_composite_snapshot(
            snapshot,
            quote,
            max_quote_age_ms=self.settings.max_quote_age_sec * 1000.0,
            max_spread_bps=self.settings.max_future_spread_bps,
        )
        if not valid:
            self._quality_rejections[reason] += 1
            return
        previous = self._previous_books.get(tag)
        features = derive_market_by_price_features(snapshot, previous)
        self._previous_books[tag] = snapshot
        composite = CompositeMarketSnapshot(
            book=snapshot,
            full_quote=quote,
            quote_age_ms=quote_age_ms,
            features=features,
            event_evidence=self._events[tag].update(snapshot, quote),
        )
        self.inference.on_book(tag, snapshot, composite)

    def _resolve_profiles(self) -> list[StockProfile]:
        profiles = []
        for root in self.settings.symbols:
            future = self.master.get_nearest_stock_future(root)
            cash = self.master.get_equity(root)
            expiry = future["expiry"]
            expiry_text = expiry.strftime("%Y-%m-%d") if hasattr(expiry, "strftime") else str(expiry)
            profile = StockProfile(
                root=root,
                future_tag=f"{root}_FUT",
                future_secid=int(future["security_id"]),
                future_symbol=str(future["symbol"]),
                future_expiry=expiry_text,
                cash_tag=f"{root}_EQ",
                cash_secid=int(cash["security_id"]),
                cash_symbol=str(cash.get("trading_symbol") or cash["symbol"]),
            )
            profiles.append(profile)
            self._profiles_by_future_tag[profile.future_tag] = profile
            self._profiles_by_future_secid[profile.future_secid] = profile
            self._profiles_by_cash_secid[profile.cash_secid] = profile
            self.executor.register(profile)
            logger.info(
                "STOCK_DEPTH_INSTRUMENT | root=%s | future=%s | future_secid=%s | "
                "expiry=%s | cash=%s | cash_secid=%s",
                root,
                profile.future_symbol,
                profile.future_secid,
                profile.future_expiry,
                profile.cash_symbol,
                profile.cash_secid,
            )
        return profiles

    def run(self) -> None:
        profiles = self._resolve_profiles()
        self.trade_summary_sink.start()
        self.inference.start_worker()
        subscriptions = []
        depth_instruments = []
        for profile in profiles:
            subscriptions.extend(
                (
                    {
                        "ExchangeSegment": "NSE_FNO",
                        "SecurityId": str(profile.future_secid),
                        "tag": profile.future_tag,
                    },
                    {
                        "ExchangeSegment": "NSE_EQ",
                        "SecurityId": str(profile.cash_secid),
                        "tag": profile.cash_tag,
                    },
                )
            )
            depth_instruments.append(
                ("NSE_FNO", profile.future_secid, profile.future_tag)
            )
        self.fullquote_feed.subscribe_full(subscriptions)
        self.fullquote_feed.connect()
        self.depth_adapter.subscribe(depth_instruments)
        logger.warning(
            "STOCK_DEPTH_PAPER_ACTIVE | symbols=%s | future_depth=200 | "
            "fullquote_instruments=%s | depth_connections=%s | cash_long_short=true | "
            "paper_only=true | orders=false | s3_prefix=%s",
            ",".join(self.settings.symbols),
            len(subscriptions),
            len(depth_instruments),
            self.settings.trade_s3_prefix,
        )
        next_health = time.monotonic()
        try:
            while True:
                time.sleep(1.0)
                self.executor.heartbeat()
                now_mono = time.monotonic()
                if now_mono < next_health:
                    continue
                next_health = now_mono + self.settings.health_interval_sec
                self.inference.log_health()
                logger.info(
                    "STOCK_DEPTH_HEALTH | received_depth=%s | received_fullquote=%s | "
                    "quality_rejections=%s | execution=%s | trade_s3=%s",
                    self._received_depth,
                    self._received_fullquote,
                    dict(self._quality_rejections),
                    self.executor.health(),
                    self.trade_summary_sink.health(),
                )
        except KeyboardInterrupt:
            logger.info("STOCK_DEPTH_PAPER_STOPPED")
        finally:
            self.depth_adapter.close()
            self.fullquote_feed.close()
            self.inference.close_worker()
            self.trade_summary_sink.close()


def build_stock_depth_paper_runtime(
    settings: StockDepthPaperSettings,
) -> StockDepthPaperRuntime:
    from dhan_engine.infrastructure.dhan.full_depth_200_adapter import FullDepth200Adapter
    from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
    from dhan_engine.infrastructure.dhan.marketfeed_ws import DhanLiveMarketFeedWS

    if not settings.client_id or not settings.access_token:
        raise RuntimeError("DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN are required")
    if not settings.trade_s3_bucket:
        raise RuntimeError(
            "DEEPLOB_S3_BUCKET is required for the stock-depth daily trade ledger"
        )
    master = InstrumentMaster(settings.csv_file, debug=False)
    trade_summary_sink = TradeSummaryS3Sink(
        TradeSummaryS3Settings(
            bucket=settings.trade_s3_bucket,
            prefix=settings.trade_s3_prefix,
            queue_size=settings.trade_s3_queue_size,
        )
    )
    portfolio = StockPaperPortfolio(
        capital=settings.capital,
        notional_per_trade=settings.notional_per_trade,
        max_positions=settings.max_positions,
        round_trip_fee=0.0,
        charge_calculator=NseIntradayChargeCalculator(),
        leverage=settings.leverage,
    )
    executor = StockDepthPaperExecutor(settings, portfolio, trade_summary_sink)
    inference = MarketByPricePaperRuntime(
        MarketByPricePaperSettings.from_env(), prediction_sink=executor.on_prediction
    )
    runtime = None
    depth_adapter = FullDepth200Adapter(
        settings.client_id,
        settings.access_token,
        lambda tag, book: runtime.on_book(tag, book),
    )
    fullquote_feed = DhanLiveMarketFeedWS(
        token=settings.access_token,
        client_id=settings.client_id,
        on_full=lambda secid, tag, ltp, depth: runtime.on_fullquote(
            secid, tag, ltp, depth
        ),
        debug=False,
    )
    runtime = StockDepthPaperRuntime(
        settings,
        master,
        depth_adapter,
        fullquote_feed,
        inference,
        executor,
        trade_summary_sink,
    )
    return runtime
