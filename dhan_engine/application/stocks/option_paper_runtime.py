from __future__ import annotations

import logging
import os
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, time as clock_time
from typing import Mapping
from zoneinfo import ZoneInfo

from dhan_engine.analytics.deeplob_recorder import (
    DepthRecorderSettings,
    ParquetDepthRecorder,
)
from dhan_engine.application.deeplob.long_option_regime import (
    LongOptionRegimeExecutor,
    LongOptionRegimeSettings,
)
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

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")
SUPPORTED_STOCK_OPTION_ROOTS = frozenset({"HDFCBANK", "RELIANCE"})


def _env_bool(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _clock(value: str) -> clock_time:
    return datetime.strptime(value, "%H:%M").time()


@dataclass(frozen=True)
class StockOptionPaperSettings:
    client_id: str
    access_token: str
    csv_file: str
    symbols: tuple[str, ...]
    max_fullquote_age_ms: float
    max_future_spread_bps: float
    execution_probe_qty: int
    option_selection_retry_sec: float
    health_interval_sec: float
    recorder: DepthRecorderSettings
    trade_summary: TradeSummaryS3Settings
    regime: LongOptionRegimeSettings

    @classmethod
    def from_env(cls) -> "StockOptionPaperSettings":
        client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
        access_token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
        if not client_id or not access_token:
            raise RuntimeError("Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN")
        symbols = tuple(
            dict.fromkeys(
                item.strip().upper()
                for item in os.getenv(
                    "STOCK_OPTION_SYMBOLS", "HDFCBANK,RELIANCE"
                ).split(",")
                if item.strip()
            )
        )
        if not symbols:
            raise ValueError("STOCK_OPTION_SYMBOLS must contain at least one symbol")
        unsupported = sorted(set(symbols) - SUPPORTED_STOCK_OPTION_ROOTS)
        if unsupported:
            raise ValueError(
                "This rollout is restricted to HDFCBANK and RELIANCE; unsupported="
                + ",".join(unsupported)
            )

        recorder = replace(
            DepthRecorderSettings.from_env(),
            output_dir=os.getenv(
                "STOCK_OPTION_OUTPUT_DIR", "/var/lib/dhan-engine-stock-options/deeplob"
            ).strip(),
            s3_prefix=os.getenv(
                "STOCK_OPTION_S3_PREFIX", "market-data/deeplob-stock-options"
            ).strip().strip("/"),
        )
        if not recorder.s3_bucket:
            raise RuntimeError(
                "DEEPLOB_S3_BUCKET is required for stock-option depth and trade storage"
            )
        trade_summary = TradeSummaryS3Settings(
            bucket=recorder.s3_bucket,
            prefix=os.getenv(
                "STOCK_OPTION_TRADE_S3_PREFIX", "paper-trades/stock-options"
            ).strip().strip("/"),
            queue_size=max(
                16, int(os.getenv("STOCK_OPTION_TRADE_S3_QUEUE_SIZE", "128"))
            ),
        )
        if trade_summary.prefix == recorder.s3_prefix or trade_summary.prefix.startswith(
            f"{recorder.s3_prefix}/"
        ):
            raise ValueError(
                "STOCK_OPTION_TRADE_S3_PREFIX must be separate from STOCK_OPTION_S3_PREFIX"
            )
        prefix = "STOCK_OPTION_REGIME_"
        regime = LongOptionRegimeSettings(
            enabled=_env_bool(prefix + "ENABLED", "1"),
            capital=float(os.getenv(prefix + "CAPITAL_PER_SYMBOL", "500000")),
            max_quote_age_sec=max(
                0.1, float(os.getenv(prefix + "MAX_QUOTE_AGE_SEC", "2"))
            ),
            observation_sec=max(
                3.0, float(os.getenv(prefix + "OBSERVATION_SEC", "12"))
            ),
            minimum_samples=max(4, int(os.getenv(prefix + "MIN_SAMPLES", "8"))),
            state_confirmations=max(
                2, int(os.getenv(prefix + "STATE_CONFIRMATIONS", "3"))
            ),
            reversal_confirmations=max(
                2, int(os.getenv(prefix + "REVERSAL_CONFIRMATIONS", "2"))
            ),
            minimum_state_score=float(
                os.getenv(prefix + "MIN_STATE_SCORE", "0.58")
            ),
            fee_buffer_multiple=float(
                os.getenv(prefix + "FEE_BUFFER_MULTIPLE", "1.25")
            ),
            round_trip_fee=float(os.getenv(prefix + "ROUND_TRIP_FEE", "60")),
            catastrophic_loss_pct=max(
                1.0, float(os.getenv(prefix + "CATASTROPHIC_LOSS_PCT", "8.0"))
            ),
            catastrophic_confirmations=max(
                2, int(os.getenv(prefix + "CATASTROPHIC_CONFIRMATIONS", "3"))
            ),
            market_start=_clock(os.getenv(prefix + "MARKET_START", "09:15")),
            entry_cutoff=_clock(os.getenv(prefix + "ENTRY_CUTOFF", "15:24")),
            market_end=_clock(os.getenv(prefix + "MARKET_END", "15:25")),
            hybrid_enabled=_env_bool(prefix + "HYBRID_ENABLED", "1"),
            hybrid_book_weight=max(
                0.0,
                min(1.0, float(os.getenv(prefix + "HYBRID_BOOK_WEIGHT", "0.55"))),
            ),
            hybrid_min_updates=max(
                2, int(os.getenv(prefix + "HYBRID_MIN_UPDATES", "4"))
            ),
        )
        return cls(
            client_id=client_id,
            access_token=access_token,
            csv_file=os.getenv("CSV_FILE", "api-scrip-master.csv").strip(),
            symbols=symbols,
            max_fullquote_age_ms=max(
                100.0, float(os.getenv("STOCK_OPTION_MAX_FULLQUOTE_AGE_MS", "1500"))
            ),
            max_future_spread_bps=max(
                1.0, float(os.getenv("STOCK_OPTION_MAX_FUTURE_SPREAD_BPS", "25"))
            ),
            execution_probe_qty=max(
                1, int(os.getenv("STOCK_OPTION_EXECUTION_PROBE_QTY", "1"))
            ),
            option_selection_retry_sec=max(
                5.0, float(os.getenv("STOCK_OPTION_SELECTION_RETRY_SEC", "30"))
            ),
            health_interval_sec=max(
                30.0, float(os.getenv("STOCK_OPTION_HEALTH_INTERVAL_SEC", "60"))
            ),
            recorder=recorder,
            trade_summary=trade_summary,
            regime=regime,
        )


class _RootedTradeSummarySink:
    def __init__(self, root: str, sink: TradeSummaryS3Sink):
        self.root = root
        self.sink = sink
        self.last_summary: dict = {}

    def record(self, summary: Mapping) -> bool:
        rooted = dict(summary)
        rooted.update(
            index=self.root,
            underlying=self.root,
            runtime="stock_option_paper_regime_v2",
            profile="stock_option_regime_v2",
            paper_profile="stock_option_regime_v2",
            stock_option_profile=True,
        )
        self.last_summary = rooted
        return self.sink.record(rooted)


class StockOptionRegimeExecutor(LongOptionRegimeExecutor):
    """PR-283 hybrid regime executor with symbol-isolated tags and summaries."""

    def __init__(self, root, settings, paper_trader, *, trade_summary_sink):
        self.root = str(root).upper()
        self.profile = "stock_option_regime_v2"
        self.strategy = "deeplob_stock_option_regime_v2"
        self._rooted_sink = _RootedTradeSummarySink(self.root, trade_summary_sink)
        self._last_stock_state_log = 0.0
        super().__init__(
            settings,
            paper_trader,
            trade_summary_sink=self._rooted_sink,
        )

    def register_contracts(self, selection: Mapping[str, Mapping]) -> list[dict]:
        subscriptions = super().register_contracts(selection)
        for side in ("CE", "PE"):
            if side in self.contracts:
                self.contracts[side]["tag"] = f"{self.root}_{side}"
        for subscription in subscriptions:
            side = "CE" if str(subscription.get("tag", "")).endswith("_CE") else "PE"
            subscription["tag"] = f"{self.root}_{side}"
        logger.info(
            "STOCK_OPTION_CONTRACTS | symbol=%s | ce_id=%s | ce_strike=%s | "
            "pe_id=%s | pe_strike=%s | expiry=%s | lot_size=%s",
            self.root,
            self.contracts.get("CE", {}).get("security_id"),
            self.contracts.get("CE", {}).get("strike"),
            self.contracts.get("PE", {}).get("security_id"),
            self.contracts.get("PE", {}).get("strike"),
            self.contracts.get("CE", {}).get("expiry"),
            self.paper_trader.LOT_SIZES.get(self.root),
        )
        return subscriptions

    def on_prediction(self, **prediction) -> None:
        super().on_prediction(**prediction)
        now = time.monotonic()
        health = self.health()
        if now - self._last_stock_state_log >= 5.0:
            self._last_stock_state_log = now
            logger.info(
                "STOCK_OPTION_STATE | symbol=%s | state=%s | instant=%s | "
                "confirmations=%s | hybrid_ready=%s | hybrid_agreement=%s | "
                "open_positions=%s | paper_only=true | orders=false",
                self.root,
                health["v1_state"],
                health["v1_instant_state"],
                health["state_confirmations"],
                health.get("v1_books", {}).get("hybrid_ready", False),
                health.get("v1_books", {}).get("hybrid_agreement", False),
                health["open_positions"],
            )

    def _try_entry(self, side: str, evidence: dict) -> None:
        entries_before = self._entries
        super()._try_entry(side, evidence)
        if self._entries > entries_before:
            position = next(iter(self.paper_trader.positions.values()))
            logger.info(
                "STOCK_OPTION_ENTRY | symbol=%s | tag=%s | entry=%.2f | qty=%s | "
                "state=%s | paper_only=true | orders=false",
                self.root,
                position.get("tag"),
                float(position.get("entry", 0.0)),
                position.get("qty"),
                self._state,
            )

    def _exit(self, reason: str) -> None:
        exits_before = self._exits
        super()._exit(reason)
        if self._exits > exits_before:
            summary = self._rooted_sink.last_summary
            logger.info(
                "STOCK_OPTION_TRADE_SUMMARY | symbol=%s | tag=%s | gross=%.2f | "
                "fees=%.2f | net=%.2f | reason=%s",
                self.root,
                summary.get("tag"),
                float(summary.get("gross_pnl", 0.0) or 0.0),
                float(summary.get("fee", 0.0) or 0.0),
                float(summary.get("net_pnl", 0.0) or 0.0),
                summary.get("exit_reason"),
            )

    def health(self) -> dict:
        health = super().health()
        health["underlying"] = self.root
        return health


@dataclass
class StockOptionProfile:
    root: str
    future: dict
    executor: StockOptionRegimeExecutor
    inference: MarketByPricePaperRuntime
    option_selection: dict | None = None
    selection_date: object | None = None
    selection_attempts: int = 0
    selection_failures: int = 0
    last_selection_mono: float = float("-inf")

    @property
    def future_tag(self) -> str:
        return f"{self.root}_FUT"


class StockOptionPaperRuntime:
    """Isolated HDFCBANK/RELIANCE FUTSTK depth-to-option paper service."""

    def __init__(
        self,
        settings,
        master,
        depth_adapter,
        fullquote_feed,
        recorder,
        trade_summary_sink,
        profiles,
    ):
        self.settings = settings
        self.master = master
        self.depth_adapter = depth_adapter
        self.fullquote_feed = fullquote_feed
        self.recorder = recorder
        self.trade_summary_sink = trade_summary_sink
        self.profiles = {profile.root: profile for profile in profiles}
        self._profile_by_future_secid = {
            int(profile.future["security_id"]): profile for profile in profiles
        }
        self._profile_by_option_secid: dict[int, StockOptionProfile] = {}
        self._quote_lock = threading.Lock()
        self._latest_fullquote: dict[int, dict] = {}
        self._previous_book = {}
        self._liquidity_events = defaultdict(LiquidityEventTracker)
        self._received_depth = 0
        self._received_fullquote = 0
        self._quality_rejections = defaultdict(int)
        self._last_quality_log = defaultdict(float)

    @staticmethod
    def _pick(raw, *names, default=0):
        for name in names:
            if name in raw and raw[name] is not None:
                return raw[name]
        return default

    def on_fullquote(self, secid, tag, ltp, depth) -> None:
        raw = depth.raw or {}
        received_ts = float(depth.ts or time.time())
        quote = {
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
        with self._quote_lock:
            self._latest_fullquote[int(secid)] = quote
            self._received_fullquote += 1
            profile = self._profile_by_option_secid.get(int(secid))
        if profile is not None:
            profile.executor.on_quote(
                int(secid),
                tag,
                float(ltp),
                bid=quote["best_bid"],
                ask=quote["best_ask"],
                received_ts=received_ts,
            )

    def on_book(self, tag, snapshot) -> None:
        root = str(tag).removesuffix("_FUT").upper()
        profile = self.profiles.get(root)
        if profile is None:
            logger.error("STOCK_OPTION_DEPTH_REJECTED | tag=%s | reason=UNKNOWN_PROFILE", tag)
            return
        self._received_depth += 1
        with self._quote_lock:
            latest_quote = dict(
                self._latest_fullquote.get(int(profile.future["security_id"]), {})
            )
        if latest_quote:
            latest_quote["age_ms"] = abs(
                (snapshot.received_ts - latest_quote["received_ts"]) * 1000.0
            )
        expiry = profile.future["expiry"]
        expiry_text = expiry.strftime("%Y-%m-%d") if hasattr(expiry, "strftime") else str(expiry)
        try:
            self.recorder.record(
                tag,
                snapshot,
                full_quote=latest_quote or None,
                index=root,
                symbol=profile.future["symbol"],
                expiry=expiry_text,
            )
        except Exception:
            logger.exception("STOCK_OPTION_RECORDER_DISPATCH_FAILED | symbol=%s", root)

        valid, reason, quote_age_ms = validate_composite_snapshot(
            snapshot,
            latest_quote,
            max_quote_age_ms=self.settings.max_fullquote_age_ms,
            max_spread_bps=self.settings.max_future_spread_bps,
        )
        if not valid:
            self._quality_rejections[(root, reason)] += 1
            now = time.monotonic()
            if now - self._last_quality_log[(root, reason)] >= 5.0:
                self._last_quality_log[(root, reason)] = now
                logger.warning(
                    "STOCK_OPTION_COMPOSITE_REJECTED | symbol=%s | reason=%s | "
                    "quote_age_ms=%s | inference_blocked=true | recorder_continues=true",
                    root,
                    reason,
                    f"{quote_age_ms:.1f}" if quote_age_ms != float("inf") else "NA",
                )
            return
        features = derive_market_by_price_features(
            snapshot,
            self._previous_book.get(root),
            probe_quantity=self.settings.execution_probe_qty,
        )
        self._previous_book[root] = snapshot
        composite = CompositeMarketSnapshot(
            book=snapshot,
            full_quote=latest_quote,
            quote_age_ms=quote_age_ms,
            features=features,
            event_evidence=self._liquidity_events[root].update(snapshot, latest_quote),
        )
        profile.inference.on_book(tag, snapshot, composite)

    def _fullquote_subscriptions(self) -> list[dict]:
        subscriptions = []
        for profile in self.profiles.values():
            subscriptions.append(
                {
                    "ExchangeSegment": "NSE_FNO",
                    "SecurityId": str(profile.future["security_id"]),
                    "tag": profile.future_tag,
                }
            )
            if profile.option_selection:
                for side in ("CE", "PE"):
                    contract = profile.option_selection[side]
                    subscriptions.append(
                        {
                            "ExchangeSegment": "NSE_FNO",
                            "SecurityId": str(contract["security_id"]),
                            "tag": f"{profile.root}_{side}",
                        }
                    )
        return subscriptions

    def _ensure_option_contracts(self) -> None:
        now_ist = datetime.now(IST)
        if not (
            self.settings.regime.market_start
            <= now_ist.time()
            < self.settings.regime.market_end
        ):
            return
        subscriptions_changed = False
        for profile in self.profiles.values():
            if profile.selection_date == now_ist.date() and profile.option_selection:
                continue
            now_mono = time.monotonic()
            if now_mono - profile.last_selection_mono < self.settings.option_selection_retry_sec:
                continue
            profile.last_selection_mono = now_mono
            profile.selection_attempts += 1
            with self._quote_lock:
                future_quote = dict(
                    self._latest_fullquote.get(int(profile.future["security_id"]), {})
                )
            age = time.time() - float(future_quote.get("received_ts", 0.0) or 0.0)
            if float(future_quote.get("ltp", 0.0) or 0.0) <= 0 or age > 5.0:
                profile.selection_failures += 1
                logger.warning(
                    "STOCK_OPTION_SELECTION_RETRY | symbol=%s | reason=FUTURE_QUOTE_UNAVAILABLE | "
                    "attempts=%s | failures=%s",
                    profile.root,
                    profile.selection_attempts,
                    profile.selection_failures,
                )
                continue
            try:
                selection = self.master.get_nearest_stock_option_pair(
                    profile.root,
                    future_quote["ltp"],
                    expiry=profile.future["expiry"],
                )
                if int(selection["CE"]["lot_size"]) != int(profile.future["lot_size"]):
                    raise RuntimeError(
                        f"{profile.root} FUTSTK and OPTSTK lot sizes do not match"
                    )
                lot_sizes = {profile.root: int(selection["CE"]["lot_size"])}
                # The inherited PR-283 executor uses the NIFTY key only to
                # calculate spread cost; this paper trader is private to the stock.
                lot_sizes["NIFTY"] = lot_sizes[profile.root]
                profile.executor.paper_trader.LOT_SIZES = lot_sizes
                registered = profile.executor.register_contracts(selection)
                profile.option_selection = selection
                profile.selection_date = now_ist.date()
                with self._quote_lock:
                    self._profile_by_option_secid = {
                        secid: owner
                        for secid, owner in self._profile_by_option_secid.items()
                        if owner is not profile
                    }
                    for subscription in registered:
                        self._profile_by_option_secid[
                            int(subscription["SecurityId"])
                        ] = profile
                subscriptions_changed = True
            except Exception as exc:
                profile.selection_failures += 1
                logger.warning(
                    "STOCK_OPTION_SELECTION_RETRY | symbol=%s | attempts=%s | failures=%s | "
                    "error=%s",
                    profile.root,
                    profile.selection_attempts,
                    profile.selection_failures,
                    exc,
                )
        if subscriptions_changed:
            subscriptions = self._fullquote_subscriptions()
            self.fullquote_feed.replace_subscriptions(
                subscriptions, reason="stock_option_daily_contract_selection"
            )
            self.fullquote_feed.refresh_full_subscriptions(
                reason="stock_option_daily_contract_selection"
            )

    def run(self) -> None:
        depth_instruments = [
            ("NSE_FNO", int(profile.future["security_id"]), profile.future_tag)
            for profile in self.profiles.values()
        ]
        initial_subscriptions = self._fullquote_subscriptions()
        self.recorder.start()
        self.trade_summary_sink.start()
        for profile in self.profiles.values():
            profile.inference.start_worker()
        self.fullquote_feed.subscribe_full(initial_subscriptions)
        self.fullquote_feed.connect()
        self.depth_adapter.subscribe(depth_instruments)
        logger.warning(
            "STOCK_OPTION_PAPER_ACTIVE | symbols=%s | depth_connections=%s | "
            "fullquote_connection=shared | future_depth=200 | v1=hybrid_strategy_books | "
            "v2=long_only_stock_ce_pe | isolated_from_nifty=true | paper_only=true | orders=false",
            ",".join(self.profiles),
            len(depth_instruments),
        )
        next_health = time.monotonic()
        try:
            while True:
                time.sleep(1.0)
                self._ensure_option_contracts()
                for profile in self.profiles.values():
                    profile.executor.heartbeat()
                now = time.monotonic()
                if now < next_health:
                    continue
                next_health = now + self.settings.health_interval_sec
                for profile in self.profiles.values():
                    profile.inference.log_health()
                logger.info(
                    "STOCK_OPTION_HEALTH | depth=%s | fullquote=%s | rejections=%s | "
                    "profiles=%s | recorder_alive=%s | trade_s3=%s",
                    self._received_depth,
                    self._received_fullquote,
                    {f"{root}:{reason}": count for (root, reason), count in self._quality_rejections.items()},
                    {root: profile.executor.health() for root, profile in self.profiles.items()},
                    self.recorder.worker_alive,
                    self.trade_summary_sink.health(),
                )
        except KeyboardInterrupt:
            logger.info("STOCK_OPTION_PAPER_STOPPED")
        finally:
            self.depth_adapter.close()
            self.fullquote_feed.close()
            for profile in self.profiles.values():
                profile.inference.close_worker()
            self.recorder.close()
            self.trade_summary_sink.close()


def build_stock_option_paper_runtime(
    settings: StockOptionPaperSettings,
) -> StockOptionPaperRuntime:
    from dhan_engine.infrastructure.dhan.full_depth_200_adapter import FullDepth200Adapter
    from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
    from dhan_engine.infrastructure.dhan.marketfeed_ws import DhanLiveMarketFeedWS
    from dhan_engine.simulations.paper_trade_manager import PaperTradeManager

    master = InstrumentMaster(settings.csv_file, debug=False)
    recorder = ParquetDepthRecorder(settings.recorder)
    trade_summary_sink = TradeSummaryS3Sink(settings.trade_summary)
    profiles = []
    for root in settings.symbols:
        future = master.get_nearest_stock_future(root)
        paper_trader = PaperTradeManager(capital=settings.regime.capital)
        paper_trader.ROUND_TRIP_FEE = settings.regime.round_trip_fee
        paper_trader.LOT_SIZES = {
            root: int(future["lot_size"]),
            "NIFTY": int(future["lot_size"]),
        }
        executor = StockOptionRegimeExecutor(
            root,
            settings.regime,
            paper_trader,
            trade_summary_sink=trade_summary_sink,
        )
        inference = MarketByPricePaperRuntime(
            MarketByPricePaperSettings.from_env(),
            prediction_sink=executor.on_prediction,
        )
        profiles.append(
            StockOptionProfile(
                root=root,
                future=future,
                executor=executor,
                inference=inference,
            )
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
    runtime = StockOptionPaperRuntime(
        settings,
        master,
        depth_adapter,
        fullquote_feed,
        recorder,
        trade_summary_sink,
        profiles,
    )
    return runtime
