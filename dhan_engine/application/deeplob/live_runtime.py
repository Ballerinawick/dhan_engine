
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass

from dhan_engine.analytics.deeplob_recorder import DepthRecorderSettings, ParquetDepthRecorder
from dhan_engine.application.deeplob.inference_runtime import (
    DeepLobInferenceSettings,
    DeepLobPaperInferenceRuntime,
)
from dhan_engine.application.deeplob.option_paper_executor import (
    DeepLobOptionPaperExecutor,
    DeepLobOptionPaperSettings,
)
from dhan_engine.application.deeplob.trade_summary_s3 import (
    TradeSummaryS3Settings,
    TradeSummaryS3Sink,
)
from dhan_engine.domain.market.market_by_price_execution import (
    CompositeMarketSnapshot,
    derive_market_by_price_features,
    validate_composite_snapshot,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeepLobLiveSettings:
    inference: DeepLobInferenceSettings
    recorder: DepthRecorderSettings
    premodel_paper: bool = False

    @classmethod
    def from_env(cls) -> "DeepLobLiveSettings":
        recorder = DepthRecorderSettings.from_env()
        if not recorder.s3_bucket:
            raise RuntimeError("DEEPLOB_S3_BUCKET is required for DHAN_SERVICE=deeplob-live")
        service = os.getenv("DHAN_SERVICE", "deeplob-live").strip().lower().replace("_", "-")
        return cls(
            inference=DeepLobInferenceSettings.from_env(),
            recorder=recorder,
            premodel_paper=service == "deeplob-paper",
        )


class DeepLobLiveRuntime:
    """Fans one NIFTY 200-depth feed into isolated recorder and inference workers."""

    def __init__(
        self,
        settings,
        master,
        depth_adapter,
        fullquote_feed,
        recorder,
        inference,
        option_paper=None,
        option_selection=None,
        trade_summary_sink=None,
        option_selector=None,
    ):
        self.settings = settings
        self.master = master
        self.depth_adapter = depth_adapter
        self.fullquote_feed = fullquote_feed
        self.recorder = recorder
        self.inference = inference
        self.option_paper = option_paper
        self.option_selection = option_selection or {}
        self.option_selector = option_selector
        self.trade_summary_sink = trade_summary_sink
        self.instrument_metadata = {}
        self._received = 0
        self._recorder_dispatch_failures = 0
        self._inference_dispatch_failures = 0
        self._quote_lock = threading.Lock()
        self._latest_fullquote = {}
        self._fullquote_received = 0
        self._previous_book = {}
        self._quality_rejections = {}
        self._max_quote_age_ms = float(os.getenv("DEEPLOB_MAX_FULLQUOTE_AGE_MS", "1500"))
        self._max_spread_bps = float(os.getenv("DEEPLOB_MAX_FUTURE_SPREAD_BPS", "25"))
        self._probe_quantity = max(1, int(os.getenv("DEEPLOB_EXECUTION_PROBE_QTY", "65")))
        self._last_quality_log = {}
        self._option_selection_attempts = 0
        self._option_selection_failures = 0
        self._last_option_selection_mono = float("-inf")
        self._option_selection_retry_sec = max(
            5.0,
            float(os.getenv("DEEPLOB_OPTION_SELECTION_RETRY_SEC", "60")),
        )

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
            "ltp": ltp,
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
            self._fullquote_received += 1
        if self.option_paper is not None:
            self.option_paper.on_quote(
                int(secid),
                tag,
                float(ltp),
                bid=quote["best_bid"],
                ask=quote["best_ask"],
                received_ts=received_ts,
            )

    def on_book(self, tag, snapshot) -> None:
        self._received += 1
        metadata = self.instrument_metadata[tag]
        with self._quote_lock:
            latest_quote = dict(self._latest_fullquote.get(snapshot.security_id, {}))
        if latest_quote:
            latest_quote["age_ms"] = abs(
                (snapshot.received_ts - latest_quote["received_ts"]) * 1000.0
            )
        valid, quality_reason, quote_age_ms = validate_composite_snapshot(
            snapshot,
            latest_quote,
            max_quote_age_ms=self._max_quote_age_ms,
            max_spread_bps=self._max_spread_bps,
        )
        quote_synchronized = bool(latest_quote) and quote_age_ms <= self._max_quote_age_ms
        # Raw 200-depth capture must never depend on Full Quote availability.
        # Unsynchronised rows carry empty quote fields and are still rejected
        # below before they can reach inference or paper execution.
        try:
            self.recorder.record(
                tag,
                snapshot,
                full_quote=latest_quote if quote_synchronized else None,
                **metadata,
            )
        except Exception:
            self._recorder_dispatch_failures += 1
            logger.exception("DEEPLOB_LIVE_RECORDER_DISPATCH_FAILED | instrument=%s", tag)
        if not valid:
            self._quality_rejections[quality_reason] = self._quality_rejections.get(quality_reason, 0) + 1
            now = time.monotonic()
            if now - self._last_quality_log.get(tag, 0.0) >= 1.0:
                self._last_quality_log[tag] = now
                logger.warning(
                    "DEEPLOB_COMPOSITE_REJECTED | instrument=%s | reason=%s | "
                    "quote_age_ms=%s | inference_blocked=true | recorder_continues=true",
                    tag,
                    quality_reason,
                    f"{quote_age_ms:.1f}" if quote_age_ms != float("inf") else "NA",
                )
            return
        features = derive_market_by_price_features(
            snapshot,
            self._previous_book.get(tag),
            probe_quantity=self._probe_quantity,
        )
        self._previous_book[tag] = snapshot
        composite = CompositeMarketSnapshot(
            book=snapshot,
            full_quote=latest_quote,
            quote_age_ms=quote_age_ms,
            features=features,
        )
        try:
            self.inference.on_book(tag, snapshot, composite)
        except Exception:
            self._inference_dispatch_failures += 1
            logger.exception("DEEPLOB_LIVE_INFERENCE_DISPATCH_FAILED | instrument=%s", tag)

    def _ensure_option_contracts(self, *, force: bool = False) -> bool:
        if self.option_paper is None or self.option_selector is None:
            return False
        if self.option_paper.contracts.get("CE") and self.option_paper.contracts.get("PE"):
            return True
        now = time.monotonic()
        if not force and now - self._last_option_selection_mono < self._option_selection_retry_sec:
            return False
        self._last_option_selection_mono = now
        self._option_selection_attempts += 1
        try:
            selection = self.option_selector.select_best("NIFTY") or {}
            if not selection.get("CE") or not selection.get("PE"):
                raise RuntimeError("selection did not contain both CE and PE")
            subscriptions = self.option_paper.register_contracts(selection)
            if len(subscriptions) != 2:
                raise RuntimeError(
                    f"expected 2 option subscriptions, received {len(subscriptions)}"
                )
            self.option_selection = selection
            self.fullquote_feed.subscribe_full(subscriptions)
            logger.info(
                "DEEPLOB_OPTION_SELECTION_READY | attempts=%s | ce_id=%s | pe_id=%s",
                self._option_selection_attempts,
                selection["CE"].get("security_id"),
                selection["PE"].get("security_id"),
            )
            return True
        except Exception as exc:
            self._option_selection_failures += 1
            logger.warning(
                "DEEPLOB_OPTION_SELECTION_RETRY | attempts=%s | failures=%s | "
                "retry_sec=%.1f | recorder_continues=true | paper_entries_blocked=true | error=%s",
                self._option_selection_attempts,
                self._option_selection_failures,
                self._option_selection_retry_sec,
                exc,
            )
            return False

    def run(self) -> None:
        instruments = []
        for index in self.settings.inference.indexes:
            future = self.master.get_nearest_future(index)
            tag = f"{index}_FUT"
            expiry = future["expiry"]
            expiry_text = expiry.strftime("%Y-%m-%d") if hasattr(expiry, "strftime") else str(expiry)
            self.instrument_metadata[tag] = {
                "index": index,
                "symbol": future["symbol"],
                "expiry": expiry_text,
            }
            instruments.append(("NSE_FNO", int(future["security_id"]), tag))
            logger.info(
                "DEEPLOB_LIVE_INSTRUMENT | index=%s | symbol=%s | secid=%s | expiry=%s",
                index,
                future["symbol"],
                future["security_id"],
                expiry_text,
            )

        self.recorder.start()
        self.inference.start_worker()
        if self.trade_summary_sink is not None:
            self.trade_summary_sink.start()
        self.fullquote_feed.subscribe_full(
            [
                {
                    "ExchangeSegment": segment,
                    "SecurityId": str(secid),
                    "tag": tag,
                }
                for segment, secid, tag in instruments
            ]
        )
        self.fullquote_feed.connect()
        self.depth_adapter.subscribe(instruments)
        self._ensure_option_contracts(force=True)
        inference_version = getattr(
            self.inference,
            "version",
            getattr(getattr(self.inference, "artifact", None), "version", "unknown"),
        )
        logger.info(
            "DEEPLOB_LIVE_PIPELINE_ACTIVE | indexes=%s | depth=200 | connections=%s | "
            "fullquote=true | recorder=true | inference=true | s3_bucket=%s | "
            "market_data_prefix=%s | paper_trade_prefix=%s | model=%s | orders=false",
            ",".join(self.settings.inference.indexes),
            len(instruments) + 1,
            self.settings.recorder.s3_bucket,
            self.settings.recorder.s3_prefix,
            (
                self.trade_summary_sink.settings.prefix
                if self.trade_summary_sink is not None
                else "disabled"
            ),
            inference_version,
        )
        try:
            while True:
                time.sleep(10)
                logger.info(
                    "DEEPLOB_LIVE_PIPELINE_HEALTH | received=%s | recorder_dispatch_failures=%s | "
                    "inference_dispatch_failures=%s | fullquote_received=%s | "
                    "recorder_worker_alive=%s | "
                    "inference_worker_alive=%s | quality_rejections=%s",
                    self._received,
                    self._recorder_dispatch_failures,
                    self._inference_dispatch_failures,
                    self._fullquote_received,
                    self.recorder.worker_alive,
                    self.inference.worker_alive,
                    self._quality_rejections,
                )
                if self.option_paper is not None:
                    self._ensure_option_contracts()
                    self.option_paper.heartbeat()
                    logger.info(
                        "DEEPLOB_OPTION_PAPER_HEALTH | state=%s",
                        {
                            **self.option_paper.health(),
                            "selection_attempts": self._option_selection_attempts,
                            "selection_failures": self._option_selection_failures,
                            "trade_summary_s3": (
                                self.trade_summary_sink.health()
                                if self.trade_summary_sink is not None
                                else None
                            ),
                        },
                    )
                self.recorder.log_health()
                self.inference.log_health()
        except KeyboardInterrupt:
            logger.info("DEEPLOB_LIVE_PIPELINE_STOPPED")
        finally:
            self.depth_adapter.close()
            self.fullquote_feed.close()
            self.inference.close_worker()
            self.recorder.close()
            if self.trade_summary_sink is not None:
                self.trade_summary_sink.close()


def build_deeplob_live_runtime(settings: DeepLobLiveSettings) -> DeepLobLiveRuntime:
    from dhan_engine.infrastructure.dhan.full_depth_200_adapter import FullDepth200Adapter
    from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
    from dhan_engine.infrastructure.dhan.marketfeed_ws import DhanLiveMarketFeedWS
    from dhan_engine.infrastructure.dhan.option_chain_selector import OptionChainSelector
    from dhan_engine.simulations.paper_trade_manager import PaperTradeManager

    inference_settings = settings.inference

    master = InstrumentMaster(inference_settings.csv_file, debug=False)
    recorder = ParquetDepthRecorder(settings.recorder)
    option_paper_settings = DeepLobOptionPaperSettings.from_env()
    option_selection = {}
    option_selector = None
    option_paper = None
    trade_summary_sink = None
    if option_paper_settings.enabled:
        option_selector = OptionChainSelector(
            access_token=inference_settings.access_token,
            client_id=inference_settings.client_id,
            instrument_master=master,
            strike_step_map={"NIFTY": 50},
            mode=2,
            max_steps_each_side=10,
            debug=False,
        )
        trade_summary_sink = TradeSummaryS3Sink(
            TradeSummaryS3Settings.from_env()
        )
        option_paper = DeepLobOptionPaperExecutor(
            option_paper_settings,
            PaperTradeManager(capital=option_paper_settings.capital),
            trade_summary_sink=trade_summary_sink,
        )
    runtime = None
    adapter = FullDepth200Adapter(
        inference_settings.client_id,
        inference_settings.access_token,
        lambda tag, book: runtime.on_book(tag, book),
    )
    fullquote_feed = DhanLiveMarketFeedWS(
        token=inference_settings.access_token,
        client_id=inference_settings.client_id,
        on_full=lambda secid, tag, ltp, depth: runtime.on_fullquote(
            secid,
            tag,
            ltp,
            depth,
        ),
        debug=False,
    )
    if settings.premodel_paper:
        from dhan_engine.application.deeplob.premodel_paper_runtime import (
            MarketByPricePaperRuntime,
            MarketByPricePaperSettings,
        )

        inference = MarketByPricePaperRuntime(
            MarketByPricePaperSettings.from_env(),
            prediction_sink=option_paper.on_prediction if option_paper else None,
        )
        logger.warning(
            "DEEPLOB_PREMODEL_MODE | inference=MBP_HEURISTIC | trained_model=false | "
            "paper_only=true | recorder=true"
        )
    else:
        from dhan_engine.domain.market.deeplob_model import DeepLobArtifact

        if not inference_settings.model_path or not inference_settings.metadata_path:
            raise RuntimeError("DEEPLOB_MODEL_PATH and DEEPLOB_METADATA_PATH are required")
        artifact = DeepLobArtifact(
            inference_settings.model_path,
            inference_settings.metadata_path,
        )
        if artifact.sample_interval_ms != inference_settings.sample_interval_ms:
            raise RuntimeError(
                "DEEPLOB_SAMPLE_INTERVAL_MS must match the model metadata "
                f"({artifact.sample_interval_ms}ms)"
            )
        inference = DeepLobPaperInferenceRuntime(
            inference_settings,
            master,
            adapter,
            artifact,
            prediction_sink=option_paper.on_prediction if option_paper else None,
        )
    runtime = DeepLobLiveRuntime(
        settings,
        master,
        adapter,
        fullquote_feed,
        recorder,
        inference,
        option_paper=option_paper,
        option_selection=option_selection,
        trade_summary_sink=trade_summary_sink,
        option_selector=option_selector,
    )
    return runtime


