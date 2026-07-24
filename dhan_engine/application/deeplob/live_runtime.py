from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass

from dhan_engine.analytics.deeplob_recorder import DepthRecorderSettings, ParquetDepthRecorder
from dhan_engine.application.deeplob.inference_runtime import (
    DeepLobInferenceSettings,
    DeepLobPaperInferenceRuntime,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeepLobLiveSettings:
    inference: DeepLobInferenceSettings
    recorder: DepthRecorderSettings

    @classmethod
    def from_env(cls) -> "DeepLobLiveSettings":
        recorder = DepthRecorderSettings.from_env()
        if not recorder.s3_bucket:
            raise RuntimeError("DEEPLOB_S3_BUCKET is required for DHAN_SERVICE=deeplob-live")
        return cls(
            inference=DeepLobInferenceSettings.from_env(),
            recorder=recorder,
        )


class DeepLobLiveRuntime:
    """Fans one NIFTY 200-depth feed into isolated recorder and inference workers."""

    def __init__(self, settings, master, depth_adapter, fullquote_feed, recorder, inference):
        self.settings = settings
        self.master = master
        self.depth_adapter = depth_adapter
        self.fullquote_feed = fullquote_feed
        self.recorder = recorder
        self.inference = inference
        self.instrument_metadata = {}
        self._received = 0
        self._recorder_dispatch_failures = 0
        self._inference_dispatch_failures = 0
        self._quote_lock = threading.Lock()
        self._latest_fullquote = {}
        self._fullquote_received = 0

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
        }
        with self._quote_lock:
            self._latest_fullquote[int(secid)] = quote
            self._fullquote_received += 1

    def on_book(self, tag, snapshot) -> None:
        self._received += 1
        metadata = self.instrument_metadata[tag]
        with self._quote_lock:
            latest_quote = dict(self._latest_fullquote.get(snapshot.security_id, {}))
        if latest_quote:
            latest_quote["age_ms"] = max(
                0.0,
                (snapshot.received_ts - latest_quote["received_ts"]) * 1000.0,
            )
        try:
            self.recorder.record(
                tag,
                snapshot,
                full_quote=latest_quote,
                **metadata,
            )
        except Exception:
            self._recorder_dispatch_failures += 1
            logger.exception("DEEPLOB_LIVE_RECORDER_DISPATCH_FAILED | instrument=%s", tag)
        try:
            self.inference.on_book(tag, snapshot)
        except Exception:
            self._inference_dispatch_failures += 1
            logger.exception("DEEPLOB_LIVE_INFERENCE_DISPATCH_FAILED | instrument=%s", tag)

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
        logger.info(
            "DEEPLOB_LIVE_PIPELINE_ACTIVE | indexes=%s | depth=200 | connections=%s | "
            "fullquote=true | recorder=true | inference=true | s3_bucket=%s | model=%s | "
            "orders=false",
            ",".join(self.settings.inference.indexes),
            len(instruments) + 1,
            self.settings.recorder.s3_bucket,
            self.inference.artifact.version,
        )
        try:
            while True:
                time.sleep(10)
                logger.info(
                    "DEEPLOB_LIVE_PIPELINE_HEALTH | received=%s | recorder_dispatch_failures=%s | "
                    "inference_dispatch_failures=%s | fullquote_received=%s | "
                    "recorder_worker_alive=%s | "
                    "inference_worker_alive=%s",
                    self._received,
                    self._recorder_dispatch_failures,
                    self._inference_dispatch_failures,
                    self._fullquote_received,
                    self.recorder.worker_alive,
                    self.inference.worker_alive,
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


def build_deeplob_live_runtime(settings: DeepLobLiveSettings) -> DeepLobLiveRuntime:
    from dhan_engine.domain.market.deeplob_model import DeepLobArtifact
    from dhan_engine.infrastructure.dhan.full_depth_200_adapter import FullDepth200Adapter
    from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
    from dhan_engine.infrastructure.dhan.marketfeed_ws import DhanLiveMarketFeedWS

    inference_settings = settings.inference
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

    master = InstrumentMaster(inference_settings.csv_file, debug=False)
    recorder = ParquetDepthRecorder(settings.recorder)
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
    inference = DeepLobPaperInferenceRuntime(
        inference_settings,
        master,
        adapter,
        artifact,
    )
    runtime = DeepLobLiveRuntime(
        settings,
        master,
        adapter,
        fullquote_feed,
        recorder,
        inference,
    )
    return runtime

