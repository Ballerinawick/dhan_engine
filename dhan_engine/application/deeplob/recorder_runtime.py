from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass

from dhan_engine.analytics.deeplob_recorder import DepthRecorderSettings, ParquetDepthRecorder

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeepLobRecorderRuntimeSettings:
    client_id: str
    access_token: str
    csv_file: str
    indexes: tuple[str, ...]
    recorder: DepthRecorderSettings

    @classmethod
    def from_env(cls) -> "DeepLobRecorderRuntimeSettings":
        client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
        token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
        if not client_id or not token:
            raise RuntimeError("Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN")
        raw = os.getenv("DEEPLOB_INDEXES", "NIFTY")
        indexes = tuple(dict.fromkeys(x.strip().upper() for x in raw.split(",") if x.strip()))
        if indexes != ("NIFTY",):
            raise ValueError("The DeepLOB recorder is intentionally restricted to DEEPLOB_INDEXES=NIFTY")
        return cls(
            client_id=client_id,
            access_token=token,
            csv_file=os.getenv("CSV_FILE", "api-scrip-master.csv").strip(),
            indexes=indexes,
            recorder=DepthRecorderSettings.from_env(),
        )


class DeepLobRecorderRuntime:
    def __init__(self, settings, master, depth_adapter, fullquote_feed, recorder):
        self.settings = settings
        self.master = master
        self.depth_adapter = depth_adapter
        self.fullquote_feed = fullquote_feed
        self.recorder = recorder
        self.instruments = {}
        self._quote_lock = threading.Lock()
        self._latest_fullquote = {}
        self._fullquote_received = 0
        self._books_received = 0
        self._books_recorded = 0
        self._sync_rejections = 0
        self._last_sync_log = 0.0
        self._require_fullquote = os.getenv(
            "DEEPLOB_RECORDER_REQUIRE_FULLQUOTE", "1"
        ).strip() == "1"
        self._max_quote_age_ms = max(
            1.0,
            float(os.getenv("DEEPLOB_MAX_FULLQUOTE_AGE_MS", "1500") or 1500),
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
        }
        with self._quote_lock:
            self._latest_fullquote[int(secid)] = quote
            self._fullquote_received += 1

    def on_book(self, tag, book) -> None:
        self._books_received += 1
        metadata = self.instruments[tag]
        with self._quote_lock:
            quote = dict(self._latest_fullquote.get(int(book.security_id), {}))
        if quote:
            quote["age_ms"] = abs(
                (float(book.received_ts) - float(quote["received_ts"])) * 1000.0
            )
        if self._require_fullquote and (
            not quote or quote["age_ms"] > self._max_quote_age_ms
        ):
            self._sync_rejections += 1
            now = time.monotonic()
            if now - self._last_sync_log >= 10.0:
                self._last_sync_log = now
                logger.warning(
                    "DEEPLOB_RECORDER_SYNC_REJECTED | instrument=%s | reason=%s | "
                    "quote_age_ms=%s | rejected=%s",
                    tag,
                    "FULLQUOTE_MISSING" if not quote else "FULLQUOTE_STALE",
                    "NA" if not quote else f"{quote['age_ms']:.1f}",
                    self._sync_rejections,
                )
            return
        self.recorder.record(tag, book, full_quote=quote, **metadata)
        self._books_recorded += 1

    def run(self) -> None:
        instruments = []
        for index in self.settings.indexes:
            future = self.master.get_nearest_future(index)
            secid = int(future["security_id"])
            tag = f"{index}_FUT"
            instruments.append(("NSE_FNO", secid, tag))
            expiry = future["expiry"]
            expiry_text = expiry.strftime("%Y-%m-%d") if hasattr(expiry, "strftime") else str(expiry)
            self.instruments[tag] = {
                "index": index,
                "symbol": future["symbol"],
                "expiry": expiry_text,
            }
            logger.info(
                "DEEPLOB_INSTRUMENT_SELECTED | index=%s | symbol=%s | secid=%s",
                index,
                future["symbol"],
                secid,
            )
        self.recorder.start()
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
            "DEEPLOB_CAPTURE_ACTIVE | indexes=%s | instruments=%s | depth=200 | "
            "connections=%s | fullquote=true | synchronized_s3=true | orders=false",
            ",".join(self.settings.indexes),
            len(instruments),
            len(instruments) + 1,
        )
        try:
            while True:
                time.sleep(10)
                logger.info(
                    "DEEPLOB_CAPTURE_HEALTH | books_received=%s | books_recorded=%s | "
                    "fullquote_received=%s | sync_rejections=%s | recorder_worker_alive=%s",
                    self._books_received,
                    self._books_recorded,
                    self._fullquote_received,
                    self._sync_rejections,
                    self.recorder.worker_alive,
                )
                self.recorder.log_health()
        except KeyboardInterrupt:
            logger.info("DEEPLOB_CAPTURE_STOPPED")
        finally:
            self.depth_adapter.close()
            self.fullquote_feed.close()
            self.recorder.close()


def build_deeplob_recorder_runtime(settings: DeepLobRecorderRuntimeSettings):
    from dhan_engine.infrastructure.dhan.full_depth_200_adapter import FullDepth200Adapter
    from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
    from dhan_engine.infrastructure.dhan.marketfeed_ws import DhanLiveMarketFeedWS

    master = InstrumentMaster(settings.csv_file, debug=False)
    runtime = None
    recorder = ParquetDepthRecorder(settings.recorder)

    adapter = FullDepth200Adapter(
        settings.client_id,
        settings.access_token,
        lambda tag, book: runtime.on_book(tag, book),
    )
    fullquote_feed = DhanLiveMarketFeedWS(
        token=settings.access_token,
        client_id=settings.client_id,
        on_full=lambda secid, tag, ltp, depth: runtime.on_fullquote(
            secid,
            tag,
            ltp,
            depth,
        ),
        debug=False,
    )
    runtime = DeepLobRecorderRuntime(
        settings,
        master,
        adapter,
        fullquote_feed,
        recorder,
    )
    return runtime

