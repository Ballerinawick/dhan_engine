from __future__ import annotations

import json
import logging
import os
import queue
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TradeSummaryS3Settings:
    bucket: str
    prefix: str = "paper-trades/deeplob"
    queue_size: int = 256

    @classmethod
    def from_env(cls) -> "TradeSummaryS3Settings":
        return cls(
            bucket=os.getenv("DEEPLOB_S3_BUCKET", "").strip(),
            prefix=os.getenv(
                "DEEPLOB_TRADE_SUMMARY_S3_PREFIX",
                "paper-trades/deeplob",
            ).strip().strip("/"),
            queue_size=max(
                16,
                int(os.getenv("DEEPLOB_TRADE_SUMMARY_S3_QUEUE_SIZE", "256")),
            ),
        )


class TradeSummaryS3Sink:
    """Asynchronously persists immutable paper-trade summaries to S3."""

    def __init__(self, settings: TradeSummaryS3Settings, *, s3_client=None):
        self.settings = settings
        self._s3_client = s3_client
        self._queue: queue.Queue[dict] = queue.Queue(maxsize=settings.queue_size)
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._worker,
            name="DeepLOBTradeSummaryS3",
            daemon=True,
        )
        self._queued = 0
        self._uploaded = 0
        self._dropped = 0
        self._failures = 0
        self._timezone = ZoneInfo("Asia/Kolkata")

    def start(self) -> None:
        if not self.settings.bucket:
            logger.warning("DEEPLOB_TRADE_SUMMARY_S3_DISABLED | reason=BUCKET_MISSING")
            return
        if not self._thread.is_alive():
            self._thread.start()
        logger.info(
            "DEEPLOB_TRADE_SUMMARY_S3_ACTIVE | bucket=%s | prefix=%s | queue_max=%s",
            self.settings.bucket,
            self.settings.prefix,
            self.settings.queue_size,
        )

    def record(self, summary: Mapping[str, object]) -> bool:
        if not self.settings.bucket:
            return False
        try:
            self._queue.put_nowait(dict(summary))
        except queue.Full:
            self._dropped += 1
            logger.error(
                "DEEPLOB_TRADE_SUMMARY_S3_DROPPED | reason=QUEUE_FULL | "
                "queue=%s/%s | dropped=%s",
                self._queue.qsize(),
                self.settings.queue_size,
                self._dropped,
            )
            return False
        self._queued += 1
        logger.info(
            "DEEPLOB_TRADE_SUMMARY_S3_QUEUED | tag=%s | exit_ts=%s | queue=%s/%s",
            summary.get("tag"),
            summary.get("exit_ts"),
            self._queue.qsize(),
            self.settings.queue_size,
        )
        return True

    def close(self, timeout: float = 15.0) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout)
        if self._thread.is_alive():
            logger.warning(
                "DEEPLOB_TRADE_SUMMARY_S3_STOP_TIMEOUT | queue=%s",
                self._queue.qsize(),
            )

    def health(self) -> dict:
        return {
            "worker_alive": self._thread.is_alive(),
            "queue": self._queue.qsize(),
            "queue_max": self.settings.queue_size,
            "queued": self._queued,
            "uploaded": self._uploaded,
            "dropped": self._dropped,
            "failures": self._failures,
        }

    def _worker(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            try:
                summary = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self._upload(summary)
            except Exception:
                self._failures += 1
                logger.exception(
                    "DEEPLOB_TRADE_SUMMARY_S3_UPLOAD_FAILED | tag=%s | exit_ts=%s",
                    summary.get("tag"),
                    summary.get("exit_ts"),
                )
            finally:
                self._queue.task_done()

    def _upload(self, summary: Mapping[str, object]) -> None:
        if self._s3_client is None:
            try:
                import boto3
            except ImportError as exc:
                raise RuntimeError(
                    "Install boto3 to upload DeepLOB trade summaries"
                ) from exc
            self._s3_client = boto3.client("s3")

        exit_ts = float(summary.get("exit_ts", 0.0) or 0.0)
        instant = datetime.fromtimestamp(exit_ts, self._timezone)
        tag = str(summary.get("tag", "UNKNOWN")).replace("/", "_")
        index = str(summary.get("index", "NIFTY")).replace("/", "_")
        secid = int(summary.get("secid", 0) or 0)
        exit_ns = int(exit_ts * 1_000_000_000)
        filename = f"{exit_ns}-{secid}.json"
        parts = [
            self.settings.prefix,
            "schema=v1",
            f"trade_date={instant:%Y-%m-%d}",
            f"index={index}",
            f"instrument={tag}",
            filename,
        ]
        key = "/".join(part for part in parts if part)
        payload = json.dumps(
            dict(summary),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        ).encode("utf-8")
        self._s3_client.put_object(
            Bucket=self.settings.bucket,
            Key=key,
            Body=payload,
            ContentType="application/json",
            Metadata={
                "strategy": str(summary.get("strategy", "deeplob_mbp_option_paper_v1")),
                "paper": "true",
            },
        )
        self._uploaded += 1
        logger.info(
            "DEEPLOB_TRADE_SUMMARY_S3_UPLOAD_OK | bucket=%s | key=%s | "
            "tag=%s | net_pnl=%s",
            self.settings.bucket,
            key,
            tag,
            summary.get("net_pnl"),
        )

