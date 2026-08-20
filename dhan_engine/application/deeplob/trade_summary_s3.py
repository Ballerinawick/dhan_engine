from __future__ import annotations

import json
import logging
import os
import queue
import re
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
        settings = cls(
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
        market_prefix = os.getenv("DEEPLOB_S3_PREFIX", "market-data/deeplob").strip().strip("/")
        if settings.prefix == market_prefix or settings.prefix.startswith(f"{market_prefix}/"):
            raise ValueError(
                "DEEPLOB_TRADE_SUMMARY_S3_PREFIX must be separate from DEEPLOB_S3_PREFIX"
            )
        return settings


class TradeSummaryS3Sink:
    """Asynchronously maintains one consolidated paper-trade ledger per day."""

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
        self._ledgers: dict[tuple[str, str], dict] = {}
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
            "daily_ledgers": len(self._ledgers),
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
        def safe_partition(value: object, default: str) -> str:
            cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or default))
            return cleaned.strip("._") or default

        index = safe_partition(summary.get("index"), "NIFTY")
        secid = int(summary.get("secid", 0) or 0)
        exit_ns = int(exit_ts * 1_000_000_000)
        trade_date = f"{instant:%Y-%m-%d}"
        parts = [
            self.settings.prefix,
            "schema=v2",
            f"trade_date={trade_date}",
            f"index={index}",
            "daily-trades.json",
        ]
        key = "/".join(part for part in parts if part)
        ledger_key = (trade_date, index)
        ledger = self._ledgers.get(ledger_key)
        if ledger is None:
            ledger = self._load_existing_ledger(key, trade_date, index)
            self._ledgers[ledger_key] = ledger

        trade = dict(summary)
        trade_id = str(
            trade.get("trade_id")
            or f"{trade.get('profile', 'dynamic')}:{exit_ns}:{secid}"
        )
        trade["trade_id"] = trade_id
        trades = [row for row in ledger["trades"] if row.get("trade_id") != trade_id]
        trades.append(trade)
        trades.sort(key=lambda row: float(row.get("exit_ts", 0.0) or 0.0))
        ledger["trades"] = trades
        ledger["updated_at"] = datetime.now(self._timezone).isoformat()
        ledger["summary"] = self._summarize(trades)
        payload = json.dumps(
            ledger,
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
                "strategy": "deeplob_multi_profile_paper",
                "paper": "true",
            },
        )
        self._uploaded += 1
        logger.info(
            "DEEPLOB_DAILY_TRADE_LEDGER_UPLOAD_OK | bucket=%s | key=%s | "
            "trade_count=%s | net_pnl=%s",
            self.settings.bucket,
            key,
            ledger["summary"]["trade_count"],
            ledger["summary"]["net_pnl"],
        )

    def _load_existing_ledger(self, key: str, trade_date: str, index: str) -> dict:
        ledger = {
            "schema_version": 2,
            "trade_date": trade_date,
            "index": index,
            "updated_at": None,
            "trades": [],
            "summary": self._summarize([]),
        }
        get_object = getattr(self._s3_client, "get_object", None)
        if get_object is None:
            return ledger
        try:
            response = get_object(Bucket=self.settings.bucket, Key=key)
            body = response["Body"].read()
            existing = json.loads(body.decode("utf-8"))
            if isinstance(existing, dict) and isinstance(existing.get("trades"), list):
                return existing
        except Exception as exc:
            code = str(
                getattr(exc, "response", {}).get("Error", {}).get("Code", "")
            )
            if code not in {"NoSuchKey", "404", "NotFound"}:
                logger.warning(
                    "DEEPLOB_DAILY_TRADE_LEDGER_LOAD_FAILED | key=%s | error=%s",
                    key,
                    exc,
                )
        return ledger

    @staticmethod
    def _summarize(trades: list[dict]) -> dict:
        by_profile: dict[str, dict] = {}
        for trade in trades:
            profile = str(trade.get("profile", "dynamic"))
            stats = by_profile.setdefault(
                profile, {"trades": 0, "wins": 0, "losses": 0, "net_pnl": 0.0}
            )
            net = float(trade.get("net_pnl", 0.0) or 0.0)
            stats["trades"] += 1
            stats["wins"] += int(net > 0)
            stats["losses"] += int(net < 0)
            stats["net_pnl"] = round(stats["net_pnl"] + net, 2)
        net_values = [float(row.get("net_pnl", 0.0) or 0.0) for row in trades]
        return {
            "trade_count": len(trades),
            "wins": sum(value > 0 for value in net_values),
            "losses": sum(value < 0 for value in net_values),
            "flat": sum(value == 0 for value in net_values),
            "gross_pnl": round(
                sum(float(row.get("gross_pnl", 0.0) or 0.0) for row in trades), 2
            ),
            "fees": round(sum(float(row.get("fee", 0.0) or 0.0) for row in trades), 2),
            "net_pnl": round(sum(net_values), 2),
            "by_profile": by_profile,
        }
