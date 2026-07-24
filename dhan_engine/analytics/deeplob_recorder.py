from __future__ import annotations

import hashlib
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from dhan_engine.domain.market.full_depth_microstructure import BookSnapshot

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DepthRecorderSettings:
    output_dir: str = "data/deeplob"
    levels: int = 200
    sample_interval_ms: int = 250
    queue_size: int = 4096
    rows_per_file: int = 2000
    flush_sec: float = 30.0
    s3_bucket: str = ""
    s3_prefix: str = "deeplob"
    delete_after_upload: bool = False

    @classmethod
    def from_env(cls) -> "DepthRecorderSettings":
        return cls(
            output_dir=os.getenv("DEEPLOB_OUTPUT_DIR", "data/deeplob").strip(),
            levels=max(1, min(200, int(os.getenv("DEEPLOB_LEVELS", "200")))),
            sample_interval_ms=max(0, int(os.getenv("DEEPLOB_SAMPLE_INTERVAL_MS", "250"))),
            queue_size=max(128, int(os.getenv("DEEPLOB_QUEUE_SIZE", "4096"))),
            rows_per_file=max(100, int(os.getenv("DEEPLOB_ROWS_PER_FILE", "2000"))),
            flush_sec=max(1.0, float(os.getenv("DEEPLOB_FLUSH_SEC", "30"))),
            s3_bucket=os.getenv("DEEPLOB_S3_BUCKET", "").strip(),
            s3_prefix=os.getenv("DEEPLOB_S3_PREFIX", "deeplob").strip().strip("/"),
            delete_after_upload=os.getenv("DEEPLOB_DELETE_AFTER_UPLOAD", "0").strip() == "1",
        )


class ParquetDepthRecorder:
    """Bounded, non-blocking 200-level snapshot recorder with optional S3 upload."""

    def __init__(self, settings: DepthRecorderSettings):
        self.settings = settings
        self._queue: queue.Queue[BookSnapshot] = queue.Queue(maxsize=settings.queue_size)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._worker, name="DeepLOBRecorder", daemon=True)
        self._rows: Dict[str, list[dict]] = {}
        self._last_flush = time.monotonic()
        self._received = 0
        self._sampled_out = 0
        self._written = 0
        self._dropped = 0
        self._uploaded = 0
        self._failures = 0
        self._last_health = 0.0
        self._last_sample_mono: Dict[str, float] = {}
        self._s3_client = None
        Path(settings.output_dir).mkdir(parents=True, exist_ok=True)

    def start(self) -> None:
        self._retry_pending_uploads()
        if not self._thread.is_alive():
            self._thread.start()
        logger.info(
            "DEEPLOB_RECORDER_ACTIVE | levels=%s | sample_interval_ms=%s | queue_max=%s | rows_per_file=%s | "
            "flush_sec=%.1f | output=%s | s3=%s",
            self.settings.levels,
            self.settings.sample_interval_ms,
            self.settings.queue_size,
            self.settings.rows_per_file,
            self.settings.flush_sec,
            self.settings.output_dir,
            self.settings.s3_bucket or "disabled",
        )

    def record(self, instrument: str, snapshot: BookSnapshot) -> None:
        self._received += 1
        interval_sec = self.settings.sample_interval_ms / 1000.0
        previous_sample = self._last_sample_mono.get(instrument, float("-inf"))
        if interval_sec and snapshot.received_mono - previous_sample < interval_sec:
            self._sampled_out += 1
            self._log_health()
            return
        self._last_sample_mono[instrument] = snapshot.received_mono
        if snapshot.name != instrument:
            snapshot = BookSnapshot(
                snapshot.security_id,
                instrument,
                snapshot.bids,
                snapshot.asks,
                snapshot.received_ts,
                snapshot.received_mono,
            )
        try:
            self._queue.put_nowait(snapshot)
        except queue.Full:
            self._dropped += 1
        self._log_health()

    def close(self, timeout: float = 15.0) -> None:
        self._stop.set()
        self._thread.join(timeout)
        if self._thread.is_alive():
            logger.warning("DEEPLOB_RECORDER_STOP_TIMEOUT | queue_size=%s", self._queue.qsize())

    def _worker(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            try:
                snapshot = self._queue.get(timeout=0.5)
                self._rows.setdefault(snapshot.name, []).append(self._to_row(snapshot))
            except queue.Empty:
                pass
            except Exception:
                self._failures += 1
                logger.exception("DEEPLOB_RECORD_ENCODE_FAILED")
            due = time.monotonic() - self._last_flush >= self.settings.flush_sec
            full = any(len(rows) >= self.settings.rows_per_file for rows in self._rows.values())
            if due or full or (self._stop.is_set() and self._queue.empty()):
                self._flush_all()

    def _to_row(self, snapshot: BookSnapshot) -> dict:
        levels = self.settings.levels

        def column(rows, attr, cast):
            values = [cast(getattr(row, attr)) for row in rows[:levels]]
            return values + [cast(0)] * (levels - len(values))

        return {
            "received_ns": int(snapshot.received_ts * 1_000_000_000),
            "security_id": snapshot.security_id,
            "instrument": snapshot.name,
            "best_bid": float(snapshot.bids[0].price),
            "best_ask": float(snapshot.asks[0].price),
            "mid_price": float((snapshot.bids[0].price + snapshot.asks[0].price) / 2.0),
            "spread": float(snapshot.asks[0].price - snapshot.bids[0].price),
            "bid_price": column(snapshot.bids, "price", float),
            "bid_qty": column(snapshot.bids, "qty", int),
            "bid_orders": column(snapshot.bids, "orders", int),
            "ask_price": column(snapshot.asks, "price", float),
            "ask_qty": column(snapshot.asks, "qty", int),
            "ask_orders": column(snapshot.asks, "orders", int),
        }

    def _flush_all(self) -> None:
        pending, self._rows = self._rows, {}
        self._last_flush = time.monotonic()
        for instrument, rows in pending.items():
            if not rows:
                continue
            try:
                path = self._write_parquet(instrument, rows)
                self._written += len(rows)
            except Exception:
                self._failures += 1
                logger.exception("DEEPLOB_FLUSH_FAILED | instrument=%s | rows=%s", instrument, len(rows))
                retained = rows + self._rows.get(instrument, [])
                if len(retained) > self.settings.queue_size:
                    overflow = len(retained) - self.settings.queue_size
                    self._dropped += overflow
                    retained = retained[overflow:]
                self._rows[instrument] = retained
                continue
            try:
                self._upload(path, instrument)
            except Exception:
                self._failures += 1
                logger.exception(
                    "DEEPLOB_S3_UPLOAD_FAILED | instrument=%s | path=%s | local_copy_retained=true",
                    instrument,
                    path,
                )

    def _write_parquet(self, instrument: str, rows: list[dict]) -> Path:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise RuntimeError("Install pyarrow to record DeepLOB Parquet data") from exc

        first_ns = rows[0]["received_ns"]
        instant = datetime.fromtimestamp(first_ns / 1_000_000_000, tz=timezone.utc)
        safe_name = instrument.replace("/", "_").replace(" ", "_")
        directory = (
            Path(self.settings.output_dir)
            / f"date={instant:%Y-%m-%d}"
            / f"instrument={safe_name}"
            / f"hour={instant:%H}"
        )
        directory.mkdir(parents=True, exist_ok=True)
        filename = f"depth-{first_ns}-{rows[-1]['received_ns']}-{len(rows)}.parquet"
        final_path = directory / filename
        temp_path = final_path.with_suffix(".parquet.tmp")
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, temp_path, compression="zstd", use_dictionary=["instrument"])
        temp_path.replace(final_path)
        digest_builder = hashlib.sha256()
        with final_path.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest_builder.update(block)
        digest = digest_builder.hexdigest()
        final_path.with_suffix(".parquet.sha256").write_text(f"{digest}  {filename}\n", encoding="ascii")
        logger.info(
            "DEEPLOB_CHUNK_WRITTEN | instrument=%s | rows=%s | bytes=%s | path=%s | sha256=%s",
            instrument,
            len(rows),
            final_path.stat().st_size,
            final_path,
            digest,
        )
        return final_path

    def _upload(self, path: Path, instrument: str) -> None:
        if not self.settings.s3_bucket:
            return
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("Install boto3 to upload DeepLOB data to S3") from exc
        if self._s3_client is None:
            self._s3_client = boto3.client("s3")
        relative = path.relative_to(Path(self.settings.output_dir)).as_posix()
        key = f"{self.settings.s3_prefix}/{relative}" if self.settings.s3_prefix else relative
        self._s3_client.upload_file(str(path), self.settings.s3_bucket, key)
        self._s3_client.upload_file(
            str(path.with_suffix(".parquet.sha256")),
            self.settings.s3_bucket,
            f"{key}.sha256",
        )
        self._uploaded += 1
        logger.info(
            "DEEPLOB_S3_UPLOAD_OK | instrument=%s | bucket=%s | key=%s",
            instrument,
            self.settings.s3_bucket,
            key,
        )
        if self.settings.delete_after_upload:
            path.unlink(missing_ok=True)
            path.with_suffix(".parquet.sha256").unlink(missing_ok=True)
        else:
            path.with_suffix(".parquet.uploaded").write_text(key + "\n", encoding="utf-8")

    def _retry_pending_uploads(self) -> None:
        if not self.settings.s3_bucket:
            return
        for path in Path(self.settings.output_dir).rglob("*.parquet"):
            if path.with_suffix(".parquet.uploaded").exists():
                continue
            try:
                self._upload(path, path.parent.parent.name.removeprefix("instrument="))
            except Exception:
                self._failures += 1
                logger.exception("DEEPLOB_S3_RETRY_FAILED | path=%s", path)

    def _log_health(self) -> None:
        now = time.monotonic()
        if now - self._last_health < 10.0:
            return
        self._last_health = now
        logger.info(
            "DEEPLOB_RECORDER_HEALTH | received=%s | sampled_out=%s | written=%s | dropped=%s | "
            "queue=%s/%s | uploads=%s | failures=%s",
            self._received,
            self._sampled_out,
            self._written,
            self._dropped,
            self._queue.qsize(),
            self.settings.queue_size,
            self._uploaded,
            self._failures,
        )
