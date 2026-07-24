from __future__ import annotations

import logging
import os
import queue
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass

from dhan_engine.domain.market.deeplob_model import DeepLobArtifact, encode_book, paper_option_action

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeepLobInferenceSettings:
    client_id: str
    access_token: str
    csv_file: str
    indexes: tuple[str, ...]
    model_path: str
    metadata_path: str
    confidence_threshold: float
    stale_after_sec: float
    sample_interval_ms: int
    log_interval_sec: float
    queue_size: int

    @classmethod
    def from_env(cls) -> "DeepLobInferenceSettings":
        client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
        token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
        if not client_id or not token:
            raise RuntimeError("Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN")
        indexes = tuple(
            dict.fromkeys(
                value.strip().upper()
                for value in os.getenv("DEEPLOB_INDEXES", "NIFTY").split(",")
                if value.strip()
            )
        )
        if indexes != ("NIFTY",):
            raise ValueError("DeepLOB inference is intentionally restricted to DEEPLOB_INDEXES=NIFTY")
        return cls(
            client_id=client_id,
            access_token=token,
            csv_file=os.getenv("CSV_FILE", "api-scrip-master.csv").strip(),
            indexes=indexes,
            model_path=os.getenv("DEEPLOB_MODEL_PATH", "").strip(),
            metadata_path=os.getenv("DEEPLOB_METADATA_PATH", "").strip(),
            confidence_threshold=max(
                0.0, min(1.0, float(os.getenv("DEEPLOB_CONFIDENCE_THRESHOLD", "0.65")))
            ),
            stale_after_sec=max(0.1, float(os.getenv("DEEPLOB_STALE_AFTER_SEC", "1.5"))),
            sample_interval_ms=max(0, int(os.getenv("DEEPLOB_SAMPLE_INTERVAL_MS", "250"))),
            log_interval_sec=max(0.1, float(os.getenv("DEEPLOB_LOG_INTERVAL_SEC", "1.0"))),
            queue_size=max(8, int(os.getenv("DEEPLOB_INFERENCE_QUEUE_SIZE", "64"))),
        )


class DeepLobPaperInferenceRuntime:
    """Runs versioned depth inference and logs observations; it cannot place orders."""

    def __init__(self, settings, master, depth_adapter, artifact):
        self.settings = settings
        self.master = master
        self.depth_adapter = depth_adapter
        self.artifact = artifact
        self._queue = queue.Queue(maxsize=settings.queue_size)
        self._stop = threading.Event()
        self._worker = threading.Thread(target=self._infer_loop, name="DeepLOBInference", daemon=True)
        self._sequences = defaultdict(lambda: deque(maxlen=artifact.sequence_length))
        self._last_log = defaultdict(float)
        self._last_sample = defaultdict(lambda: float("-inf"))
        self._received = 0
        self._sampled_out = 0
        self._dropped = 0
        self._predictions = 0
        self._stale = 0

    def on_book(self, tag, snapshot) -> None:
        self._received += 1
        interval_sec = self.settings.sample_interval_ms / 1000.0
        if interval_sec and snapshot.received_mono - self._last_sample[tag] < interval_sec:
            self._sampled_out += 1
            return
        self._last_sample[tag] = snapshot.received_mono
        try:
            self._queue.put_nowait((tag, snapshot))
        except queue.Full:
            self._dropped += 1

    def run(self) -> None:
        instruments = []
        for index in self.settings.indexes:
            future = self.master.get_nearest_future(index)
            tag = f"{index}_FUT"
            instruments.append(("NSE_FNO", int(future["security_id"]), tag))
            logger.info(
                "DEEPLOB_INFERENCE_INSTRUMENT | index=%s | symbol=%s | secid=%s",
                index,
                future["symbol"],
                future["security_id"],
            )
        self.start_worker()
        self.depth_adapter.subscribe(instruments)
        logger.info(
            "DEEPLOB_PAPER_INFERENCE_ACTIVE | model=%s | indexes=%s | depth=%s | "
            "sequence=%s | confidence=%.3f | orders=false",
            self.artifact.version,
            ",".join(self.settings.indexes),
            self.artifact.levels,
            self.artifact.sequence_length,
            self.settings.confidence_threshold,
        )
        try:
            while True:
                time.sleep(10)
                self.log_health()
        except KeyboardInterrupt:
            logger.info("DEEPLOB_PAPER_INFERENCE_STOPPED")
        finally:
            self.depth_adapter.close()
            self.close_worker()

    def start_worker(self) -> None:
        if not self._worker.is_alive():
            self._worker.start()

    def close_worker(self, timeout: float = 10.0) -> None:
        self._stop.set()
        self._worker.join(timeout)
        if self._worker.is_alive():
            logger.warning(
                "DEEPLOB_INFERENCE_STOP_TIMEOUT | queue=%s/%s",
                self._queue.qsize(),
                self.settings.queue_size,
            )

    @property
    def worker_alive(self) -> bool:
        return self._worker.is_alive()

    def log_health(self) -> None:
        logger.info(
            "DEEPLOB_INFERENCE_HEALTH | received=%s | sampled_out=%s | predictions=%s | "
            "dropped=%s | stale=%s | queue=%s/%s | worker_alive=%s | model=%s",
            self._received,
            self._sampled_out,
            self._predictions,
            self._dropped,
            self._stale,
            self._queue.qsize(),
            self.settings.queue_size,
            self._worker.is_alive(),
            self.artifact.version,
        )

    def _infer_loop(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            try:
                tag, snapshot = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            age_sec = time.monotonic() - snapshot.received_mono
            if age_sec > self.settings.stale_after_sec:
                self._stale += 1
                logger.warning(
                    "DEEPLOB_STALE_SNAPSHOT_DROPPED | instrument=%s | age_ms=%.1f",
                    tag,
                    age_sec * 1000,
                )
                continue
            try:
                sequence = self._sequences[tag]
                sequence.append(encode_book(snapshot, self.artifact.levels))
                if len(sequence) < self.artifact.sequence_length:
                    continue
                prediction = self.artifact.predict(list(sequence))
                self._predictions += 1
                now = time.monotonic()
                if now - self._last_log[tag] < self.settings.log_interval_sec:
                    continue
                self._last_log[tag] = now
                confidence = max(
                    prediction.probability_down,
                    prediction.probability_flat,
                    prediction.probability_up,
                )
                paper_action = paper_option_action(
                    prediction.direction,
                    confidence,
                    self.settings.confidence_threshold,
                )
                observation = prediction.direction if paper_action != "NO_TRADE" else "NO_TRADE"
                logger.info(
                    "DEEPLOB_PAPER_PREDICTION | instrument=%s | observation=%s | paper_action=%s | "
                    "horizon_sec=%s | "
                    "down=%.4f | flat=%.4f | up=%.4f | confidence=%.4f | model=%s | "
                    "snapshot_age_ms=%.1f | orders=false",
                    tag,
                    observation,
                    paper_action,
                    self.artifact.horizon_sec,
                    prediction.probability_down,
                    prediction.probability_flat,
                    prediction.probability_up,
                    confidence,
                    prediction.model_version,
                    age_sec * 1000,
                )
            except Exception:
                logger.exception("DEEPLOB_INFERENCE_FAILED | instrument=%s", tag)


def build_deeplob_inference_runtime(settings: DeepLobInferenceSettings):
    from dhan_engine.infrastructure.dhan.full_depth_200_adapter import FullDepth200Adapter
    from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster

    if not settings.model_path or not settings.metadata_path:
        raise RuntimeError("DEEPLOB_MODEL_PATH and DEEPLOB_METADATA_PATH are required")
    artifact = DeepLobArtifact(settings.model_path, settings.metadata_path)
    if artifact.sample_interval_ms != settings.sample_interval_ms:
        raise RuntimeError(
            "DEEPLOB_SAMPLE_INTERVAL_MS must match the model metadata "
            f"({artifact.sample_interval_ms}ms)"
        )
    master = InstrumentMaster(settings.csv_file, debug=False)
    runtime = None
    adapter = FullDepth200Adapter(
        settings.client_id,
        settings.access_token,
        lambda tag, book: runtime.on_book(tag, book),
    )
    runtime = DeepLobPaperInferenceRuntime(settings, master, adapter, artifact)
    return runtime

