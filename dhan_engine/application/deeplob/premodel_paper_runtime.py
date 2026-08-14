from __future__ import annotations

import logging
import os
import queue
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass

from dhan_engine.domain.market.market_by_price_execution import CompositeMarketSnapshot

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketByPricePaperSettings:
    sample_interval_ms: int
    signal_interval_sec: float
    sequence_length: int
    signal_threshold: float
    stale_after_sec: float
    queue_size: int
    horizon_sec: int
    horizons_sec: tuple[int, ...] = (30, 60, 120, 300)
    option_beta_pct_per_future_bps: float = 0.10

    @classmethod
    def from_env(cls) -> "MarketByPricePaperSettings":
        horizons = tuple(
            sorted(
                {
                    max(5, min(600, int(value.strip())))
                    for value in os.getenv(
                        "DEEPLOB_PREMODEL_HORIZONS_SEC", "30,60,120,300"
                    ).split(",")
                    if value.strip()
                }
            )
        )
        return cls(
            sample_interval_ms=max(
                50, int(os.getenv("DEEPLOB_PREMODEL_SAMPLE_INTERVAL_MS", "250"))
            ),
            signal_interval_sec=max(
                0.25, float(os.getenv("DEEPLOB_PREMODEL_SIGNAL_INTERVAL_SEC", "1"))
            ),
            sequence_length=max(
                4, int(os.getenv("DEEPLOB_PREMODEL_SEQUENCE_LENGTH", "12"))
            ),
            signal_threshold=max(
                0.01,
                min(
                    1.0,
                    float(os.getenv("DEEPLOB_PREMODEL_SIGNAL_THRESHOLD", "0.08")),
                ),
            ),
            stale_after_sec=max(
                0.1, float(os.getenv("DEEPLOB_STALE_AFTER_SEC", "1.5"))
            ),
            queue_size=max(8, int(os.getenv("DEEPLOB_INFERENCE_QUEUE_SIZE", "64"))),
            horizon_sec=max(1, int(os.getenv("DEEPLOB_PREMODEL_HORIZON_SEC", "600"))),
            horizons_sec=horizons or (30, 60, 120, 300),
            option_beta_pct_per_future_bps=max(
                0.0,
                float(
                    os.getenv(
                        "DEEPLOB_PREMODEL_OPTION_BETA_PCT_PER_FUTURE_BPS", "0.10"
                    )
                ),
            ),
        )


class MarketByPricePaperRuntime:
    """Produce paper-only directional evidence before a trained model exists."""

    version = "MBP_PREMODEL_V1"

    def __init__(self, settings: MarketByPricePaperSettings, prediction_sink=None):
        self.settings = settings
        self.prediction_sink = prediction_sink
        self._queue: queue.Queue[tuple[str, object, CompositeMarketSnapshot]] = queue.Queue(
            maxsize=settings.queue_size
        )
        self._stop = threading.Event()
        self._worker = threading.Thread(
            target=self._signal_loop,
            name="MBPPreModelPaper",
            daemon=True,
        )
        self._scores = defaultdict(
            lambda: deque(maxlen=self.settings.sequence_length)
        )
        self._mids = defaultdict(lambda: deque(maxlen=self.settings.sequence_length))
        self._sample_times = defaultdict(
            lambda: deque(maxlen=self.settings.sequence_length)
        )
        self._last_sample = defaultdict(lambda: float("-inf"))
        self._last_signal = defaultdict(lambda: float("-inf"))
        self._received = 0
        self._sampled_out = 0
        self._dropped = 0
        self._stale = 0
        self._signals = 0

    def start_worker(self) -> None:
        if not self._worker.is_alive():
            self._worker.start()
        logger.info(
            "MBP_PREMODEL_PAPER_ACTIVE | version=%s | sequence=%s | sample_ms=%s | "
            "threshold=%.4f | horizons=%s | depth_levels=200 | orders=false",
            self.version,
            self.settings.sequence_length,
            self.settings.sample_interval_ms,
            self.settings.signal_threshold,
            ",".join(map(str, self.settings.horizons_sec)),
        )

    def close_worker(self, timeout: float = 10.0) -> None:
        self._stop.set()
        if self._worker.is_alive():
            self._worker.join(timeout)
        if self._worker.is_alive():
            logger.warning(
                "MBP_PREMODEL_STOP_TIMEOUT | queue=%s/%s",
                self._queue.qsize(),
                self.settings.queue_size,
            )

    @property
    def worker_alive(self) -> bool:
        return self._worker.is_alive()

    def on_book(self, tag, snapshot, composite: CompositeMarketSnapshot | None = None) -> None:
        self._received += 1
        if composite is None:
            return
        interval_sec = self.settings.sample_interval_ms / 1000.0
        if snapshot.received_mono - self._last_sample[tag] < interval_sec:
            self._sampled_out += 1
            return
        self._last_sample[tag] = snapshot.received_mono
        item = (tag, snapshot, composite)
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            # Latest-state replacement prevents old books reaching the strategy.
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                self._dropped += 1
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                self._dropped += 1

    def log_health(self) -> None:
        logger.info(
            "MBP_PREMODEL_HEALTH | received=%s | sampled_out=%s | signals=%s | "
            "dropped=%s | stale=%s | queue=%s/%s | worker_alive=%s | orders=false",
            self._received,
            self._sampled_out,
            self._signals,
            self._dropped,
            self._stale,
            self._queue.qsize(),
            self.settings.queue_size,
            self._worker.is_alive(),
        )

    @staticmethod
    def _evidence_score(composite: CompositeMarketSnapshot) -> float:
        features = composite.features
        value = lambda name, default=0.0: float(getattr(features, name, default))
        microprice = max(-1.0, min(1.0, value("microprice_bps") / 2.0))
        raw = (
            0.46 * value("pressure_score")
            + 0.10 * value("weighted_imbalance_20")
            + 0.08 * value("weighted_imbalance_50", value("weighted_imbalance_20"))
            + 0.06 * value("weighted_imbalance_100", value("weighted_imbalance_20"))
            + 0.05 * value("weighted_imbalance_200", value("weighted_imbalance_20"))
            + 0.05 * value("depth_flow_20")
            + 0.04 * value("depth_flow_50")
            + 0.03 * value("depth_flow_100")
            + 0.03 * value("depth_flow_200")
            + 0.05 * value("depth_consensus", value("imbalance_20"))
            + 0.05 * microprice
        )
        return max(-1.0, min(1.0, raw))

    def _signal_loop(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            try:
                tag, snapshot, composite = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                age_sec = time.monotonic() - snapshot.received_mono
                if age_sec > self.settings.stale_after_sec:
                    self._stale += 1
                    continue
                scores = self._scores[tag]
                scores.append(self._evidence_score(composite))
                mids = self._mids[tag]
                sample_times = self._sample_times[tag]
                fallback_mid = (
                    float(snapshot.bids[0].price) + float(snapshot.asks[0].price)
                ) / 2.0
                mids.append(float(getattr(composite.features, "mid", fallback_mid)))
                sample_times.append(float(snapshot.received_mono))
                if len(scores) < self.settings.sequence_length:
                    continue
                now = time.monotonic()
                if now - self._last_signal[tag] < self.settings.signal_interval_sec:
                    continue
                self._last_signal[tag] = now
                recent = list(scores)
                mean_score = sum(recent) / len(recent)
                aligned = sum(1 for value in recent if value * mean_score > 0) / len(
                    recent
                )
                threshold = self.settings.signal_threshold
                strong = abs(mean_score) >= threshold and aligned >= 0.75
                action = (
                    "BUY_CE"
                    if strong and mean_score > 0
                    else ("BUY_PE" if strong else "NO_TRADE")
                )
                confidence = min(
                    0.99,
                    0.50
                    + 0.50 * min(1.0, abs(mean_score) / (threshold * 2.0)),
                )
                directional = max(0.0, min(0.99, confidence))
                residual = max(0.01, 1.0 - directional)
                if mean_score > 0:
                    down, flat, up = residual * 0.35, residual * 0.65, directional
                elif mean_score < 0:
                    down, flat, up = directional, residual * 0.65, residual * 0.35
                else:
                    down, flat, up = 0.1, 0.8, 0.1
                elapsed = max(sample_times[-1] - sample_times[0], 0.001)
                current_mid = max(mids[-1], 0.001)
                velocity_bps_sec = (
                    (mids[-1] - mids[0]) / current_mid * 10_000.0 / elapsed
                )
                horizon_scores = {
                    str(horizon): max(
                        -50.0,
                        min(
                            50.0,
                            velocity_bps_sec * horizon
                            + mean_score * 6.0 * (horizon / 30.0) ** 0.5,
                        ),
                    )
                    for horizon in self.settings.horizons_sec
                }
                aligned_horizons = [
                    horizon
                    for horizon in self.settings.horizons_sec
                    if horizon_scores[str(horizon)] * mean_score > 0
                ]
                selected_horizon = (
                    min(
                        aligned_horizons,
                        key=lambda horizon: (
                            abs(horizon_scores[str(horizon)]) < 2.0,
                            horizon,
                        ),
                    )
                    if aligned_horizons
                    else self.settings.horizon_sec
                )
                expected_future_bps = abs(
                    horizon_scores.get(str(selected_horizon), 0.0)
                )
                expected_premium_move_pct = (
                    expected_future_bps
                    * self.settings.option_beta_pct_per_future_bps
                )
                metadata = {
                    "profile": "dynamic",
                    "edge_active": strong,
                    "edge_strength": abs(mean_score),
                    "depth_alignment": aligned,
                    "depth_consensus": float(
                        getattr(composite.features, "depth_consensus", 0.0)
                    ),
                    "mid_velocity_bps_sec": velocity_bps_sec,
                    "horizon_scores_bps": horizon_scores,
                    "expected_future_move_bps": expected_future_bps,
                    "expected_premium_move_pct": expected_premium_move_pct,
                    "estimate_kind": "MBP_HEURISTIC_NOT_CALIBRATED",
                }
                self._signals += 1
                if self.prediction_sink is not None:
                    self.prediction_sink(
                        paper_action=action,
                        confidence=confidence,
                        composite=composite,
                        probability_down=down,
                        probability_flat=flat,
                        probability_up=up,
                        model_version=self.version,
                        horizon_sec=selected_horizon,
                        signal_metadata=metadata,
                    )
                logger.info(
                    "MBP_PREMODEL_SIGNAL | instrument=%s | action=%s | confidence=%.4f | "
                    "evidence=%.4f | alignment=%.3f | pressure=%.4f | quote_age_ms=%.1f | "
                    "depth_consensus=%+.3f | expected_future_bps=%.3f | "
                    "horizon_sec=%s | orders=false",
                    tag,
                    action,
                    confidence,
                    mean_score,
                    aligned,
                    composite.features.pressure_score,
                    composite.quote_age_ms,
                    float(getattr(composite.features, "depth_consensus", 0.0)),
                    expected_future_bps,
                    selected_horizon,
                )
            except Exception:
                logger.exception("MBP_PREMODEL_SIGNAL_FAILED | instrument=%s", tag)
            finally:
                self._queue.task_done()
