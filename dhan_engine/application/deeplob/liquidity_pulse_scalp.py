from __future__ import annotations

import logging
import os
import queue
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass

from dhan_engine.domain.market.market_by_price_execution import CompositeMarketSnapshot
from dhan_engine.domain.market.liquidity_event_state import adaptive_horizon_seconds
from dhan_engine.domain.market.ltp_execution_path import (
    sample_ltp_execution_path,
    summarize_ltp_execution_path,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiquidityPulseScalpSettings:
    enabled: bool
    sample_interval_ms: int
    sequence_length: int
    signal_interval_sec: float
    minimum_alignment: float
    minimum_strength: float
    stale_after_sec: float
    queue_size: int
    horizons_sec: tuple[int, ...] = (10, 20, 30, 60)

    @classmethod
    def from_env(cls) -> "LiquidityPulseScalpSettings":
        horizons = tuple(
            sorted(
                {
                    max(5, min(60, int(value.strip())))
                    for value in os.getenv(
                        "DEEPLOB_SCALP_HORIZONS_SEC", "10,20,30,60"
                    ).split(",")
                    if value.strip()
                }
            )
        )
        return cls(
            enabled=os.getenv("DEEPLOB_SCALP_ENABLED", "1").strip().lower()
            in {"1", "true", "yes", "on"},
            sample_interval_ms=max(
                50, int(os.getenv("DEEPLOB_SCALP_SAMPLE_INTERVAL_MS", "250"))
            ),
            sequence_length=max(
                8, int(os.getenv("DEEPLOB_SCALP_SEQUENCE_LENGTH", "16"))
            ),
            signal_interval_sec=max(
                0.25, float(os.getenv("DEEPLOB_SCALP_SIGNAL_INTERVAL_SEC", "1"))
            ),
            minimum_alignment=max(
                0.5,
                min(1.0, float(os.getenv("DEEPLOB_SCALP_MIN_ALIGNMENT", "0.75"))),
            ),
            minimum_strength=max(
                0.01,
                min(1.0, float(os.getenv("DEEPLOB_SCALP_MIN_STRENGTH", "0.14"))),
            ),
            stale_after_sec=max(
                0.1, float(os.getenv("DEEPLOB_STALE_AFTER_SEC", "1.5"))
            ),
            queue_size=max(8, int(os.getenv("DEEPLOB_SCALP_QUEUE_SIZE", "64"))),
            horizons_sec=horizons or (10, 20, 30, 60),
        )


class LiquidityPulseScalpRuntime:
    """Short-horizon, paper-only MBP liquidity pulse evaluator.

    It intentionally does not reuse the 600-second DeepLOB label. Signals are
    derived from causal book persistence, depletion/replenishment and mid-price
    velocity over a bounded latest-state queue.
    """

    version = "MBP_LIQUIDITY_PULSE_V2_CALIBRATED"

    def __init__(self, settings: LiquidityPulseScalpSettings, prediction_sink=None):
        self.settings = settings
        self.prediction_sink = prediction_sink
        self._queue: queue.Queue = queue.Queue(maxsize=settings.queue_size)
        self._stop = threading.Event()
        self._worker = threading.Thread(
            target=self._loop, name="DeepLOBLiquidityPulseScalp", daemon=True
        )
        self._history = defaultdict(
            lambda: deque(maxlen=self.settings.sequence_length)
        )
        self._last_sample = defaultdict(lambda: float("-inf"))
        self._last_signal = defaultdict(lambda: float("-inf"))
        self._received = self._sampled_out = self._dropped = 0
        self._stale = self._signals = self._no_trade = 0

    def start_worker(self) -> None:
        if not self.settings.enabled:
            return
        if not self._worker.is_alive():
            self._worker.start()
        logger.info(
            "DEEPLOB_SCALP_ACTIVE | version=%s | horizons=%s | sequence=%s | "
            "sample_ms=%s | min_strength=%.3f | min_alignment=%.2f | paper=true",
            self.version,
            ",".join(map(str, self.settings.horizons_sec)),
            self.settings.sequence_length,
            self.settings.sample_interval_ms,
            self.settings.minimum_strength,
            self.settings.minimum_alignment,
        )

    @property
    def worker_alive(self) -> bool:
        return not self.settings.enabled or self._worker.is_alive()

    def close_worker(self, timeout: float = 10.0) -> None:
        self._stop.set()
        if self._worker.is_alive():
            self._worker.join(timeout)

    def on_book(self, tag, snapshot, composite: CompositeMarketSnapshot | None = None) -> None:
        if not self.settings.enabled or composite is None:
            return
        self._received += 1
        interval = self.settings.sample_interval_ms / 1000.0
        if snapshot.received_mono - self._last_sample[tag] < interval:
            self._sampled_out += 1
            return
        self._last_sample[tag] = snapshot.received_mono
        item = (tag, snapshot, composite)
        try:
            self._queue.put_nowait(item)
        except queue.Full:
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

    @staticmethod
    def _pulse(composite: CompositeMarketSnapshot) -> float:
        f = composite.features
        value = lambda name, default=0.0: float(getattr(f, name, default))
        microprice = max(-1.0, min(1.0, f.microprice_bps / 2.0))
        depletion = max(-1.0, min(1.0, f.ask_depletion - f.bid_depletion))
        replenishment = max(
            -1.0, min(1.0, f.bid_replenishment - f.ask_replenishment)
        )
        deep_confirmation = max(-1.0, min(1.0, f.imbalance_50))
        far_depth = max(
            -1.0,
            min(
                1.0,
                0.35 * value("weighted_imbalance_100", f.weighted_imbalance_20)
                + 0.25 * value("weighted_imbalance_200", f.weighted_imbalance_20)
                + 0.20 * value("depth_flow_100")
                + 0.20 * value("depth_flow_200"),
            ),
        )
        event = getattr(composite, "event_evidence", None)
        event_score = event.score if event is not None else 0.0
        return max(
            -1.0,
            min(
                1.0,
                0.10 * f.weighted_imbalance_20
                + 0.09 * f.imbalance_5
                + 0.08 * f.ofi_top
                + 0.07 * microprice
                + 0.07 * depletion
                + 0.06 * replenishment
                + 0.08 * deep_confirmation
                + 0.09 * value("depth_consensus", f.imbalance_50)
                + 0.07 * value("depth_flow_50")
                + 0.05 * far_depth
                + 0.24 * event_score,
            ),
        )

    def _loop(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            try:
                tag, snapshot, composite = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                if time.monotonic() - snapshot.received_mono > self.settings.stale_after_sec:
                    self._stale += 1
                    continue
                history = self._history[tag]
                history.append(
                    (
                        snapshot.received_mono,
                        self._pulse(composite),
                        sample_ltp_execution_path(snapshot.received_mono, composite),
                    )
                )
                if len(history) < self.settings.sequence_length:
                    continue
                now = time.monotonic()
                if now - self._last_signal[tag] < self.settings.signal_interval_sec:
                    continue
                self._last_signal[tag] = now

                rows = list(history)
                pulse_mean = sum(row[1] for row in rows) / len(rows)
                ltp_path = summarize_ltp_execution_path([row[2] for row in rows])
                direction = 1.0 if pulse_mean > 0 else -1.0 if pulse_mean < 0 else 0.0
                alignment = (
                    sum(1 for row in rows if row[1] * direction > 0) / len(rows)
                    if direction
                    else 0.0
                )
                strength = max(
                    -1.0, min(1.0, 0.50 * pulse_mean + 0.50 * ltp_path.strength)
                )
                aligned_velocity = (
                    strength == 0
                    or abs(ltp_path.recent_velocity_bps_sec) < 0.02
                    or ltp_path.recent_velocity_bps_sec * strength >= 0
                )
                active = (
                    abs(strength) >= self.settings.minimum_strength
                    and alignment >= self.settings.minimum_alignment
                    and aligned_velocity
                    and composite.features.spread_bps <= 10.0
                    and (
                        getattr(composite, "event_evidence", None) is None
                        or composite.event_evidence.evidence_quality >= 0.20
                    )
                )
                action = "BUY_CE" if active and strength > 0 else (
                    "BUY_PE" if active else "NO_TRADE"
                )
                confidence = min(
                    0.95,
                    0.50
                    + 0.45 * abs(strength) * ltp_path.forecast_reliability,
                )
                horizon_scores = {
                    str(horizon): ltp_path.forecast_bps(horizon, pressure=strength)
                    for horizon in self.settings.horizons_sec
                }
                selected_horizon = adaptive_horizon_seconds(
                    getattr(composite, "event_evidence", None),
                    minimum=min(self.settings.horizons_sec),
                    maximum=max(self.settings.horizons_sec),
                    profile="scalp",
                )
                horizon_scores[str(selected_horizon)] = ltp_path.forecast_bps(
                    selected_horizon, pressure=strength
                )
                expected_future_bps = abs(horizon_scores[str(selected_horizon)])
                expected_premium_move_pct = min(
                    5.0, max(0.0, expected_future_bps * 0.12)
                )
                residual = max(0.01, 1.0 - confidence)
                if strength > 0:
                    down, flat, up = residual * 0.3, residual * 0.7, confidence
                elif strength < 0:
                    down, flat, up = confidence, residual * 0.7, residual * 0.3
                else:
                    down, flat, up = 0.1, 0.8, 0.1
                metadata = {
                    "profile": "scalp",
                    "edge_active": active,
                    "pulse_strength": strength,
                    "pulse_alignment": alignment,
                    "ltp_now": ltp_path.ltp_now,
                    "ltp_return_bps": ltp_path.ltp_return_bps,
                    "ltp_velocity_bps_sec": ltp_path.velocity_bps_sec,
                    "ltp_recent_velocity_bps_sec": ltp_path.recent_velocity_bps_sec,
                    "ltp_acceleration_bps_sec2": ltp_path.acceleration_bps_sec2,
                    "classified_buy_qty": ltp_path.classified_buy_qty,
                    "classified_sell_qty": ltp_path.classified_sell_qty,
                    "execution_imbalance": ltp_path.execution_imbalance,
                    "liquidity_imbalance": ltp_path.liquidity_imbalance,
                    "ltp_path_alignment": ltp_path.path_alignment,
                    "ltp_path_quality": ltp_path.evidence_quality,
                    "ltp_path_strength": ltp_path.strength,
                    "forecast_reliability": ltp_path.forecast_reliability,
                    "forecast_observation_sec": ltp_path.elapsed_sec,
                    "horizon_scores_bps": horizon_scores,
                    "expected_future_move_bps": expected_future_bps,
                    "expected_premium_move_pct": expected_premium_move_pct,
                    "depth_consensus": float(
                        getattr(composite.features, "depth_consensus", 0.0)
                    ),
                    "estimate_kind": "LTP_ANCHORED_MBP_NOT_CALIBRATED",
                    "adaptive_horizon": True,
                    "mbp_event_score": getattr(
                        getattr(composite, "event_evidence", None), "score", 0.0
                    ),
                    "mbp_event_quality": getattr(
                        getattr(composite, "event_evidence", None),
                        "evidence_quality",
                        0.0,
                    ),
                    "mbp_event_persistence": getattr(
                        getattr(composite, "event_evidence", None),
                        "persistence",
                        0.0,
                    ),
                }
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
                self._signals += int(active)
                self._no_trade += int(not active)
                logger.info(
                    "DEEPLOB_SCALP_SIGNAL | instrument=%s | action=%s | horizon_sec=%s | "
                    "strength=%.4f | alignment=%.3f | ltp=%0.2f | "
                    "ltp_velocity_bps_sec=%+.4f | execution_imbalance=%+.3f | reliability=%.3f | "
                    "expected_future_bps=%.3f | expected_premium_pct=%.3f | "
                    "event_score=%+.3f | event_quality=%.3f | "
                    "event_persistence=%.3f | paper=true",
                    tag,
                    action,
                    selected_horizon,
                    strength,
                    alignment,
                    ltp_path.ltp_now,
                    ltp_path.recent_velocity_bps_sec,
                    ltp_path.execution_imbalance,
                    ltp_path.forecast_reliability,
                    expected_future_bps,
                    expected_premium_move_pct,
                    metadata["mbp_event_score"],
                    metadata["mbp_event_quality"],
                    metadata["mbp_event_persistence"],
                )
            except Exception:
                logger.exception(
                    "DEEPLOB_SCALP_SIGNAL_FAILED | instrument=%s", tag
                )
            finally:
                self._queue.task_done()

    def log_health(self) -> None:
        logger.info(
            "DEEPLOB_SCALP_HEALTH | received=%s | sampled_out=%s | signals=%s | "
            "no_trade=%s | stale=%s | dropped=%s | queue=%s/%s | worker_alive=%s",
            self._received,
            self._sampled_out,
            self._signals,
            self._no_trade,
            self._stale,
            self._dropped,
            self._queue.qsize(),
            self.settings.queue_size,
            self.worker_alive,
        )
