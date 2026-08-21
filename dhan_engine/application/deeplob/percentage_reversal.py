from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass

from dhan_engine.domain.market.liquidity_event_state import adaptive_horizon_seconds
from dhan_engine.domain.market.market_by_price_execution import CompositeMarketSnapshot

logger = logging.getLogger(__name__)


def _percent(value: float) -> float:
    """Map a directional value in [-1, 1] to a stable 0..100 scale."""
    return max(0.0, min(100.0, 50.0 * (float(value) + 1.0)))


@dataclass(frozen=True)
class PercentageReversalSettings:
    enabled: bool
    sample_interval_ms: int
    sequence_length: int
    signal_interval_sec: float
    minimum_turn_score: float
    minimum_event_quality: float
    minimum_adverse_bps: float
    stale_after_sec: float
    queue_size: int
    horizons_sec: tuple[int, ...]
    s3_bucket: str
    prior_key: str

    @classmethod
    def from_env(cls) -> "PercentageReversalSettings":
        horizons = tuple(
            sorted(
                {
                    max(10, min(180, int(item.strip())))
                    for item in os.getenv(
                        "DEEPLOB_REVERSAL_HORIZONS_SEC", "15,30,60,90"
                    ).split(",")
                    if item.strip()
                }
            )
        )
        return cls(
            enabled=os.getenv("DEEPLOB_REVERSAL_ENABLED", "1").strip().lower()
            in {"1", "true", "yes", "on"},
            sample_interval_ms=max(
                100, int(os.getenv("DEEPLOB_REVERSAL_SAMPLE_INTERVAL_MS", "250"))
            ),
            sequence_length=max(
                12, int(os.getenv("DEEPLOB_REVERSAL_SEQUENCE_LENGTH", "24"))
            ),
            signal_interval_sec=max(
                0.5, float(os.getenv("DEEPLOB_REVERSAL_SIGNAL_INTERVAL_SEC", "1"))
            ),
            minimum_turn_score=max(
                0.05,
                min(1.0, float(os.getenv("DEEPLOB_REVERSAL_MIN_TURN_SCORE", "0.58"))),
            ),
            minimum_event_quality=max(
                0.0,
                min(1.0, float(os.getenv("DEEPLOB_REVERSAL_MIN_EVENT_QUALITY", "0.30"))),
            ),
            minimum_adverse_bps=max(
                0.1, float(os.getenv("DEEPLOB_REVERSAL_MIN_ADVERSE_BPS", "1.5"))
            ),
            stale_after_sec=max(
                0.1, float(os.getenv("DEEPLOB_STALE_AFTER_SEC", "1.5"))
            ),
            queue_size=max(8, int(os.getenv("DEEPLOB_REVERSAL_QUEUE_SIZE", "64"))),
            horizons_sec=horizons or (15, 30, 60, 90),
            s3_bucket=os.getenv("DEEPLOB_S3_BUCKET", "").strip(),
            prior_key=os.getenv(
                "DEEPLOB_REVERSAL_PRIOR_S3_KEY",
                "analysis/deeplob/reversal-prior/latest.json",
            ).strip().strip("/"),
        )


class HistoricalReversalPrior:
    """Immutable prior loaded once at startup, never from the tick path."""

    def __init__(self, settings: PercentageReversalSettings, *, s3_client=None):
        self.settings = settings
        self._s3_client = s3_client
        self.sample_count = 0
        self.bullish_probability = 0.5
        self.bearish_probability = 0.5
        self.source = "NEUTRAL_FALLBACK"

    def load(self) -> None:
        if not self.settings.s3_bucket or not self.settings.prior_key:
            logger.warning("DEEPLOB_REVERSAL_PRIOR_UNAVAILABLE | reason=not_configured")
            return
        try:
            if self._s3_client is None:
                import boto3

                self._s3_client = boto3.client("s3")
            body = self._s3_client.get_object(
                Bucket=self.settings.s3_bucket,
                Key=self.settings.prior_key,
            )["Body"].read()
            payload = json.loads(body.decode("utf-8"))
            self.sample_count = max(0, int(payload.get("sample_count", 0) or 0))
            self.bullish_probability = max(
                0.0, min(1.0, float(payload.get("bullish_probability", 0.5)))
            )
            self.bearish_probability = max(
                0.0, min(1.0, float(payload.get("bearish_probability", 0.5)))
            )
            self.source = self.settings.prior_key
            logger.info(
                "DEEPLOB_REVERSAL_PRIOR_READY | samples=%s | bullish=%.3f | "
                "bearish=%.3f | source=%s",
                self.sample_count,
                self.bullish_probability,
                self.bearish_probability,
                self.source,
            )
        except Exception as exc:
            logger.warning(
                "DEEPLOB_REVERSAL_PRIOR_UNAVAILABLE | reason=%s | fallback=neutral", exc
            )

    def probability(self, direction: int) -> float:
        return self.bullish_probability if direction > 0 else self.bearish_probability


class PercentageReversalRuntime:
    """Causal, percentage-normalized reversal detector for paper execution."""

    version = "MBP_PERCENTAGE_REVERSAL_V1"

    def __init__(self, settings, prediction_sink=None, historical_prior=None):
        self.settings = settings
        self.prediction_sink = prediction_sink
        self.historical_prior = historical_prior or HistoricalReversalPrior(settings)
        self._queue: queue.Queue = queue.Queue(maxsize=settings.queue_size)
        self._stop = threading.Event()
        self._worker = threading.Thread(
            target=self._loop, name="DeepLOBPercentageReversal", daemon=True
        )
        self._history = defaultdict(lambda: deque(maxlen=settings.sequence_length))
        self._last_sample = defaultdict(lambda: float("-inf"))
        self._last_signal = defaultdict(lambda: float("-inf"))
        self._received = self._sampled_out = self._dropped = 0
        self._stale = self._signals = self._no_trade = 0

    def start_worker(self) -> None:
        if not self.settings.enabled:
            return
        self.historical_prior.load()
        if not self._worker.is_alive():
            self._worker.start()
        logger.info(
            "DEEPLOB_REVERSAL_ACTIVE | version=%s | sequence=%s | sample_ms=%s | "
            "min_turn=%.3f | prior_samples=%s | paper=true",
            self.version,
            self.settings.sequence_length,
            self.settings.sample_interval_ms,
            self.settings.minimum_turn_score,
            self.historical_prior.sample_count,
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
        if (
            snapshot.received_mono - self._last_sample[tag]
            < self.settings.sample_interval_ms / 1000.0
        ):
            self._sampled_out += 1
            return
        self._last_sample[tag] = snapshot.received_mono
        try:
            self._queue.put_nowait((tag, snapshot, composite))
        except queue.Full:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                self._dropped += 1
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait((tag, snapshot, composite))
            except queue.Full:
                self._dropped += 1

    @staticmethod
    def _observation(composite: CompositeMarketSnapshot) -> dict:
        f = composite.features
        event = composite.event_evidence
        pressure = max(-1.0, min(1.0, float(f.pressure_score)))
        near = max(-1.0, min(1.0, float(f.imbalance_5)))
        deep = max(
            -1.0,
            min(
                1.0,
                (
                    float(f.imbalance_200)
                    + float(f.weighted_imbalance_200)
                    + float(f.order_imbalance_200)
                )
                / 3.0,
            ),
        )
        flow = max(-1.0, min(1.0, float(f.depth_flow_200)))
        event_score = max(-1.0, min(1.0, float(event.score if event else 0.0)))
        replenishment = max(
            -1.0, min(1.0, float(f.bid_replenishment - f.ask_replenishment))
        )
        depletion = max(-1.0, min(1.0, float(f.ask_depletion - f.bid_depletion)))
        return {
            "mid": float(f.mid),
            "pressure": pressure,
            "near": near,
            "deep": deep,
            "flow": flow,
            "event": event_score,
            "replenishment": replenishment,
            "depletion": depletion,
            "quality": float(event.evidence_quality if event else 0.0),
            "persistence": float(event.persistence if event else 0.0),
            "percentages": {
                "buyer_pressure": _percent(pressure),
                "near_depth": _percent(near),
                "deep_depth": _percent(deep),
                "depth_flow": _percent(flow),
                "event_flow": _percent(event_score),
                "replenishment": _percent(replenishment),
                "depletion": _percent(depletion),
            },
        }

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
                observation = self._observation(composite)
                history.append((snapshot.received_mono, observation))
                if len(history) < self.settings.sequence_length:
                    continue
                now = time.monotonic()
                if now - self._last_signal[tag] < self.settings.signal_interval_sec:
                    continue
                self._last_signal[tag] = now

                rows = list(history)
                split = max(3, len(rows) // 2)
                before = [row[1] for row in rows[:split]]
                after = [row[1] for row in rows[split:]]
                mean = lambda values, key: sum(row[key] for row in values) / len(values)
                first_mid = max(rows[0][1]["mid"], 0.001)
                trough_mid = min(row[1]["mid"] for row in rows)
                peak_mid = max(row[1]["mid"] for row in rows)
                last_mid = rows[-1][1]["mid"]
                window_range = max(peak_mid - trough_mid, 0.000001)
                range_position_pct = (last_mid - trough_mid) / window_range * 100.0
                window_return_pct = (last_mid - first_mid) / first_mid * 100.0
                prior_move_bps = (mean(before, "mid") - first_mid) / first_mid * 10_000
                recent_move_bps = (last_mid - mean(after, "mid")) / first_mid * 10_000
                pressure_turn = mean(after, "pressure") - mean(before, "pressure")
                event_turn = mean(after, "event") - mean(before, "event")
                flow_turn = mean(after, "flow") - mean(before, "flow")
                replenish_turn = mean(after, "replenishment") - mean(before, "replenishment")
                bullish_recovery_bps = (last_mid - trough_mid) / first_mid * 10_000
                bearish_recovery_bps = (peak_mid - last_mid) / first_mid * 10_000

                bullish_score = max(
                    0.0,
                    min(
                        1.0,
                        0.30 * max(0.0, pressure_turn)
                        + 0.24 * max(0.0, event_turn)
                        + 0.18 * max(0.0, flow_turn)
                        + 0.14 * max(0.0, replenish_turn)
                        + 0.14 * min(1.0, bullish_recovery_bps / 3.0),
                    ),
                )
                bearish_score = max(
                    0.0,
                    min(
                        1.0,
                        0.30 * max(0.0, -pressure_turn)
                        + 0.24 * max(0.0, -event_turn)
                        + 0.18 * max(0.0, -flow_turn)
                        + 0.14 * max(0.0, -replenish_turn)
                        + 0.14 * min(1.0, bearish_recovery_bps / 3.0),
                    ),
                )
                direction = 1 if bullish_score >= bearish_score else -1
                live_score = bullish_score if direction > 0 else bearish_score
                historical_probability = self.historical_prior.probability(direction)
                blended_score = live_score * (0.85 + 0.15 * historical_probability)
                adverse = (
                    prior_move_bps <= -self.settings.minimum_adverse_bps
                    if direction > 0
                    else prior_move_bps >= self.settings.minimum_adverse_bps
                )
                price_confirmed = recent_move_bps > 0 if direction > 0 else recent_move_bps < 0
                quality = rows[-1][1]["quality"]
                active = (
                    adverse
                    and price_confirmed
                    and blended_score >= self.settings.minimum_turn_score
                    and quality >= self.settings.minimum_event_quality
                    and composite.features.spread_bps <= 10.0
                )
                action = "BUY_CE" if active and direction > 0 else (
                    "BUY_PE" if active else "NO_TRADE"
                )
                confidence = min(0.99, 0.50 + 0.45 * blended_score)
                horizon = adaptive_horizon_seconds(
                    composite.event_evidence,
                    minimum=min(self.settings.horizons_sec),
                    maximum=max(self.settings.horizons_sec),
                    profile="scalp",
                )
                expected_future_bps = max(
                    0.0,
                    abs(recent_move_bps) + blended_score * (2.0 + horizon / 20.0),
                )
                expected_premium_pct = min(5.0, expected_future_bps * 0.12)
                residual = max(0.01, 1.0 - confidence)
                down, flat, up = (
                    (residual * 0.3, residual * 0.7, confidence)
                    if direction > 0
                    else (confidence, residual * 0.7, residual * 0.3)
                )
                before_pct = {
                    key: round(sum(row["percentages"][key] for row in before) / len(before), 2)
                    for key in before[0]["percentages"]
                }
                after_pct = {
                    key: round(sum(row["percentages"][key] for row in after) / len(after), 2)
                    for key in after[0]["percentages"]
                }
                metadata = {
                    "profile": "reversal",
                    "edge_active": active,
                    "reversal_direction": "BULLISH" if direction > 0 else "BEARISH",
                    "turn_score": blended_score,
                    "historical_probability": historical_probability,
                    "historical_sample_count": self.historical_prior.sample_count,
                    "prior_move_bps": prior_move_bps,
                    "confirmation_move_bps": recent_move_bps,
                    "expected_future_move_bps": expected_future_bps,
                    "expected_premium_move_pct": expected_premium_pct,
                    "window_open": first_mid,
                    "window_high": peak_mid,
                    "window_low": trough_mid,
                    "window_close": last_mid,
                    "window_return_pct": window_return_pct,
                    "range_position_pct": range_position_pct,
                    "depth_before_pct": before_pct,
                    "depth_after_pct": after_pct,
                    "estimate_kind": "MBP_PERCENTAGE_REVERSAL_NOT_CALIBRATED",
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
                        horizon_sec=horizon,
                        signal_metadata=metadata,
                    )
                self._signals += int(active)
                self._no_trade += int(not active)
                if active:
                    logger.info(
                        "DEEPLOB_REVERSAL_DETECTED | instrument=%s | direction=%s | "
                        "action=%s | turn_price=%.2f | prior_bps=%+.3f | confirm_bps=%+.3f | "
                        "live_score=%.3f | historical_probability=%.3f | prior_samples=%s | "
                        "range_position_pct=%.2f | window_return_pct=%+.4f | "
                        "before_pct=%s | after_pct=%s | horizon_sec=%s | paper=true",
                        tag,
                        metadata["reversal_direction"],
                        action,
                        last_mid,
                        prior_move_bps,
                        recent_move_bps,
                        blended_score,
                        historical_probability,
                        self.historical_prior.sample_count,
                        range_position_pct,
                        window_return_pct,
                        before_pct,
                        after_pct,
                        horizon,
                    )
            except Exception:
                logger.exception("DEEPLOB_REVERSAL_SIGNAL_FAILED | instrument=%s", tag)
            finally:
                self._queue.task_done()

    def log_health(self) -> None:
        logger.info(
            "DEEPLOB_REVERSAL_HEALTH | received=%s | sampled_out=%s | signals=%s | "
            "no_trade=%s | stale=%s | dropped=%s | queue=%s/%s | worker_alive=%s | "
            "prior_samples=%s",
            self._received,
            self._sampled_out,
            self._signals,
            self._no_trade,
            self._stale,
            self._dropped,
            self._queue.qsize(),
            self.settings.queue_size,
            self.worker_alive,
            self.historical_prior.sample_count,
        )
