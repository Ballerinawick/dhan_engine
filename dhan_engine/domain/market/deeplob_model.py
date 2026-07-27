Exit code: 0
Wall time: 7.3 seconds
Output:
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from dhan_engine.domain.market.full_depth_microstructure import BookSnapshot


@dataclass(frozen=True)
class DeepLobPrediction:
    direction: str
    probability_down: float
    probability_flat: float
    probability_up: float
    model_version: str


def paper_option_action(direction: str, confidence: float, threshold: float) -> str:
    if confidence < threshold:
        return "NO_TRADE"
    return {"UP": "BUY_CE", "DOWN": "BUY_PE"}.get(direction, "NO_TRADE")


def encode_book(snapshot: BookSnapshot, levels: int = 200) -> list[float]:
    """Causal, scale-normalized [price, qty, orders] features for both sides."""
    levels = max(1, min(int(levels), 200))
    if not snapshot.bids or not snapshot.asks:
        raise ValueError("Cannot encode an incomplete order book")
    mid = (snapshot.bids[0].price + snapshot.asks[0].price) / 2.0
    if mid <= 0:
        raise ValueError("Order-book midpoint must be positive")

    def side(rows: Iterable, price_transform) -> list[float]:
        rows = list(rows)[:levels]
        padding = levels - len(rows)
        prices = [price_transform(float(row.price), mid) for row in rows] + [0.0] * padding
        quantities = [math.log1p(max(0, int(row.qty))) for row in rows] + [0.0] * padding
        orders = [math.log1p(max(0, int(row.orders))) for row in rows] + [0.0] * padding
        return prices + quantities + orders

    return side(snapshot.bids, lambda price, center: (price - center) / center) + side(
        snapshot.asks,
        lambda price, center: (center - price) / center,
    )


class DeepLobArtifact:
    """Loads a validated TorchScript artifact; it has no broker/order dependency."""

    def __init__(self, model_path: str, metadata_path: str):
        model_file = Path(model_path)
        metadata_file = Path(metadata_path)
        if not model_file.is_file() or not metadata_file.is_file():
            raise FileNotFoundError("DeepLOB model and metadata files are both required")
        metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
        required = {
            "model_version",
            "schema_version",
            "levels",
            "feature_width",
            "sequence_length",
            "sample_interval_ms",
            "horizon_sec",
            "classes",
        }
        missing = required - set(metadata)
        if missing:
            raise ValueError(f"DeepLOB metadata missing: {sorted(missing)}")
        if metadata["classes"] != ["DOWN", "FLAT", "UP"]:
            raise ValueError("DeepLOB class order must be DOWN, FLAT, UP")
        try:
            import torch
        except ImportError as exc:
            raise RuntimeError("Install requirements-deeplob.txt for live inference") from exc
        self._torch = torch
        self.model = torch.jit.load(str(model_file), map_location="cpu")
        self.model.eval()
        self.version = str(metadata["model_version"])
        self.schema_version = int(metadata["schema_version"])
        self.levels = int(metadata["levels"])
        self.feature_width = int(metadata["feature_width"])
        self.sequence_length = int(metadata["sequence_length"])
        self.sample_interval_ms = int(metadata["sample_interval_ms"])
        self.horizon_sec = int(metadata["horizon_sec"])
        if self.schema_version != 1:
            raise ValueError(f"Unsupported DeepLOB schema version: {self.schema_version}")
        if self.feature_width != self.levels * 6:
            raise ValueError("DeepLOB feature width does not match configured depth levels")
        if self.horizon_sec not in {600, 900}:
            raise ValueError("DeepLOB horizon must be either 600 or 900 seconds")
        if self.sample_interval_ms <= 0:
            raise ValueError("DeepLOB sample interval must be positive")

    def predict(self, sequence: list[list[float]]) -> DeepLobPrediction:
        if len(sequence) != self.sequence_length:
            raise ValueError(f"Expected {self.sequence_length} snapshots, got {len(sequence)}")
        if any(len(row) != self.feature_width for row in sequence):
            raise ValueError(f"Every DeepLOB snapshot must contain {self.feature_width} features")
        tensor = self._torch.tensor(sequence, dtype=self._torch.float32).unsqueeze(0)
        with self._torch.inference_mode():
            probabilities = self._torch.softmax(self.model(tensor), dim=-1)[0].tolist()
        best = max(range(3), key=probabilities.__getitem__)
        return DeepLobPrediction(
            direction=("DOWN", "FLAT", "UP")[best],
            probability_down=float(probabilities[0]),
            probability_flat=float(probabilities[1]),
            probability_up=float(probabilities[2]),
            model_version=self.version,
        )

