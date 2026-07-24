"""Offline DeepLOB-style trainer. Never run this inside the live trading service."""

from __future__ import annotations

import argparse
import json
from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Parquet dataset directory")
    parser.add_argument("--output", required=True, help="Artifact output directory")
    parser.add_argument("--levels", type=int, default=200)
    parser.add_argument("--sequence-length", type=int, default=100)
    parser.add_argument("--sample-interval-ms", type=int, default=250)
    parser.add_argument("--sample-tolerance-ms", type=int, default=100)
    parser.add_argument("--horizon-sec", type=int, choices=(300, 600), default=300)
    parser.add_argument("--label-smoothing-sec", type=int, default=10)
    parser.add_argument("--flat-bps", type=float, default=0.5)
    parser.add_argument("--epochs", type=int, default=10)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        import numpy as np
        import pyarrow.dataset as ds
        import torch
        from torch import nn
        from torch.utils.data import DataLoader, TensorDataset
    except ImportError as exc:
        raise SystemExit("Install requirements-deeplob.txt before training") from exc

    table = ds.dataset(args.input, format="parquet", partitioning="hive").to_table()
    rows = table.to_pylist()
    rows.sort(key=lambda row: (row["instrument"], row["received_ns"]))
    feature_rows, mids, instruments, session_dates, feature_timestamps = [], [], [], [], []
    for row in rows:
        bid = np.asarray(row["bid_price"][: args.levels], dtype=np.float32)
        ask = np.asarray(row["ask_price"][: args.levels], dtype=np.float32)
        mid = (bid[0] + ask[0]) / 2.0
        if mid <= 0:
            continue
        features = np.stack(
            (
                (bid - mid) / mid,
                np.log1p(row["bid_qty"][: args.levels]),
                np.log1p(row["bid_orders"][: args.levels]),
                (mid - ask) / mid,
                np.log1p(row["ask_qty"][: args.levels]),
                np.log1p(row["ask_orders"][: args.levels]),
            ),
            axis=0,
        ).reshape(-1)
        feature_rows.append(features)
        mids.append(mid)
        instruments.append(row["instrument"])
        feature_timestamps.append(int(row["received_ns"]))
        session_dates.append(
            datetime.fromtimestamp(row["received_ns"] / 1_000_000_000, tz=timezone.utc).date().isoformat()
        )

    groups = defaultdict(list)
    for index, (instrument, session_date) in enumerate(zip(instruments, session_dates)):
        groups[(instrument, session_date)].append(index)
    observed_intervals_ms = []
    for indices in groups.values():
        observed_intervals_ms.extend(
            (feature_timestamps[right] - feature_timestamps[left]) / 1_000_000
            for left, right in zip(indices, indices[1:])
            if feature_timestamps[right] > feature_timestamps[left]
        )
    if not observed_intervals_ms:
        raise SystemExit("No consecutive snapshots are available to validate sampling")
    observed_sample_interval_ms = float(np.median(observed_intervals_ms))
    if abs(observed_sample_interval_ms - args.sample_interval_ms) > args.sample_tolerance_ms:
        raise SystemExit(
            "Observed sample interval "
            f"{observed_sample_interval_ms:.1f}ms does not match expected "
            f"{args.sample_interval_ms}ms"
        )

    x, y, sample_dates = [], [], []
    horizon_ns = args.horizon_sec * 1_000_000_000
    smoothing_ns = max(0, args.label_smoothing_sec) * 1_000_000_000
    for (_, session_date), indices in groups.items():
        timestamps = [feature_timestamps[index] for index in indices]
        for local_index in range(args.sequence_length - 1, len(indices)):
            target_ns = timestamps[local_index] + horizon_ns
            target_local = bisect_left(timestamps, target_ns, lo=local_index + 1)
            if target_local >= len(indices):
                break
            smoothing_end = bisect_right(
                timestamps,
                target_ns + smoothing_ns,
                lo=target_local,
            )
            future_mid = float(np.mean([mids[indices[i]] for i in range(target_local, smoothing_end)]))
            current_index = indices[local_index]
            future_bps = (future_mid / mids[current_index] - 1.0) * 10_000
            label = 2 if future_bps > args.flat_bps else 0 if future_bps < -args.flat_bps else 1
            start_local = local_index - args.sequence_length + 1
            x.append([feature_rows[indices[i]] for i in range(start_local, local_index + 1)])
            y.append(label)
            sample_dates.append(session_date)
    if len(x) < 1000:
        raise SystemExit(f"Only {len(x)} sequences available; collect more data before training")
    dates = sorted(set(sample_dates))
    if len(dates) < 5:
        raise SystemExit("At least five separate trading sessions are required for leakage-safe validation")
    validation_date_count = max(1, int(len(dates) * 0.2))
    validation_dates = set(dates[-validation_date_count:])

    class DepthLobNet(nn.Module):
        def __init__(self, input_size: int):
            super().__init__()
            self.temporal = nn.Sequential(
                nn.Conv1d(input_size, 128, kernel_size=3, padding=1),
                nn.ReLU(),
                nn.Conv1d(128, 64, kernel_size=3, padding=1),
                nn.ReLU(),
            )
            self.lstm = nn.LSTM(64, 64, batch_first=True)
            self.head = nn.Linear(64, 3)

        def forward(self, values):
            values = self.temporal(values.transpose(1, 2)).transpose(1, 2)
            values, _ = self.lstm(values)
            return self.head(values[:, -1])

    features = torch.tensor(np.asarray(x), dtype=torch.float32)
    labels = torch.tensor(y, dtype=torch.long)
    train_mask = torch.tensor([date not in validation_dates for date in sample_dates], dtype=torch.bool)
    validation_mask = ~train_mask
    train_x, train_y = features[train_mask], labels[train_mask]
    validation_x, validation_y = features[validation_mask], labels[validation_mask]
    train = TensorDataset(train_x, train_y)
    model = DepthLobNet(features.shape[-1])
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3)
    counts = torch.bincount(train_y, minlength=3).float().clamp_min(1)
    class_weights = counts.sum() / (3 * counts)
    loss_fn = nn.CrossEntropyLoss(weight=class_weights)
    for epoch in range(args.epochs):
        model.train()
        for batch_x, batch_y in DataLoader(train, batch_size=64, shuffle=True):
            optimizer.zero_grad()
            loss = loss_fn(model(batch_x), batch_y)
            loss.backward()
            optimizer.step()
        model.eval()
        with torch.inference_mode():
            accuracy = (model(validation_x).argmax(dim=-1) == validation_y).float().mean().item()
        print(f"epoch={epoch + 1} validation_accuracy={accuracy:.4f}")

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    scripted = torch.jit.script(model.eval())
    scripted.save(str(output / f"deeplob-{version}.pt"))
    metadata = {
        "model_version": version,
        "schema_version": 1,
        "levels": args.levels,
        "feature_width": args.levels * 6,
        "sequence_length": args.sequence_length,
        "sample_interval_ms": args.sample_interval_ms,
        "observed_sample_interval_ms": observed_sample_interval_ms,
        "horizon_sec": args.horizon_sec,
        "label_smoothing_sec": args.label_smoothing_sec,
        "flat_bps": args.flat_bps,
        "classes": ["DOWN", "FLAT", "UP"],
        "validation_accuracy": accuracy,
        "training_sequences": len(train_x),
        "validation_sequences": len(validation_x),
        "training_dates": sorted(set(sample_dates) - validation_dates),
        "validation_dates": sorted(validation_dates),
        "class_counts": counts.int().tolist(),
    }
    (output / f"deeplob-{version}.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
