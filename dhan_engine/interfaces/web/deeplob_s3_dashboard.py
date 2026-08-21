from __future__ import annotations

import io
import json
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any


_DATE_RE = re.compile(r"(?:^|/)trade_date=(\d{4}-\d{2}-\d{2})(?:/|$)")
_EXPIRY_RE = re.compile(r"(?:^|/)expiry=([^/]+)(?:/|$)")
_SYMBOL_RE = re.compile(r"(?:^|/)symbol=([^/]+)(?:/|$)")


@dataclass(frozen=True)
class DeepLobDashboardSettings:
    bucket: str
    market_prefix: str = "market-data/deeplob"
    trade_prefix: str = "paper-trades/deeplob"
    max_points: int = 5000
    cache_seconds: float = 15.0

    @classmethod
    def from_env(cls) -> "DeepLobDashboardSettings":
        return cls(
            bucket=os.getenv("DEEPLOB_S3_BUCKET", "").strip(),
            market_prefix=os.getenv(
                "DEEPLOB_S3_PREFIX", "market-data/deeplob"
            ).strip().strip("/"),
            trade_prefix=os.getenv(
                "DEEPLOB_TRADE_SUMMARY_S3_PREFIX", "paper-trades/deeplob"
            ).strip().strip("/"),
            max_points=max(500, int(os.getenv("DEEPLOB_DASHBOARD_MAX_POINTS", "5000"))),
            cache_seconds=max(
                2.0, float(os.getenv("DEEPLOB_DASHBOARD_CACHE_SECONDS", "15"))
            ),
        )


class DeepLobS3Dashboard:
    """Read-only, bounded S3 projection for the DeepLOB dashboard."""

    def __init__(self, settings: DeepLobDashboardSettings, *, s3_client=None):
        self.settings = settings
        self._s3 = s3_client
        self._lock = threading.Lock()
        self._session_cache: tuple[float, list[dict[str, Any]]] = (0.0, [])
        self._payload_cache: dict[tuple[str, int], tuple[float, dict[str, Any]]] = {}
        self._object_cache: dict[
            tuple[str, int, int], tuple[list[dict[str, Any]], int]
        ] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.settings.bucket)

    def list_sessions(self) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        with self._lock:
            cached_at, cached = self._session_cache
            if time.monotonic() - cached_at < self.settings.cache_seconds:
                return list(cached)

        grouped: dict[str, dict[str, Any]] = {}
        for item in self._list_objects(f"{self.settings.market_prefix}/"):
            key = str(item.get("Key", ""))
            if not key.endswith(".parquet") or "instrument=NIFTY_FUT" not in key:
                continue
            date_match = _DATE_RE.search(key)
            if not date_match:
                continue
            trade_date = date_match.group(1)
            session = grouped.setdefault(
                trade_date,
                {
                    "date": trade_date,
                    "expiry": _partition(_EXPIRY_RE, key, "unknown"),
                    "symbol": _partition(_SYMBOL_RE, key, "NIFTY_FUT"),
                    "source": "s3_deeplob",
                    "object_count": 0,
                    "bytes": 0,
                    "latest_modified": "",
                },
            )
            session["object_count"] += 1
            session["bytes"] += int(item.get("Size", 0) or 0)
            modified = item.get("LastModified")
            modified_text = modified.isoformat() if hasattr(modified, "isoformat") else str(modified or "")
            session["latest_modified"] = max(session["latest_modified"], modified_text)

        sessions = sorted(grouped.values(), key=lambda row: row["date"], reverse=True)
        with self._lock:
            self._session_cache = (time.monotonic(), sessions)
        return list(sessions)

    def session_payload(self, trade_date: str, bucket_sec: int = 5) -> dict[str, Any]:
        bucket_sec = max(1, min(300, int(bucket_sec)))
        cache_key = (trade_date, bucket_sec)
        with self._lock:
            cached = self._payload_cache.get(cache_key)
            if cached and time.monotonic() - cached[0] < self.settings.cache_seconds:
                return cached[1]

        session = next(
            (row for row in self.list_sessions() if row["date"] == trade_date), None
        )
        if session is None:
            return self._empty_payload(trade_date, bucket_sec)

        object_rows = [
            item
            for item in self._list_objects(f"{self.settings.market_prefix}/")
            if item.get("Key", "").endswith(".parquet")
            and "instrument=NIFTY_FUT" in item.get("Key", "")
            and f"trade_date={trade_date}/" in item.get("Key", "")
        ]
        object_rows.sort(key=lambda item: item.get("Key", ""))
        ticks, raw_rows = self._read_and_bucket(object_rows, bucket_sec)
        trades, ledger = self._read_trade_ledger(trade_date)
        signals = _trade_signals(trades)
        latest_ts = ticks[-1]["ts"] if ticks else 0.0
        source = {
            "kind": "s3_deeplob",
            "bucket": self.settings.bucket,
            "market_prefix": self.settings.market_prefix,
            "trade_prefix": self.settings.trade_prefix,
            "object_count": len(object_rows),
            "raw_rows": raw_rows,
            "points": len(ticks),
            "bucket_sec": bucket_sec,
            "first_ts": ticks[0]["ts"] if ticks else 0.0,
            "last_ts": latest_ts,
            "freshness_sec": max(0.0, time.time() - latest_ts) if latest_ts else None,
            "trade_ledger_updated_at": ledger.get("updated_at") if ledger else None,
        }
        payload = {
            "session": session,
            "source": source,
            "ticks": ticks,
            "trades": trades,
            "signals": signals,
            "portfolio": [],
        }
        with self._lock:
            self._payload_cache[cache_key] = (time.monotonic(), payload)
            if len(self._payload_cache) > 6:
                oldest = min(self._payload_cache, key=lambda key: self._payload_cache[key][0])
                self._payload_cache.pop(oldest, None)
        return payload

    def latest_payload(self, bucket_sec: int = 5) -> dict[str, Any]:
        sessions = self.list_sessions()
        if not sessions:
            return self._empty_payload("", bucket_sec)
        return self.session_payload(sessions[0]["date"], bucket_sec)

    def _read_and_bucket(
        self, objects: list[dict[str, Any]], bucket_sec: int
    ) -> tuple[list[dict[str, Any]], int]:
        try:
            import pyarrow.parquet as parquet
        except ImportError as exc:
            raise RuntimeError("pyarrow is required for the DeepLOB dashboard") from exc

        columns = [
            "received_ns", "security_id", "ltp", "mid_price", "spread", "volume",
            "ltq", "oi", "best_bid", "best_ask", "bid_qty", "ask_qty",
            "bid_orders", "ask_orders", "fullquote_age_ms", "fullquote_synchronized",
        ]
        buckets: dict[int, dict[str, Any]] = {}
        raw_rows = 0
        for item in objects:
            projected, object_raw_rows = self._project_object(
                item, bucket_sec, columns, parquet
            )
            raw_rows += object_raw_rows
            for row in projected:
                bucket = int(row["ts"])
                ltp = _number(row["close"])
                samples = max(1, int(row.get("samples", 1)))
                current = buckets.get(bucket)
                if current is None:
                    current = {
                        "ts": float(bucket),
                        "index": "NIFTY",
                        "stream": "FUT",
                        "secid": int(row.get("security_id", 0) or 0),
                        "ltp": ltp,
                        "open": _number(row["open"]),
                        "high": _number(row["high"]),
                        "low": _number(row["low"]),
                        "close": ltp,
                        "samples": 0,
                        "features": {},
                        "_feature_sums": {},
                    }
                    buckets[bucket] = current
                current["high"] = max(current["high"], _number(row["high"]))
                current["low"] = min(current["low"], _number(row["low"]))
                current["close"] = ltp
                current["ltp"] = ltp
                current["samples"] += samples
                for name, value in row["features"].items():
                    current["_feature_sums"][name] = (
                        current["_feature_sums"].get(name, 0.0) + value * samples
                    )

        ticks = []
        for current in sorted(buckets.values(), key=lambda row: row["ts"]):
            samples = max(1, current["samples"])
            current["features"] = {
                name: value / samples for name, value in current.pop("_feature_sums").items()
            }
            current["features"].update(
                {
                    "open": current["open"],
                    "high": current["high"],
                    "low": current["low"],
                    "close": current["close"],
                }
            )
            ticks.append(current)
        if len(ticks) > self.settings.max_points:
            stride = max(1, len(ticks) // self.settings.max_points)
            ticks = ticks[::stride][-self.settings.max_points :]
        return ticks, raw_rows

    def _project_object(self, item, bucket_sec, columns, parquet):
        cache_key = (str(item["Key"]), int(item.get("Size", 0) or 0), bucket_sec)
        with self._lock:
            cached = self._object_cache.get(cache_key)
        if cached is not None:
            return cached

        response = self._client().get_object(
            Bucket=self.settings.bucket, Key=item["Key"]
        )
        data = response["Body"].read()
        available = set(parquet.read_schema(io.BytesIO(data)).names)
        required = {"received_ns", "security_id"}
        if not required.issubset(available) or not {
            "ltp", "mid_price"
        }.intersection(available):
            result = ([], 0)
        else:
            selected = [name for name in columns if name in available]
            table = parquet.read_table(io.BytesIO(data), columns=selected)
            result = self._bucket_rows(table.to_pylist(), bucket_sec)

        with self._lock:
            self._object_cache[cache_key] = result
            if len(self._object_cache) > 256:
                oldest_key = next(iter(self._object_cache))
                self._object_cache.pop(oldest_key, None)
        return result

    @staticmethod
    def _bucket_rows(rows: list[dict[str, Any]], bucket_sec: int):
        buckets: dict[int, dict[str, Any]] = {}
        raw_rows = 0
        for row in rows:
            received_ns = int(row.get("received_ns", 0) or 0)
            if received_ns <= 0:
                continue
            raw_rows += 1
            ts = received_ns / 1_000_000_000
            bucket = int(ts // bucket_sec) * bucket_sec
            ltp = _number(row.get("ltp")) or _number(row.get("mid_price"))
            if ltp <= 0:
                continue
            current = buckets.get(bucket)
            if current is None:
                current = {
                    "ts": float(bucket),
                    "security_id": int(row.get("security_id", 0) or 0),
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                    "samples": 0,
                    "features": {},
                    "_feature_sums": {},
                }
                buckets[bucket] = current
            current["high"] = max(current["high"], ltp)
            current["low"] = min(current["low"], ltp)
            current["close"] = ltp
            current["samples"] += 1
            for name, value in _book_features(row).items():
                current["_feature_sums"][name] = (
                    current["_feature_sums"].get(name, 0.0) + value
                )

        projected = []
        for current in sorted(buckets.values(), key=lambda value: value["ts"]):
            samples = max(1, current["samples"])
            current["features"] = {
                name: value / samples
                for name, value in current.pop("_feature_sums").items()
            }
            projected.append(current)
        return projected, raw_rows

    def _read_trade_ledger(self, trade_date: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        key = (
            f"{self.settings.trade_prefix}/schema=v2/trade_date={trade_date}/"
            "index=NIFTY/daily-trades.json"
        )
        try:
            response = self._client().get_object(Bucket=self.settings.bucket, Key=key)
            ledger = json.loads(response["Body"].read().decode("utf-8"))
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code in {"NoSuchKey", "404", "NotFound"}:
                return [], {}
            raise
        trades = ledger.get("trades", []) if isinstance(ledger, dict) else []
        return [row for row in trades if isinstance(row, dict)], ledger

    def _list_objects(self, prefix: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        token = None
        while True:
            kwargs = {"Bucket": self.settings.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            response = self._client().list_objects_v2(**kwargs)
            rows.extend(response.get("Contents", []))
            if not response.get("IsTruncated"):
                break
            token = response.get("NextContinuationToken")
        return rows

    def _client(self):
        if self._s3 is None:
            try:
                import boto3
            except ImportError as exc:
                raise RuntimeError("boto3 is required for the DeepLOB dashboard") from exc
            self._s3 = boto3.client("s3")
        return self._s3

    def _empty_payload(self, trade_date: str, bucket_sec: int) -> dict[str, Any]:
        return {
            "session": {"date": trade_date, "expiry": "unknown", "source": "s3_deeplob"},
            "source": {"kind": "s3_deeplob", "points": 0, "bucket_sec": bucket_sec},
            "ticks": [],
            "trades": [],
            "signals": [],
            "portfolio": [],
        }


def _book_features(row: dict[str, Any]) -> dict[str, float]:
    bid_qty = [_number(value) for value in (row.get("bid_qty") or [])]
    ask_qty = [_number(value) for value in (row.get("ask_qty") or [])]
    bid_orders = [_number(value) for value in (row.get("bid_orders") or [])]
    ask_orders = [_number(value) for value in (row.get("ask_orders") or [])]
    return {
        "spread": _number(row.get("spread")),
        "mid_price": _number(row.get("mid_price")),
        "best_bid": _number(row.get("best_bid")),
        "best_ask": _number(row.get("best_ask")),
        "volume": _number(row.get("volume")),
        "ltq": _number(row.get("ltq")),
        "oi": _number(row.get("oi")),
        "book_imbalance_5": _imbalance(bid_qty[:5], ask_qty[:5]),
        "book_imbalance_20": _imbalance(bid_qty[:20], ask_qty[:20]),
        "book_imbalance_200": _imbalance(bid_qty, ask_qty),
        "order_imbalance_20": _imbalance(bid_orders[:20], ask_orders[:20]),
        "bid_qty_20": sum(bid_qty[:20]),
        "ask_qty_20": sum(ask_qty[:20]),
        "fullquote_age_ms": _number(row.get("fullquote_age_ms")),
        "fullquote_synchronized": 1.0 if row.get("fullquote_synchronized") else 0.0,
    }


def _trade_signals(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for trade in trades:
        tag = str(trade.get("tag", ""))
        side = str(trade.get("side") or (tag.split("_")[-1] if tag else "")).upper()
        signals.append(
            {
                "ts": _number(trade.get("entry_ts")),
                "kind": "PAPER_ENTRY",
                "action": side,
                "profile": str(trade.get("profile", "dynamic")),
                "tag": tag,
                "price": _number(trade.get("entry")),
                "reason": str(trade.get("entry_reason", "")),
            }
        )
        if _number(trade.get("exit_ts")):
            signals.append(
                {
                    "ts": _number(trade.get("exit_ts")),
                    "kind": "PAPER_EXIT",
                    "action": side,
                    "profile": str(trade.get("profile", "dynamic")),
                    "tag": tag,
                    "price": _number(trade.get("exit")),
                    "reason": str(trade.get("exit_reason", "")),
                    "net_pnl": _number(trade.get("net_pnl")),
                }
            )
    return sorted(signals, key=lambda row: row["ts"])


def _imbalance(bids: list[float], asks: list[float]) -> float:
    bid = sum(bids)
    ask = sum(asks)
    total = bid + ask
    return (bid - ask) / total if total > 0 else 0.0


def _partition(pattern: re.Pattern[str], key: str, default: str) -> str:
    match = pattern.search(key)
    return match.group(1) if match else default


def _number(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
