from __future__ import annotations

import bisect
import io
import json
import logging
import os
import threading
from dataclasses import dataclass
from datetime import datetime, time as market_time
from zoneinfo import ZoneInfo

from dhan_engine.domain.market.expiry_cycle import expiry_cycle_context

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PostMarketAnalysisSettings:
    enabled: bool
    bucket: str
    market_prefix: str
    report_prefix: str
    run_after: market_time
    sideways_bps: float
    trade_prefix: str = "paper-trades/deeplob"

    @classmethod
    def from_env(cls) -> "PostMarketAnalysisSettings":
        return cls(
            enabled=os.getenv("DEEPLOB_POST_MARKET_ENABLED", "1").strip().lower()
            in {"1", "true", "yes", "on"},
            bucket=os.getenv("DEEPLOB_S3_BUCKET", "").strip(),
            market_prefix=os.getenv(
                "DEEPLOB_S3_PREFIX", "market-data/deeplob"
            ).strip().strip("/"),
            report_prefix=os.getenv(
                "DEEPLOB_POST_MARKET_S3_PREFIX", "analysis/deeplob"
            ).strip().strip("/"),
            run_after=datetime.strptime(
                os.getenv("DEEPLOB_POST_MARKET_RUN_AFTER", "15:35"), "%H:%M"
            ).time(),
            sideways_bps=max(
                0.1, float(os.getenv("DEEPLOB_POST_MARKET_SIDEWAYS_BPS", "5"))
            ),
            trade_prefix=os.getenv(
                "DEEPLOB_TRADE_SUMMARY_S3_PREFIX", "paper-trades/deeplob"
            ).strip().strip("/"),
        )


class PostMarketAnalysisRuntime:
    """Creates a compact daily report from the canonical S3 Parquet capture."""

    def __init__(self, settings: PostMarketAnalysisSettings, *, s3_client=None):
        self.settings = settings
        self._s3_client = s3_client
        self._timezone = ZoneInfo("Asia/Kolkata")
        self._started_dates: set[str] = set()
        self._completed_dates: set[str] = set()
        self._failures = 0
        self._lock = threading.Lock()

    def maybe_start(self, now: datetime | None = None) -> bool:
        if not self.settings.enabled or not self.settings.bucket:
            return False
        now = now or datetime.now(self._timezone)
        trade_date = now.date().isoformat()
        if now.time() < self.settings.run_after:
            return False
        with self._lock:
            if trade_date in self._started_dates:
                return False
            self._started_dates.add(trade_date)
        threading.Thread(
            target=self._run_guarded,
            args=(trade_date,),
            name=f"DeepLOBPostMarket-{trade_date}",
            daemon=True,
        ).start()
        logger.info("DEEPLOB_POST_MARKET_STARTED | trade_date=%s", trade_date)
        return True

    def _run_guarded(self, trade_date: str) -> None:
        try:
            self._analyze(trade_date)
            self._completed_dates.add(trade_date)
        except Exception:
            self._failures += 1
            logger.exception("DEEPLOB_POST_MARKET_FAILED | trade_date=%s", trade_date)

    def _client(self):
        if self._s3_client is None:
            import boto3

            self._s3_client = boto3.client("s3")
        return self._s3_client

    def _analyze(self, trade_date: str) -> None:
        import pyarrow.parquet as parquet

        prefix = (
            f"{self.settings.market_prefix}/schema=v1/index=NIFTY/"
        )
        objects = []
        token = None
        while True:
            kwargs = {
                "Bucket": self.settings.bucket,
                "Prefix": prefix,
            }
            if token:
                kwargs["ContinuationToken"] = token
            response = self._client().list_objects_v2(**kwargs)
            objects.extend(
                item["Key"]
                for item in response.get("Contents", [])
                if f"trade_date={trade_date}/" in item["Key"]
                and item["Key"].endswith(".parquet")
            )
            if not response.get("IsTruncated"):
                break
            token = response.get("NextContinuationToken")

        first = last = None
        low = high = None
        first_ns = last_ns = 0
        expiry = "unknown"
        rows = 0
        imbalance_sum = 0.0
        price_timeline = []
        for key in sorted(objects):
            body = self._client().get_object(
                Bucket=self.settings.bucket, Key=key
            )["Body"].read()
            table = parquet.read_table(
                io.BytesIO(body),
                columns=["received_ns", "ltp", "mid_price", "bid_qty", "ask_qty"],
            )
            data = table.to_pydict()
            for received_ns, ltp, mid, bids, asks in zip(
                data["received_ns"],
                data["ltp"],
                data["mid_price"],
                data["bid_qty"],
                data["ask_qty"],
            ):
                price = float(ltp or mid or 0.0)
                if price <= 0:
                    continue
                if first is None or int(received_ns) < first_ns:
                    first, first_ns = price, int(received_ns)
                if last is None or int(received_ns) > last_ns:
                    last, last_ns = price, int(received_ns)
                low = price if low is None else min(low, price)
                high = price if high is None else max(high, price)
                bid_total = sum(int(value or 0) for value in bids[:20])
                ask_total = sum(int(value or 0) for value in asks[:20])
                total = bid_total + ask_total
                imbalance_sum += (bid_total - ask_total) / total if total else 0.0
                price_timeline.append((int(received_ns), price))
                rows += 1
            if "expiry=" in key:
                expiry = key.split("expiry=", 1)[1].split("/", 1)[0]

        if not rows or first is None or last is None:
            raise RuntimeError("no valid synchronized NIFTY rows found in S3")
        return_bps = (last - first) / first * 10_000.0
        trend = (
            "UP"
            if return_bps > self.settings.sideways_bps
            else "DOWN"
            if return_bps < -self.settings.sideways_bps
            else "SIDEWAYS"
        )
        price_timeline.sort(key=lambda row: row[0])
        trade_ledger = self._load_trade_ledger(trade_date)
        option_expiries = [
            str(row.get("option_expiry", ""))[:10]
            for row in trade_ledger.get("trades", [])
            if row.get("option_expiry")
        ]
        if option_expiries:
            expiry = max(set(option_expiries), key=option_expiries.count)
        prediction_evaluation = self._evaluate_trades(
            trade_ledger.get("trades", []), price_timeline
        )
        cycle = {}
        try:
            cycle = expiry_cycle_context(trade_date, expiry).as_dict()
        except ValueError:
            cycle = {"expiry_date": expiry, "cycle_label": "UNKNOWN"}
        next_cycle_day = (
            1 if int(cycle.get("cycle_day", 0) or 0) == 5
            else int(cycle.get("cycle_day", 0) or 0) + 1
        )
        next_cycle_label = f"DAY_{next_cycle_day}" if next_cycle_day else "UNKNOWN"
        cycle_history = self._cycle_history(exclude_trade_date=trade_date)
        report = {
            "schema_version": 1,
            "trade_date": trade_date,
            "generated_at": datetime.now(self._timezone).isoformat(),
            "source": "S3_SYNCHRONIZED_FULLQUOTE_200_DEPTH",
            "instrument": "NIFTY_FUT",
            "files": len(objects),
            "rows": rows,
            "open": first,
            "high": high,
            "low": low,
            "close": last,
            "return_bps": round(return_bps, 4),
            "mean_top20_imbalance": round(imbalance_sum / rows, 6),
            "realized_trend": trend,
            "expiry_cycle": cycle,
            "prediction_evaluation": prediction_evaluation,
            "paper_trade_summary": trade_ledger.get("summary", {}),
            "cycle_history": cycle_history,
            "next_session_outlook": {
                "cycle_label": next_cycle_label,
                **cycle_history.get(next_cycle_label, self._empty_cycle_stats()),
            },
            "note": "Descriptive post-market evidence; not a next-session guarantee.",
        }
        key = (
            f"{self.settings.report_prefix}/schema=v1/trade_date={trade_date}/"
            "nifty-session-report.json"
        )
        self._client().put_object(
            Bucket=self.settings.bucket,
            Key=key,
            Body=json.dumps(report, separators=(",", ":"), sort_keys=True).encode(),
            ContentType="application/json",
        )
        logger.info(
            "DEEPLOB_POST_MARKET_REPORT_OK | trade_date=%s | files=%s | rows=%s | "
            "trend=%s | return_bps=%+.3f | key=%s",
            trade_date,
            len(objects),
            rows,
            trend,
            return_bps,
            key,
        )

    def _load_trade_ledger(self, trade_date: str) -> dict:
        key = (
            f"{self.settings.trade_prefix}/schema=v2/trade_date={trade_date}/"
            "index=NIFTY/daily-trades.json"
        )
        try:
            body = self._client().get_object(
                Bucket=self.settings.bucket, Key=key
            )["Body"].read()
            ledger = json.loads(body.decode("utf-8"))
            return ledger if isinstance(ledger, dict) else {}
        except Exception as exc:
            logger.warning(
                "DEEPLOB_POST_MARKET_TRADE_LEDGER_UNAVAILABLE | trade_date=%s | error=%s",
                trade_date,
                exc,
            )
            return {}

    @staticmethod
    def _evaluate_trades(
        trades: list[dict], timeline: list[tuple[int, float]]
    ) -> dict:
        if not trades or not timeline:
            return {"evaluated": 0, "missing": len(trades), "by_profile_horizon": {}}
        timestamps = [row[0] for row in timeline]
        grouped: dict[str, dict] = {}
        missing = 0
        for trade in trades:
            entry_ts = float(trade.get("entry_ts", 0.0) or 0.0)
            horizon = int(trade.get("model_horizon_sec", 0) or 0)
            if entry_ts <= 0 or horizon <= 0:
                missing += 1
                continue
            entry_ns = int(entry_ts * 1_000_000_000)
            future_ns = entry_ns + horizon * 1_000_000_000
            start_index = bisect.bisect_left(timestamps, entry_ns)
            future_index = bisect.bisect_left(timestamps, future_ns)
            if start_index >= len(timeline) or future_index >= len(timeline):
                missing += 1
                continue
            start_price = timeline[start_index][1]
            future_price = timeline[future_index][1]
            if start_price <= 0:
                missing += 1
                continue
            move_bps = (future_price - start_price) / start_price * 10_000.0
            tag = str(trade.get("tag", ""))
            predicted_up = tag.endswith("_CE")
            correct = move_bps > 0 if predicted_up else move_bps < 0
            probability = float(
                trade.get("probability_up" if predicted_up else "probability_down", 0.5)
                or 0.5
            )
            key = f"{trade.get('profile', 'dynamic')}:{horizon}s"
            stats = grouped.setdefault(
                key,
                {"evaluated": 0, "correct": 0, "move_bps_sum": 0.0, "brier_sum": 0.0},
            )
            stats["evaluated"] += 1
            stats["correct"] += int(correct)
            stats["move_bps_sum"] += move_bps
            stats["brier_sum"] += (probability - float(correct)) ** 2
        for stats in grouped.values():
            count = stats["evaluated"]
            stats["accuracy_pct"] = round(stats["correct"] / count * 100.0, 2)
            stats["mean_future_move_bps"] = round(stats.pop("move_bps_sum") / count, 4)
            stats["brier_score"] = round(stats.pop("brier_sum") / count, 6)
        evaluated = sum(row["evaluated"] for row in grouped.values())
        correct = sum(row["correct"] for row in grouped.values())
        weighted_brier = sum(
            row["brier_score"] * row["evaluated"] for row in grouped.values()
        )
        return {
            "evaluated": evaluated,
            "missing": missing,
            "accuracy_pct": round(correct / evaluated * 100.0, 2) if evaluated else None,
            "brier_score": round(weighted_brier / evaluated, 6) if evaluated else None,
            "by_profile_horizon": grouped,
            "note": "Directional outcome at the signal horizon; Brier score lower is better.",
        }

    @staticmethod
    def _empty_cycle_stats() -> dict:
        return {
            "sample_sessions": 0,
            "up": 0,
            "down": 0,
            "sideways": 0,
            "mean_return_bps": 0.0,
            "empirical_bias": "INSUFFICIENT_DATA",
        }

    def _cycle_history(self, *, exclude_trade_date: str) -> dict[str, dict]:
        prefix = f"{self.settings.report_prefix}/schema=v1/trade_date="
        report_keys = []
        token = None
        while True:
            kwargs = {"Bucket": self.settings.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            response = self._client().list_objects_v2(**kwargs)
            report_keys.extend(
                item["Key"] for item in response.get("Contents", [])
            )
            if not response.get("IsTruncated"):
                break
            token = response.get("NextContinuationToken")

        grouped: dict[str, list[dict]] = {}
        for key in report_keys:
            if (
                not key.endswith("nifty-session-report.json")
                or f"trade_date={exclude_trade_date}/" in key
            ):
                continue
            try:
                body = self._client().get_object(
                    Bucket=self.settings.bucket, Key=key
                )["Body"].read()
                report = json.loads(body.decode("utf-8"))
                label = str(
                    report.get("expiry_cycle", {}).get("cycle_label", "UNKNOWN")
                )
                grouped.setdefault(label, []).append(report)
            except Exception as exc:
                logger.warning(
                    "DEEPLOB_POST_MARKET_HISTORY_SKIPPED | key=%s | error=%s",
                    key,
                    exc,
                )
        history = {}
        for label, reports in grouped.items():
            counts = {
                "UP": sum(row.get("realized_trend") == "UP" for row in reports),
                "DOWN": sum(row.get("realized_trend") == "DOWN" for row in reports),
                "SIDEWAYS": sum(
                    row.get("realized_trend") == "SIDEWAYS" for row in reports
                ),
            }
            leader = max(counts, key=counts.get)
            sample_sessions = len(reports)
            leader_share = counts[leader] / sample_sessions if sample_sessions else 0.0
            history[label] = {
                "sample_sessions": sample_sessions,
                "up": counts["UP"],
                "down": counts["DOWN"],
                "sideways": counts["SIDEWAYS"],
                "mean_return_bps": round(
                    sum(float(row.get("return_bps", 0.0)) for row in reports)
                    / sample_sessions,
                    4,
                ),
                "empirical_bias": (
                    leader if sample_sessions >= 5 and leader_share >= 0.55 else "MIXED"
                ),
            }
        return history

    def health(self) -> dict:
        return {
            "enabled": self.settings.enabled,
            "started_dates": sorted(self._started_dates),
            "completed_dates": sorted(self._completed_dates),
            "failures": self._failures,
        }


