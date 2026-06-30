from __future__ import annotations

import logging
import os
import socket
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Mapping
from zoneinfo import ZoneInfo


logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


def _truthy(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off", "disabled"}


class TradeSummarySink:
    """Best-effort MongoDB writer for completed paper trade summaries."""

    def __init__(self) -> None:
        self.uri = (
            os.getenv("TRADE_SUMMARY_MONGO_URI", "").strip()
            or os.getenv("MONGODB_URI", "").strip()
        )
        self.db_name = os.getenv("TRADE_SUMMARY_MONGO_DB", "dhan_engine").strip() or "dhan_engine"
        self.collection_name = (
            os.getenv("TRADE_SUMMARY_MONGO_COLLECTION", "trade_summaries").strip()
            or "trade_summaries"
        )
        self.enabled = bool(self.uri) and _truthy(os.getenv("TRADE_SUMMARY_MONGO_ENABLED"), True)
        self.timeout_ms = int(os.getenv("TRADE_SUMMARY_MONGO_TIMEOUT_MS", "2500") or 2500)
        self._client: Any = None
        self._collection: Any = None

    def _get_collection(self) -> Any | None:
        if not self.enabled:
            return None
        if self._collection is not None:
            return self._collection

        try:
            from pymongo import MongoClient
        except Exception as exc:
            self.enabled = False
            logger.warning(
                "TRADE_SUMMARY_MONGO_DISABLED | reason=pymongo_import_failed | error=%s",
                exc,
            )
            return None

        try:
            self._client = MongoClient(
                self.uri,
                serverSelectionTimeoutMS=self.timeout_ms,
                connectTimeoutMS=self.timeout_ms,
                appname="dhan-engine-trade-summary",
            )
            self._collection = self._client[self.db_name][self.collection_name]
            return self._collection
        except Exception as exc:
            logger.warning(
                "TRADE_SUMMARY_MONGO_CONNECT_FAILED | db=%s | collection=%s | error=%s",
                self.db_name,
                self.collection_name,
                exc,
            )
            return None

    def record(self, section: str, payload: Mapping[str, Any]) -> bool:
        collection = self._get_collection()
        if collection is None:
            return False

        now_utc = datetime.now(timezone.utc)
        now_ist = now_utc.astimezone(IST)
        document = dict(payload)
        document.update(
            {
                "section": str(section),
                "paper": bool(document.get("paper", True)),
                "created_at_utc": now_utc,
                "created_at_ist": now_ist.isoformat(),
                "trade_date_ist": now_ist.date().isoformat(),
                "service": os.getenv("DHAN_SERVICE", "").strip(),
                "deployment_id": (
                    os.getenv("RAILWAY_DEPLOYMENT_ID", "").strip()
                    or os.getenv("RAILWAY_REPLICA_ID", "").strip()
                    or os.getenv("HOSTNAME", socket.gethostname()).strip()
                ),
            }
        )

        try:
            collection.insert_one(document)
            logger.info(
                "TRADE_SUMMARY_MONGO_STORED | section=%s | symbol=%s | net_pnl=%s",
                section,
                document.get("symbol") or document.get("tag") or document.get("trading_symbol"),
                document.get("net_pnl"),
            )
            return True
        except Exception as exc:
            logger.warning(
                "TRADE_SUMMARY_MONGO_STORE_FAILED | section=%s | error=%s",
                section,
                exc,
            )
            return False


@lru_cache(maxsize=1)
def get_trade_summary_sink() -> TradeSummarySink:
    return TradeSummarySink()
