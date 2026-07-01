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

    def __init__(
        self,
        *,
        collection_name: str | None = None,
        portfolio_collection_name: str | None = None,
    ) -> None:
        self.uri = (
            os.getenv("TRADE_SUMMARY_MONGO_URI", "").strip()
            or os.getenv("MONGODB_URI", "").strip()
        )
        self.db_name = os.getenv("TRADE_SUMMARY_MONGO_DB", "dhan_engine").strip() or "dhan_engine"
        self.collection_name = collection_name or (
            os.getenv("TRADE_SUMMARY_MONGO_COLLECTION", "trade_summaries").strip()
            or "trade_summaries"
        )
        self.portfolio_collection_name = portfolio_collection_name or (
            os.getenv("PORTFOLIO_MONGO_COLLECTION", "portfolio_daily").strip()
            or "portfolio_daily"
        )
        self.enabled = bool(self.uri) and _truthy(os.getenv("TRADE_SUMMARY_MONGO_ENABLED"), True)
        self.timeout_ms = int(os.getenv("TRADE_SUMMARY_MONGO_TIMEOUT_MS", "2500") or 2500)
        self._client: Any = None
        self._collection: Any = None
        self._portfolio_collection: Any = None

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

    def _get_portfolio_collection(self) -> Any | None:
        if not self.enabled:
            return None
        if self._portfolio_collection is not None:
            return self._portfolio_collection
        trade_collection = self._get_collection()
        if trade_collection is None or self._client is None:
            return None
        self._portfolio_collection = self._client[self.db_name][self.portfolio_collection_name]
        return self._portfolio_collection

    def _common_fields(self, document: dict[str, Any], now_utc: datetime | None = None) -> dict[str, Any]:
        now_utc = now_utc or datetime.now(timezone.utc)
        now_ist = now_utc.astimezone(IST)
        document.update(
            {
                "paper": bool(document.get("paper", True)),
                "updated_at_utc": now_utc,
                "updated_at_ist": now_ist.isoformat(),
                "trade_date_ist": now_ist.date().isoformat(),
                "service": os.getenv("DHAN_SERVICE", "").strip(),
                "deployment_id": (
                    os.getenv("RAILWAY_DEPLOYMENT_ID", "").strip()
                    or os.getenv("RAILWAY_REPLICA_ID", "").strip()
                    or os.getenv("HOSTNAME", socket.gethostname()).strip()
                ),
            }
        )
        return document

    def record(self, section: str, payload: Mapping[str, Any]) -> bool:
        collection = self._get_collection()
        if collection is None:
            return False

        now_utc = datetime.now(timezone.utc)
        document = dict(payload)
        document["section"] = str(section)
        document = self._common_fields(document, now_utc)
        document["created_at_utc"] = now_utc
        document["created_at_ist"] = document["updated_at_ist"]

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

    def record_portfolio(self, section: str, payload: Mapping[str, Any]) -> bool:
        collection = self._get_portfolio_collection()
        if collection is None:
            return False

        now_utc = datetime.now(timezone.utc)
        document = dict(payload)
        document["section"] = str(section)
        document = self._common_fields(document, now_utc)
        query = {
            "section": document["section"],
            "trade_date_ist": document["trade_date_ist"],
            "paper": document["paper"],
            "service": document.get("service", ""),
            "deployment_id": document.get("deployment_id", ""),
        }

        try:
            collection.update_one(
                query,
                {
                    "$set": document,
                    "$setOnInsert": {"created_at_utc": now_utc, "created_at_ist": document["updated_at_ist"]},
                },
                upsert=True,
            )
            logger.info(
                "PORTFOLIO_MONGO_STORED | section=%s | daily_net_pnl=%s | net_pnl=%s | open_positions=%s",
                section,
                document.get("daily_net_pnl"),
                document.get("net_pnl"),
                document.get("open_positions"),
            )
            return True
        except Exception as exc:
            logger.warning(
                "PORTFOLIO_MONGO_STORE_FAILED | section=%s | error=%s",
                section,
                exc,
            )
            return False


@lru_cache(maxsize=1)
def get_trade_summary_sink() -> TradeSummarySink:
    return TradeSummarySink()
