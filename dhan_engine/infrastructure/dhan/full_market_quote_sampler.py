import asyncio
import json
import logging
import threading
import time
from datetime import datetime
from typing import Dict

from dhan_engine.config.settings import RuntimeSettings


logger = logging.getLogger(__name__)

try:
    from dhanhq.marketfeed import DhanFeed
except Exception:  # pragma: no cover - optional dependency path
    DhanFeed = None


def _first_non_none(payload: dict, *keys):
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


class FullMarketQuoteSampler:
    """Optional debug sampler for broker market-feed payloads."""

    def __init__(self, *, client_id: str, token: str, secid_tag_map: Dict[int, str], settings: RuntimeSettings):
        if DhanFeed is None:
            raise RuntimeError("dhanhq is not installed. Run: pip install dhanhq")

        self.client_id = str(client_id)
        self.token = str(token)
        self.secid_tag_map = {int(secid): str(tag) for secid, tag in secid_tag_map.items()}
        self.settings = settings
        self._latest: Dict[int, dict] = {}
        self._count = {int(secid): 0 for secid in self.secid_tag_map}
        self._lock = threading.Lock()

    def _on_ticks(self, message):
        ticks = message if isinstance(message, list) else [message]
        with self._lock:
            for tick in ticks:
                sid = _first_non_none(tick, "security_id", "securityId", "sec_id", "SecurityId")
                if sid is None:
                    continue
                try:
                    sid = int(sid)
                except Exception:
                    continue
                if sid in self.secid_tag_map:
                    self._latest[sid] = tick

    def _extract(self, tick: dict, secid: int, tag: str) -> dict:
        broker_ts = _first_non_none(
            tick,
            "exchange_time",
            "exchangeTime",
            "last_trade_time",
            "lastTradeTime",
            "ltt",
            "timestamp",
            "time",
        )
        return {
            "secid": secid,
            "tag": tag,
            "ltp": _first_non_none(tick, "LTP", "ltp", "last_traded_price", "lastTradedPrice"),
            "ltq": _first_non_none(tick, "LTQ", "ltq", "last_traded_quantity", "lastTradedQuantity"),
            "total_volume": _first_non_none(tick, "volume", "Volume", "total_volume", "totalTradedVolume"),
            "oi": _first_non_none(tick, "oi", "OI", "open_interest", "openInterest"),
            "bid_price": _first_non_none(tick, "best_bid_price", "bestBidPrice", "bid_price", "bidPrice"),
            "ask_price": _first_non_none(tick, "best_ask_price", "bestAskPrice", "ask_price", "askPrice"),
            "iv": _first_non_none(tick, "iv", "IV", "implied_volatility", "impliedVolatility"),
            "delta": _first_non_none(tick, "delta", "Delta"),
            "timestamp": broker_ts if broker_ts is not None else datetime.now(self.settings.timezone).isoformat(),
            "raw": tick,
        }

    def run(self):
        secids = list(self.secid_tag_map.keys())
        if not secids:
            return

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        instruments = [
            (self.settings.full_quote_segment, secid, self.settings.full_quote_request_code)
            for secid in secids
        ]
        feed = DhanFeed(
            client_id=self.client_id,
            access_token=self.token,
            instruments=instruments,
            version="v2",
        )
        feed.on_ticks = self._on_ticks

        ws_thread = threading.Thread(target=feed.run_forever, name="FullQuoteWS", daemon=True)
        ws_thread.start()

        try:
            while True:
                time.sleep(1.0)
                with self._lock:
                    snapshot = dict(self._latest)

                for secid in secids:
                    if self._count[secid] >= self.settings.full_quote_log_sec:
                        continue
                    tick = snapshot.get(secid)
                    if not tick:
                        continue
                    logger.info(json.dumps(self._extract(tick, secid, self.secid_tag_map[secid]), ensure_ascii=False))
                    self._count[secid] += 1

                if all(self._count[secid] >= self.settings.full_quote_log_sec for secid in secids):
                    logger.info("Full marketfeed sampling completed")
                    break
        finally:
            try:
                if hasattr(feed, "disconnect"):
                    loop.run_until_complete(feed.disconnect())
            except Exception:
                logger.exception("Full marketfeed disconnect warning")
            finally:
                loop.close()
                asyncio.set_event_loop(None)
