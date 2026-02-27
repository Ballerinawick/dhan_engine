import asyncio
import json
import os
import signal
import subprocess
import threading
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Optional

from dotenv import load_dotenv

# Reuse the exact same depth secids as the existing depth stream pipeline.
from ws_fut_live import SEC_IDS

try:
    from dhanhq.marketfeed import DhanFeed
except ImportError as exc:
    raise RuntimeError(
        "dhanhq package is required for full marketfeed stream. "
        "Install with: pip install dhanhq"
    ) from exc


NSE_FNO = 2
FULL = 21
PRINT_SECONDS_PER_INSTRUMENT = 10


# Python 3.13 compatibility for SDKs that still call get_event_loop().
_LOOP_HOLDER: Dict[str, asyncio.AbstractEventLoop] = {}

def _compat_get_event_loop():
    loop = _LOOP_HOLDER.get("loop")
    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop()
        _LOOP_HOLDER["loop"] = loop
    return loop

if not hasattr(asyncio, "get_event_loop"):
    asyncio.get_event_loop = _compat_get_event_loop  # type: ignore[attr-defined]


def _first_non_none(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _extract_tick_fields(tick: Dict[str, Any], secid: int, tag: str) -> Dict[str, Any]:
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

    out = {
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
        "timestamp": broker_ts if broker_ts is not None else datetime.now().isoformat(),
        "raw": tick,
    }
    return out


class FullFeedLogger:
    def __init__(self, client_id: str, access_token: str):
        self.client_id = str(client_id)
        self.access_token = str(access_token)
        self._latest: Dict[int, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._feed: Optional[DhanFeed] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

        self._counts = defaultdict(int)
        self._secid_to_tag = {int(v): k for k, v in SEC_IDS.items()}

    def _on_ticks(self, msg: Any):
        ticks = msg if isinstance(msg, list) else [msg]
        with self._lock:
            for tick in ticks:
                secid = _first_non_none(tick, "security_id", "securityId", "sec_id", "SecurityId")
                if secid is None:
                    continue
                try:
                    sid = int(secid)
                except Exception:
                    continue
                self._latest[sid] = tick

    def _run_feed(self):
        instruments = [(NSE_FNO, int(secid), FULL) for secid in SEC_IDS.values()]
        self._feed = DhanFeed(
            client_id=self.client_id,
            access_token=self.access_token,
            instruments=instruments,
            version="v2",
        )
        self._feed.on_ticks = self._on_ticks
        self._feed.run_forever()

    def start(self):
        self._thread = threading.Thread(target=self._run_feed, name="FullFeedWS", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        try:
            if self._feed and hasattr(self._feed, "disconnect"):
                loop = asyncio.new_event_loop()
                loop.run_until_complete(self._feed.disconnect())
                loop.close()
        except Exception:
            pass

    def print_for_10s_each(self):
        target = {int(v): PRINT_SECONDS_PER_INSTRUMENT for v in SEC_IDS.values()}

        while True:
            time.sleep(1.0)

            with self._lock:
                snapshot = dict(self._latest)

            for sid, limit in target.items():
                if self._counts[sid] >= limit:
                    continue

                tick = snapshot.get(sid)
                if not tick:
                    continue

                payload = _extract_tick_fields(tick, sid, self._secid_to_tag.get(sid, str(sid)))
                print(json.dumps(payload, ensure_ascii=False))
                self._counts[sid] += 1

            if all(self._counts[sid] >= limit for sid, limit in target.items()):
                print("✅ Full-feed logging completed (10 ticks per instrument).")
                return


def _start_depth_pipeline_subprocess() -> subprocess.Popen:
    cmd = ["python", "ws_fut_live.py"]
    return subprocess.Popen(cmd)


def _stop_depth_pipeline_subprocess(proc: subprocess.Popen):
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def main():
    load_dotenv()
    token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
    client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
    if not token or not client_id:
        raise RuntimeError("Missing DHAN_ACCESS_TOKEN / DHAN_CLIENT_ID in environment")

    depth_proc = _start_depth_pipeline_subprocess()

    logger = FullFeedLogger(client_id=client_id, access_token=token)
    try:
        logger.start()
        logger.print_for_10s_each()
    finally:
        logger.stop()
        _stop_depth_pipeline_subprocess(depth_proc)


if __name__ == "__main__":
    main()
